/*
Copyright 2026 Tander.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"crypto/tls"
	"flag"
	"os"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	metricsv "k8s.io/metrics/pkg/client/clientset/versioned"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	sparkinteractiveiov1alpha1 "github.com/tander/spark-session-operator/api/v1alpha1"
	"github.com/tander/spark-session-operator/internal/auth"
	"github.com/tander/spark-session-operator/internal/controller"
	"github.com/tander/spark-session-operator/internal/gateway"
	"github.com/tander/spark-session-operator/internal/proxy"
	// +kubebuilder:scaffold:imports
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(sparkinteractiveiov1alpha1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

// nolint:gocyclo
func main() {
	var metricsAddr string
	var metricsCertPath, metricsCertName, metricsCertKey string
	var webhookCertPath, webhookCertName, webhookCertKey string
	var enableLeaderElection bool
	var probeAddr string
	var secureMetrics bool
	var enableHTTP2 bool
	var tlsOpts []func(*tls.Config)

	// Kubebuilder standard flags
	flag.StringVar(&metricsAddr, "metrics-bind-address", "0", "The address the metrics endpoint binds to. "+
		"Use :8443 for HTTPS or :8080 for HTTP, or leave as 0 to disable the metrics service.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.BoolVar(&secureMetrics, "metrics-secure", true,
		"If set, the metrics endpoint is served securely via HTTPS. Use --metrics-secure=false to use HTTP instead.")
	flag.StringVar(&webhookCertPath, "webhook-cert-path", "", "The directory that contains the webhook certificate.")
	flag.StringVar(&webhookCertName, "webhook-cert-name", "tls.crt", "The name of the webhook certificate file.")
	flag.StringVar(&webhookCertKey, "webhook-cert-key", "tls.key", "The name of the webhook key file.")
	flag.StringVar(&metricsCertPath, "metrics-cert-path", "",
		"The directory that contains the metrics server certificate.")
	flag.StringVar(&metricsCertName, "metrics-cert-name", "tls.crt", "The name of the metrics server certificate file.")
	flag.StringVar(&metricsCertKey, "metrics-cert-key", "tls.key", "The name of the metrics server key file.")
	flag.BoolVar(&enableHTTP2, "enable-http2", false,
		"If set, HTTP/2 will be enabled for the metrics and webhook servers")

	// Custom operator flags
	var (
		namespace          string
		proxyNamespace     string
		gatewayAddr        string
		thriftProxyAddr    string
		connectProxyAddr   string
		oidcIssuerURL      string
		oidcAudience       string
		oidcUserClaim      string
		oidcGroupsClaim    string
		oidcSkipValidation bool
		oidcClientID       string
		oidcClientSecret   string
	)
	flag.StringVar(&namespace, "namespace", "spark-dev", "Namespace where pools and sessions live.")
	flag.StringVar(&proxyNamespace, "proxy-namespace", "spark-session-operator", "Namespace where the proxy Service and Ingresses live.")
	flag.StringVar(&gatewayAddr, "gateway-addr", ":8080", "Address for the REST API gateway.")
	flag.StringVar(&thriftProxyAddr, "thrift-proxy-addr", ":10009", "Address for the Thrift HTTP proxy.")
	flag.StringVar(&connectProxyAddr, "connect-proxy-addr", ":15002", "Address for the Spark Connect gRPC proxy.")
	flag.StringVar(&oidcIssuerURL, "oidc-issuer-url", "", "OIDC issuer URL for token validation.")
	flag.StringVar(&oidcAudience, "oidc-audience", "", "Expected OIDC audience.")
	flag.StringVar(&oidcUserClaim, "oidc-user-claim", "sub", "JWT claim for username.")
	flag.StringVar(&oidcGroupsClaim, "oidc-groups-claim", "groups", "JWT claim for groups.")
	flag.BoolVar(&oidcSkipValidation, "oidc-skip-validation", false, "Skip OIDC token validation (dev only).")
	flag.StringVar(&oidcClientID, "oidc-client-id", "", "OAuth client ID for Keycloak ROPC grant (used by proxy).")
	flag.StringVar(&oidcClientSecret, "oidc-client-secret", "", "OAuth client secret for Keycloak ROPC grant (optional).")

	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	// if the enable-http2 flag is false (the default), http/2 should be disabled
	// due to its vulnerabilities. More specifically, disabling http/2 will
	// prevent from being vulnerable to the HTTP/2 Stream Cancellation and
	// Rapid Reset CVEs. For more information see:
	// - https://github.com/advisories/GHSA-qppj-fm5r-hxr3
	// - https://github.com/advisories/GHSA-4374-p667-p6c8
	disableHTTP2 := func(c *tls.Config) {
		setupLog.Info("Disabling HTTP/2")
		c.NextProtos = []string{"http/1.1"}
	}

	if !enableHTTP2 {
		tlsOpts = append(tlsOpts, disableHTTP2)
	}

	// Initial webhook TLS options
	webhookTLSOpts := tlsOpts
	webhookServerOptions := webhook.Options{
		TLSOpts: webhookTLSOpts,
	}

	if len(webhookCertPath) > 0 {
		setupLog.Info("Initializing webhook certificate watcher using provided certificates",
			"webhook-cert-path", webhookCertPath, "webhook-cert-name", webhookCertName, "webhook-cert-key", webhookCertKey)

		webhookServerOptions.CertDir = webhookCertPath
		webhookServerOptions.CertName = webhookCertName
		webhookServerOptions.KeyName = webhookCertKey
	}

	webhookServer := webhook.NewServer(webhookServerOptions)

	metricsServerOptions := metricsserver.Options{
		BindAddress:   metricsAddr,
		SecureServing: secureMetrics,
		TLSOpts:       tlsOpts,
	}

	if secureMetrics {
		metricsServerOptions.FilterProvider = filters.WithAuthenticationAndAuthorization
	}

	if len(metricsCertPath) > 0 {
		setupLog.Info("Initializing metrics certificate watcher using provided certificates",
			"metrics-cert-path", metricsCertPath, "metrics-cert-name", metricsCertName, "metrics-cert-key", metricsCertKey)

		metricsServerOptions.CertDir = metricsCertPath
		metricsServerOptions.CertName = metricsCertName
		metricsServerOptions.KeyName = metricsCertKey
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsServerOptions,
		WebhookServer:          webhookServer,
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "spark-session-operator.sparkinteractive.io",
	})
	if err != nil {
		setupLog.Error(err, "Failed to start manager")
		os.Exit(1)
	}

	// Initialize Kubernetes Metrics API client
	metricsClientset, err := metricsv.NewForConfig(ctrl.GetConfigOrDie())
	if err != nil {
		setupLog.Error(err, "Failed to create metrics clientset")
		os.Exit(1)
	}

	// Setup controllers
	if err := (&controller.SparkSessionPoolReconciler{
		Client:         mgr.GetClient(),
		Scheme:         mgr.GetScheme(),
		Log:            ctrl.Log.WithName("controllers").WithName("SparkSessionPool"),
		MetricsClient:  metricsClientset,
		ProxyNamespace: proxyNamespace,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "SparkSessionPool")
		os.Exit(1)
	}
	if err := (&controller.SparkInteractiveSessionReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		Log:    ctrl.Log.WithName("controllers").WithName("SparkInteractiveSession"),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "SparkInteractiveSession")
		os.Exit(1)
	}
	// +kubebuilder:scaffold:builder

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "Failed to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "Failed to set up ready check")
		os.Exit(1)
	}

	// Create shared authenticator
	authenticator := auth.NewAuthenticator(auth.OIDCConfig{
		IssuerURL:      oidcIssuerURL,
		Audience:       oidcAudience,
		ClientID:       oidcClientID,
		ClientSecret:   oidcClientSecret,
		UserClaim:      oidcUserClaim,
		GroupsClaim:    oidcGroupsClaim,
		SkipValidation: oidcSkipValidation,
	})

	// Start REST API gateway
	gw := gateway.NewSessionGateway(
		mgr.GetClient(),
		ctrl.Log,
		namespace,
		authenticator,
	)
	go func() {
		if err := gw.Start(gatewayAddr); err != nil {
			setupLog.Error(err, "gateway server failed")
			os.Exit(1)
		}
	}()

	// Start proxies
	sessionProxy := proxy.NewSessionProxy(mgr.GetClient(), ctrl.Log, namespace, authenticator)
	if thriftProxyAddr != "" {
		if err := sessionProxy.StartThriftHTTPProxy(thriftProxyAddr); err != nil {
			setupLog.Error(err, "failed to start thrift proxy")
			os.Exit(1)
		}
	}
	if connectProxyAddr != "" {
		if err := sessionProxy.StartConnectProxy(connectProxyAddr); err != nil {
			setupLog.Error(err, "failed to start connect proxy")
			os.Exit(1)
		}
	}

	setupLog.Info("Starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "Failed to run manager")
		os.Exit(1)
	}
}
