/*
Copyright 2026.

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

package proxy

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	sparkv1alpha1 "github.com/padod/spark-session-operator/api/v1alpha1"
	"github.com/padod/spark-session-operator/internal/auth"
)

const (
	sessionPollInterval = 500 * time.Millisecond
	sessionPollTimeout  = 60 * time.Second
	keepaliveInterval   = 2 * time.Minute

	// keepaliveUpdateTimeout caps the per-tick LastActivityAt update so a
	// slow apiserver can't pile up unbounded in-flight writes.
	keepaliveUpdateTimeout = 5 * time.Second

	// proxyShutdownTimeout bounds the graceful drain of the Thrift HTTP
	// proxy. The gRPC proxy uses GracefulStop, which has no internal timeout —
	// we wrap it with the same budget so SIGTERM doesn't hang the pod past
	// terminationGracePeriodSeconds.
	proxyShutdownTimeout = 30 * time.Second

	// backendFailureProbeTimeout bounds the apiserver Get used to translate
	// a generic transport error into a specific driver-pod cause (Evicted,
	// OOMKilled, NodeShutdown). Kept tight so a slow apiserver can't turn a
	// bad situation worse — we degrade to the generic message on miss.
	backendFailureProbeTimeout = 1 * time.Second

	// maxGRPCMessageBytes caps the size of the grpc-message string we forward
	// to the client. Spark Connect embeds the full analyzed plan into its
	// AnalysisException messages, which can run to many KB and blow past the
	// nginx ingress proxy_buffer_size — nginx then returns HTTP 502 instead
	// of forwarding the gRPC error, and PySpark surfaces UNAVAILABLE rather
	// than AnalysisException. The rich error info (ErrorInfo, stack traces,
	// error class) lives in status details and is preserved untouched.
	maxGRPCMessageBytes = 1024
)

// truncateStatusError rebuilds a gRPC status error with its Message capped at
// maxGRPCMessageBytes, preserving Code and Details. Non-status errors and
// errors with short messages pass through unchanged.
func truncateStatusError(err error) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return err
	}
	p := st.Proto()
	if p == nil || len(p.Message) <= maxGRPCMessageBytes {
		return err
	}
	p.Message = p.Message[:maxGRPCMessageBytes] + "... [truncated by proxy]"
	return status.FromProto(p).Err()
}

// SessionProxy handles incoming Thrift and gRPC connections, auto-creating sessions
// and proxying traffic to the assigned backend.
type SessionProxy struct {
	client    client.Client
	log       logr.Logger
	namespace string
	auth      *auth.Authenticator

	// sessions maps "user:pool" → session name and absorbs races when
	// multiple RPCs arrive before the informer cache updates. Entries
	// expire after sessionCacheTTL so the map stays memory-bounded and
	// stale references self-heal once the TTL lapses.
	sessions *ttlMap

	// endpoints caches session name → resolved backend endpoint. TTL-bounded
	// for the same reasons; on a backend connection failure callers should
	// delete the entry to force a fresh resolution on the next request.
	endpoints *ttlMap
}

// NewSessionProxy creates a new proxy.
func NewSessionProxy(c client.Client, log logr.Logger, namespace string, authenticator *auth.Authenticator) *SessionProxy {
	return &SessionProxy{
		client:    c,
		log:       log.WithName("proxy"),
		namespace: namespace,
		auth:      authenticator,
		sessions:  newTTLMap(sessionCacheTTL),
		endpoints: newTTLMap(sessionCacheTTL),
	}
}

// findPool finds exactly one pool of the given type in the namespace.
// Returns the pool name or an error if 0 or 2+ pools match.
func (p *SessionProxy) findPool(ctx context.Context, poolType string) (string, error) {
	poolList := &sparkv1alpha1.SparkSessionPoolList{}
	if err := p.client.List(ctx, poolList, client.InNamespace(p.namespace)); err != nil {
		return "", fmt.Errorf("list pools: %w", err)
	}

	var matches []string
	for _, pool := range poolList.Items {
		if pool.Spec.Type == poolType {
			matches = append(matches, pool.Name)
		}
	}

	switch len(matches) {
	case 0:
		return "", fmt.Errorf("no pool of type %q found in namespace %s", poolType, p.namespace)
	case 1:
		return matches[0], nil
	default:
		return "", fmt.Errorf("found %d pools of type %q in namespace %s (expected exactly 1): %v",
			len(matches), poolType, p.namespace, matches)
	}
}

// findPoolByHost finds a pool by its spec.host field in the namespace.
// Returns the pool name and pool type, or an error if no pool matches.
func (p *SessionProxy) findPoolByHost(ctx context.Context, host string) (string, string, error) {
	poolList := &sparkv1alpha1.SparkSessionPoolList{}
	if err := p.client.List(ctx, poolList, client.InNamespace(p.namespace)); err != nil {
		return "", "", fmt.Errorf("list pools: %w", err)
	}
	for _, pool := range poolList.Items {
		if pool.Spec.Host == host {
			return pool.Name, pool.Spec.Type, nil
		}
	}
	return "", "", fmt.Errorf("no pool with host %q in namespace %s", host, p.namespace)
}

// sessionKey returns the map key for session tracking.
func sessionKey(username, poolName string) string {
	return username + ":" + poolName
}

// findOrCreateSession returns an existing active/idle session for the user in the pool,
// or creates a new one if none exists. Uses an in-memory map to prevent races when
// multiple RPCs arrive before the informer cache reflects the newly created session.
func (p *SessionProxy) findOrCreateSession(ctx context.Context, username, poolName string) (string, error) {
	key := sessionKey(username, poolName)

	// 1. Check in-memory cache first (handles concurrent RPCs).
	if name, ok := p.sessions.get(key); ok {
		// Validate the session still exists and is usable.
		session := &sparkv1alpha1.SparkInteractiveSession{}
		if err := p.client.Get(ctx, client.ObjectKey{
			Namespace: p.namespace,
			Name:      name,
		}, session); err == nil {
			switch session.Status.State {
			case "Active", "Idle", "Pending", "Assigning", "":
				p.log.Info("Reusing session (cached)", "name", name, "user", username, "state", session.Status.State)
				return name, nil
			}
		}
		// Cached session is gone or failed — remove from cache.
		p.sessions.delete(key)
	}

	// 2. Check the informer cache for existing sessions.
	sessionList := &sparkv1alpha1.SparkInteractiveSessionList{}
	if err := p.client.List(ctx, sessionList,
		client.InNamespace(p.namespace),
		client.MatchingLabels{
			"sparkinteractive.io/user": username,
			"sparkinteractive.io/pool": poolName,
		},
	); err != nil {
		return "", fmt.Errorf("list sessions: %w", err)
	}

	for _, s := range sessionList.Items {
		switch s.Status.State {
		case "Active", "Idle", "Pending", "Assigning":
			p.log.Info("Reusing session (discovered)", "name", s.Name, "user", username, "state", s.Status.State)
			p.sessions.set(key, s.Name)
			return s.Name, nil
		}
	}

	// 3. No reusable session — create a new one and cache it.
	name, err := p.createSession(ctx, username, poolName)
	if err != nil {
		return "", err
	}
	p.sessions.set(key, name)
	return name, nil
}

// createSession creates a SparkInteractiveSession CR for the user.
func (p *SessionProxy) createSession(ctx context.Context, username, poolName string) (string, error) {
	// Sanitize username for RFC 1123 subdomain: lowercase, replace underscores/dots with hyphens.
	safeName := strings.ToLower(username)
	safeName = strings.NewReplacer("_", "-", ".", "-").Replace(safeName)
	sessionName := fmt.Sprintf("session-%s-%d", safeName, time.Now().UnixNano()%100000)
	session := &sparkv1alpha1.SparkInteractiveSession{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sessionName,
			Namespace: p.namespace,
			Labels: map[string]string{
				"sparkinteractive.io/user":   username,
				"sparkinteractive.io/pool":   poolName,
				"sparkinteractive.io/origin": "proxy",
			},
		},
		Spec: sparkv1alpha1.SparkInteractiveSessionSpec{
			User: username,
			Pool: poolName,
		},
	}

	if err := p.client.Create(ctx, session); err != nil {
		return "", fmt.Errorf("create session: %w", err)
	}

	p.log.Info("Session created", "name", sessionName, "user", username, "pool", poolName)
	return sessionName, nil
}

// quotaExceededError is the typed failure waitForSessionActive returns when
// a session was rejected by pool quota policy. Callers branch on it to
// translate to codes.ResourceExhausted / HTTP 429 instead of a generic
// Unavailable / 503, which the client would otherwise read as "operator
// outage" rather than "you hit your limit."
type quotaExceededError struct{ message string }

func (e *quotaExceededError) Error() string { return e.message }

func isQuotaExceeded(err error) bool {
	var qe *quotaExceededError
	return errors.As(err, &qe)
}

// waitForSessionActive polls the session CR until it reaches Active state.
// Returns the endpoint or an error on timeout/failure.
//
// As an early-exit optimization, the controller may stamp an
// InstanceReady=False condition on the session when the backing
// SparkApplication is stuck/failed; when present we surface its message
// immediately instead of waiting out the full 60 s poll budget. This is the
// path that turns an opaque "session failed to start" into something like
// "SparkApplication X in state FAILED: ImagePullBackOff" for the client.
func (p *SessionProxy) waitForSessionActive(ctx context.Context, sessionName string) (string, error) {
	deadline := time.Now().Add(sessionPollTimeout)
	ticker := time.NewTicker(sessionPollInterval)
	defer ticker.Stop()

	for {
		session := &sparkv1alpha1.SparkInteractiveSession{}
		if err := p.client.Get(ctx, client.ObjectKey{
			Namespace: p.namespace,
			Name:      sessionName,
		}, session); err != nil {
			return "", fmt.Errorf("get session %s: %w", sessionName, err)
		}

		switch session.Status.State {
		case "Active", "Idle":
			if session.Status.Endpoint == "" {
				return "", fmt.Errorf("session %s is %s but has no endpoint", sessionName, session.Status.State)
			}
			return session.Status.Endpoint, nil
		case "Failed", "Terminated", "Terminating":
			if msg := conditionMessage(session, sparkv1alpha1.ConditionQuotaExceeded, metav1.ConditionTrue); msg != "" {
				return "", &quotaExceededError{message: msg}
			}
			if reason := terminalReason(session); reason != "" {
				return "", fmt.Errorf("session %s entered state %s: %s", sessionName, session.Status.State, reason)
			}
			return "", fmt.Errorf("session %s entered state %s", sessionName, session.Status.State)
		}

		if msg := instanceNotReadyMessage(session); msg != "" {
			return "", fmt.Errorf("session %s cannot become active: %s", sessionName, msg)
		}

		if time.Now().After(deadline) {
			return "", fmt.Errorf("timeout waiting for session %s to become active (current state: %s)", sessionName, session.Status.State)
		}

		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-ticker.C:
		}
	}
}

// conditionMessage returns the Message of the first condition matching the
// given type+status, or "" when no such condition is present. Used to peek
// at typed reasons (QuotaExceeded, PoolDeleted) so the proxy can choose the
// right gRPC code / HTTP status for the client.
func conditionMessage(session *sparkv1alpha1.SparkInteractiveSession, condType string, want metav1.ConditionStatus) string {
	for _, c := range session.Status.Conditions {
		if c.Type == condType && c.Status == want {
			return c.Message
		}
	}
	return ""
}

// instanceNotReadyMessage returns the InstanceReady=False message set by the
// session controller when assignment is blocked, or "" if no such condition
// is present.
func instanceNotReadyMessage(session *sparkv1alpha1.SparkInteractiveSession) string {
	for _, c := range session.Status.Conditions {
		if c.Type == sparkv1alpha1.ConditionInstanceReady && c.Status == metav1.ConditionFalse {
			if c.Message != "" {
				return c.Message
			}
			return c.Reason
		}
	}
	return ""
}

// terminalReason picks the most informative condition message off a session
// that landed in Failed/Terminated, so the proxy can surface "quota
// exceeded" / "pool deleted" / "instance terminated" instead of bare state.
func terminalReason(session *sparkv1alpha1.SparkInteractiveSession) string {
	for _, t := range []string{
		sparkv1alpha1.ConditionQuotaExceeded,
		sparkv1alpha1.ConditionPoolDeleted,
		sparkv1alpha1.ConditionInstanceTerminated,
		sparkv1alpha1.ConditionInstanceReady,
	} {
		for _, c := range session.Status.Conditions {
			if c.Type == t && c.Status == metav1.ConditionTrue && c.Message != "" {
				return c.Message
			}
			if c.Type == t && c.Status == metav1.ConditionFalse && c.Message != "" {
				return c.Message
			}
		}
	}
	return ""
}

// updateLastActivity updates the session's LastActivityAt timestamp and transitions Idle→Active.
func (p *SessionProxy) updateLastActivity(ctx context.Context, sessionName string) error {
	session := &sparkv1alpha1.SparkInteractiveSession{}
	if err := p.client.Get(ctx, client.ObjectKey{
		Namespace: p.namespace,
		Name:      sessionName,
	}, session); err != nil {
		return fmt.Errorf("get session for keepalive: %w", err)
	}

	now := metav1.Now()
	session.Status.LastActivityAt = &now
	if session.Status.State == "Idle" {
		session.Status.State = "Active"
	}

	if err := p.client.Status().Update(ctx, session); err != nil {
		return fmt.Errorf("update session activity: %w", err)
	}
	return nil
}

// StartThriftHTTPProxy runs the Thrift HTTP reverse proxy until ctx is
// canceled, then performs a graceful shutdown bounded by proxyShutdownTimeout.
// It implements the controller-runtime manager.Runnable contract so SIGTERM
// drains in-flight Thrift requests instead of cutting them mid-query.
func (p *SessionProxy) StartThriftHTTPProxy(ctx context.Context, addr string) error {
	server := &http.Server{
		Addr:              addr,
		Handler:           http.HandlerFunc(p.handleThriftHTTPRequest),
		ReadHeaderTimeout: 10 * time.Second,
	}

	p.log.Info("Starting Thrift HTTP proxy", "addr", addr)

	serveErr := make(chan error, 1)
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			serveErr <- err
			return
		}
		serveErr <- nil
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), proxyShutdownTimeout)
		defer cancel()
		p.log.Info("Shutting down Thrift HTTP proxy")
		if err := server.Shutdown(shutdownCtx); err != nil {
			p.log.Error(err, "Thrift HTTP proxy graceful shutdown failed")
			return err
		}
		return nil
	case err := <-serveErr:
		return err
	}
}

// handleThriftHTTPRequest handles a single Thrift HTTP transport request.
// It mirrors the handleConnectStream logic: extract creds from Authorization header,
// authenticate, route by Host header, find/create session, reverse proxy to backend.
func (p *SessionProxy) handleThriftHTTPRequest(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// 1. Parse Basic auth from Authorization header
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		http.Error(w, "missing Authorization header", http.StatusUnauthorized)
		return
	}
	username, password, err := parseBasicAuth(authHeader)
	if err != nil {
		p.log.Error(err, "Failed to parse Authorization header")
		http.Error(w, "invalid Authorization header", http.StatusUnauthorized)
		return
	}

	// 2. Authenticate via Keycloak ROPC
	userInfo, err := p.auth.AuthenticateWithCredentials(ctx, username, password)
	if err != nil {
		p.log.Error(err, "Credential authentication failed", "user", username)
		http.Error(w, "authentication failed", http.StatusUnauthorized)
		return
	}

	p.log.Info("Thrift user authenticated", "user", userInfo.Username, "remote", r.RemoteAddr)

	// 3. Find pool by hostname (prefer X-Forwarded-Host set by nginx, fallback to Host header)
	host := r.Header.Get("X-Forwarded-Host")
	if host == "" {
		host = r.Host
	}
	if idx := strings.LastIndex(host, ":"); idx > 0 {
		host = host[:idx]
	}

	var poolName string
	if host != "" {
		var poolType string
		poolName, poolType, err = p.findPoolByHost(ctx, host)
		if err != nil {
			p.log.Error(err, "Failed to find pool by host", "host", host, "user", userInfo.Username)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if poolType != "thrift" {
			p.log.Error(nil, "Pool matched by host is not a thrift pool", "host", host, "poolType", poolType)
			http.Error(w, fmt.Sprintf("pool %q is type %q, expected thrift", poolName, poolType), http.StatusBadRequest)
			return
		}
	} else {
		poolName, err = p.findPool(ctx, "thrift")
		if err != nil {
			p.log.Error(err, "Failed to find thrift pool", "user", userInfo.Username)
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
	}

	// 4. Find existing or create new session
	sessionName, err := p.findOrCreateSession(ctx, userInfo.Username, poolName)
	if err != nil {
		p.log.Error(err, "Failed to find/create session", "user", userInfo.Username, "pool", poolName)
		http.Error(w, "failed to create session", http.StatusInternalServerError)
		return
	}

	// 5. Resolve endpoint (use cache to avoid re-polling on every request)
	endpoint, err := p.resolveEndpoint(ctx, sessionName)
	if err != nil {
		p.log.Error(err, "Session did not become active", "session", sessionName)
		if isQuotaExceeded(err) {
			http.Error(w, err.Error(), http.StatusTooManyRequests)
			return
		}
		http.Error(w, "session failed to start: "+err.Error(), http.StatusServiceUnavailable)
		return
	}

	// 6. Reverse proxy to backend
	backendURL := &url.URL{
		Scheme: "http",
		Host:   endpoint,
	}
	// HiveServer2 in HTTP transport mode requires an Authorization header.
	// Replace the proxy credentials with the authenticated username
	// so the backend sees who the user is without receiving their Keycloak password.
	backendAuth := "Basic " + base64.StdEncoding.EncodeToString(
		[]byte(userInfo.Username+":x"),
	)
	rp := &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			req.URL.Scheme = backendURL.Scheme
			req.URL.Host = backendURL.Host
			req.URL.Path = "/cliservice"
			req.Host = backendURL.Host
			req.Header.Set("Authorization", backendAuth)
		},
		// On backend transport errors, drop the cached endpoint so the next
		// request re-resolves against the session CR. We can't retry in-band:
		// the request body has already been streamed, so the client must
		// reconnect — but at least it will hit a fresh address and see why
		// the previous one died (Evicted, OOMKilled, ...) in the 502 body.
		ErrorHandler: func(rw http.ResponseWriter, req *http.Request, err error) {
			p.log.Error(err, "Thrift backend transport failed; invalidating endpoint cache",
				"session", sessionName, "endpoint", endpoint)
			p.invalidateEndpoint(sessionName)
			msg := describeOrDefault(p.describeBackendFailure(req.Context(), sessionName), "transport error")
			http.Error(rw, "backend unavailable: "+msg, http.StatusBadGateway)
		},
	}

	p.log.V(1).Info("Thrift HTTP proxying request", "session", sessionName, "user", userInfo.Username, "endpoint", endpoint)

	// 7. Update activity per-request (no background keepalive needed for HTTP)
	if err := p.updateLastActivity(ctx, sessionName); err != nil {
		p.log.Error(err, "Failed to update activity", "session", sessionName)
	}

	rp.ServeHTTP(w, r)
}

// resolveEndpoint returns a cached endpoint for the session, or waits for it to become active.
func (p *SessionProxy) resolveEndpoint(ctx context.Context, sessionName string) (string, error) {
	if cached, ok := p.endpoints.get(sessionName); ok {
		return cached, nil
	}

	endpoint, err := p.waitForSessionActive(ctx, sessionName)
	if err != nil {
		return "", err
	}

	p.endpoints.set(sessionName, endpoint)
	return endpoint, nil
}

// invalidateEndpoint drops a cached endpoint so the next resolve goes back to
// the session CR. Used when a backend connection fails — usually because the
// driver pod restarted with a new IP — so the next request gets a fresh
// address instead of replaying the stale one.
func (p *SessionProxy) invalidateEndpoint(sessionName string) {
	p.endpoints.delete(sessionName)
}

// describeOrDefault returns specific when non-empty, otherwise generic.
// Keeps the error-construction sites at the call sites tidy.
func describeOrDefault(specific, generic string) string {
	if specific != "" {
		return specific
	}
	return generic
}

// describeBackendFailure converts a generic transport failure into a
// specific cause by inspecting the driver pod's status. Returns a short
// human-readable phrase (e.g. "driver pod Evicted", "driver container
// OOMKilled") or "" when no useful signal is available. Bounded by
// backendFailureProbeTimeout so a slow apiserver doesn't pile latency on top
// of an already-failing request.
func (p *SessionProxy) describeBackendFailure(ctx context.Context, sessionName string) string {
	probeCtx, cancel := context.WithTimeout(ctx, backendFailureProbeTimeout)
	defer cancel()

	session := &sparkv1alpha1.SparkInteractiveSession{}
	if err := p.client.Get(probeCtx, client.ObjectKey{Namespace: p.namespace, Name: sessionName}, session); err != nil {
		return ""
	}
	if session.Status.AssignedInstance == "" {
		return ""
	}

	pod := &corev1.Pod{}
	if err := p.client.Get(probeCtx, client.ObjectKey{
		Namespace: p.namespace,
		Name:      session.Status.AssignedInstance + "-driver",
	}, pod); err != nil {
		return ""
	}

	// Pod-level reason: node eviction, graceful node shutdown, etc.
	if pod.Status.Reason != "" {
		return "driver pod " + pod.Status.Reason
	}
	// Container termination reason: OOMKilled, Error, ContainerCannotRun.
	// Prefer the current state's reason, then fall back to LastTerminationState
	// (covers the window after a crash where the pod is being recreated).
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Terminated != nil && cs.State.Terminated.Reason != "" {
			return "driver container " + cs.State.Terminated.Reason
		}
		if cs.LastTerminationState.Terminated != nil && cs.LastTerminationState.Terminated.Reason != "" {
			return "driver container " + cs.LastTerminationState.Terminated.Reason
		}
	}
	return ""
}

// resolveFreshEndpoint forces a re-resolution from the session CR, bypassing
// the cache. Used as the second attempt after a stale-endpoint failure.
func (p *SessionProxy) resolveFreshEndpoint(ctx context.Context, sessionName string) (string, error) {
	p.invalidateEndpoint(sessionName)
	endpoint, err := p.waitForSessionActive(ctx, sessionName)
	if err != nil {
		return "", err
	}
	p.endpoints.set(sessionName, endpoint)
	return endpoint, nil
}

// StartConnectProxy runs the Spark Connect gRPC server until ctx is canceled,
// at which point it triggers GracefulStop so active streams can finish
// (subject to proxyShutdownTimeout) before forcing a Stop.
func (p *SessionProxy) StartConnectProxy(ctx context.Context, addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}

	server := grpc.NewServer(
		grpc.ForceServerCodec(rawCodec{}),
		grpc.UnknownServiceHandler(p.handleConnectStream),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    20 * time.Second, // ping client every 20s if idle
			Timeout: 10 * time.Second, // wait 10s for ping ack
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second, // allow client pings as often as every 10s
			PermitWithoutStream: true,
		}),
	)

	p.log.Info("Starting Connect gRPC proxy", "addr", addr)

	serveErr := make(chan error, 1)
	go func() {
		if err := server.Serve(listener); err != nil && err != grpc.ErrServerStopped {
			serveErr <- err
			return
		}
		serveErr <- nil
	}()

	select {
	case <-ctx.Done():
		// GracefulStop blocks until all active RPCs complete; bound it so a
		// stuck stream can't keep the pod alive past its terminationGracePeriod.
		done := make(chan struct{})
		go func() {
			server.GracefulStop()
			close(done)
		}()
		p.log.Info("Shutting down Connect gRPC proxy")
		select {
		case <-done:
			return nil
		case <-time.After(proxyShutdownTimeout):
			p.log.Info("Connect gRPC proxy graceful shutdown timeout, forcing stop")
			server.Stop()
			<-done
			return nil
		}
	case err := <-serveErr:
		return err
	}
}

// authenticateConnect pulls credentials off the gRPC stream and validates them
// against the configured authenticator. On insecure channels where gRPC metadata
// credentials aren't available, it reads the first protobuf message to recover
// user_context.user_id; that frame is returned so the caller can forward it to
// the backend after the proxy hop has been established.
func (p *SessionProxy) authenticateConnect(ctx context.Context, serverStream grpc.ServerStream) (*auth.UserInfo, *rawFrame, error) {
	var firstMsg *rawFrame
	username, password, metaErr := extractCredentialsFromGRPCMetadata(ctx)
	if metaErr != nil {
		// PySpark on insecure channels can't send gRPC metadata creds, but
		// user_id is always embedded in the protobuf request body via
		// sc://host:port/;user_id=base64(user:pass) (or bare username).
		firstMsg = &rawFrame{}
		if err := serverStream.RecvMsg(firstMsg); err != nil {
			p.log.Error(err, "Failed to read first gRPC message")
			return nil, nil, status.Errorf(codes.Internal, "failed to read request: %v", err)
		}
		var protoErr error
		username, password, protoErr = extractCredentialsFromProto(firstMsg.payload)
		if protoErr != nil {
			p.log.Error(protoErr, "Failed to extract credentials from protobuf")
			return nil, nil, status.Errorf(codes.Unauthenticated,
				"missing credentials: set user_id in sc:// URL, e.g. sc://host:port/;user_id=base64(user:pass)")
		}
	}

	userInfo, err := p.auth.AuthenticateWithCredentials(ctx, username, password)
	if err != nil {
		p.log.Error(err, "Credential authentication failed", "user", username)
		return nil, nil, status.Errorf(codes.Unauthenticated, "authentication failed")
	}
	return userInfo, firstMsg, nil
}

// resolveConnectPool maps the inbound gRPC request to a pool. Hostname routing
// prefers x-forwarded-host (set by nginx ingress) over :authority because nginx
// rewrites :authority to the upstream service name when proxying gRPC, losing
// the original client hostname. Returns a gRPC status error suitable for return
// to the client.
func (p *SessionProxy) resolveConnectPool(ctx context.Context) (string, error) {
	host := ""
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if vals := md.Get("x-forwarded-host"); len(vals) > 0 {
			host = vals[0]
		} else if vals := md.Get(":authority"); len(vals) > 0 {
			host = vals[0]
		}
	}
	if idx := strings.LastIndex(host, ":"); idx > 0 {
		host = host[:idx]
	}
	p.log.V(1).Info("Connect routing", "host", host)

	if host == "" {
		poolName, err := p.findPool(ctx, "connect")
		if err != nil {
			return "", status.Errorf(codes.FailedPrecondition, "%v", err)
		}
		return poolName, nil
	}

	poolName, poolType, err := p.findPoolByHost(ctx, host)
	if err != nil {
		return "", status.Errorf(codes.FailedPrecondition, "%v", err)
	}
	if poolType != "connect" {
		return "", status.Errorf(codes.FailedPrecondition, "pool %q is type %q, expected connect", poolName, poolType)
	}
	return poolName, nil
}

// dialBackendStream opens a gRPC client connection to endpoint and starts a
// bidirectional stream for fullMethod. Returns both the connection and the
// stream so the caller can defer Close() on the connection. Either return is
// nil if err is non-nil.
//
// ForceCodec keeps the content-type as "application/grpc" (which the backend
// Spark Connect server expects) while still using raw byte passthrough for
// marshal/unmarshal.
func (p *SessionProxy) dialBackendStream(ctx context.Context, endpoint, fullMethod string) (*grpc.ClientConn, grpc.ClientStream, error) {
	backendConn, err := grpc.NewClient("passthrough:///"+endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(rawCodec{})),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                20 * time.Second,
			Timeout:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("dial backend %s: %w", endpoint, err)
	}

	backendStream, err := backendConn.NewStream(ctx, &grpc.StreamDesc{
		ServerStreams: true,
		ClientStreams: true,
	}, fullMethod, grpc.ForceCodec(rawCodec{}))
	if err != nil {
		backendConn.Close()
		return nil, nil, fmt.Errorf("open backend stream %s: %w", fullMethod, err)
	}
	return backendConn, backendStream, nil
}

// handleConnectStream handles a single gRPC stream for Spark Connect.
func (p *SessionProxy) handleConnectStream(_ interface{}, serverStream grpc.ServerStream) error {
	ctx := serverStream.Context()

	userInfo, firstMsg, err := p.authenticateConnect(ctx, serverStream)
	if err != nil {
		return err
	}
	p.log.Info("Connect user authenticated", "user", userInfo.Username)

	poolName, err := p.resolveConnectPool(ctx)
	if err != nil {
		p.log.Error(err, "Failed to resolve connect pool", "user", userInfo.Username)
		return err
	}

	sessionName, err := p.findOrCreateSession(ctx, userInfo.Username, poolName)
	if err != nil {
		p.log.Error(err, "Failed to find/create session", "user", userInfo.Username, "pool", poolName)
		return status.Errorf(codes.Internal, "failed to create session")
	}

	endpoint, err := p.resolveEndpoint(ctx, sessionName)
	if err != nil {
		p.log.Error(err, "Session did not become active", "session", sessionName)
		if isQuotaExceeded(err) {
			return status.Errorf(codes.ResourceExhausted, "%s", err.Error())
		}
		return status.Errorf(codes.Unavailable, "session failed to start: %s", err.Error())
	}

	fullMethod, ok := grpc.MethodFromServerStream(serverStream)
	if !ok {
		return status.Errorf(codes.Internal, "failed to get method name")
	}

	// Try the cached endpoint first; if the dial or stream creation fails the
	// driver may have restarted with a new IP, so invalidate the cache and try
	// once with a freshly resolved address before giving up.
	backendCtx, backendCancel := context.WithCancel(ctx)
	defer backendCancel()

	backendConn, backendStream, err := p.dialBackendStream(backendCtx, endpoint, fullMethod)
	if err != nil {
		p.log.Info("Backend dial failed on cached endpoint; re-resolving", "session", sessionName, "endpoint", endpoint, "error", err.Error())
		fresh, resolveErr := p.resolveFreshEndpoint(ctx, sessionName)
		if resolveErr != nil {
			p.log.Error(resolveErr, "Failed to re-resolve endpoint", "session", sessionName)
			return status.Errorf(codes.Unavailable, "backend unavailable: %s", describeOrDefault(p.describeBackendFailure(ctx, sessionName), resolveErr.Error()))
		}
		endpoint = fresh
		backendConn, backendStream, err = p.dialBackendStream(backendCtx, endpoint, fullMethod)
		if err != nil {
			p.log.Error(err, "Backend dial failed after re-resolve", "session", sessionName, "endpoint", endpoint)
			return status.Errorf(codes.Unavailable, "backend stream failed: %s", describeOrDefault(p.describeBackendFailure(ctx, sessionName), err.Error()))
		}
	}
	defer backendConn.Close()

	p.log.Info("Connect session proxying started", "session", sessionName, "user", userInfo.Username, "endpoint", endpoint, "method", fullMethod)

	// If we consumed the first message during auth, forward it to the backend
	// before the bidirectional pump starts so message ordering is preserved.
	if firstMsg != nil {
		if err := backendStream.SendMsg(firstMsg); err != nil {
			p.log.Error(err, "Failed to forward first message to backend", "session", sessionName)
			return status.Errorf(codes.Unavailable, "failed to forward request to backend")
		}
	}

	// Start keepalive
	keepaliveCtx, keepaliveCancel := context.WithCancel(context.Background())
	defer keepaliveCancel()
	go p.runKeepalive(keepaliveCtx, sessionName)

	// Bidirectional gRPC stream proxy
	errCh := make(chan error, 2)

	// Server→Backend (client sends to proxy, proxy forwards to backend)
	go func() {
		for {
			frame := &rawFrame{}
			if err := serverStream.RecvMsg(frame); err != nil {
				if err == io.EOF {
					_ = backendStream.CloseSend()
					errCh <- nil
					return
				}
				errCh <- err
				return
			}
			if err := backendStream.SendMsg(frame); err != nil {
				errCh <- err
				return
			}
		}
	}()

	// Backend→Server (backend responds, proxy forwards to client)
	go func() {
		for {
			frame := &rawFrame{}
			if err := backendStream.RecvMsg(frame); err != nil {
				if err == io.EOF {
					errCh <- nil
					return
				}
				errCh <- err
				return
			}
			if err := serverStream.SendMsg(frame); err != nil {
				errCh <- err
				return
			}
		}
	}()

	// Wait for either direction to finish
	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			p.log.V(1).Info("gRPC stream ended", "session", sessionName, "error", err)
			return truncateStatusError(err)
		}
	}

	return nil
}

// runKeepalive periodically updates LastActivityAt for the session. Each
// update is bounded by keepaliveUpdateTimeout and tied to the keepalive
// context so a cancelled stream cleans up promptly instead of letting an
// in-flight apiserver write outlive the session.
//
// Exits early on NotFound — the session CR has been deleted (idle timeout,
// pool teardown, explicit user delete) and the surrounding gRPC stream will
// see EOF on its own. Without this short-circuit the loop would log an
// error every keepaliveInterval until the stream noticed independently.
func (p *SessionProxy) runKeepalive(ctx context.Context, sessionName string) {
	ticker := time.NewTicker(keepaliveInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			updateCtx, cancel := context.WithTimeout(ctx, keepaliveUpdateTimeout)
			err := p.updateLastActivity(updateCtx, sessionName)
			cancel()
			if err == nil {
				continue
			}
			if apierrors.IsNotFound(err) {
				p.log.Info("Keepalive stopping: session no longer exists", "session", sessionName)
				return
			}
			p.log.Error(err, "Keepalive update failed", "session", sessionName)
		}
	}
}
