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

package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	metricsv "k8s.io/metrics/pkg/client/clientset/versioned"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	sparkv1alpha1 "github.com/tander/spark-session-operator/api/v1alpha1"
)

const (
	poolFinalizer = "sparkinteractive.io/pool-finalizer"
)

var sparkAppGVR = schema.GroupVersionResource{
	Group:    "sparkoperator.k8s.io",
	Version:  "v1beta2",
	Resource: "sparkapplications",
}

// SparkSessionPoolReconciler reconciles a SparkSessionPool object
type SparkSessionPoolReconciler struct {
	client.Client
	Scheme         *runtime.Scheme
	Log            logr.Logger
	MetricsClient  metricsv.Interface
	ProxyNamespace string // Namespace where the proxy Service lives (for Ingress creation)
}

// +kubebuilder:rbac:groups=sparkinteractive.io,resources=sparksessionpools,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=sparkinteractive.io,resources=sparksessionpools/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=sparkinteractive.io,resources=sparksessionpools/finalizers,verbs=update
// +kubebuilder:rbac:groups=sparkinteractive.io,resources=sparkinteractivesessions,verbs=get;list;watch
// +kubebuilder:rbac:groups=sparkoperator.k8s.io,resources=sparkapplications,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups=metrics.k8s.io,resources=pods,verbs=get;list
// +kubebuilder:rbac:groups=networking.k8s.io,resources=ingresses,verbs=get;list;watch;create;update;patch;delete

func (r *SparkSessionPoolReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithValues("pool", req.NamespacedName)

	// Fetch the SparkSessionPool
	pool := &sparkv1alpha1.SparkSessionPool{}
	if err := r.Get(ctx, req.NamespacedName, pool); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Handle deletion
	if !pool.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, log, pool)
	}

	// Ensure finalizer
	if !controllerutil.ContainsFinalizer(pool, poolFinalizer) {
		patch := client.MergeFrom(pool.DeepCopy())
		controllerutil.AddFinalizer(pool, poolFinalizer)
		if err := r.Patch(ctx, pool, patch); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Reconcile Ingress for hostname-based routing
	if err := r.reconcileIngress(ctx, log, pool); err != nil {
		log.Error(err, "Failed to reconcile Ingress")
		return ctrl.Result{}, err
	}

	// List existing SparkApplications owned by this pool
	existingApps, err := r.listPoolInstances(ctx, pool)
	if err != nil {
		log.Error(err, "Failed to list pool instances")
		return ctrl.Result{}, err
	}

	// Count sessions per instance
	sessionCounts, err := r.countSessionsPerInstance(ctx, pool)
	if err != nil {
		log.Error(err, "Failed to count sessions")
		return ctrl.Result{}, err
	}

	// Update instance statuses
	instanceStatuses := r.buildInstanceStatuses(ctx, pool.Namespace, existingApps, sessionCounts)

	// Calculate desired replicas
	currentRunning := int32(0)
	currentReady := int32(0)
	currentPending := int32(0)
	totalSessions := int32(0)
	for _, inst := range instanceStatuses {
		if inst.State == "Running" || inst.State == "Draining" {
			currentRunning++
		}
		if inst.State == "Running" {
			currentReady++
		}
		if inst.State == "Pending" || inst.State == "Submitted" || inst.State == "" {
			currentPending++
		}
		totalSessions += inst.ActiveSessions
	}

	desiredReplicas := r.calculateDesiredReplicas(ctx, pool, currentReady, totalSessions, instanceStatuses)

	// Scale up if needed (count pending instances to avoid creating duplicates while Spark apps are starting)
	currentTotal := currentRunning + currentPending
	if currentTotal < desiredReplicas {
		toCreate := desiredReplicas - currentTotal
		log.Info("Scaling up", "current", currentRunning, "pending", currentPending, "desired", desiredReplicas, "creating", toCreate)
		for i := int32(0); i < toCreate; i++ {
			if err := r.createPoolInstance(ctx, log, pool, existingApps); err != nil {
				log.Error(err, "Failed to create pool instance")
				return ctrl.Result{}, err
			}
		}
	}

	// Scale down if needed (respect cooldown)
	if currentRunning > desiredReplicas {
		if r.canScaleDown(pool) {
			toRemove := currentRunning - desiredReplicas
			log.Info("Scaling down", "current", currentRunning, "desired", desiredReplicas, "removing", toRemove)
			if err := r.scaleDown(ctx, log, pool, instanceStatuses, toRemove); err != nil {
				log.Error(err, "Failed to scale down")
				return ctrl.Result{}, err
			}
		}
	}

	// Replace failed instances
	for _, inst := range instanceStatuses {
		if inst.State == "Failed" {
			log.Info("Replacing failed instance", "instance", inst.Name)
			if err := r.deleteSparkApplication(ctx, pool.Namespace, inst.Name); err != nil {
				log.Error(err, "Failed to delete failed instance", "instance", inst.Name)
			}
			if err := r.createPoolInstance(ctx, log, pool, existingApps); err != nil {
				log.Error(err, "Failed to create replacement instance")
			}
		}
	}

	// Update pool status
	pool.Status.Instances = instanceStatuses
	pool.Status.TotalActiveSessions = totalSessions
	pool.Status.CurrentReplicas = currentRunning
	pool.Status.ReadyReplicas = currentReady
	if err := r.Status().Update(ctx, pool); err != nil {
		log.Error(err, "Failed to update pool status")
		return ctrl.Result{}, err
	}

	// Requeue to periodically check metrics
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

func (r *SparkSessionPoolReconciler) calculateDesiredReplicas(
	ctx context.Context,
	pool *sparkv1alpha1.SparkSessionPool,
	currentReady int32,
	totalSessions int32,
	instances []sparkv1alpha1.PoolInstanceStatus,
) int32 {
	metricsType := pool.Spec.Scaling.Metrics.Type
	if metricsType == "" {
		metricsType = "activeSessions"
	}

	var desired int32

	switch metricsType {
	case "cpu", "memory":
		target := float64(pool.Spec.Scaling.Metrics.TargetPerInstance)
		if target <= 0 {
			target = 80
		}

		avgUtil, err := r.getResourceUtilization(ctx, pool, metricsType, instances)
		if err != nil {
			r.Log.V(1).Info("Failed to get resource utilization, falling back to min replicas",
				"error", err, "metricsType", metricsType)
			desired = pool.Spec.Replicas.Min
		} else if currentReady <= 0 {
			desired = pool.Spec.Replicas.Min
		} else {
			// HPA formula: desired = ceil(currentReplicas * currentUtilization / targetUtilization)
			desired = int32(math.Ceil(float64(currentReady) * avgUtil / target))
		}

	default: // "activeSessions"
		target := pool.Spec.Scaling.Metrics.TargetPerInstance
		if target <= 0 {
			target = 20
		}

		if totalSessions > 0 {
			desired = (totalSessions + target - 1) / target // ceiling division
		} else {
			desired = pool.Spec.Replicas.Min
		}

		// Apply scale up threshold
		scaleUpThreshold, _ := strconv.ParseFloat(pool.Spec.Scaling.ScaleUpThreshold, 64)
		if scaleUpThreshold == 0 {
			scaleUpThreshold = 0.8
		}

		// If current load exceeds threshold, add headroom
		if currentReady > 0 {
			loadPerInstance := float64(totalSessions) / float64(currentReady)
			if loadPerInstance > float64(target)*scaleUpThreshold {
				desired = currentReady + 1
			}
		}
	}

	// Clamp to min/max
	if desired < pool.Spec.Replicas.Min {
		desired = pool.Spec.Replicas.Min
	}
	if desired > pool.Spec.Replicas.Max {
		desired = pool.Spec.Replicas.Max
	}

	return desired
}

// getResourceUtilization calculates the average CPU or memory utilization across running pool instances
// by querying the Kubernetes Metrics API for driver pod metrics.
func (r *SparkSessionPoolReconciler) getResourceUtilization(
	ctx context.Context,
	pool *sparkv1alpha1.SparkSessionPool,
	metricsType string,
	instances []sparkv1alpha1.PoolInstanceStatus,
) (float64, error) {
	var totalUtil float64
	var measuredPods int

	for _, inst := range instances {
		if inst.State != "Running" {
			continue
		}

		driverPodName := inst.Name + "-driver"
		ns := pool.Namespace

		// Get the driver pod to read resource requests
		pod := &corev1.Pod{}
		if err := r.Get(ctx, types.NamespacedName{Name: driverPodName, Namespace: ns}, pod); err != nil {
			r.Log.V(1).Info("Could not get driver pod", "pod", driverPodName, "error", err)
			continue
		}

		// Get pod metrics from Kubernetes Metrics API
		podMetrics, err := r.MetricsClient.MetricsV1beta1().PodMetricses(ns).Get(ctx, driverPodName, metav1.GetOptions{})
		if err != nil {
			r.Log.V(1).Info("Could not get pod metrics", "pod", driverPodName, "error", err)
			continue
		}

		// Sum usage and requests across all containers
		var totalUsage, totalRequests resource.Quantity
		usageMap := make(map[string]resource.Quantity)
		for _, container := range podMetrics.Containers {
			switch metricsType {
			case "cpu":
				usageMap[container.Name] = container.Usage[corev1.ResourceCPU]
			case "memory":
				usageMap[container.Name] = container.Usage[corev1.ResourceMemory]
			}
		}

		for _, container := range pod.Spec.Containers {
			usage, ok := usageMap[container.Name]
			if !ok {
				continue
			}
			totalUsage.Add(usage)

			var req resource.Quantity
			switch metricsType {
			case "cpu":
				req = container.Resources.Requests[corev1.ResourceCPU]
			case "memory":
				req = container.Resources.Requests[corev1.ResourceMemory]
			}
			totalRequests.Add(req)
		}

		if !totalRequests.IsZero() {
			util := float64(totalUsage.MilliValue()) / float64(totalRequests.MilliValue()) * 100
			totalUtil += util
			measuredPods++
		}
	}

	if measuredPods == 0 {
		return 0, fmt.Errorf("no pods have metrics available")
	}

	return totalUtil / float64(measuredPods), nil
}

func (r *SparkSessionPoolReconciler) canScaleDown(pool *sparkv1alpha1.SparkSessionPool) bool {
	if pool.Status.LastScaleTime == nil {
		return true
	}
	cooldown := time.Duration(pool.Spec.Scaling.CooldownSeconds) * time.Second
	if cooldown == 0 {
		cooldown = 5 * time.Minute
	}
	return time.Since(pool.Status.LastScaleTime.Time) > cooldown
}

func (r *SparkSessionPoolReconciler) scaleDown(
	ctx context.Context,
	log logr.Logger,
	pool *sparkv1alpha1.SparkSessionPool,
	instances []sparkv1alpha1.PoolInstanceStatus,
	count int32,
) error {
	// Sort instances by session count ascending — remove least loaded first
	// If drainBeforeScaleDown, prefer instances with 0 sessions
	removed := int32(0)
	for _, inst := range instances {
		if removed >= count {
			break
		}
		if inst.State != "Running" {
			continue
		}
		if pool.Spec.Scaling.DrainBeforeScaleDown && inst.ActiveSessions > 0 {
			// Mark as draining instead of deleting
			log.Info("Marking instance for drain", "instance", inst.Name, "activeSessions", inst.ActiveSessions)
			// TODO: update instance label to "draining" so no new sessions are routed here
			continue
		}
		log.Info("Removing instance", "instance", inst.Name)
		if err := r.deleteSparkApplication(ctx, pool.Namespace, inst.Name); err != nil {
			return err
		}
		removed++
	}

	now := metav1.Now()
	pool.Status.LastScaleTime = &now
	return nil
}

func (r *SparkSessionPoolReconciler) createPoolInstance(
	ctx context.Context,
	log logr.Logger,
	pool *sparkv1alpha1.SparkSessionPool,
	existing []unstructured.Unstructured,
) error {
	// Generate unique name
	instanceName := fmt.Sprintf("%s-%d", pool.Name, time.Now().UnixNano()%100000)

	// Build SparkApplication from template
	sparkApp := &unstructured.Unstructured{}
	sparkApp.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "sparkoperator.k8s.io",
		Version: "v1beta2",
		Kind:    "SparkApplication",
	})
	sparkApp.SetName(instanceName)
	sparkApp.SetNamespace(pool.Namespace)

	// Set labels for ownership tracking
	labels := map[string]string{
		"sparkinteractive.io/pool":          pool.Name,
		"sparkinteractive.io/pool-type":     pool.Spec.Type,
		"sparkinteractive.io/managed-by":    "spark-session-operator",
		"sparkinteractive.io/instance-role": "active",
	}
	sparkApp.SetLabels(labels)

	// Copy template spec from raw JSON
	if pool.Spec.SparkApplicationTemplate.Spec != nil {
		var specMap map[string]interface{}
		if err := json.Unmarshal(pool.Spec.SparkApplicationTemplate.Spec.Raw, &specMap); err != nil {
			return fmt.Errorf("failed to unmarshal template spec: %w", err)
		}
		if err := unstructured.SetNestedField(sparkApp.Object, specMap, "spec"); err != nil {
			return fmt.Errorf("failed to set spec from template: %w", err)
		}
	}

	// Set owner reference for GC
	ownerRef := metav1.OwnerReference{
		APIVersion: pool.APIVersion,
		Kind:       pool.Kind,
		Name:       pool.Name,
		UID:        pool.UID,
		Controller: boolPtr(true),
	}
	sparkApp.SetOwnerReferences([]metav1.OwnerReference{ownerRef})

	log.Info("Creating pool instance", "name", instanceName, "pool", pool.Name)
	return r.Create(ctx, sparkApp)
}

func (r *SparkSessionPoolReconciler) listPoolInstances(
	ctx context.Context,
	pool *sparkv1alpha1.SparkSessionPool,
) ([]unstructured.Unstructured, error) {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "sparkoperator.k8s.io",
		Version: "v1beta2",
		Kind:    "SparkApplicationList",
	})

	if err := r.List(ctx, list,
		client.InNamespace(pool.Namespace),
		client.MatchingLabels{"sparkinteractive.io/pool": pool.Name},
	); err != nil {
		return nil, err
	}

	return list.Items, nil
}

func (r *SparkSessionPoolReconciler) countSessionsPerInstance(
	ctx context.Context,
	pool *sparkv1alpha1.SparkSessionPool,
) (map[string]int32, error) {
	sessionList := &sparkv1alpha1.SparkInteractiveSessionList{}
	if err := r.List(ctx, sessionList,
		client.InNamespace(pool.Namespace),
		client.MatchingFields{"spec.pool": pool.Name},
	); err != nil {
		return nil, err
	}

	counts := make(map[string]int32)
	for _, session := range sessionList.Items {
		if session.Status.State == "Active" || session.Status.State == "Idle" {
			counts[session.Status.AssignedInstance]++
		}
	}
	return counts, nil
}

func (r *SparkSessionPoolReconciler) buildInstanceStatuses(
	ctx context.Context,
	namespace string,
	apps []unstructured.Unstructured,
	sessionCounts map[string]int32,
) []sparkv1alpha1.PoolInstanceStatus {
	var statuses []sparkv1alpha1.PoolInstanceStatus

	for _, app := range apps {
		name := app.GetName()

		// Extract SparkApplication state
		sparkState, _, _ := unstructured.NestedString(app.Object, "status", "applicationState", "state")

		state := "Pending"
		endpoint := ""

		switch sparkState {
		case "RUNNING":
			role, _, _ := unstructured.NestedString(app.Object, "metadata", "labels", "sparkinteractive.io/instance-role")
			if role == "draining" {
				state = "Draining"
			} else {
				state = "Running"
			}
			// Look up the driver service by spark-app-selector label.
			// The spark-operator creates a headless service labeled with
			// spark-app-selector=<sparkApplicationId>.
			sparkAppID, _, _ := unstructured.NestedString(app.Object, "status", "sparkApplicationId")
			if sparkAppID != "" {
				svcList := &corev1.ServiceList{}
				if err := r.List(ctx, svcList,
					client.InNamespace(namespace),
					client.MatchingLabels{"spark-app-selector": sparkAppID},
				); err == nil {
					for _, svc := range svcList.Items {
						if strings.HasSuffix(svc.Name, "-driver-svc") {
							endpoint = fmt.Sprintf("%s.%s.svc", svc.Name, namespace)
							break
						}
					}
				}
			}
		case "FAILED", "FAILING":
			state = "Failed"
		case "COMPLETED":
			state = "Failed" // Thrift/Connect servers shouldn't complete
		case "SUBMITTED", "PENDING_RERUN", "":
			state = "Pending"
		}

		statuses = append(statuses, sparkv1alpha1.PoolInstanceStatus{
			Name:                  name,
			State:                 state,
			ActiveSessions:        sessionCounts[name],
			Endpoint:              endpoint,
			SparkApplicationState: sparkState,
		})
	}

	return statuses
}

func (r *SparkSessionPoolReconciler) deleteSparkApplication(ctx context.Context, namespace, name string) error {
	app := &unstructured.Unstructured{}
	app.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "sparkoperator.k8s.io",
		Version: "v1beta2",
		Kind:    "SparkApplication",
	})
	app.SetName(name)
	app.SetNamespace(namespace)
	return r.Delete(ctx, app)
}

func (r *SparkSessionPoolReconciler) handleDeletion(
	ctx context.Context,
	log logr.Logger,
	pool *sparkv1alpha1.SparkSessionPool,
) (ctrl.Result, error) {
	if controllerutil.ContainsFinalizer(pool, poolFinalizer) {
		// Delete all pool instances
		apps, err := r.listPoolInstances(ctx, pool)
		if err != nil {
			return ctrl.Result{}, err
		}
		for _, app := range apps {
			log.Info("Deleting pool instance", "name", app.GetName())
			if err := r.Delete(ctx, &app); err != nil && !errors.IsNotFound(err) {
				return ctrl.Result{}, err
			}
		}

		// Delete the Ingress created for this pool (cross-namespace, no ownerRef GC)
		if err := r.deletePoolIngress(ctx, log, pool); err != nil {
			log.Error(err, "Failed to delete pool Ingress")
		}

		patch := client.MergeFrom(pool.DeepCopy())
		controllerutil.RemoveFinalizer(pool, poolFinalizer)
		if err := r.Patch(ctx, pool, patch); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, nil
}

// deletePoolIngress deletes the Ingress associated with a pool.
func (r *SparkSessionPoolReconciler) deletePoolIngress(
	ctx context.Context,
	log logr.Logger,
	pool *sparkv1alpha1.SparkSessionPool,
) error {
	ingressNamespace := r.ProxyNamespace
	if ingressNamespace == "" {
		ingressNamespace = pool.Namespace
	}

	var suffix string
	switch pool.Spec.Type {
	case "connect":
		suffix = "-connect"
	case "thrift":
		suffix = "-thrift"
	default:
		return nil
	}

	ingress := &networkingv1.Ingress{}
	key := client.ObjectKey{Namespace: ingressNamespace, Name: pool.Name + suffix}
	if err := r.Get(ctx, key, ingress); err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}

	log.Info("Deleting Ingress for pool", "ingress", ingress.Name, "namespace", ingressNamespace)
	return r.Delete(ctx, ingress)
}

// reconcileIngress creates or updates an Ingress for pools to enable hostname-based routing.
func (r *SparkSessionPoolReconciler) reconcileIngress(
	ctx context.Context,
	log logr.Logger,
	pool *sparkv1alpha1.SparkSessionPool,
) error {
	var ingressSuffix string
	var backendProtocol string
	var backendPort int32
	extraAnnotations := map[string]string{}

	switch pool.Spec.Type {
	case "connect":
		ingressSuffix = "-connect"
		backendProtocol = "GRPC"
		backendPort = 15002
	case "thrift":
		ingressSuffix = "-thrift"
		backendProtocol = "HTTP"
		backendPort = 10009
		extraAnnotations["nginx.ingress.kubernetes.io/proxy-body-size"] = "0"
		extraAnnotations["nginx.ingress.kubernetes.io/proxy-read-timeout"] = "3600"
		extraAnnotations["nginx.ingress.kubernetes.io/proxy-send-timeout"] = "3600"
	default:
		return nil // Unknown pool type — skip ingress
	}

	// Ingress must live in the same namespace as the proxy Service.
	ingressNamespace := r.ProxyNamespace
	if ingressNamespace == "" {
		ingressNamespace = pool.Namespace // fallback if ProxyNamespace not configured
	}

	ingressName := pool.Name + ingressSuffix
	pathType := networkingv1.PathTypePrefix

	const proxyServiceName = "spark-session-operator-proxy"

	annotations := map[string]string{
		"nginx.ingress.kubernetes.io/backend-protocol": backendProtocol,
		"yandex.cloud/load-balancer-type":              "internal",
	}
	for k, v := range extraAnnotations {
		annotations[k] = v
	}

	// Cross-namespace ownerReferences are not allowed, so we use labels to track
	// which pool owns this Ingress. Cleanup happens in handleDeletion.
	desired := &networkingv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ingressName,
			Namespace: ingressNamespace,
			Labels: map[string]string{
				"sparkinteractive.io/pool":           pool.Name,
				"sparkinteractive.io/pool-namespace": pool.Namespace,
				"sparkinteractive.io/managed-by":     "spark-session-operator",
			},
			Annotations: annotations,
		},
		Spec: networkingv1.IngressSpec{
			IngressClassName: stringPtr("nginx"),
			Rules: []networkingv1.IngressRule{{
				Host: pool.Spec.Host,
				IngressRuleValue: networkingv1.IngressRuleValue{
					HTTP: &networkingv1.HTTPIngressRuleValue{
						Paths: []networkingv1.HTTPIngressPath{{
							Path:     "/",
							PathType: &pathType,
							Backend: networkingv1.IngressBackend{
								Service: &networkingv1.IngressServiceBackend{
									Name: proxyServiceName,
									Port: networkingv1.ServiceBackendPort{Number: backendPort},
								},
							},
						}},
					},
				},
			}},
		},
	}

	existing := &networkingv1.Ingress{}
	err := r.Get(ctx, client.ObjectKey{Namespace: ingressNamespace, Name: ingressName}, existing)
	if errors.IsNotFound(err) {
		log.Info("Creating Ingress for pool", "ingress", ingressName, "namespace", ingressNamespace, "host", pool.Spec.Host)
		return r.Create(ctx, desired)
	}
	if err != nil {
		return fmt.Errorf("get ingress %s: %w", ingressName, err)
	}

	// Only update if something we manage actually changed.
	// The existing Ingress may have extra annotations added by nginx or other controllers,
	// so we only check that our desired annotations/labels are present with correct values.
	if reflect.DeepEqual(existing.Spec, desired.Spec) &&
		mapsContainAll(existing.Labels, desired.Labels) &&
		mapsContainAll(existing.Annotations, desired.Annotations) {
		return nil
	}

	existing.Spec = desired.Spec
	// Merge our labels/annotations into existing (preserving any extras added by other controllers)
	for k, v := range desired.Labels {
		existing.Labels[k] = v
	}
	for k, v := range desired.Annotations {
		if existing.Annotations == nil {
			existing.Annotations = make(map[string]string)
		}
		existing.Annotations[k] = v
	}
	log.V(1).Info("Updating Ingress for pool", "ingress", ingressName, "namespace", ingressNamespace, "host", pool.Spec.Host)
	return r.Update(ctx, existing)
}

func stringPtr(s string) *string {
	return &s
}

// SetupWithManager sets up the controller with the Manager.
func (r *SparkSessionPoolReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Index sessions by pool for efficient lookup
	if err := mgr.GetFieldIndexer().IndexField(
		context.Background(),
		&sparkv1alpha1.SparkInteractiveSession{},
		"spec.pool",
		func(obj client.Object) []string {
			session := obj.(*sparkv1alpha1.SparkInteractiveSession)
			return []string{session.Spec.Pool}
		},
	); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&sparkv1alpha1.SparkSessionPool{}).
		Named("sparksessionpool").
		Complete(r)
}

func boolPtr(b bool) *bool {
	return &b
}

// mapsContainAll returns true if all key-value pairs in wanted exist in actual.
func mapsContainAll(actual, wanted map[string]string) bool {
	for k, v := range wanted {
		if actual[k] != v {
			return false
		}
	}
	return true
}
