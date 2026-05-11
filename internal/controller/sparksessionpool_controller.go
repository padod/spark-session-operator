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
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/client-go/util/retry"
	metricsv "k8s.io/metrics/pkg/client/clientset/versioned"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	sparkv1alpha1 "github.com/padod/spark-session-operator/api/v1alpha1"
)

const (
	poolFinalizer = "sparkinteractive.io/pool-finalizer"

	// conditionTypeDegraded marks the pool as not fully reconciled. Reset to
	// False when a clean reconcile pass completes, set to True with an
	// aggregated reason whenever any phase reports an error.
	conditionTypeDegraded = "Degraded"
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

	pool := &sparkv1alpha1.SparkSessionPool{}
	if err := r.Get(ctx, req.NamespacedName, pool); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if !pool.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, log, pool)
	}

	if !controllerutil.ContainsFinalizer(pool, poolFinalizer) {
		patch := client.MergeFrom(pool.DeepCopy())
		controllerutil.AddFinalizer(pool, poolFinalizer)
		if err := r.Patch(ctx, pool, patch); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Collect errors across phases so partial failures still produce a
	// status update with an accurate Degraded condition instead of leaving
	// the observable state stuck on the previous reconcile's snapshot.
	var errs []error

	if err := r.reconcileIngress(ctx, log, pool); err != nil {
		log.Error(err, "Failed to reconcile Ingress")
		errs = append(errs, fmt.Errorf("ingress: %w", err))
	}

	existingApps, listErr := r.listPoolInstances(ctx, pool)
	if listErr != nil {
		log.Error(listErr, "Failed to list pool instances")
		errs = append(errs, fmt.Errorf("list instances: %w", listErr))
	}

	sessionCounts, pendingSessions, sessErr := r.countSessionsPerInstance(ctx, pool)
	if sessErr != nil {
		log.Error(sessErr, "Failed to count sessions")
		errs = append(errs, fmt.Errorf("count sessions: %w", sessErr))
	}

	var instances []sparkv1alpha1.PoolInstanceStatus
	var counts poolCounts
	// Skip scaling/replacement when we couldn't enumerate instances —
	// acting on an empty list would let us mass-create duplicates.
	if listErr == nil {
		instances = r.buildInstanceStatuses(ctx, pool.Namespace, existingApps, sessionCounts)
		counts = computePoolCounts(instances)

		if err := r.reconcileScaling(ctx, log, pool, instances, counts, pendingSessions, existingApps); err != nil {
			errs = append(errs, fmt.Errorf("scaling: %w", err))
		}
		if err := r.reconcileFailedInstances(ctx, log, pool, instances, existingApps); err != nil {
			errs = append(errs, fmt.Errorf("failed-instance replacement: %w", err))
		}
	}

	pool.Status.Instances = instances
	pool.Status.TotalActiveSessions = counts.totalSessions
	pool.Status.CurrentReplicas = counts.running
	pool.Status.ReadyReplicas = counts.ready
	setDegradedCondition(&pool.Status.Conditions, pool.Generation, errs)

	if err := r.updatePoolStatus(ctx, pool, instances, counts, errs); err != nil {
		log.Error(err, "Failed to update pool status")
		errs = append(errs, fmt.Errorf("status update: %w", err))
	}

	if len(errs) > 0 {
		return ctrl.Result{}, utilerrors.NewAggregate(errs)
	}
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

// updatePoolStatus writes the reconcile result back to status, retrying with
// a fresh read on conflict. Two reconcilers running in parallel (e.g. during
// a leader-election handoff window or — as in the test harness — a manual
// reconcile call layered on top of the manager's worker) can race each other,
// so we re-fetch and reapply the same logical update rather than surface a
// transient 409 as a real reconcile failure.
func (r *SparkSessionPoolReconciler) updatePoolStatus(
	ctx context.Context,
	pool *sparkv1alpha1.SparkSessionPool,
	instances []sparkv1alpha1.PoolInstanceStatus,
	counts poolCounts,
	errs []error,
) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &sparkv1alpha1.SparkSessionPool{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(pool), fresh); err != nil {
			return err
		}
		fresh.Status.Instances = instances
		fresh.Status.TotalActiveSessions = counts.totalSessions
		fresh.Status.CurrentReplicas = counts.running
		fresh.Status.ReadyReplicas = counts.ready
		// LastScaleTime is set on scale-down by scaleDown(); preserve any
		// concurrent writer's value while reapplying our other fields.
		if pool.Status.LastScaleTime != nil {
			fresh.Status.LastScaleTime = pool.Status.LastScaleTime
		}
		setDegradedCondition(&fresh.Status.Conditions, fresh.Generation, errs)
		return r.Status().Update(ctx, fresh)
	})
}

// setDegradedCondition writes a Degraded condition reflecting whether this
// reconcile pass collected any errors. ObservedGeneration tracks the spec
// generation the status corresponds to so consumers can tell stale data
// from current.
func setDegradedCondition(conditions *[]metav1.Condition, generation int64, errs []error) {
	cond := metav1.Condition{
		Type:               conditionTypeDegraded,
		ObservedGeneration: generation,
	}
	if len(errs) == 0 {
		cond.Status = metav1.ConditionFalse
		cond.Reason = "Healthy"
		cond.Message = "All reconciliation steps succeeded"
	} else {
		cond.Status = metav1.ConditionTrue
		cond.Reason = "ReconcileError"
		cond.Message = utilerrors.NewAggregate(errs).Error()
	}
	apimeta.SetStatusCondition(conditions, cond)
}

// poolCounts aggregates instance-state tallies derived from a slice of
// PoolInstanceStatus so scaling and status updates can share the same view.
type poolCounts struct {
	running, ready, pending, totalSessions int32
}

func computePoolCounts(instances []sparkv1alpha1.PoolInstanceStatus) poolCounts {
	var c poolCounts
	for _, inst := range instances {
		switch inst.State {
		case "Running":
			c.running++
			c.ready++
		case "Draining":
			c.running++
		case "Pending", "Submitted", "":
			c.pending++
		}
		c.totalSessions += inst.ActiveSessions
	}
	return c
}

// reconcileScaling brings the pool toward its desired replica count, creating
// new instances on scale-up and (respecting cooldown) removing them on scale-down.
func (r *SparkSessionPoolReconciler) reconcileScaling(
	ctx context.Context,
	log logr.Logger,
	pool *sparkv1alpha1.SparkSessionPool,
	instances []sparkv1alpha1.PoolInstanceStatus,
	counts poolCounts,
	pendingSessions int32,
	existingApps []unstructured.Unstructured,
) error {
	desired := r.calculateDesiredReplicas(ctx, pool, counts.ready, counts.totalSessions, pendingSessions, instances)

	// Count pending instances toward the total so we don't create duplicates
	// while previously-submitted Spark apps are still starting.
	currentTotal := counts.running + counts.pending
	if currentTotal < desired {
		toCreate := desired - currentTotal
		log.Info("Scaling up", "current", counts.running, "pending", counts.pending, "desired", desired, "creating", toCreate, "pendingSessions", pendingSessions)
		for i := int32(0); i < toCreate; i++ {
			if err := r.createPoolInstance(ctx, log, pool, existingApps); err != nil {
				log.Error(err, "Failed to create pool instance")
				return err
			}
		}
	}

	if counts.running > desired && r.canScaleDown(pool) {
		toRemove := counts.running - desired
		log.Info("Scaling down", "current", counts.running, "desired", desired, "removing", toRemove)
		if err := r.scaleDown(ctx, log, pool, instances, toRemove); err != nil {
			log.Error(err, "Failed to scale down")
			return err
		}
	}

	return nil
}

// reconcileFailedInstances replaces any instance in the Failed state with a
// fresh one. Each failed instance is processed independently and errors are
// aggregated so one stuck replacement doesn't starve the others. The
// replacement is skipped when its delete fails — otherwise the operator could
// race itself and oversubscribe the pool.
func (r *SparkSessionPoolReconciler) reconcileFailedInstances(
	ctx context.Context,
	log logr.Logger,
	pool *sparkv1alpha1.SparkSessionPool,
	instances []sparkv1alpha1.PoolInstanceStatus,
	existingApps []unstructured.Unstructured,
) error {
	var errs []error
	for _, inst := range instances {
		if inst.State != "Failed" {
			continue
		}
		log.Info("Replacing failed instance", "instance", inst.Name)
		if err := r.deleteSparkApplication(ctx, pool.Namespace, inst.Name); err != nil {
			log.Error(err, "Failed to delete failed instance", "instance", inst.Name)
			errs = append(errs, fmt.Errorf("delete %s: %w", inst.Name, err))
			continue
		}
		if err := r.createPoolInstance(ctx, log, pool, existingApps); err != nil {
			log.Error(err, "Failed to create replacement instance", "for", inst.Name)
			errs = append(errs, fmt.Errorf("create replacement for %s: %w", inst.Name, err))
		}
	}
	return utilerrors.NewAggregate(errs)
}

func (r *SparkSessionPoolReconciler) calculateDesiredReplicas(
	ctx context.Context,
	pool *sparkv1alpha1.SparkSessionPool,
	currentReady int32,
	totalSessions int32,
	pendingSessions int32,
	instances []sparkv1alpha1.PoolInstanceStatus,
) int32 {
	metricsType := pool.Spec.Scaling.Metrics.Type
	if metricsType == "" {
		metricsType = "activeSessions"
	}

	// Include pending (unassigned) sessions in the total so we scale from zero.
	effectiveSessions := totalSessions + pendingSessions

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
		} else if currentReady > 0 {
			// HPA formula: desired = ceil(currentReplicas * currentUtilization / targetUtilization)
			desired = int32(math.Ceil(float64(currentReady) * avgUtil / target))
		}

	default: // "activeSessions"
		target := pool.Spec.Scaling.Metrics.TargetPerInstance
		if target <= 0 {
			target = 20
		}

		if effectiveSessions > 0 {
			desired = (effectiveSessions + target - 1) / target // ceiling division
		}

		scaleUpThreshold, _ := strconv.ParseFloat(pool.Spec.Scaling.ScaleUpThreshold, 64)
		if scaleUpThreshold == 0 {
			scaleUpThreshold = 0.8
		}

		// If current load exceeds threshold, add headroom.
		if currentReady > 0 {
			loadPerInstance := float64(effectiveSessions) / float64(currentReady)
			if loadPerInstance > float64(target)*scaleUpThreshold {
				desired = currentReady + 1
			}
		}
	}

	// Scale-from-zero floor: pending sessions need at least one instance even
	// when no signal otherwise suggests scaling up.
	if pendingSessions > 0 && desired == 0 {
		desired = 1
	}

	return clampReplicas(desired, pool.Spec.Replicas.Min, pool.Spec.Replicas.Max)
}

func clampReplicas(desired, min, max int32) int32 {
	if desired < min {
		return min
	}
	if desired > max {
		return max
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
		// Mark dependent sessions Failed BEFORE deleting the SparkApplication
		// so clients see a specific InstanceTerminated reason instead of a
		// dangling "Active" session that can never serve traffic. Failure
		// here is logged but non-fatal — the Watch on SparkApplication from
		// the session controller is the defense-in-depth backstop.
		if err := r.markSessionsFailedForInstance(ctx, log, pool.Namespace, inst.Name, "scale-down"); err != nil {
			log.Error(err, "Failed to mark sessions on removed instance", "instance", inst.Name)
		}
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
		Controller: ptr.To(true),
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
) (map[string]int32, int32, error) {
	sessionList := &sparkv1alpha1.SparkInteractiveSessionList{}
	if err := r.List(ctx, sessionList,
		client.InNamespace(pool.Namespace),
		client.MatchingFields{"spec.pool": pool.Name},
	); err != nil {
		return nil, 0, err
	}

	counts := make(map[string]int32)
	var pendingSessions int32
	for _, session := range sessionList.Items {
		switch session.Status.State {
		case "Active", "Idle":
			counts[session.Status.AssignedInstance]++
		case "Pending", "":
			// Sessions not yet assigned to an instance (scale-from-zero trigger)
			if session.Status.AssignedInstance == "" {
				pendingSessions++
			}
		}
	}
	return counts, pendingSessions, nil
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

		// Cascade to sessions: mark them Failed with PoolDeleted so clients
		// see a specific reason instead of timing out against a dead pool.
		// Logged but non-fatal — stuck sessions will eventually hit idle
		// timeout; we'd rather complete pool teardown than block on it.
		if err := r.cascadeFailSessionsForPool(ctx, log, pool); err != nil {
			log.Error(err, "Failed to cascade session cleanup")
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

// cascadeFailSessionsForPool walks every session in the pool's namespace
// whose spec.pool matches and transitions it to Failed with a PoolDeleted
// condition. Uses the spec.pool field index registered in SetupWithManager.
func (r *SparkSessionPoolReconciler) cascadeFailSessionsForPool(
	ctx context.Context,
	log logr.Logger,
	pool *sparkv1alpha1.SparkSessionPool,
) error {
	list := &sparkv1alpha1.SparkInteractiveSessionList{}
	if err := r.List(ctx, list,
		client.InNamespace(pool.Namespace),
		client.MatchingFields{"spec.pool": pool.Name},
	); err != nil {
		return fmt.Errorf("list sessions in pool %s: %w", pool.Name, err)
	}
	var errs []error
	for i := range list.Items {
		s := &list.Items[i]
		switch s.Status.State {
		case "Failed", "Terminated", "Terminating":
			continue
		}
		log.Info("Marking session Failed because pool is being deleted", "session", s.Name, "pool", pool.Name)
		s.Status.State = "Failed"
		apimeta.SetStatusCondition(&s.Status.Conditions, metav1.Condition{
			Type:    sparkv1alpha1.ConditionPoolDeleted,
			Status:  metav1.ConditionTrue,
			Reason:  "PoolDeleted",
			Message: fmt.Sprintf("Pool %s was deleted; reconnect to a different pool to start a new session", pool.Name),
		})
		if err := r.Status().Update(ctx, s); err != nil {
			errs = append(errs, fmt.Errorf("update session %s: %w", s.Name, err))
		}
	}
	return utilerrors.NewAggregate(errs)
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
		extraAnnotations["nginx.ingress.kubernetes.io/proxy-read-timeout"] = "3600"
		extraAnnotations["nginx.ingress.kubernetes.io/proxy-send-timeout"] = "3600"
		// Spark Connect error responses carry the full analyzed plan in the
		// grpc-message header / grpc-status-details-bin trailer. The default
		// nginx proxy_buffer_size (4–8k) is too small for those, and nginx
		// synthesizes a 502 instead of forwarding the gRPC error, which the
		// client sees as UNAVAILABLE instead of AnalysisException.
		extraAnnotations["nginx.ingress.kubernetes.io/proxy-buffer-size"] = "32k"
		extraAnnotations["nginx.ingress.kubernetes.io/proxy-buffers-number"] = "8"
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
			IngressClassName: ptr.To("nginx"),
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
	key := client.ObjectKey{Namespace: ingressNamespace, Name: ingressName}
	err := r.Get(ctx, key, existing)
	if errors.IsNotFound(err) {
		log.Info("Creating Ingress for pool", "ingress", ingressName, "namespace", ingressNamespace, "host", pool.Spec.Host)
		if createErr := r.Create(ctx, desired); createErr != nil {
			// A concurrent reconciler (another manager worker, a leader-
			// election handoff window, or the test harness running its own
			// reconcile alongside the manager) may have just created the
			// Ingress between our Get and our Create. Re-fetch and fall
			// through to the update path so we still converge on the
			// desired spec instead of bailing with a 409.
			if !errors.IsAlreadyExists(createErr) {
				return fmt.Errorf("create ingress %s: %w", ingressName, createErr)
			}
			if err := r.Get(ctx, key, existing); err != nil {
				return fmt.Errorf("re-fetch ingress %s after AlreadyExists: %w", ingressName, err)
			}
		} else {
			return nil
		}
	} else if err != nil {
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

// markSessionsFailedForInstance finds every session currently assigned to
// instanceName and transitions it to Failed with an InstanceTerminated
// condition explaining why. Called from scaleDown (proactive) and shared
// with the SparkApplication-delete watch path (defensive). Errors are
// aggregated so one stuck update doesn't starve the rest of the cleanup.
func (r *SparkSessionPoolReconciler) markSessionsFailedForInstance(
	ctx context.Context,
	log logr.Logger,
	namespace, instanceName, cause string,
) error {
	list := &sparkv1alpha1.SparkInteractiveSessionList{}
	if err := r.List(ctx, list, client.InNamespace(namespace)); err != nil {
		return fmt.Errorf("list sessions for instance %s: %w", instanceName, err)
	}
	var errs []error
	for i := range list.Items {
		s := &list.Items[i]
		if s.Status.AssignedInstance != instanceName {
			continue
		}
		switch s.Status.State {
		case "Failed", "Terminated", "Terminating":
			continue
		}
		log.Info("Marking session Failed because assigned instance is gone",
			"session", s.Name, "instance", instanceName, "cause", cause)
		s.Status.State = "Failed"
		apimeta.SetStatusCondition(&s.Status.Conditions, metav1.Condition{
			Type:    sparkv1alpha1.ConditionInstanceTerminated,
			Status:  metav1.ConditionTrue,
			Reason:  "InstanceTerminated",
			Message: fmt.Sprintf("Pool instance %s was removed (%s); the user must reconnect — session state on the previous driver is lost", instanceName, cause),
		})
		if err := r.Status().Update(ctx, s); err != nil {
			errs = append(errs, fmt.Errorf("update session %s: %w", s.Name, err))
		}
	}
	return utilerrors.NewAggregate(errs)
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
