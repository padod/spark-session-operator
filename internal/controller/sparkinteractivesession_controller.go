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
	"fmt"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	sparkv1alpha1 "github.com/padod/spark-session-operator/api/v1alpha1"
)

const (
	sessionFinalizer = "sparkinteractive.io/session-finalizer"

	// pendingInstanceProbeThreshold is how long a session is allowed to sit
	// in Pending without an assignable instance before we probe the
	// SparkApplications in the pool and surface their error message via
	// the InstanceReady condition. 30 s is short enough to beat the proxy's
	// 60 s waitForSessionActive timeout, long enough that healthy scale-up
	// (which can take 10-20 s for image pull + JVM start) doesn't trip it.
	pendingInstanceProbeThreshold = 30 * time.Second
)

var sparkApplicationGVK = schema.GroupVersionKind{
	Group:   "sparkoperator.k8s.io",
	Version: "v1beta2",
	Kind:    "SparkApplication",
}

// SparkInteractiveSessionReconciler reconciles a SparkInteractiveSession object
type SparkInteractiveSessionReconciler struct {
	client.Client
	// APIReader is an uncached client used for the quota-admission List so
	// the count reflects the absolute latest apiserver state instead of the
	// informer cache (which lags by a few hundred milliseconds and is the
	// most common over-admission race). Wired from mgr.GetAPIReader().
	APIReader client.Reader
	Scheme    *runtime.Scheme
	Log       logr.Logger
}

// +kubebuilder:rbac:groups=sparkinteractive.io,resources=sparkinteractivesessions,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=sparkinteractive.io,resources=sparkinteractivesessions/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=sparkinteractive.io,resources=sparkinteractivesessions/finalizers,verbs=update
// +kubebuilder:rbac:groups=sparkoperator.k8s.io,resources=sparkapplications,verbs=get;list;watch

func (r *SparkInteractiveSessionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithValues("session", req.NamespacedName)

	session := &sparkv1alpha1.SparkInteractiveSession{}
	if err := r.Get(ctx, req.NamespacedName, session); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Handle deletion
	if !session.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, log, session)
	}

	// Ensure finalizer
	if !controllerutil.ContainsFinalizer(session, sessionFinalizer) {
		controllerutil.AddFinalizer(session, sessionFinalizer)
		if err := r.Update(ctx, session); err != nil {
			return ctrl.Result{}, err
		}
	}

	switch session.Status.State {
	case "", "Pending":
		return r.handlePending(ctx, log, session)
	case "Active", "Idle":
		return r.handleActive(ctx, log, session)
	case "Terminating":
		return r.handleTerminating(ctx, log, session)
	case "Terminated", "Failed":
		// Terminal states — nothing to do
		return ctrl.Result{}, nil
	}

	return ctrl.Result{}, nil
}

func (r *SparkInteractiveSessionReconciler) handlePending(
	ctx context.Context,
	log logr.Logger,
	session *sparkv1alpha1.SparkInteractiveSession,
) (ctrl.Result, error) {
	if err := r.validateQuota(ctx, session); err != nil {
		log.Info("Quota exceeded", "user", session.Spec.User, "error", err)
		session.Status.State = "Failed"
		apimeta.SetStatusCondition(&session.Status.Conditions, metav1.Condition{
			Type:    sparkv1alpha1.ConditionQuotaExceeded,
			Status:  metav1.ConditionTrue,
			Reason:  "QuotaExceeded",
			Message: err.Error(),
		})
		return ctrl.Result{}, r.Status().Update(ctx, session)
	}

	instance, endpoint, err := r.assignToInstance(ctx, log, session)
	if err != nil {
		log.Info("Cannot assign session to instance yet", "user", session.Spec.User, "error", err.Error())
		// If we've been waiting past the threshold, look for an underlying
		// SparkApplication error and surface it so the proxy can return
		// something more specific than "session failed to start" when the
		// 60 s waitForSessionActive timeout fires.
		if time.Since(session.CreationTimestamp.Time) > pendingInstanceProbeThreshold {
			r.surfaceInstanceReadyError(ctx, log, session, err)
			if updateErr := r.Status().Update(ctx, session); updateErr != nil {
				log.Error(updateErr, "Failed to update session InstanceReady condition")
			}
		}
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	// Commit Pending→Active with retry-on-conflict. A 409 here normally
	// means another reconcile already advanced the session (e.g. the
	// SparkApplication-delete watch interleaved with our admission), so we
	// re-Get + reapply our intent instead of dropping the work and waiting
	// for the next requeue.
	updateErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &sparkv1alpha1.SparkInteractiveSession{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(session), fresh); err != nil {
			return err
		}
		if fresh.Status.State != "" && fresh.Status.State != "Pending" {
			// Another reconcile won the race and already advanced state;
			// don't clobber it.
			return nil
		}
		apimeta.RemoveStatusCondition(&fresh.Status.Conditions, sparkv1alpha1.ConditionInstanceReady)
		now := metav1.Now()
		fresh.Status.State = "Active"
		fresh.Status.AssignedInstance = instance
		fresh.Status.Endpoint = endpoint
		fresh.Status.CreatedAt = &now
		fresh.Status.LastActivityAt = &now
		return r.Status().Update(ctx, fresh)
	})

	log.Info("Session assigned", "user", session.Spec.User, "instance", instance, "endpoint", endpoint)
	return ctrl.Result{RequeueAfter: 5 * time.Minute}, updateErr
}

// surfaceInstanceReadyError inspects SparkApplications in the pool and writes
// an InstanceReady=False condition onto the session with the most
// diagnostically-useful underlying reason. A FAILED/FAILING SparkApplication
// takes priority over SUBMITTED/PENDING because the failure message is more
// actionable; otherwise we report "no capacity" so clients see something
// other than a generic timeout.
func (r *SparkInteractiveSessionReconciler) surfaceInstanceReadyError(
	ctx context.Context,
	log logr.Logger,
	session *sparkv1alpha1.SparkInteractiveSession,
	assignErr error,
) {
	apps := &unstructured.UnstructuredList{}
	apps.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   sparkApplicationGVK.Group,
		Version: sparkApplicationGVK.Version,
		Kind:    sparkApplicationGVK.Kind + "List",
	})
	if err := r.List(ctx, apps,
		client.InNamespace(session.Namespace),
		client.MatchingLabels{"sparkinteractive.io/pool": session.Spec.Pool},
	); err != nil {
		log.V(1).Info("Could not list SparkApplications for diagnostics", "error", err.Error())
		// Fall back to a generic message so the client at least sees the
		// assignment error instead of nothing.
		apimeta.SetStatusCondition(&session.Status.Conditions, metav1.Condition{
			Type:    sparkv1alpha1.ConditionInstanceReady,
			Status:  metav1.ConditionFalse,
			Reason:  "NoInstancesAvailable",
			Message: assignErr.Error(),
		})
		return
	}

	var failedMsg, pendingMsg string
	for _, app := range apps.Items {
		state, _, _ := unstructured.NestedString(app.Object, "status", "applicationState", "state")
		errMsg, _, _ := unstructured.NestedString(app.Object, "status", "applicationState", "errorMessage")
		switch state {
		case "FAILED", "FAILING":
			failedMsg = fmt.Sprintf("SparkApplication %s in state %s", app.GetName(), state)
			if errMsg != "" {
				failedMsg += ": " + errMsg
			}
		case "SUBMITTED", "PENDING_RERUN", "":
			pendingMsg = fmt.Sprintf("SparkApplication %s stuck in state %q for >%s — likely scheduler/image-pull/capacity issue",
				app.GetName(), state, pendingInstanceProbeThreshold)
		}
	}

	cond := metav1.Condition{
		Type:   sparkv1alpha1.ConditionInstanceReady,
		Status: metav1.ConditionFalse,
	}
	switch {
	case failedMsg != "":
		cond.Reason = "InstanceFailed"
		cond.Message = failedMsg
	case pendingMsg != "":
		cond.Reason = "InstancePending"
		cond.Message = pendingMsg
	default:
		cond.Reason = "NoInstancesAvailable"
		cond.Message = assignErr.Error()
	}
	apimeta.SetStatusCondition(&session.Status.Conditions, cond)
}

func (r *SparkInteractiveSessionReconciler) handleActive(
	ctx context.Context,
	log logr.Logger,
	session *sparkv1alpha1.SparkInteractiveSession,
) (ctrl.Result, error) {
	// Check if assigned instance is still running
	pool := &sparkv1alpha1.SparkSessionPool{}
	if err := r.Get(ctx, client.ObjectKey{
		Namespace: session.Namespace,
		Name:      session.Spec.Pool,
	}, pool); err != nil {
		if errors.IsNotFound(err) {
			// Pool was deleted — terminate session
			session.Status.State = "Terminated"
			return ctrl.Result{}, r.Status().Update(ctx, session)
		}
		return ctrl.Result{}, err
	}

	// Check if instance is still alive
	instanceAlive := false
	for _, inst := range pool.Status.Instances {
		if inst.Name == session.Status.AssignedInstance && inst.State == "Running" {
			instanceAlive = true
			break
		}
	}

	if !instanceAlive {
		// Don't try to "reassign" — session state (temp views, variables,
		// uploaded UDFs) lives on the previous driver and is unrecoverable.
		// Mark Failed with a specific reason so the client knows to start a
		// fresh connection instead of believing they kept their session.
		log.Info("Assigned instance is no longer running; marking session Failed",
			"user", session.Spec.User,
			"oldInstance", session.Status.AssignedInstance)
		session.Status.State = "Failed"
		apimeta.SetStatusCondition(&session.Status.Conditions, metav1.Condition{
			Type:    sparkv1alpha1.ConditionInstanceTerminated,
			Status:  metav1.ConditionTrue,
			Reason:  "InstanceTerminated",
			Message: fmt.Sprintf("Pool instance %s is no longer running; reconnect to start a new session", session.Status.AssignedInstance),
		})
		return ctrl.Result{}, r.Status().Update(ctx, session)
	}

	// Check idle timeout
	idleTimeout := time.Duration(pool.Spec.SessionPolicy.IdleTimeoutMinutes) * time.Minute
	if idleTimeout == 0 {
		idleTimeout = 12 * time.Hour // default
	}

	if session.Status.LastActivityAt != nil {
		idleDuration := time.Since(session.Status.LastActivityAt.Time)
		if idleDuration > idleTimeout {
			log.Info("Session idle timeout exceeded",
				"user", session.Spec.User,
				"idle", idleDuration.String(),
				"timeout", idleTimeout.String())
			session.Status.State = "Terminating"
			return ctrl.Result{Requeue: true}, r.Status().Update(ctx, session)
		}

		// Mark as idle if inactive for more than 10 minutes
		if idleDuration > 10*time.Minute && session.Status.State == "Active" {
			session.Status.State = "Idle"
			if err := r.Status().Update(ctx, session); err != nil {
				return ctrl.Result{}, err
			}
		}
	}

	// Requeue to check idle timeout periodically
	return ctrl.Result{RequeueAfter: 5 * time.Minute}, nil
}

func (r *SparkInteractiveSessionReconciler) handleTerminating(
	ctx context.Context,
	log logr.Logger,
	session *sparkv1alpha1.SparkInteractiveSession,
) (ctrl.Result, error) {
	// TODO: actively close the Spark session on the server via JDBC/gRPC
	// For now, just mark as terminated
	log.Info("Terminating session", "user", session.Spec.User, "instance", session.Status.AssignedInstance)
	session.Status.State = "Terminated"
	return ctrl.Result{}, r.Status().Update(ctx, session)
}

func (r *SparkInteractiveSessionReconciler) handleDeletion(
	ctx context.Context,
	log logr.Logger,
	session *sparkv1alpha1.SparkInteractiveSession,
) (ctrl.Result, error) {
	if controllerutil.ContainsFinalizer(session, sessionFinalizer) {
		// Cleanup: close session on the backend if needed
		log.Info("Cleaning up session", "user", session.Spec.User)

		controllerutil.RemoveFinalizer(session, sessionFinalizer)
		if err := r.Update(ctx, session); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, nil
}

func (r *SparkInteractiveSessionReconciler) validateQuota(
	ctx context.Context,
	session *sparkv1alpha1.SparkInteractiveSession,
) error {
	// Get the pool
	pool := &sparkv1alpha1.SparkSessionPool{}
	if err := r.Get(ctx, client.ObjectKey{
		Namespace: session.Namespace,
		Name:      session.Spec.Pool,
	}, pool); err != nil {
		return fmt.Errorf("pool %s not found: %w", session.Spec.Pool, err)
	}

	// Use the uncached APIReader so admission sees the absolute latest count
	// instead of a stale informer view — this is the most common
	// over-admission race. APIReader doesn't support MatchingFields, so we
	// filter in memory; the working set is bounded by pool size and the
	// extra cost is negligible compared to the apiserver round-trip.
	sessionList := &sparkv1alpha1.SparkInteractiveSessionList{}
	if err := r.APIReader.List(ctx, sessionList, client.InNamespace(session.Namespace)); err != nil {
		return err
	}

	userSessionCount := int32(0)
	totalSessionCount := int32(0)
	for _, s := range sessionList.Items {
		if s.Spec.Pool != session.Spec.Pool {
			continue
		}
		if s.Name == session.Name {
			continue // don't count self
		}
		if s.Status.State == "Active" || s.Status.State == "Idle" || s.Status.State == "Pending" {
			totalSessionCount++
			if s.Spec.User == session.Spec.User {
				userSessionCount++
			}
		}
	}

	// Check per-user quota (apply overrides if any)
	maxPerUser := pool.Spec.SessionPolicy.MaxSessionsPerUser
	for _, quota := range pool.Spec.SessionPolicy.Quotas {
		for _, u := range quota.Match.Users {
			if u == session.Spec.User && quota.MaxSessionsPerUser > 0 {
				maxPerUser = quota.MaxSessionsPerUser
			}
		}
	}

	if maxPerUser > 0 && userSessionCount >= maxPerUser {
		return fmt.Errorf("user %s has reached max sessions (%d/%d)", session.Spec.User, userSessionCount, maxPerUser)
	}

	// Check total quota
	if pool.Spec.SessionPolicy.MaxTotalSessions > 0 && totalSessionCount >= pool.Spec.SessionPolicy.MaxTotalSessions {
		return fmt.Errorf("pool %s has reached max total sessions (%d/%d)", pool.Name, totalSessionCount, pool.Spec.SessionPolicy.MaxTotalSessions)
	}

	return nil
}

func (r *SparkInteractiveSessionReconciler) assignToInstance(
	ctx context.Context,
	log logr.Logger,
	session *sparkv1alpha1.SparkInteractiveSession,
) (string, string, error) {
	// Get pool status
	pool := &sparkv1alpha1.SparkSessionPool{}
	if err := r.Get(ctx, client.ObjectKey{
		Namespace: session.Namespace,
		Name:      session.Spec.Pool,
	}, pool); err != nil {
		return "", "", err
	}

	// Find instance with least sessions that is Running (not Draining)
	var bestInstance *sparkv1alpha1.PoolInstanceStatus
	var bestSessions int32 = int32(^uint32(0) >> 1) // max int32

	for i := range pool.Status.Instances {
		inst := &pool.Status.Instances[i]
		if inst.State != "Running" {
			continue
		}
		if inst.ActiveSessions < bestSessions {
			bestSessions = inst.ActiveSessions
			bestInstance = inst
		}
	}

	if bestInstance == nil {
		return "", "", fmt.Errorf("no running instances available in pool %s", pool.Name)
	}

	// Build endpoint with the appropriate port
	port := ""
	switch pool.Spec.Type {
	case "thrift":
		port = "10001" // HiveServer2 HTTP transport port
	case "connect":
		port = "8424" // TODO: make configurable from pool spec
	}

	endpoint := fmt.Sprintf("%s:%s", bestInstance.Endpoint, port)

	return bestInstance.Name, endpoint, nil
}

// SetupWithManager sets up the controller with the Manager. Adds a Watch on
// SparkApplication so the controller is notified within seconds when an
// instance disappears (admin delete, node loss, pool scale-down race) and
// can move dependent sessions to Failed instead of leaving them stuck Active
// against a phantom backend.
func (r *SparkInteractiveSessionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	sparkApp := &unstructured.Unstructured{}
	sparkApp.SetGroupVersionKind(sparkApplicationGVK)

	return ctrl.NewControllerManagedBy(mgr).
		For(&sparkv1alpha1.SparkInteractiveSession{}).
		WatchesRawSource(source.Kind(
			mgr.GetCache(),
			client.Object(sparkApp),
			handler.EnqueueRequestsFromMapFunc(r.sessionsForSparkApplication),
		)).
		Named("sparkinteractivesession").
		Complete(r)
}

// sessionsForSparkApplication returns reconcile requests for every session
// whose AssignedInstance matches the SparkApplication that just changed.
// Used by the SparkApplication watch so a delete (or move to FAILED/COMPLETED)
// promptly wakes the affected sessions.
func (r *SparkInteractiveSessionReconciler) sessionsForSparkApplication(ctx context.Context, obj client.Object) []reconcile.Request {
	list := &sparkv1alpha1.SparkInteractiveSessionList{}
	if err := r.List(ctx, list, client.InNamespace(obj.GetNamespace())); err != nil {
		r.Log.V(1).Info("Could not list sessions for SparkApplication event", "error", err.Error())
		return nil
	}
	var reqs []reconcile.Request
	for i := range list.Items {
		s := &list.Items[i]
		if s.Status.AssignedInstance != obj.GetName() {
			continue
		}
		reqs = append(reqs, reconcile.Request{
			NamespacedName: client.ObjectKeyFromObject(s),
		})
	}
	return reqs
}
