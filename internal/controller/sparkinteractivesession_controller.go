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
	"fmt"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	sparkv1alpha1 "github.com/tander/spark-session-operator/api/v1alpha1"
)

const (
	sessionFinalizer = "sparkinteractive.io/session-finalizer"
)

// SparkInteractiveSessionReconciler reconciles a SparkInteractiveSession object
type SparkInteractiveSessionReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Log    logr.Logger
}

// +kubebuilder:rbac:groups=sparkinteractive.io,resources=sparkinteractivesessions,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=sparkinteractive.io,resources=sparkinteractivesessions/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=sparkinteractive.io,resources=sparkinteractivesessions/finalizers,verbs=update

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
	// Validate quota
	if err := r.validateQuota(ctx, session); err != nil {
		log.Info("Quota exceeded", "user", session.Spec.User, "error", err)
		session.Status.State = "Failed"
		session.Status.Conditions = append(session.Status.Conditions, metav1.Condition{
			Type:               "QuotaExceeded",
			Status:             metav1.ConditionTrue,
			Reason:             "QuotaExceeded",
			Message:            err.Error(),
			LastTransitionTime: metav1.Now(),
		})
		return ctrl.Result{}, r.Status().Update(ctx, session)
	}

	// Find the best pool instance to assign this session to
	instance, endpoint, err := r.assignToInstance(ctx, log, session)
	if err != nil {
		log.Error(err, "Failed to assign session to instance")
		// Requeue — maybe instances are still starting
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	// Update status
	now := metav1.Now()
	session.Status.State = "Active"
	session.Status.AssignedInstance = instance
	session.Status.Endpoint = endpoint
	session.Status.CreatedAt = &now
	session.Status.LastActivityAt = &now

	log.Info("Session assigned", "user", session.Spec.User, "instance", instance, "endpoint", endpoint)
	return ctrl.Result{RequeueAfter: 5 * time.Minute}, r.Status().Update(ctx, session)
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
		log.Info("Assigned instance is no longer running, reassigning",
			"user", session.Spec.User,
			"oldInstance", session.Status.AssignedInstance)
		session.Status.State = "Pending"
		session.Status.AssignedInstance = ""
		session.Status.Endpoint = ""
		return ctrl.Result{Requeue: true}, r.Status().Update(ctx, session)
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

	// Count user's active sessions
	sessionList := &sparkv1alpha1.SparkInteractiveSessionList{}
	if err := r.List(ctx, sessionList,
		client.InNamespace(session.Namespace),
		client.MatchingFields{"spec.pool": session.Spec.Pool},
	); err != nil {
		return err
	}

	userSessionCount := int32(0)
	totalSessionCount := int32(0)
	for _, s := range sessionList.Items {
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

// SetupWithManager sets up the controller with the Manager.
func (r *SparkInteractiveSessionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&sparkv1alpha1.SparkInteractiveSession{}).
		Named("sparkinteractivesession").
		Complete(r)
}
