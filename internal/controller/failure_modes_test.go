/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	sparkv1alpha1 "github.com/padod/spark-session-operator/api/v1alpha1"
)

const testNamespace = "default"

func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := sparkv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	return s
}

// TestMarkSessionsFailedForInstance pins the P0-3 contract: when the pool
// controller scales an instance down, every session pinned to that instance
// must be transitioned to Failed with an InstanceTerminated condition
// BEFORE the SparkApplication is deleted. Without this, the session list
// would keep showing entries that can never serve traffic, and those
// entries would continue to count against the user's quota.
func TestMarkSessionsFailedForInstance(t *testing.T) {
	scheme := newTestScheme(t)

	doomed := &sparkv1alpha1.SparkInteractiveSession{
		ObjectMeta: metav1.ObjectMeta{Name: "sess-doomed", Namespace: testNamespace},
		Spec:       sparkv1alpha1.SparkInteractiveSessionSpec{User: "alice", Pool: "p"},
		Status: sparkv1alpha1.SparkInteractiveSessionStatus{
			State:            "Active",
			AssignedInstance: "instance-going-away",
		},
	}
	otherInstance := &sparkv1alpha1.SparkInteractiveSession{
		ObjectMeta: metav1.ObjectMeta{Name: "sess-other", Namespace: testNamespace},
		Spec:       sparkv1alpha1.SparkInteractiveSessionSpec{User: "bob", Pool: "p"},
		Status: sparkv1alpha1.SparkInteractiveSessionStatus{
			State:            "Active",
			AssignedInstance: "instance-staying",
		},
	}
	alreadyDone := &sparkv1alpha1.SparkInteractiveSession{
		ObjectMeta: metav1.ObjectMeta{Name: "sess-done", Namespace: testNamespace},
		Spec:       sparkv1alpha1.SparkInteractiveSessionSpec{User: "carol", Pool: "p"},
		Status: sparkv1alpha1.SparkInteractiveSessionStatus{
			State:            "Terminated",
			AssignedInstance: "instance-going-away",
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(doomed, otherInstance, alreadyDone).
		WithStatusSubresource(&sparkv1alpha1.SparkInteractiveSession{}).
		Build()

	r := &SparkSessionPoolReconciler{Client: cli, Scheme: scheme}
	if err := r.markSessionsFailedForInstance(context.Background(), logr.Discard(), testNamespace, "instance-going-away", "scale-down"); err != nil {
		t.Fatalf("markSessionsFailedForInstance: %v", err)
	}

	got := &sparkv1alpha1.SparkInteractiveSession{}
	if err := cli.Get(context.Background(), namespacedName("sess-doomed"), got); err != nil {
		t.Fatalf("get doomed session: %v", err)
	}
	if got.Status.State != "Failed" {
		t.Errorf("doomed session state: got %q want Failed", got.Status.State)
	}
	if c := apimeta.FindStatusCondition(got.Status.Conditions, sparkv1alpha1.ConditionInstanceTerminated); c == nil || c.Status != metav1.ConditionTrue {
		t.Errorf("doomed session: InstanceTerminated condition missing or not True: %+v", got.Status.Conditions)
	} else if !strings.Contains(c.Message, "scale-down") {
		t.Errorf("InstanceTerminated message should mention cause; got %q", c.Message)
	}

	if err := cli.Get(context.Background(), namespacedName("sess-other"), got); err != nil {
		t.Fatalf("get other-instance session: %v", err)
	}
	if got.Status.State != "Active" {
		t.Errorf("other-instance session must not be touched: got state %q", got.Status.State)
	}

	if err := cli.Get(context.Background(), namespacedName("sess-done"), got); err != nil {
		t.Fatalf("get already-done session: %v", err)
	}
	if got.Status.State != "Terminated" {
		t.Errorf("terminal-state session must not be touched: got state %q", got.Status.State)
	}
}

// TestCascadeFailSessionsForPool pins the P1-2 contract: pool deletion
// transitions every non-terminal session in that pool to Failed with the
// PoolDeleted condition. Sessions in other pools and already-terminal
// sessions are left alone.
func TestCascadeFailSessionsForPool(t *testing.T) {
	scheme := newTestScheme(t)

	pool := &sparkv1alpha1.SparkSessionPool{
		ObjectMeta: metav1.ObjectMeta{Name: "doomed-pool", Namespace: testNamespace},
		Spec:       sparkv1alpha1.SparkSessionPoolSpec{Type: "connect", Host: "x"},
	}
	inPool := &sparkv1alpha1.SparkInteractiveSession{
		ObjectMeta: metav1.ObjectMeta{Name: "sess-in-pool", Namespace: testNamespace},
		Spec:       sparkv1alpha1.SparkInteractiveSessionSpec{User: "alice", Pool: "doomed-pool"},
		Status:     sparkv1alpha1.SparkInteractiveSessionStatus{State: "Active"},
	}
	otherPool := &sparkv1alpha1.SparkInteractiveSession{
		ObjectMeta: metav1.ObjectMeta{Name: "sess-other-pool", Namespace: testNamespace},
		Spec:       sparkv1alpha1.SparkInteractiveSessionSpec{User: "bob", Pool: "keeper-pool"},
		Status:     sparkv1alpha1.SparkInteractiveSessionStatus{State: "Active"},
	}
	terminated := &sparkv1alpha1.SparkInteractiveSession{
		ObjectMeta: metav1.ObjectMeta{Name: "sess-already-done", Namespace: testNamespace},
		Spec:       sparkv1alpha1.SparkInteractiveSessionSpec{User: "carol", Pool: "doomed-pool"},
		Status:     sparkv1alpha1.SparkInteractiveSessionStatus{State: "Terminated"},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, inPool, otherPool, terminated).
		WithStatusSubresource(&sparkv1alpha1.SparkInteractiveSession{}).
		WithIndex(&sparkv1alpha1.SparkInteractiveSession{}, "spec.pool", func(obj client.Object) []string {
			s := obj.(*sparkv1alpha1.SparkInteractiveSession)
			return []string{s.Spec.Pool}
		}).
		Build()

	r := &SparkSessionPoolReconciler{Client: cli, Scheme: scheme}
	if err := r.cascadeFailSessionsForPool(context.Background(), logr.Discard(), pool); err != nil {
		t.Fatalf("cascadeFailSessionsForPool: %v", err)
	}

	got := &sparkv1alpha1.SparkInteractiveSession{}
	if err := cli.Get(context.Background(), namespacedName("sess-in-pool"), got); err != nil {
		t.Fatalf("get in-pool session: %v", err)
	}
	if got.Status.State != "Failed" {
		t.Errorf("in-pool session state: got %q want Failed", got.Status.State)
	}
	if c := apimeta.FindStatusCondition(got.Status.Conditions, sparkv1alpha1.ConditionPoolDeleted); c == nil || c.Status != metav1.ConditionTrue {
		t.Errorf("in-pool session: PoolDeleted condition missing or not True")
	}

	if err := cli.Get(context.Background(), namespacedName("sess-other-pool"), got); err != nil {
		t.Fatalf("get other-pool session: %v", err)
	}
	if got.Status.State != "Active" {
		t.Errorf("other-pool session must not be touched: got %q", got.Status.State)
	}

	if err := cli.Get(context.Background(), namespacedName("sess-already-done"), got); err != nil {
		t.Fatalf("get terminated session: %v", err)
	}
	if got.Status.State != "Terminated" {
		t.Errorf("terminated session must not flip to Failed: got %q", got.Status.State)
	}
}

// TestSurfaceInstanceReadyError pins the P0-1 contract: when a session has
// been stuck Pending past pendingInstanceProbeThreshold and the pool's
// SparkApplication is in a non-Running state, the session gets an
// InstanceReady=False condition carrying the underlying SparkApplication
// reason — which the proxy then surfaces to the client instead of timing
// out with a generic "session failed to start".
func TestSurfaceInstanceReadyError(t *testing.T) {
	scheme := newTestScheme(t)

	session := &sparkv1alpha1.SparkInteractiveSession{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "sess-stuck",
			Namespace:         testNamespace,
			CreationTimestamp: metav1.NewTime(time.Now().Add(-2 * time.Minute)),
		},
		Spec: sparkv1alpha1.SparkInteractiveSessionSpec{User: "alice", Pool: "p"},
	}

	failedApp := &unstructured.Unstructured{}
	failedApp.SetGroupVersionKind(sparkApplicationGVK)
	failedApp.SetName("p-failed-instance")
	failedApp.SetNamespace(testNamespace)
	failedApp.SetLabels(map[string]string{"sparkinteractive.io/pool": "p"})
	_ = unstructured.SetNestedField(failedApp.Object, "FAILED", "status", "applicationState", "state")
	_ = unstructured.SetNestedField(failedApp.Object, "ImagePullBackOff: pull access denied for spark/spark:4.0", "status", "applicationState", "errorMessage")

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(session, failedApp).
		WithStatusSubresource(&sparkv1alpha1.SparkInteractiveSession{}).
		Build()

	r := &SparkInteractiveSessionReconciler{Client: cli, APIReader: cli, Scheme: scheme, Log: logr.Discard()}
	r.surfaceInstanceReadyError(context.Background(), logr.Discard(), session, errAssign("no running instances available in pool p"))

	c := apimeta.FindStatusCondition(session.Status.Conditions, sparkv1alpha1.ConditionInstanceReady)
	if c == nil {
		t.Fatal("InstanceReady condition not set")
	}
	if c.Status != metav1.ConditionFalse {
		t.Errorf("InstanceReady status: got %q want False", c.Status)
	}
	if c.Reason != "InstanceFailed" {
		t.Errorf("InstanceReady reason: got %q want InstanceFailed", c.Reason)
	}
	if !strings.Contains(c.Message, "ImagePullBackOff") {
		t.Errorf("InstanceReady message should carry SparkApplication errorMessage; got %q", c.Message)
	}
}

type errAssign string

func (e errAssign) Error() string { return string(e) }

func namespacedName(name string) client.ObjectKey {
	return client.ObjectKey{Namespace: testNamespace, Name: name}
}
