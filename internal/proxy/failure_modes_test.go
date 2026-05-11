/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package proxy

import (
	"context"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	sparkv1alpha1 "github.com/padod/spark-session-operator/api/v1alpha1"
)

func newProxyTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := sparkv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("add api scheme: %v", err)
	}
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	return s
}

// TestDescribeBackendFailure_Evicted pins the P0-2 contract: when the
// driver pod has been evicted (or otherwise terminated with a meaningful
// reason), the proxy surfaces that reason in the returned error string so
// the client sees "driver pod Evicted" instead of an opaque "backend
// connection failed" — analogous to the prior grpc-message truncation fix.
func TestDescribeBackendFailure_Evicted(t *testing.T) {
	scheme := newProxyTestScheme(t)

	session := &sparkv1alpha1.SparkInteractiveSession{
		ObjectMeta: metav1.ObjectMeta{Name: "sess-1", Namespace: "default"},
		Status: sparkv1alpha1.SparkInteractiveSessionStatus{
			State:            "Active",
			AssignedInstance: "instance-x",
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "instance-x-driver", Namespace: "default"},
		Status:     corev1.PodStatus{Reason: "Evicted"},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(session, pod).Build()

	p := &SessionProxy{
		client:    cli,
		log:       logr.Discard(),
		namespace: "default",
		sessions:  newTTLMap(sessionCacheTTL),
		endpoints: newTTLMap(sessionCacheTTL),
	}

	got := p.describeBackendFailure(context.Background(), "sess-1")
	if !strings.Contains(got, "Evicted") {
		t.Fatalf("expected reason to mention Evicted, got %q", got)
	}
	if !strings.Contains(got, "driver pod") {
		t.Fatalf("expected reason to mention driver pod, got %q", got)
	}
}

// TestDescribeBackendFailure_OOMKilled covers the second discriminator we
// rely on when the pod-level Reason is empty: the container's
// LastTerminationState. Common when the driver crashed but the kubelet
// hasn't yet propagated a pod-level reason.
func TestDescribeBackendFailure_OOMKilled(t *testing.T) {
	scheme := newProxyTestScheme(t)

	session := &sparkv1alpha1.SparkInteractiveSession{
		ObjectMeta: metav1.ObjectMeta{Name: "sess-2", Namespace: "default"},
		Status: sparkv1alpha1.SparkInteractiveSessionStatus{
			State:            "Active",
			AssignedInstance: "instance-y",
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "instance-y-driver", Namespace: "default"},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "spark-kubernetes-driver",
				LastTerminationState: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{Reason: "OOMKilled"},
				},
			}},
		},
	}
	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(session, pod).Build()
	p := &SessionProxy{
		client:    cli,
		log:       logr.Discard(),
		namespace: "default",
		sessions:  newTTLMap(sessionCacheTTL),
		endpoints: newTTLMap(sessionCacheTTL),
	}

	got := p.describeBackendFailure(context.Background(), "sess-2")
	if !strings.Contains(got, "OOMKilled") {
		t.Fatalf("expected OOMKilled in reason, got %q", got)
	}
}

// TestDescribeBackendFailure_NoUsefulSignal verifies the helper returns ""
// (not a misleading bogus message) when no actionable pod-status info is
// available. Callers fall back to the generic transport error.
func TestDescribeBackendFailure_NoUsefulSignal(t *testing.T) {
	scheme := newProxyTestScheme(t)

	session := &sparkv1alpha1.SparkInteractiveSession{
		ObjectMeta: metav1.ObjectMeta{Name: "sess-3", Namespace: "default"},
		Status: sparkv1alpha1.SparkInteractiveSessionStatus{
			State:            "Active",
			AssignedInstance: "instance-z",
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "instance-z-driver", Namespace: "default"},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(session, pod).Build()
	p := &SessionProxy{
		client:    cli,
		log:       logr.Discard(),
		namespace: "default",
		sessions:  newTTLMap(sessionCacheTTL),
		endpoints: newTTLMap(sessionCacheTTL),
	}

	if got := p.describeBackendFailure(context.Background(), "sess-3"); got != "" {
		t.Fatalf("expected empty diagnostic when pod healthy, got %q", got)
	}
}

// TestUpdateLastActivity_NotFoundIsTyped pins the contract runKeepalive
// depends on for P1-3: when the session CR has been deleted, the
// updateLastActivity error must be detectable via apierrors.IsNotFound so
// the keepalive loop can exit instead of logging an error every tick.
// Tests the building block end-to-end without depending on the 2-minute
// ticker that drives the loop in production.
func TestUpdateLastActivity_NotFoundIsTyped(t *testing.T) {
	scheme := newProxyTestScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()

	p := &SessionProxy{
		client:    cli,
		log:       logr.Discard(),
		namespace: "default",
		sessions:  newTTLMap(sessionCacheTTL),
		endpoints: newTTLMap(sessionCacheTTL),
	}

	err := p.updateLastActivity(context.Background(), "ghost-session")
	if err == nil {
		t.Fatal("expected error for missing session")
	}
	if !apierrors.IsNotFound(err) {
		t.Fatalf("expected IsNotFound to recognize the wrapped error; got %v", err)
	}
}

// TestWaitForSessionActive_SurfacesInstanceReadyMessage exercises the P0-1
// integration point on the proxy side: when the session controller has
// stamped an InstanceReady=False condition, waitForSessionActive returns
// immediately with that message instead of waiting out its 60s timeout —
// turning the previously-opaque "session failed to start" into a specific
// SparkApplication error class the client can act on.
func TestWaitForSessionActive_SurfacesInstanceReadyMessage(t *testing.T) {
	scheme := newProxyTestScheme(t)
	session := &sparkv1alpha1.SparkInteractiveSession{
		ObjectMeta: metav1.ObjectMeta{Name: "sess-stuck", Namespace: "default"},
		Status: sparkv1alpha1.SparkInteractiveSessionStatus{
			State: "Pending",
			Conditions: []metav1.Condition{{
				Type:               sparkv1alpha1.ConditionInstanceReady,
				Status:             metav1.ConditionFalse,
				Reason:             "InstanceFailed",
				Message:            "SparkApplication p-x in state FAILED: ImagePullBackOff",
				LastTransitionTime: metav1.Now(),
			}},
		},
	}
	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(session).Build()
	p := &SessionProxy{
		client:    cli,
		log:       logr.Discard(),
		namespace: "default",
		sessions:  newTTLMap(sessionCacheTTL),
		endpoints: newTTLMap(sessionCacheTTL),
	}

	_, err := p.waitForSessionActive(context.Background(), "sess-stuck")
	if err == nil {
		t.Fatal("expected error from stuck session")
	}
	if !strings.Contains(err.Error(), "ImagePullBackOff") {
		t.Fatalf("expected underlying SparkApplication reason in error; got %q", err.Error())
	}
}

// TestWaitForSessionActive_QuotaExceededIsTyped covers the P1-1 contract:
// when admission rejects a session with QuotaExceeded, waitForSessionActive
// returns a *quotaExceededError so the handler can return
// codes.ResourceExhausted / HTTP 429 instead of the generic Unavailable / 503.
func TestWaitForSessionActive_QuotaExceededIsTyped(t *testing.T) {
	scheme := newProxyTestScheme(t)
	session := &sparkv1alpha1.SparkInteractiveSession{
		ObjectMeta: metav1.ObjectMeta{Name: "sess-quota", Namespace: "default"},
		Status: sparkv1alpha1.SparkInteractiveSessionStatus{
			State: "Failed",
			Conditions: []metav1.Condition{{
				Type:               sparkv1alpha1.ConditionQuotaExceeded,
				Status:             metav1.ConditionTrue,
				Reason:             "QuotaExceeded",
				Message:            `user alice has reached max sessions (5/5)`,
				LastTransitionTime: metav1.Now(),
			}},
		},
	}
	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(session).Build()
	p := &SessionProxy{
		client:    cli,
		log:       logr.Discard(),
		namespace: "default",
		sessions:  newTTLMap(sessionCacheTTL),
		endpoints: newTTLMap(sessionCacheTTL),
	}

	_, err := p.waitForSessionActive(context.Background(), "sess-quota")
	if err == nil {
		t.Fatal("expected error from quota-failed session")
	}
	if !isQuotaExceeded(err) {
		t.Fatalf("expected typed quotaExceededError; got %T %v", err, err)
	}
	if !strings.Contains(err.Error(), "5/5") {
		t.Fatalf("expected quota limit in message; got %q", err.Error())
	}
}
