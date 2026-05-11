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
	"testing"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	sparkv1alpha1 "github.com/padod/spark-session-operator/api/v1alpha1"
)

// TestSessionProxy_StaleEndpointReResolves verifies the failure path that
// kicks in when the proxy's cached endpoint becomes stale (typically because
// a driver pod restarted with a new IP): invalidating the cache and calling
// resolveFreshEndpoint must bypass the stale value and return the current
// endpoint advertised on the SparkInteractiveSession status. Pins the spec's
// requirement that "stale endpoint triggers re-resolution."
func TestSessionProxy_StaleEndpointReResolves(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := sparkv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}

	const (
		ns          = "default"
		sessionName = "session-alice-1"
		freshAddr   = "spark-driver-new.spark.svc:15002"
		staleAddr   = "spark-driver-old.spark.svc:15002"
	)

	session := &sparkv1alpha1.SparkInteractiveSession{
		ObjectMeta: metav1.ObjectMeta{Name: sessionName, Namespace: ns},
		Spec: sparkv1alpha1.SparkInteractiveSessionSpec{
			User: "alice",
			Pool: "connect-default-pool",
		},
		Status: sparkv1alpha1.SparkInteractiveSessionStatus{
			State:    "Active",
			Endpoint: freshAddr,
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(session).
		WithStatusSubresource(&sparkv1alpha1.SparkInteractiveSession{}).
		Build()

	p := &SessionProxy{
		client:    cli,
		log:       logr.Discard(),
		namespace: ns,
		sessions:  newTTLMap(sessionCacheTTL),
		endpoints: newTTLMap(sessionCacheTTL),
	}

	// Seed the endpoint cache with a stale value as if the driver had moved.
	p.endpoints.set(sessionName, staleAddr)

	// Sanity: cache returns the stale value until we invalidate.
	if got, ok := p.endpoints.get(sessionName); !ok || got != staleAddr {
		t.Fatalf("cache seed failed: got %q ok=%v", got, ok)
	}

	// resolveFreshEndpoint must bypass the cache and pick up the new endpoint
	// from the session CR's status.
	fresh, err := p.resolveFreshEndpoint(context.Background(), sessionName)
	if err != nil {
		t.Fatalf("resolveFreshEndpoint: %v", err)
	}
	if fresh != freshAddr {
		t.Fatalf("got endpoint %q, want %q", fresh, freshAddr)
	}

	// Cache should now reflect the refreshed value.
	if got, ok := p.endpoints.get(sessionName); !ok || got != freshAddr {
		t.Fatalf("post-refresh cache: got %q ok=%v, want %q", got, ok, freshAddr)
	}
}

// TestSessionProxy_InvalidateEndpoint covers the lighter-weight invalidation
// hook used by the Thrift reverse proxy's ErrorHandler: it just drops the
// cache entry, so the next request takes the full resolution path. Without
// this, a stale entry would persist for the full TTL even after a known-bad
// transport error.
func TestSessionProxy_InvalidateEndpoint(t *testing.T) {
	p := &SessionProxy{
		log:       logr.Discard(),
		sessions:  newTTLMap(sessionCacheTTL),
		endpoints: newTTLMap(sessionCacheTTL),
	}

	p.endpoints.set("sess-x", "old:1234")
	if _, ok := p.endpoints.get("sess-x"); !ok {
		t.Fatal("seed failed")
	}

	p.invalidateEndpoint("sess-x")

	if _, ok := p.endpoints.get("sess-x"); ok {
		t.Fatal("expected entry removed after invalidate")
	}
}
