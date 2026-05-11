/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"errors"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestSetDegradedCondition pins the contract that Reconcile relies on:
//   - clean pass → Degraded=False with reason Healthy
//   - any error  → Degraded=True with reason ReconcileError and a message
//     containing every aggregated underlying error so operators can see all
//     simultaneous failures without re-running the controller.
//
// Without aggregation, an inconsistent observable state — e.g. ingress
// reconcile failing AND status update failing — would surface only the last
// error, hiding the other from `kubectl describe`.
func TestSetDegradedCondition(t *testing.T) {
	t.Run("no errors marks pool healthy", func(t *testing.T) {
		var conds []metav1.Condition
		setDegradedCondition(&conds, 7, nil)

		if len(conds) != 1 {
			t.Fatalf("want 1 condition, got %d", len(conds))
		}
		c := conds[0]
		if c.Type != conditionTypeDegraded {
			t.Errorf("type: got %q want %q", c.Type, conditionTypeDegraded)
		}
		if c.Status != metav1.ConditionFalse {
			t.Errorf("status: got %q want False", c.Status)
		}
		if c.Reason != "Healthy" {
			t.Errorf("reason: got %q want Healthy", c.Reason)
		}
		if c.ObservedGeneration != 7 {
			t.Errorf("observedGeneration: got %d want 7", c.ObservedGeneration)
		}
	})

	t.Run("aggregates every error into the message", func(t *testing.T) {
		errs := []error{
			errors.New("ingress: nope"),
			errors.New("scaling: also broken"),
			errors.New("status update: 409"),
		}
		var conds []metav1.Condition
		setDegradedCondition(&conds, 12, errs)

		if len(conds) != 1 {
			t.Fatalf("want 1 condition, got %d", len(conds))
		}
		c := conds[0]
		if c.Status != metav1.ConditionTrue {
			t.Errorf("status: got %q want True", c.Status)
		}
		if c.Reason != "ReconcileError" {
			t.Errorf("reason: got %q want ReconcileError", c.Reason)
		}
		if c.ObservedGeneration != 12 {
			t.Errorf("observedGeneration: got %d want 12", c.ObservedGeneration)
		}
		for _, want := range []string{"ingress: nope", "scaling: also broken", "status update: 409"} {
			if !strings.Contains(c.Message, want) {
				t.Errorf("message missing %q; got: %s", want, c.Message)
			}
		}
	})

	t.Run("recovery from degraded flips status without duplicating condition", func(t *testing.T) {
		var conds []metav1.Condition
		setDegradedCondition(&conds, 1, []error{errors.New("transient: boom")})
		setDegradedCondition(&conds, 2, nil)

		// apimeta.SetStatusCondition updates in place; we should still have a
		// single Degraded condition reflecting the latest call.
		if len(conds) != 1 {
			t.Fatalf("want 1 condition after recovery, got %d", len(conds))
		}
		if conds[0].Status != metav1.ConditionFalse {
			t.Errorf("after recovery status: got %q want False", conds[0].Status)
		}
		if conds[0].ObservedGeneration != 2 {
			t.Errorf("after recovery observedGeneration: got %d want 2", conds[0].ObservedGeneration)
		}
	})
}
