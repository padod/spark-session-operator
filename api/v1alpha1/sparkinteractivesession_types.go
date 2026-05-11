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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Session state values mirror the CRD's enum and are used by every actor
// (session controller, pool controller, proxy, gateway) — keep them as
// constants so a typo doesn't silently match nothing.
const (
	SessionStatePending     = "Pending"
	SessionStateActive      = "Active"
	SessionStateIdle        = "Idle"
	SessionStateTerminating = "Terminating"
	SessionStateTerminated  = "Terminated"
	SessionStateFailed      = "Failed"
)

// Session condition types. Keep these as string constants so the proxy and
// gateway can branch on them without importing controller internals.
const (
	// ConditionInstanceReady signals whether the SparkApplication backing
	// this session is in a state that can accept connections. Set to False
	// with a diagnostic Reason/Message when assignment is blocked (instance
	// stuck Pending, SparkApplication failed, no capacity) so the proxy can
	// short-circuit waitForSessionActive instead of timing out opaquely.
	ConditionInstanceReady = "InstanceReady"

	// ConditionQuotaExceeded is set True when admission rejects a session
	// because the pool's MaxSessionsPerUser / MaxTotalSessions limit has been
	// reached. The Message carries the actual limit and current count so the
	// gateway/proxy can surface ResourceExhausted / 429 to the client.
	ConditionQuotaExceeded = "QuotaExceeded"

	// ConditionPoolDeleted is set True when the parent pool has been deleted
	// and the session has been cascade-terminated. Distinct from
	// InstanceTerminated which targets a single instance going away.
	ConditionPoolDeleted = "PoolDeleted"

	// ConditionInstanceTerminated is set True when the SparkApplication
	// hosting this session was removed (scale-down, node loss, manual
	// delete) and the session can no longer be served.
	ConditionInstanceTerminated = "InstanceTerminated"
)

// SparkInteractiveSessionSpec defines the desired state of a user session
type SparkInteractiveSessionSpec struct {
	// User identifier (from OIDC token)
	User string `json:"user"`

	// Pool reference - name of the SparkSessionPool
	Pool string `json:"pool"`

	// SparkConf - per-session Spark configuration overrides
	SparkConf map[string]string `json:"sparkConf,omitempty"`
}

// SparkInteractiveSessionStatus defines the observed state
type SparkInteractiveSessionStatus struct {
	// State of the session
	// +kubebuilder:validation:Enum=Pending;Active;Idle;Terminating;Terminated;Failed
	State string `json:"state"`

	// AssignedInstance - which pool instance this session is on
	AssignedInstance string `json:"assignedInstance,omitempty"`

	// Endpoint - connection string for the user
	Endpoint string `json:"endpoint,omitempty"`

	// SessionID - internal Spark session identifier
	SessionID string `json:"sessionId,omitempty"`

	// CreatedAt
	CreatedAt *metav1.Time `json:"createdAt,omitempty"`

	// LastActivityAt - last time the session was active
	LastActivityAt *metav1.Time `json:"lastActivityAt,omitempty"`

	// Conditions
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="User",type=string,JSONPath=`.spec.user`
// +kubebuilder:printcolumn:name="Pool",type=string,JSONPath=`.spec.pool`
// +kubebuilder:printcolumn:name="State",type=string,JSONPath=`.status.state`
// +kubebuilder:printcolumn:name="Instance",type=string,JSONPath=`.status.assignedInstance`
// +kubebuilder:printcolumn:name="Idle Since",type=date,JSONPath=`.status.lastActivityAt`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// SparkInteractiveSession is the Schema for the sparkinteractivesessions API
type SparkInteractiveSession struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SparkInteractiveSessionSpec   `json:"spec,omitempty"`
	Status SparkInteractiveSessionStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// SparkInteractiveSessionList contains a list of SparkInteractiveSession
type SparkInteractiveSessionList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SparkInteractiveSession `json:"items"`
}

func init() {
	SchemeBuilder.Register(&SparkInteractiveSession{}, &SparkInteractiveSessionList{})
}
