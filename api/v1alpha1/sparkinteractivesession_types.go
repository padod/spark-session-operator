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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
