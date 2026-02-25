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
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SparkSessionPoolSpec defines the desired state of a pool of Spark server instances
type SparkSessionPoolSpec struct {
	// Type of Spark server: "connect" or "thrift"
	// +kubebuilder:validation:Enum=connect;thrift
	Type string `json:"type"`

	// Replicas defines min/max pool size
	Replicas ReplicaSpec `json:"replicas"`

	// Scaling defines autoscaling behavior
	Scaling ScalingSpec `json:"scaling"`

	// Template for creating SparkApplication instances
	// This is the raw SparkApplication spec that will be used as a template
	SparkApplicationTemplate SparkApplicationTemplateSpec `json:"sparkApplicationTemplate"`

	// SessionPolicy defines session management rules
	SessionPolicy SessionPolicySpec `json:"sessionPolicy"`
}

type ReplicaSpec struct {
	// Minimum number of pool instances (always running)
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1
	Min int32 `json:"min"`

	// Maximum number of pool instances
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=10
	Max int32 `json:"max"`
}

type ScalingSpec struct {
	// Metrics defines what metrics drive scaling decisions
	Metrics ScalingMetricsSpec `json:"metrics"`

	// ScaleUpThreshold - when load exceeds this fraction of target, scale up
	// +kubebuilder:default="0.8"
	ScaleUpThreshold string `json:"scaleUpThreshold,omitempty"`

	// ScaleDownThreshold - when load drops below this fraction of target, scale down
	// +kubebuilder:default="0.3"
	ScaleDownThreshold string `json:"scaleDownThreshold,omitempty"`

	// CooldownSeconds - minimum time between scaling actions
	// +kubebuilder:default=300
	CooldownSeconds int32 `json:"cooldownSeconds,omitempty"`

	// DrainBeforeScaleDown - if true, stop routing new sessions before removing instance
	// +kubebuilder:default=true
	DrainBeforeScaleDown bool `json:"drainBeforeScaleDown,omitempty"`
}

type ScalingMetricsSpec struct {
	// Primary metric for scaling (activeSessions, cpu, memory)
	// +kubebuilder:validation:Enum=activeSessions;cpu;memory
	// +kubebuilder:default=activeSessions
	Type string `json:"type"`

	// TargetPerInstance - target value per instance for the primary metric.
	//   activeSessions: target session count per instance (default 20)
	//   cpu: target CPU utilization percentage 0-100 (default 80)
	//   memory: target memory utilization percentage 0-100 (default 80)
	// +kubebuilder:default=20
	TargetPerInstance int32 `json:"targetPerInstance"`
}

// SparkApplicationTemplateSpec holds the template for creating SparkApplication CRs
type SparkApplicationTemplateSpec struct {
	// Spec is the raw SparkApplication spec stored as embedded JSON.
	// We use apiextensionsv1.JSON to avoid importing the full SparkOperator types.
	// +kubebuilder:pruning:PreserveUnknownFields
	Spec *apiextensionsv1.JSON `json:"spec"`
}

type SessionPolicySpec struct {
	// MaxSessionsPerUser - default max concurrent sessions per user
	// +kubebuilder:default=5
	MaxSessionsPerUser int32 `json:"maxSessionsPerUser,omitempty"`

	// MaxTotalSessions - max total sessions across the pool
	// +kubebuilder:default=200
	MaxTotalSessions int32 `json:"maxTotalSessions,omitempty"`

	// IdleTimeoutMinutes - kill sessions idle longer than this
	// +kubebuilder:default=720
	IdleTimeoutMinutes int32 `json:"idleTimeoutMinutes,omitempty"`

	// DefaultSessionConf - default Spark configs applied to each session
	DefaultSessionConf map[string]string `json:"defaultSessionConf,omitempty"`

	// Quotas - per-user or per-group overrides
	Quotas []QuotaOverride `json:"quotas,omitempty"`
}

type QuotaOverride struct {
	// Match defines which users/groups this quota applies to
	Match QuotaMatch `json:"match"`

	// MaxSessionsPerUser overrides the pool-level default
	MaxSessionsPerUser int32 `json:"maxSessionsPerUser,omitempty"`

	// SessionConf overrides default session spark configs
	SessionConf map[string]string `json:"sessionConf,omitempty"`
}

type QuotaMatch struct {
	// Users to match
	Users []string `json:"users,omitempty"`

	// Groups to match (from OIDC token claims)
	Groups []string `json:"groups,omitempty"`
}

// SparkSessionPoolStatus defines the observed state of SparkSessionPool
type SparkSessionPoolStatus struct {
	// Instances tracks the state of each pool instance
	Instances []PoolInstanceStatus `json:"instances,omitempty"`

	// TotalActiveSessions across all instances
	TotalActiveSessions int32 `json:"totalActiveSessions"`

	// CurrentReplicas - number of running instances
	CurrentReplicas int32 `json:"currentReplicas"`

	// ReadyReplicas - number of instances ready to accept sessions
	ReadyReplicas int32 `json:"readyReplicas"`

	// LastScaleTime - timestamp of last scaling action
	LastScaleTime *metav1.Time `json:"lastScaleTime,omitempty"`

	// Conditions
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

type PoolInstanceStatus struct {
	// Name of the SparkApplication CR
	Name string `json:"name"`

	// State of the instance
	// +kubebuilder:validation:Enum=Pending;Running;Draining;Failed;Terminated
	State string `json:"state"`

	// ActiveSessions on this instance
	ActiveSessions int32 `json:"activeSessions"`

	// Endpoint for client connections
	Endpoint string `json:"endpoint,omitempty"`

	// SparkApplicationState from the SparkOperator
	SparkApplicationState string `json:"sparkApplicationState,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Type",type=string,JSONPath=`.spec.type`
// +kubebuilder:printcolumn:name="Replicas",type=integer,JSONPath=`.status.currentReplicas`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.readyReplicas`
// +kubebuilder:printcolumn:name="Sessions",type=integer,JSONPath=`.status.totalActiveSessions`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// SparkSessionPool is the Schema for the sparksessionpools API
type SparkSessionPool struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SparkSessionPoolSpec   `json:"spec,omitempty"`
	Status SparkSessionPoolStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// SparkSessionPoolList contains a list of SparkSessionPool
type SparkSessionPoolList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SparkSessionPool `json:"items"`
}

func init() {
	SchemeBuilder.Register(&SparkSessionPool{}, &SparkSessionPoolList{})
}
