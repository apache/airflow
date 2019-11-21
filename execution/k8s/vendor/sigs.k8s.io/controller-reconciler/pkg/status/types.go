/*
Copyright 2018 Google LLC
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

package status

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Constants
const (
	// Ready => controller considers this resource Ready
	Ready = "Ready"
	// TrafficReady => pod sees external traffic
	TrafficReady = "TrafficReady"
	// Qualified => functionally tested
	Qualified = "Qualified"
	// Settled => observed generation == generation + settled means controller is done acting functionally tested
	// Cleanup => it is set to track finalizer failures
	Cleanup = "Cleanup"
	// Error => last recorded error
	Error = "Error"

	Settled = "Settled"

	ReasonInit = "Init"
)

// Statefulset is a generic status holder for stateful-set
// +k8s:deepcopy-gen=true
type Statefulset struct {
	// Replicas defines the no of MySQL instances desired
	Replicas int32 `json:"replicas"`
	// ReadyReplicas defines the no of MySQL instances that are ready
	ReadyReplicas int32 `json:"readycount"`
	// CurrentReplicas defines the no of MySQL instances that are created
	CurrentReplicas int32 `json:"currentcount"`
	// progress is a fuzzy indicator. Interpret as a percentage (0-100)
	// eg: for statefulsets, progress = 100*readyreplicas/replicas
	Progress int32 `json:"progress"`
}

// Pdb is a generic status holder for pdb
type Pdb struct {
	// currentHealthy
	CurrentHealthy int32 `json:"currenthealthy"`
	// desiredHealthy
	DesiredHealthy int32 `json:"desiredhealthy"`
}

// ExtendedStatus is a holder of additional status for well known types
// +k8s:deepcopy-gen=true
type ExtendedStatus struct {
	// StatefulSet status
	STS *Statefulset `json:"sts,omitempty"`
	// PDB status
	PDB *Pdb `json:"pdb,omitempty"`
}

// ComponentMeta is a generic set of fields for component status objects
// +k8s:deepcopy-gen=true
type ComponentMeta struct {
	// Resources embeds a list of object statuses
	// +optional
	ComponentList `json:",inline,omitempty"`
}

// Meta is a generic set of fields for status objects
// +k8s:deepcopy-gen=true
type Meta struct {
	// ObservedGeneration is the most recent generation observed. It corresponds to the
	// Object's generation, which is updated on mutation by the API Server.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty" protobuf:"varint,1,opt,name=observedGeneration"`
	// Conditions represents the latest state of the object
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	Conditions []Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,10,rep,name=conditions"`
}

// ComponentList is a generic status holder for the top level resource
// +k8s:deepcopy-gen=true
type ComponentList struct {
	// Object status array for all matching objects
	Objects []ObjectStatus `json:"components,omitempty"`
}

// ObjectStatus is a generic status holder for objects
// +k8s:deepcopy-gen=true
type ObjectStatus struct {
	// Link to object
	Link string `json:"link,omitempty"`
	// Name of object
	Name string `json:"name,omitempty"`
	// Kind of object
	Kind string `json:"kind,omitempty"`
	// Object group
	Group string `json:"group,omitempty"`
	// Status. Values: InProgress, Ready, Unknown
	Status string `json:"status,omitempty"`
	// ExtendedStatus adds Kind specific status information for well known types
	ExtendedStatus `json:",inline,omitempty"`
}

// ConditionType encodes information on the condition
type ConditionType string

// Condition describes the state of an object at a certain point.
// +k8s:deepcopy-gen=true
type Condition struct {
	// Type of condition.
	Type ConditionType `json:"type" protobuf:"bytes,1,opt,name=type,casttype=StatefulSetConditionType"`
	// Status of the condition, one of True, False, Unknown.
	Status corev1.ConditionStatus `json:"status" protobuf:"bytes,2,opt,name=status,casttype=k8s.io/api/core/v1.ConditionStatus"`
	// The reason for the condition's last transition.
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,4,opt,name=reason"`
	// A human readable message indicating details about the transition.
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,5,opt,name=message"`
	// Last time the condition was probed
	// +optional
	LastUpdateTime metav1.Time `json:"lastUpdateTime,omitempty" protobuf:"bytes,3,opt,name=lastProbeTime"`
	// Last time the condition transitioned from one status to another.
	// +optional
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty" protobuf:"bytes,3,opt,name=lastTransitionTime"`
}
