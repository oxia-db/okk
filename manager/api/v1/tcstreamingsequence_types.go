/*
Copyright 2025.

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

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TCStreamingSequenceSpec defines the desired state of TCStreamingSequence.
type TCStreamingSequenceSpec struct {
	TargetCluster TargetCluster `json:"targetCluster,omitempty"`
}

// TCStreamingSequenceStatus defines the observed state of TCStreamingSequence.
type TCStreamingSequenceStatus struct {
	Conditions []metav1.Condition `json:"conditions"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// TCStreamingSequence is the Schema for the tcstreamingsequences API.
type TCStreamingSequence struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   TCStreamingSequenceSpec   `json:"spec,omitempty"`
	Status TCStreamingSequenceStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// TCStreamingSequenceList contains a list of TCStreamingSequence.
type TCStreamingSequenceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []TCStreamingSequence `json:"items"`
}

func init() {
	SchemeBuilder.Register(&TCStreamingSequence{}, &TCStreamingSequenceList{})
}
