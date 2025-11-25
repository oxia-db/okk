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
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TCMetadataEphemeralSpec defines the desired state of TCMetadataEphemeral.
type TCMetadataEphemeralSpec struct {
	//+kubebuilder:validation:Required
	Duration time.Duration `json:"duration,omitempty"`

	//+kubebuilder:validation:Required
	Worker Worker `json:"worker,omitempty"`
}

// TCMetadataEphemeralStatus defines the observed state of TCMetadataEphemeral.
type TCMetadataEphemeralStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// TCMetadataEphemeral is the Schema for the tcmetadataephemerals API.
type TCMetadataEphemeral struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   TCMetadataEphemeralSpec   `json:"spec,omitempty"`
	Status TCMetadataEphemeralStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// TCMetadataEphemeralList contains a list of TCMetadataEphemeral.
type TCMetadataEphemeralList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []TCMetadataEphemeral `json:"items"`
}

func init() {
	SchemeBuilder.Register(&TCMetadataEphemeral{}, &TCMetadataEphemeralList{})
}
