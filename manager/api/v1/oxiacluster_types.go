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
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type AntiAffinity struct {
	Labels []string `json:"labels,omitempty" `
	Mode   string   `json:"mode,omitempty" `
}

type OxiaNamespacePolicies struct {
	AntiAffinities []AntiAffinity `json:"antiAffinities,omitempty"`
}
type OxiaNamespace struct {
	Name                 string                 `json:"name,omitempty"`
	InitialShardCount    uint32                 `json:"initialShardCount,omitempty" `
	ReplicationFactor    uint32                 `json:"replicationFactor,omitempty"`
	NotificationsEnabled bool                   `json:"notificationsEnabled,omitempty" `
	Policies             *OxiaNamespacePolicies `json:"policies,omitempty" `
}

type OxiaClusterCoordinator struct {
	Namespaces []OxiaNamespace             `json:"namespaces,omitempty"`
	Resource   corev1.ResourceRequirements `json:"resource,omitempty"`
}

type OxiaClusterDataServer struct {
	Replicas int32                       `json:"replicas,omitempty"`
	Resource corev1.ResourceRequirements `json:"resource,omitempty"`
}
type OxiaClusterSpec struct {
	Image                  *string                `json:"image,omitempty"`
	OxiaClusterCoordinator OxiaClusterCoordinator `json:"coordinator,omitempty"`
	OxiaClusterDataServer  OxiaClusterDataServer  `json:"dataServer,omitempty"`
}

type OxiaClusterStatus struct {
	Conditions []metav1.Condition `json:"conditions"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// OxiaCluster is the Schema for the oxiaclusters API.
type OxiaCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   OxiaClusterSpec   `json:"spec,omitempty"`
	Status OxiaClusterStatus `json:"status,omitempty"`
}

func (cluster *OxiaCluster) GetCoordinatorName() string {
	return fmt.Sprintf("coordinator-%s", cluster.Name)
}

func (cluster *OxiaCluster) GetDataServerName() string {
	return fmt.Sprintf("data-server-%s", cluster.Name)
}

func (cluster *OxiaCluster) GetImage() string {
	if cluster.Spec.Image == nil {
		return "oxia/oxia:latest"
	}
	return *cluster.Spec.Image
}

// +kubebuilder:object:root=true

// OxiaClusterList contains a list of OxiaCluster.
type OxiaClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []OxiaCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&OxiaCluster{}, &OxiaClusterList{})
}
