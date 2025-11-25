package v1

import corev1 "k8s.io/api/core/v1"

type Worker struct {
	//+kubebuilder:validation:Required
	Image string `json:"image,omitempty"`

	//+kubebuilder:validation:Required
	TargetCluster TargetCluster `json:"targetCluster,omitempty"`

	Resource corev1.ResourceRequirements `json:"resource,omitempty"`
}
