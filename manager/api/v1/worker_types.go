package v1

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
)

type Worker struct {
	//+kubebuilder:validation:Required
	Image string `json:"image,omitempty"`

	//+kubebuilder:validation:Required
	TargetCluster TargetCluster `json:"targetCluster,omitempty"`

	Resource corev1.ResourceRequirements `json:"resource,omitempty"`
}

func MakeServiceName(name string) string {
	return fmt.Sprintf("worker-%s", name)
}

func MakeWorkerServiceURL(testcaseName string, namespace string) string {
	return fmt.Sprintf("%s.%s.svc.cluster.local:6666", MakeServiceName(testcaseName), namespace)
}
