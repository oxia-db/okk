package v1

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
)

type TargetCluster struct {
	//+kubebuilder:validation:Required
	Name string `json:"name,omitempty"`
	//+kubebuilder:validation:Required
	Namespace string `json:"namespace,omitempty"`
}

func (t *TargetCluster) GetServiceURL() string {
	return fmt.Sprintf("data-server-%s.%s.svc.cluster.local:%d", t.Name, t.Namespace, 6648)
}

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
