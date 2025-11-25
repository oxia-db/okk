package v1

import "fmt"

type TargetCluster struct {
	//+kubebuilder:validation:Required
	Name string `json:"name,omitempty"`
	//+kubebuilder:validation:Required
	Namespace string `json:"namespace,omitempty"`
}

func (t *TargetCluster) GetServiceURL() string {
	return fmt.Sprintf("data-server-%s.data-server-%s.%s.svc.cluster.local:%d", t.Name, t.Name, t.Namespace, 6648)
}
