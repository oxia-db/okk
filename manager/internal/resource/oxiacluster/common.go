package oxiacluster

import (
	oxiav1 "github.com/oxia-io/okk/api/v1"
	"golang.org/x/exp/maps"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const (
	ComponentCoordinator = "coordinator"
	ComponentNode        = "node"
)

var (
	PublicPort = v1.ServicePort{
		Name:       "public",
		TargetPort: intstr.FromString("public"),
		Port:       int32(6648),
	}
	InternalPort = v1.ServicePort{
		Name:       "internal",
		TargetPort: intstr.FromString("internal"),
		Port:       int32(6649),
	}
	MetricsPort = v1.ServicePort{
		Name:       "metrics",
		TargetPort: intstr.FromString("metrics"),
		Port:       int32(8080),
	}
)

func SelectLabels(component string, name string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":      "oxia-cluster",
		"app.kubernetes.io/component": component,
		"app.kubernetes.io/instance":  name,
	}
}

func ExtraLabels() map[string]string {
	return map[string]string{
		"app.kubernetes.io/part-of":    "okk-manager",
		"app.kubernetes.io/managed-by": "okk-manager",
	}
}

func Labels(component string, name string) map[string]string {
	labels := make(map[string]string)
	maps.Copy(labels, SelectLabels(component, name))
	maps.Copy(labels, ExtraLabels())
	return labels
}

func injectLabelsAndOwnership(objMeta *metav1.ObjectMeta, oxiaCluster *oxiav1.OxiaCluster, component string) {
	objMeta.SetLabels(Labels(component, oxiaCluster.Name))
	objMeta.SetOwnerReferences([]metav1.OwnerReference{
		*metav1.NewControllerRef(oxiaCluster, oxiaCluster.GetObjectKind().GroupVersionKind()),
	})
}

func getIstioAnnotations(oxiaCluster *oxiav1.OxiaCluster) map[string]string {
	if !oxiaCluster.Spec.EnableIstio {
		return nil
	}
	return map[string]string{
		"sidecar.istio.io/inject": "true",
	}
}
