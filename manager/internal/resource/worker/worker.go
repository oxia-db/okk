package worker

import (
	"context"

	v1 "github.com/oxia-io/okk/api/v1"
	"golang.org/x/exp/maps"
	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/pointer"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

var (
	ManagementPort = corev1.ServicePort{
		Name:       "public",
		TargetPort: intstr.FromString("public"),
		Port:       int32(6666),
	}
	MetricsPort = corev1.ServicePort{
		Name:       "metrics",
		TargetPort: intstr.FromString("metrics"),
		Port:       int32(8080),
	}
)

func SelectLabels(name string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":      "tc-worker",
		"app.kubernetes.io/component": "worker",
		"app.kubernetes.io/instance":  name,
	}
}

func ExtraLabels() map[string]string {
	return map[string]string{
		"app.kubernetes.io/part-of":    "okk-manager",
		"app.kubernetes.io/managed-by": "okk-manager",
	}
}

func Labels(name string) map[string]string {
	labels := make(map[string]string)
	maps.Copy(labels, SelectLabels(name))
	maps.Copy(labels, ExtraLabels())
	return labels
}

func injectLabelsAndOwnership(objMeta *metav1.ObjectMeta, testcase client.Object) {
	objMeta.SetLabels(Labels(testcase.GetName()))
	objMeta.SetOwnerReferences([]metav1.OwnerReference{
		*metav1.NewControllerRef(testcase, testcase.GetObjectKind().GroupVersionKind()),
	})
}

func ApplyWorker(ctx context.Context, client client.Client, object client.Object, worker *v1.Worker) error {
	targetCluster := worker.TargetCluster
	oxiaCluster := &v1.OxiaCluster{}
	if err := client.Get(ctx, types.NamespacedName{Name: targetCluster.Name, Namespace: targetCluster.Namespace}, oxiaCluster); err != nil {
		return err
	}
	if err := applyWorkerService(ctx, client, object, worker); err != nil {
		return err
	}
	if err := applyWorkerDeployment(ctx, client, object, worker); err != nil {
		return err
	}
	return nil
}

func applyWorkerService(ctx context.Context, client client.Client, testCase client.Object, worker *v1.Worker) error {
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      v1.MakeServiceName(testCase.GetName()),
			Namespace: testCase.GetNamespace(),
		},
	}
	if _, err := controllerutil.CreateOrPatch(ctx, client, service, func() error {
		injectLabelsAndOwnership(&service.ObjectMeta, testCase)
		service.Spec = corev1.ServiceSpec{
			Selector: SelectLabels(testCase.GetName()),
			Ports: []corev1.ServicePort{
				ManagementPort,
				MetricsPort,
			},
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func applyWorkerDeployment(ctx context.Context, client client.Client, testcase client.Object, worker *v1.Worker) error {
	deployment := &appv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testcase.GetName(),
			Namespace: testcase.GetNamespace(),
		},
	}
	if _, err := controllerutil.CreateOrPatch(ctx, client, deployment, func() error {
		injectLabelsAndOwnership(&deployment.ObjectMeta, testcase)
		deployment.Spec = appv1.DeploymentSpec{
			Strategy: appv1.DeploymentStrategy{
				Type: appv1.RecreateDeploymentStrategyType,
			},
			Replicas: pointer.Int32Ptr(int32(1)),
			Selector: &metav1.LabelSelector{
				MatchLabels: SelectLabels(testcase.GetName()),
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Name:      testcase.GetName(),
					Namespace: testcase.GetNamespace(),
					Labels:    Labels(testcase.GetName()),
					OwnerReferences: []metav1.OwnerReference{
						*metav1.NewControllerRef(testcase, testcase.GetObjectKind().GroupVersionKind()),
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:            "coordinator",
							Resources:       worker.Resource,
							Image:           worker.Image,
							ImagePullPolicy: corev1.PullAlways,
							Env: []corev1.EnvVar{
								{
									Name:  "OKK_WORKER_ENGINE_NAME",
									Value: "oxia",
								},
								{
									Name:  "OKK_WORKER_OXIA_SERVICE_URL",
									Value: worker.TargetCluster.GetServiceURL(),
								},
								{
									Name:  "OKK_WORKER_OXIA_NAMESPACE",
									Value: "okk",
								},
							},
							Ports: []corev1.ContainerPort{
								{
									Name:          ManagementPort.Name,
									ContainerPort: ManagementPort.Port,
								},
								{
									Name:          MetricsPort.Name,
									ContainerPort: MetricsPort.Port,
								},
							},
						},
					},
				},
			},
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}
