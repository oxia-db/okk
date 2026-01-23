package oxiacluster

import (
	"context"
	"fmt"

	v1 "github.com/oxia-io/okk/api/v1"
	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func ApplyDataServer(ctx context.Context, client client.Client, cluster *v1.OxiaCluster) error {
	if err := applyDataServerService(ctx, client, cluster); err != nil {
		return err
	}
	if err := applyDataServerHeadlessService(ctx, client, cluster); err != nil {
		return err
	}
	if err := applyDataServerStatefulSet(ctx, client, cluster); err != nil {
		return err
	}
	return nil
}

func applyDataServerService(ctx context.Context, client client.Client, cluster *v1.OxiaCluster) error {
	dataServerName := cluster.GetDataServerName()
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dataServerName,
			Namespace: cluster.Namespace,
		},
	}
	if _, err := controllerutil.CreateOrPatch(ctx, client, service, func() error {
		injectLabelsAndOwnership(&service.ObjectMeta, cluster, ComponentNode)
		service.Spec = corev1.ServiceSpec{
			Selector: SelectLabels(ComponentNode, cluster.Name),
			Ports: []corev1.ServicePort{
				PublicPort,
				InternalPort,
				MetricsPort,
			},
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func applyDataServerHeadlessService(ctx context.Context, client client.Client, cluster *v1.OxiaCluster) error {
	dataServerName := cluster.GetDataServerName()
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-headless", dataServerName),
			Namespace: cluster.Namespace,
		},
	}
	if _, err := controllerutil.CreateOrPatch(ctx, client, service, func() error {
		injectLabelsAndOwnership(&service.ObjectMeta, cluster, ComponentNode)
		service.Spec = corev1.ServiceSpec{
			ClusterIP:                "None",
			PublishNotReadyAddresses: true,
			Selector:                 SelectLabels(ComponentNode, cluster.Name),
			Ports: []corev1.ServicePort{
				PublicPort,
				InternalPort,
				MetricsPort,
			},
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func applyDataServerStatefulSet(ctx context.Context, client client.Client, cluster *v1.OxiaCluster) error {
	dataServerName := cluster.GetDataServerName()
	dataServerSpec := cluster.Spec.OxiaClusterDataServer
	sts := appv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dataServerName,
			Namespace: cluster.Namespace,
		},
	}
	if _, err := controllerutil.CreateOrPatch(ctx, client, &sts, func() error {
		injectLabelsAndOwnership(&sts.ObjectMeta, cluster, ComponentNode)
		sts.Spec = appv1.StatefulSetSpec{
			Replicas:            pointer.Int32(dataServerSpec.GetReplicas()),
			PodManagementPolicy: appv1.ParallelPodManagement,
			Selector:            &metav1.LabelSelector{MatchLabels: SelectLabels(ComponentNode, cluster.Name)},
			ServiceName:         fmt.Sprintf("%s-headless", dataServerName),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Name:        dataServerName,
					Namespace:   cluster.Namespace,
					Labels:      Labels(ComponentNode, cluster.Name),
					Annotations: getIstioAnnotations(cluster),
					OwnerReferences: []metav1.OwnerReference{
						*metav1.NewControllerRef(cluster, cluster.GetObjectKind().GroupVersionKind()),
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name: "server",
						Command: []string{
							"oxia",
							"server",
							"--profile",
							"--log-json",
							"--data-dir=/data/db",
							"--wal-dir=/data/wal",
							"--log-level=debug",
						},
						Resources: dataServerSpec.Resource,
						Image:     cluster.GetImage(),
						Ports: []corev1.ContainerPort{
							{
								Name:          PublicPort.Name,
								ContainerPort: PublicPort.Port,
							},
							{
								Name:          InternalPort.Name,
								ContainerPort: InternalPort.Port,
							},
							{
								Name:          MetricsPort.Name,
								ContainerPort: MetricsPort.Port,
							},
						},
						VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: "/data"}},
						LivenessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								Exec: &corev1.ExecAction{
									Command: []string{"oxia", "health", fmt.Sprintf("--port=%d", InternalPort.Port)},
								},
							},
							InitialDelaySeconds: 10,
							TimeoutSeconds:      10,
						},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								Exec: &corev1.ExecAction{
									Command: []string{"oxia", "health", fmt.Sprintf("--port=%d", InternalPort.Port), "--service=oxia-readiness"},
								},
							},
							InitialDelaySeconds: 10,
							TimeoutSeconds:      10,
						},
						StartupProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								Exec: &corev1.ExecAction{
									Command: []string{"oxia", "health", fmt.Sprintf("--port=%d", InternalPort.Port)},
								},
							},
							InitialDelaySeconds: 60,
							TimeoutSeconds:      10,
						},
					}},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:   "data",
						Labels: Labels(ComponentNode, cluster.Name),
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						AccessModes: []corev1.PersistentVolumeAccessMode{
							corev1.ReadWriteOnce,
						},
						Resources: corev1.VolumeResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: resource.MustParse("10Gi"),
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
