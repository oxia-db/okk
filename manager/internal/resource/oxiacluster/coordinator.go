package oxiacluster

import (
	"context"
	"fmt"

	v1 "github.com/oxia-io/okk/api/v1"
	"gopkg.in/yaml.v2"
	v4 "k8s.io/api/apps/v1"
	v2 "k8s.io/api/core/v1"
	v3 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func ApplyCoordinator(ctx context.Context, client client.Client, cluster *v1.OxiaCluster) error {
	if err := applyCoordinatorServiceAccount(ctx, client, cluster); err != nil {
		return err
	}
	if err := applyCoordinatorRole(ctx, client, cluster); err != nil {
		return err
	}
	if err := applyCoordinatorRoleBinding(ctx, client, cluster); err != nil {
		return err
	}
	if err := applyCoordinatorService(ctx, client, cluster); err != nil {
		return err
	}
	if err := applyCoordinatorConfigmap(ctx, client, cluster); err != nil {
		return err
	}
	if err := applyCoordinatorDeployment(ctx, client, cluster); err != nil {
		return err
	}
	return nil
}

func applyCoordinatorServiceAccount(ctx context.Context, client client.Client, cluster *v1.OxiaCluster) error {
	coordinatorName := cluster.GetCoordinatorName()
	sa := &v2.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      coordinatorName,
			Namespace: cluster.Namespace,
		},
	}
	if _, err := controllerutil.CreateOrPatch(ctx, client, sa, func() error {
		injectLabelsAndOwnership(&sa.ObjectMeta, cluster, ComponentCoordinator)
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func applyCoordinatorRole(ctx context.Context, client client.Client, cluster *v1.OxiaCluster) error {
	coordinatorName := cluster.GetCoordinatorName()
	role := &v3.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:      coordinatorName,
			Namespace: cluster.Namespace,
		},
	}
	if _, err := controllerutil.CreateOrPatch(ctx, client, role, func() error {
		injectLabelsAndOwnership(&role.ObjectMeta, cluster, ComponentCoordinator)
		role.Rules = []v3.PolicyRule{
			{
				APIGroups: []string{""},
				Resources: []string{"configmaps"},
				Verbs:     []string{"create", "delete", "get", "list", "patch", "update", "watch"},
			},
			{
				APIGroups: []string{"coordination.k8s.io"},
				Resources: []string{"leases"},
				Verbs:     []string{"create", "get", "list", "update"},
			},
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func applyCoordinatorRoleBinding(ctx context.Context, client client.Client, cluster *v1.OxiaCluster) error {
	coordinatorName := cluster.GetCoordinatorName()
	binding := &v3.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      coordinatorName,
			Namespace: cluster.Namespace,
		},
	}
	if _, err := controllerutil.CreateOrPatch(ctx, client, binding, func() error {
		injectLabelsAndOwnership(&binding.ObjectMeta, cluster, ComponentCoordinator)
		binding.RoleRef = v3.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "Role",
			Name:     coordinatorName,
		}
		binding.Subjects = []v3.Subject{
			{
				Kind:      "ServiceAccount",
				Name:      coordinatorName,
				Namespace: cluster.Namespace,
			},
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func applyCoordinatorService(ctx context.Context, client client.Client, cluster *v1.OxiaCluster) error {
	coordinatorName := cluster.GetCoordinatorName()
	service := &v2.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      coordinatorName,
			Namespace: cluster.Namespace,
		},
	}
	if _, err := controllerutil.CreateOrPatch(ctx, client, service, func() error {
		injectLabelsAndOwnership(&service.ObjectMeta, cluster, ComponentCoordinator)
		service.Spec = v2.ServiceSpec{
			Selector: SelectLabels(ComponentCoordinator, cluster.Name),
			Ports: []v2.ServicePort{
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

func applyCoordinatorConfigmap(ctx context.Context, client client.Client, cluster *v1.OxiaCluster) error {
	coordinatorName := cluster.GetCoordinatorName()
	configmap := &v2.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      coordinatorName,
			Namespace: cluster.Namespace,
		},
	}
	if _, err := controllerutil.CreateOrPatch(ctx, client, configmap, func() error {
		injectLabelsAndOwnership(&configmap.ObjectMeta, cluster, ComponentCoordinator)

		// parse namespaces
		cm := make(map[string]any)
		bData, err := yaml.Marshal(cluster.Spec.OxiaClusterCoordinator.Namespaces)
		if err != nil {
			return err
		}
		cm["namespaces"] = string(bData)

		// parse nodes
		nodeSpec := cluster.Spec.OxiaClusterNode
		replicas := nodeSpec.Replicas
		nodes := make([]map[string]string, replicas)
		for idx := range nodes {
			nodes[idx] = map[string]string{
				"public":   fmt.Sprintf("%s-%d.%s.headless.svc.cluster.local:%d", cluster.GetNodeName(), idx, cluster.Namespace, PublicPort.Port),
				"internal": fmt.Sprintf("%s-%d.headless:%d", cluster.GetNodeName(), idx, InternalPort.Port),
			}
		}
		bData, err = yaml.Marshal(nodes)
		if err != nil {
			return err
		}
		cm["servers"] = string(bData)

		// parse configmap
		bData, err = yaml.Marshal(cm)
		if err != nil {
			return err
		}
		configmap.Data = map[string]string{
			"config.yaml": string(bData),
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func applyCoordinatorDeployment(ctx context.Context, client client.Client, cluster *v1.OxiaCluster) error {
	coordinatorName := cluster.GetCoordinatorName()
	deployment := &v4.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      coordinatorName,
			Namespace: cluster.Namespace,
		},
	}
	if _, err := controllerutil.CreateOrPatch(ctx, client, deployment, func() error {
		injectLabelsAndOwnership(&deployment.ObjectMeta, cluster, ComponentCoordinator)
		deployment.Spec = v4.DeploymentSpec{
			Strategy: v4.DeploymentStrategy{
				Type: v4.RecreateDeploymentStrategyType,
			},
			Replicas: pointer.Int32Ptr(int32(1)),
			Selector: &metav1.LabelSelector{
				MatchLabels: SelectLabels(ComponentCoordinator, cluster.Name),
			},
			Template: v2.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Name:      coordinatorName,
					Namespace: cluster.Namespace,
					Labels:    Labels(ComponentCoordinator, cluster.Name),
					OwnerReferences: []metav1.OwnerReference{
						*metav1.NewControllerRef(cluster, cluster.GetObjectKind().GroupVersionKind()),
					},
				},
				Spec: v2.PodSpec{
					ServiceAccountName: coordinatorName,
					Containers: []v2.Container{
						{
							Name: "coordinator",
							Command: []string{
								"oxia",
								"coordinator",
								"--log-json",
								"--metadata=configmap",
								"--profile",
								fmt.Sprintf("--k8s-namespace=%s", cluster.Namespace),
								fmt.Sprintf("--k8s-configmap-name=%s", fmt.Sprintf("%s-status", coordinatorName)),
							},
							Image: cluster.GetImage(),
							Ports: []v2.ContainerPort{
								{
									Name:          InternalPort.Name,
									ContainerPort: InternalPort.Port,
								},
								{
									Name:          MetricsPort.Name,
									ContainerPort: MetricsPort.Port,
								},
							},
							LivenessProbe: &v2.Probe{
								ProbeHandler: v2.ProbeHandler{
									Exec: &v2.ExecAction{
										Command: []string{"oxia", "health", fmt.Sprintf("--port=%d", InternalPort.Port)},
									},
								},
								InitialDelaySeconds: 10,
								TimeoutSeconds:      10,
							},
							ReadinessProbe: &v2.Probe{
								ProbeHandler: v2.ProbeHandler{
									Exec: &v2.ExecAction{
										Command: []string{"oxia", "health", fmt.Sprintf("--port=%d", InternalPort.Port)},
									},
								},
								InitialDelaySeconds: 10,
								TimeoutSeconds:      10,
							},
						},
					},
					Volumes: []v2.Volume{
						{
							Name: "conf",
							VolumeSource: v2.VolumeSource{
								ConfigMap: &v2.ConfigMapVolumeSource{
									LocalObjectReference: v2.LocalObjectReference{Name: coordinatorName},
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
