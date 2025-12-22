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

package controller

import (
	"context"

	"github.com/oxia-io/okk/internal/resource/worker"
	"github.com/oxia-io/okk/internal/task"
	"github.com/oxia-io/okk/internal/task/generator"
	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	v1 "github.com/oxia-io/okk/api/v1"
)

// TestCaseReconciler reconciles a TestCase object
type TestCaseReconciler struct {
	client.Client
	Scheme      *runtime.Scheme
	TaskManager *task.Manager
}

// +kubebuilder:rbac:groups=core.oxia.io,resources=tcmetadataephemerals,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core.oxia.io,resources=tcmetadataephemerals/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=core.oxia.io,resources=tcmetadataephemerals/finalizers,verbs=update

func (r *TestCaseReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	tc := &v1.TestCase{}
	if err := r.Client.Get(ctx, req.NamespacedName, tc); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	wc := &tc.Spec.Worker
	if err := worker.ApplyWorker(ctx, r.Client, tc, wc); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.TaskManager.ApplyTask(tc.Name, v1.MakeWorkerServiceURL(tc.Name, tc.Namespace), func() generator.Generator {
		switch tc.Spec.Type {
		case v1.TestCaseTypeBasicKv:
			return generator.NewBasicKv(ctx, tc)
		case v1.TestCaseTypeSecondaryIndex:
			panic("not implemented")
		case v1.TestCaseTypeStreamingSequence:
			return generator.NewStreamingSequence(ctx, tc)
		case v1.TestCaseTypeMetadataWithNotification:
			return generator.NewMetadataNotificationGenerator(ctx, tc)
		case v1.TestCaseTypeMetadataWithEphemeral:
			return generator.NewMetadataEphemeralGenerator(ctx, tc)
		case v1.TestCaseTypeMetadataWithVersionId:
			panic("not implemented")
		}
	}); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *TestCaseReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1.TestCase{}).
		Owns(&corev1.Service{}).
		Owns(&appv1.Deployment{}).
		Named("tcmetadataephemeral").
		Complete(r)
}
