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

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	corev1 "github.com/oxia-io/okk/api/v1"
)

// TCMetadataEphemeralReconciler reconciles a TCMetadataEphemeral object
type TCMetadataEphemeralReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=core.oxia.io,resources=tcmetadataephemerals,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core.oxia.io,resources=tcmetadataephemerals/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=core.oxia.io,resources=tcmetadataephemerals/finalizers,verbs=update

func (r *TCMetadataEphemeralReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = logf.FromContext(ctx)

	// TODO(user): your logic here

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *TCMetadataEphemeralReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.TCMetadataEphemeral{}).
		Named("tcmetadataephemeral").
		Complete(r)
}
