/*
Copyright github.com/futurewei-cloud.

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

package controllers

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	aiv1alpha1 "elastictraining/api/v1alpha1"
)

// UnifiedJobReconciler reconciles a UnifiedJob object
type UnifiedJobReconciler struct {
	client.Client
	Log             logr.Logger
	Scheme          *runtime.Scheme
	JobInterfaceMap map[string]UnifiedJobInterface
}

type UnifiedJobInterface interface {
	Test() string
	//check if names of services, workers, etc. are mismatched
	//NamesMismatch(ujob aiv1alpha1.UnifiedJob) bool

	//update all names and patch if necessary
	//UpdateNames(ctx context.Context, ujob aiv1alpha1.UnifiedJob) error

	//update status of UnifiedJob.Status.UnifiedJobStatus to match actual job
	//status update will be: if job running, change to running,
	UpdateStatus(reconciler *UnifiedJobReconciler, ctx context.Context, ujob aiv1alpha1.UnifiedJob, applyOpts []client.PatchOption) error

	//delete job, release resources, but not necessarily wipe everything out
	ReleaseResources(reconciler *UnifiedJobReconciler, ctx context.Context, ujob aiv1alpha1.UnifiedJob, deleteOpts []client.DeleteOption) error

	//delete everything, ie resources + service(s)
	DeleteAll(reconciler *UnifiedJobReconciler, ctx context.Context, ujob aiv1alpha1.UnifiedJob, deleteOpts []client.DeleteOption) error

	//check if service exists
	ServiceExists(reconciler *UnifiedJobReconciler, ctx context.Context) bool

	//create and patch service
	CreateService(reconciler *UnifiedJobReconciler, ctx context.Context, applyOpts []client.PatchOption, ujob aiv1alpha1.UnifiedJob) error

	//check if job is stuck in pending; ie resource conflict / race condition
	StuckInPending(reconciler *UnifiedJobReconciler, ctx context.Context, ujob aiv1alpha1.UnifiedJob) bool

	//check if the job is running as expected
	IsJobRunning(reconciler *UnifiedJobReconciler, ctx context.Context, ujob aiv1alpha1.UnifiedJob) bool

	//create and patch all
	PatchAll(reconciler *UnifiedJobReconciler, ctx context.Context, ujob aiv1alpha1.UnifiedJob, applyOpts []client.PatchOption) error
}

// +kubebuilder:rbac:groups=ai.centauruscloud.io,resources=Unifiedjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ai.centauruscloud.io,resources=Unifiedjobs/status,verbs=get;update;patch

func (r *UnifiedJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithValues("UnifiedJob", req.NamespacedName)
	log.Info("Start reconciling")

	var ujob aiv1alpha1.UnifiedJob
	if err := r.Get(ctx, req.NamespacedName, &ujob); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if r.JobInterfaceMap[ujob.JobType] == nil {
		log.Info(fmt.Sprintf("UnifiedJobType %s is invalid.", ujob.JobType))
		return ctrl.Result{}, nil
	}

	if ujob.Spec.ReplicaSpec.TargetReplicas == nil {
		log.Info("Nothing in TargetReplicas")
		return ctrl.Result{}, nil
	}

	jobController := r.JobInterfaceMap[ujob.JobType]

	applyOpts := []client.PatchOption{client.ForceOwnership, client.FieldOwner("unifiedjob-controller")}

	//if jobController.NamesMismatch(ujob) {
	//	jobController.UpdateNames(ctx, ujob)
	//}

	if err := jobController.UpdateStatus(r, ctx, ujob, applyOpts); err != nil {
		log.Info(fmt.Sprintf("Error in updating status: %s.", err.Error()))
		return ctrl.Result{Requeue: true}, nil
	}

	log.Info(fmt.Sprintf("Job %s currently has status %s", ujob.Name, ujob.Status.UnifiedJobStatus))

	zero := int64(0)
	deletepolicy := metav1.DeletePropagationForeground
	deleteOpts := []client.DeleteOption{&client.DeleteOptions{
		GracePeriodSeconds: &zero,
		PropagationPolicy:  &deletepolicy,
	}}

	//check if job completed
	if ujob.Status.UnifiedJobStatus == aiv1alpha1.JobCompleted {
		log.Info("Job has completed.")
		if err := jobController.DeleteAll(r, ctx, ujob, deleteOpts); err != nil {
			log.Info(fmt.Sprintf("Error in cleaning up resources: %s.", err.Error()))
		}
		return ctrl.Result{}, nil
	}

	//may want to treat completed jobs and errored jobs differently
	if ujob.Status.UnifiedJobStatus == aiv1alpha1.JobFailed {
		log.Info("Job has failed.")
		if err := jobController.DeleteAll(r, ctx, ujob, deleteOpts); err != nil {
			log.Info(fmt.Sprintf("Error in cleaning up resources: %s.", err.Error()))
		}
		return ctrl.Result{}, nil
	}

	//no logic for in-job elasticity yet

	//create service if necessary
	if !jobController.ServiceExists(r, ctx) {
		log.Info("Service created.")
		if err := jobController.CreateService(r, ctx, applyOpts, ujob); err != nil {
			log.Info(fmt.Sprintf("Error in creating service: %s.", err.Error()))
		}
		return ctrl.Result{}, nil
	}

	//if the job is just waiting, do nothing
	if len(ujob.Spec.ReplicaSpec.TargetReplicas) == 0 {
		log.Info(fmt.Sprintf("Target Replicas has not been set. Job status is %s", ujob.Status.UnifiedJobStatus))
		return ctrl.Result{}, nil
	}

	// check if job has conflicts in scheduling (podgroup, stuck in pending)
	if jobController.StuckInPending(r, ctx, ujob) {
		log.Info("Job is stuck in pending due to possible scheduling conflicts; " +
			"releasing resources and resetting TargetReplicas.")
		if err := jobController.ReleaseResources(r, ctx, ujob, deleteOpts); err != nil {
			log.Info(fmt.Sprintf("Error in cleaning up resources: %s.", err.Error()))
		}
		if err := r.resetTargetReplicas(ctx, ujob); err != nil {
			log.Info(fmt.Sprintf("Error in resetting target replicas: %s.", err.Error()))
		}
		return ctrl.Result{}, nil
	}

	//check if job is running as expected
	if jobController.IsJobRunning(r, ctx, ujob) {
		log.Info("Job is running as expected.")
		return ctrl.Result{}, nil
	}

	//check if workers are not ready
	if err := jobController.PatchAll(r, ctx, ujob, applyOpts); err != nil {
		log.Info(fmt.Sprintf("Job was unable to be created; %s", err.Error()))
		return ctrl.Result{RequeueAfter: 5}, nil
	}

	return ctrl.Result{}, nil
}

func (r *UnifiedJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	//basejob_controller := &BaseJobController{}
	var basejob_controller UnifiedJobInterface = BaseJobController{}
	r.JobInterfaceMap = map[string]UnifiedJobInterface{
		string(aiv1alpha1.BasicJobType): basejob_controller,
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&aiv1alpha1.UnifiedJob{}).
		Owns(&corev1.Service{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&batchv1.Job{}).
		Complete(r)
}

func (r *UnifiedJobReconciler) resetTargetReplicas(ctx context.Context, ujob aiv1alpha1.UnifiedJob) error {
	ujob.Spec.ReplicaSpec.TargetReplicas = make(map[string]int64)
	return r.Update(ctx, &ujob)
}
