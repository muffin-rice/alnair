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
	aiv1alpha1 "elastictraining/api/v1alpha1"
	"fmt"
	"sort"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/common/log"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const TIME_RESCHEDULING = 10 //time in minutes for a rescheduling
const TIME_REQUEUE = 10      //time in seconds for a failed requeue

// UnifiedSchedulerReconciler sets the TargetReplicas of a Unified Job
type UnifiedSchedulerReconciler struct {
	client.Client
	Log       logr.Logger
	Scheme    *runtime.Scheme
	timestamp time.Time
}

func (r *UnifiedSchedulerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// TODO: Check for conflicts in the scheduling
	log := r.Log.WithValues("UnifiedJob", req.NamespacedName)
	log.Info("Start scheduling")

	var ujob aiv1alpha1.UnifiedJob
	if err := r.Get(ctx, req.NamespacedName, &ujob); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	//rescheduling to find global optima
	if time.Now().Sub(r.timestamp).Minutes() > TIME_RESCHEDULING {

		nodeMap := r.TotalNodeMap(ctx)
		log.Info("Rescheduling Jobs")
		listOpts := []client.ListOption{client.InNamespace(ujob.Namespace)}
		queuedJobs, runningJobs := r.BuildJobLists(ctx, listOpts)
		r.RescheduleJobs(nodeMap, queuedJobs, runningJobs)
		r.timestamp = time.Now()

		return ctrl.Result{RequeueAfter: TIME_REQUEUE * time.Second}, nil
	}

	if ujob.Spec.ReplicaSpec.TargetReplicas != nil {

		log.Info("Target replicas already specified, do nothing")
		return ctrl.Result{RequeueAfter: TIME_REQUEUE * time.Second}, nil
	}

	//New job, find available resources
	nodeMap, sortedKeys := r.AllocatableNodeMap(ctx)

	//find possible nodes
	//var totalAllocatable int64 = 0

	// TODO: add partial allocation
	//commented because no partial allocation yet

	//criteria is 1. fulfills maxreplicas 2. num left
	currNodeConfig := make(map[string]int64)

	for _, nodeName := range sortedKeys {
		if nodeMap[nodeName] > *ujob.Spec.ReplicaSpec.MaxReplicas {
			currNodeConfig = map[string]int64{nodeName: *ujob.Spec.ReplicaSpec.MaxReplicas}
			continue
		}
		if nodeMap[nodeName] < *ujob.Spec.ReplicaSpec.MaxReplicas {
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		currNodeConfig[nodeName] = nodeMap[nodeName]
		break
	}

	if err := r.updateTargetReplicas(ctx, ujob, currNodeConfig); err != nil {
		return ctrl.Result{RequeueAfter: TIME_REQUEUE * time.Second}, nil
	}

	// cannot find enough GPU even after preemption, wait for some time and try again
	return ctrl.Result{RequeueAfter: TIME_REQUEUE * time.Second}, nil
}

func (r *UnifiedSchedulerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	//set up clock
	r.timestamp = time.Now()

	return ctrl.NewControllerManagedBy(mgr).
		For(&aiv1alpha1.UnifiedJob{}).
		Complete(r)
}

func (r *UnifiedSchedulerReconciler) TotalNodeMap(ctx context.Context) map[string]int64 {
	//returns a NodeName:numGpu map
	var nodeList corev1.NodeList
	_ = r.List(ctx, &nodeList)

	var nodeMap map[string]int64

	for _, node := range nodeList.Items {
		gpus := node.Status.Capacity["nvidia.com/gpu"]
		numGpus, ok := gpus.AsInt64()
		if ok {
			nodeMap[node.ObjectMeta.Name] = numGpus
		}
	}

	return nodeMap
}

type kvPair struct {
	key   string
	value int64
}

type keySorter []kvPair

func (k keySorter) Len() int           { return len(k) }
func (k keySorter) Swap(i, j int)      { k[i], k[j] = k[j], k[i] }
func (k keySorter) Less(i, j int) bool { return k[i].value > k[j].value }

func (r *UnifiedSchedulerReconciler) AllocatableNodeMap(ctx context.Context) (map[string]int64, []string) {
	//returns a NodeName:numGpu map
	var nodeList corev1.NodeList
	_ = r.List(ctx, &nodeList)

	var nodeMap map[string]int64

	for _, node := range nodeList.Items {
		gpus := node.Status.Allocatable["nvidia.com/gpu"]
		numGpus, ok := gpus.AsInt64()
		if ok {
			nodeMap[node.ObjectMeta.Name] = numGpus
		}
	}

	//use keySorter instead

	var sortedKeys keySorter = make([]kvPair, 0, len(nodeMap))
	sortedKeyList := make([]string, 0, len(nodeMap))
	i := 0
	for k := range nodeMap {
		sortedKeys[i] = kvPair{k, nodeMap[k]}
		i++
	}

	sort.Sort(sortedKeys)

	for i, kv := range sortedKeys {
		sortedKeyList[i] = kv.key
	}

	return nodeMap, sortedKeyList
}

func (r *UnifiedSchedulerReconciler) BuildJobLists(ctx context.Context, listOpts []client.ListOption) ([]aiv1alpha1.UnifiedJob, []aiv1alpha1.UnifiedJob) {
	//returns a list of 1. Queued jobs 2. Running jobs
	//might want one of migrating jobs?
	var queuedJobs []aiv1alpha1.UnifiedJob
	var runningJobs []aiv1alpha1.UnifiedJob

	var ujobList aiv1alpha1.UnifiedJobList
	_ = r.List(ctx, &ujobList, listOpts...)

	for _, ujob := range ujobList.Items {
		if ujob.Status.UnifiedJobStatus == "Completed" {
			continue
		}

		if ujob.Spec.ReplicaSpec.TargetReplicas == nil {
			queuedJobs = append(queuedJobs, ujob)
		} else {
			runningJobs = append(runningJobs, ujob)
		}
	}

	return queuedJobs, runningJobs
}

func (r *UnifiedSchedulerReconciler) RescheduleJobs(nodeMap map[string]int64, queuedJobList []aiv1alpha1.UnifiedJob,
	runningJobList []aiv1alpha1.UnifiedJob) error {
	//Reallocates jobs
	//sort jobs by
	//do simple nothing
	return nil
}

func (r *UnifiedSchedulerReconciler) updateTargetReplicas(ctx context.Context, ujob aiv1alpha1.UnifiedJob, nodeConfig map[string]int64) error {
	ujob.Spec.ReplicaSpec.TargetReplicas = nodeConfig
	if err := r.Update(ctx, &ujob); err != nil {
		log.Info(fmt.Sprintf("Error in updating target replicas for ElasticHorovodJob %s/%s: %s.",
			ujob.Namespace, ujob.Name, err.Error()))
		return err
	}
	return nil
}
