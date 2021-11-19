package controllers

import (
	"context"
	aiv1alpha1 "elastictraining/api/v1alpha1"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	sigsv1alpha1 "sigs.k8s.io/scheduler-plugins/pkg/apis/scheduling/v1alpha1"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

type TorchElasticJobController struct {
	etcdSvcName    string
	etcdServerName string
	jobName        string
	workerName     string
	pgName         string
	jobID          string
}

func (r TorchElasticJobController) Test() string {
	return "TorchElasticJobController"
}

func (r TorchElasticJobController) Init() {
	r.etcdSvcName = "torchelastic-etcd-service"
	r.etcdServerName = "etcd"
	r.jobName = "epjob-%s"
	r.jobID = "epjobid-%s"
	r.workerName = "epworkers-%s"
	r.pgName = "eppodgroup-%s"
}

func (r TorchElasticJobController) UpdateStatus(reconciler *UnifiedJobReconciler, ctx context.Context, ujob aiv1alpha1.UnifiedJob, applyOpts []client.PatchOption) error {
	jobName := fmt.Sprintf(r.jobName, ujob.Name)

	//check statefulset status
	if ujob.Spec.ReplicaSpec.TargetReplicas == nil {
		ujob.Status.UnifiedJobStatus = aiv1alpha1.JobWaiting
	} else {
		job, err := r.getWorkers(reconciler, ctx, jobName, ujob.Namespace)
		if err != nil {
			ujob.Status.UnifiedJobStatus = aiv1alpha1.JobWaiting
			if errors.IsNotFound(err) {
				reconciler.Log.Info("Job not found (not ready).")
			} else {
				reconciler.Log.Info(fmt.Sprintf("Error in querying workers: %s.", err.Error()))
			}
		} else {
			var numGpu int64
			for _, v := range ujob.Spec.ReplicaSpec.TargetReplicas {
				numGpu = v
			}

			//TODO: accurate status
			if job.Status.ReadyReplicas == int32(numGpu) {
				ujob.Status.UnifiedJobStatus = aiv1alpha1.JobRunning
			}

			// if job.Status.Succeeded == 1 {
			// 	ujob.Status.UnifiedJobStatus = aiv1alpha1.JobCompleted
			// } else if job.Status.Failed == 1 {
			// 	ujob.Status.UnifiedJobStatus = aiv1alpha1.JobFailed
			// } else if job.Status.Active == 1 {
			// 	ujob.Status.UnifiedJobStatus = aiv1alpha1.JobRunning
			// }

		}

	}

	return reconciler.Status().Update(ctx, &ujob)

}

func (r TorchElasticJobController) ReleaseResources(reconciler *UnifiedJobReconciler, ctx context.Context, ujob aiv1alpha1.UnifiedJob, deleteOpts []client.DeleteOption) error {
	//only workers to delete
	workersName := fmt.Sprintf(r.workerName, ujob.Name)

	if err := r.deleteWorkers(reconciler, ctx, workersName, ujob.Namespace, deleteOpts); err != nil {
		return err
	}

	return nil
}

func (r TorchElasticJobController) DeleteAll(reconciler *UnifiedJobReconciler, ctx context.Context, ujob aiv1alpha1.UnifiedJob, deleteOpts []client.DeleteOption) error {
	//same as releaseresources
	//etcd server should persist
	return r.ReleaseResources(reconciler, ctx, ujob, deleteOpts)
}

func (r TorchElasticJobController) ServiceExists(reconciler *UnifiedJobReconciler, ctx context.Context, ujob aiv1alpha1.UnifiedJob) bool {
	//only make it once
	var svc corev1.Service
	svcName := r.etcdSvcName
	key := types.NamespacedName{
		Namespace: ujob.Namespace,
		Name:      svcName,
	}

	if err := reconciler.Get(ctx, key, &svc); err != nil {
		return false
	}

	return true
}

func (r TorchElasticJobController) CreateService(reconciler *UnifiedJobReconciler, ctx context.Context, applyOpts []client.PatchOption, ujob aiv1alpha1.UnifiedJob) error {
	svc, svcPod, err := r.desiredService(reconciler, ctx, ujob)
	if err != nil {
		reconciler.Log.Info(fmt.Sprintf("Error in declaring Service: %s", err.Error()))
		return err
	}

	if err := reconciler.Patch(ctx, &svc, client.Apply, applyOpts...); err != nil {
		reconciler.Log.Info(fmt.Sprintf("Error in patching etcd service: %s", err.Error()))
		return err
	}

	if err := reconciler.Patch(ctx, &svcPod, client.Apply, applyOpts...); err != nil {
		reconciler.Log.Info(fmt.Sprintf("Error in patching etcd server: %s", err.Error()))
		return err
	}

	//no podgroup for now
	/*
		pg, err := r.desiredPodGroup(reconciler, ctx, ujob)
		if err != nil {
			//reconciler.Log.Info(fmt.Sprintf("Error in creating pod group: %s", err))
			return err
		}

		if err := reconciler.Patch(ctx, &pg, client.Apply, applyOpts...); err != nil {
			return err
		}
	*/

	return nil
}

func (r TorchElasticJobController) StuckInPending(reconciler *UnifiedJobReconciler, ctx context.Context, ujob aiv1alpha1.UnifiedJob) bool {

	return false

	// job, err := r.getJob(ctx, fmt.Sprintf("unifiedjob-%s", ujob.Name), ujob.Namespace, reconciler)
	// if err != nil {
	// 	if errors.IsNotFound(err) {
	// 		reconciler.Log.Info("Job not found.")
	// 	} else {
	// 		reconciler.Log.Info(fmt.Sprintf("Job error: %s", err))
	// 	}
	// 	return job, err
	// }

	// if job.Status.Failed == 1 {
	// 	time.Sleep(10 * time.Second)
	// 	return true
	// }

	// return false

}

func (r TorchElasticJobController) IsJobRunning(reconciler *UnifiedJobReconciler, ctx context.Context, ujob aiv1alpha1.UnifiedJob) bool {

	var numGpu int64
	for _, v := range ujob.Spec.ReplicaSpec.TargetReplicas {
		numGpu = v
	}

	workersName := fmt.Sprintf(r.workerName, ujob.Name)
	ss, err := r.getWorkers(reconciler, ctx, workersName, ujob.Namespace)
	if err != nil {
		reconciler.Log.Info(fmt.Sprintf("Job error: %s", err.Error()))
		return false
	}

	return ss.Status.ReadyReplicas == int32(numGpu)

}

func (r TorchElasticJobController) PatchAll(reconciler *UnifiedJobReconciler, ctx context.Context, ujob aiv1alpha1.UnifiedJob, applyOpts []client.PatchOption) error {
	//create and patch the job
	// TODO: Complete
	m := ujob.Spec.ReplicaSpec.TargetReplicas
	if len(m) != 1 {
		reconciler.Log.Info("Target Replicas is incorrect ")
	}

	var numGpu int64
	var nodeName string
	for k, v := range m {
		numGpu = v
		nodeName = k
	}

	ss, err := r.desiredWorkers(reconciler, ctx, ujob, numGpu, nodeName)
	if err != nil {
		reconciler.Log.Info(fmt.Sprintf("Error in creating workers: %s.", err.Error()))
		return err
	}

	if err := reconciler.Patch(ctx, &ss, client.Apply, applyOpts...); err != nil {
		reconciler.Log.Info(fmt.Sprintf("Error in patching workers: %s.", err.Error()))
		return err
	}

	//TODO: add breaks to check if desired workers are ready

	return nil
}

//DESIRED OBJECTS ------------------------------------------------------

func (r TorchElasticJobController) desiredService(reconciler *UnifiedJobReconciler, ctx context.Context, ujob aiv1alpha1.UnifiedJob) (corev1.Service, corev1.Pod, error) {

	svcName := r.etcdSvcName
	etcdName := r.etcdServerName
	svc := corev1.Service{
		TypeMeta: metav1.TypeMeta{APIVersion: corev1.SchemeGroupVersion.String(), Kind: "Service"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      svcName,
			Namespace: ujob.Namespace,
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{
					Name:     "etcd-client-port",
					Port:     2379,
					Protocol: "TCP",
				},
			},
			Selector: map[string]string{"app": "etcd"},
		},
	}

	svcPod := corev1.Pod{
		TypeMeta: metav1.TypeMeta{APIVersion: corev1.SchemeGroupVersion.String(), Kind: "Pod"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      etcdName,
			Namespace: ujob.Namespace,
			Labels:    map[string]string{"app": etcdName},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  etcdName,
					Image: "quay.io/coreos/etcd:latest",
					Command: []string{
						"usr/local/bin/etcd",
						"--data-dir",
						"/var/lib/etcd",
						"--enable-v2",
						"--listen-client-urls",
						"http://0.0.0.0:2379",
						"--advertise-client-urls",
						"http://0.0.0.0:2379",
						"--initial-cluster-state",
						"new",
					},
					Ports: []corev1.ContainerPort{
						{
							Name:          "client",
							ContainerPort: 2379,
							Protocol:      "TCP",
						},
						{
							Name:          "server",
							ContainerPort: 2380,
							Protocol:      "TCP",
						},
					},
				},
			},
			//RestartPolicy: corev1.RestartPolicyAlways,
		},
	}

	if err := ctrl.SetControllerReference(&ujob, &svc, reconciler.Scheme); err != nil {
		return svc, svcPod, err
	}

	if err := ctrl.SetControllerReference(&ujob, &svcPod, reconciler.Scheme); err != nil {
		return svc, svcPod, err
	}

	return svc, svcPod, nil
}

func (r TorchElasticJobController) desiredPodGroup(reconciler *UnifiedJobReconciler, ctx context.Context, ujob aiv1alpha1.UnifiedJob) (sigsv1alpha1.PodGroup, error) {
	pgName := r.pgName

	pg := sigsv1alpha1.PodGroup{
		TypeMeta: metav1.TypeMeta{APIVersion: "scheduling.sigs.k8s.io/v1alpha1", Kind: "PodGroup"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      pgName,
			Namespace: ujob.Namespace,
		},
		Spec: sigsv1alpha1.PodGroupSpec{
			MinMember: 0, //*ehjob.Spec.WorkersSpec.MinReplicas
			MinResources: &corev1.ResourceList{
				corev1.ResourceName("nvidia.com/gpu"): *resource.NewQuantity(0, resource.DecimalSI),
			},
		},
		Status: sigsv1alpha1.PodGroupStatus{
			ScheduleStartTime: metav1.Now(),
		},
	}

	if err := ctrl.SetControllerReference(&ujob, &pg, reconciler.Scheme); err != nil {
		return pg, err
	}

	return pg, nil
}

func (r TorchElasticJobController) desiredWorkers(reconciler *UnifiedJobReconciler, ctx context.Context, ujob aiv1alpha1.UnifiedJob, numGpus int64, nodeName string) (appsv1.StatefulSet, error) {
	//TODO: for now it is just a single node
	//TODO: patch podgroup as well
	// the desired job in this case is simply the workers

	svcName := r.etcdSvcName
	svc, err := r.getService(reconciler, ctx, svcName, ujob.Namespace)
	if err != nil {
		reconciler.Log.Info("Service not available for TorchElastic.")
		return appsv1.StatefulSet{}, err
	}

	numGpus2 := int32(numGpus)
	workersName := fmt.Sprintf(r.workerName, ujob.Name)
	jobID := fmt.Sprintf(r.jobID, ujob.Name)
	port := "2379"
	rdzv_id := jobID
	rdzv_backend := "etcd"
	rdzv_endpoint := fmt.Sprintf("%s:%s", svc.Spec.ClusterIP, port)
	nnodes := numGpus
	nproc_per_node := 1
	//podGroupName := fmt.Sprintf("unifiedjobpodgroup-%s", ujob.Name)

	launchCommand := "python -m torchelastic.distributed.launch"
	launchArgs := fmt.Sprintf("--nnodes %d --nproc_per_node %d --rdzv_id %s --rdzv_backend %s --rdzv_endpoint %s",
		nnodes, nproc_per_node, rdzv_id, rdzv_backend, rdzv_endpoint)
	pythonCommand := strings.Join(ujob.Spec.JobSpec.PythonCommand, " ")

	statefulset := appsv1.StatefulSet{
		TypeMeta: metav1.TypeMeta{APIVersion: appsv1.SchemeGroupVersion.String(), Kind: "StatefulSet"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      workersName,
			Namespace: ujob.Namespace,
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName:         svcName,
			Replicas:            &numGpus2,
			PodManagementPolicy: appsv1.PodManagementPolicyType(string(appsv1.ParallelPodManagement)),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"UnifiedEPTJob": ujob.Name, "role": "worker"},
			},
			Template: corev1.PodTemplateSpec{
				//no podgroup
				ObjectMeta: metav1.ObjectMeta{ //add labels such as podgroup # and statefulset
					Labels: map[string]string{"UnifiedEPTJob": ujob.Name, "role": "worker"},
				},
				Spec: corev1.PodSpec{
					SchedulerName: "default-scheduler", //change if adding other schedulers
					Volumes: []corev1.Volume{
						//shared memory for pytorch
						{
							Name: "dshm",
							VolumeSource: corev1.VolumeSource{
								EmptyDir: &corev1.EmptyDirVolumeSource{
									Medium: "Memory",
								},
							},
						},
					},
					Containers: []corev1.Container{
						{
							Name:  "worker",
							Image: ujob.Spec.JobSpec.Image,
							VolumeMounts: []corev1.VolumeMount{
								//add shared memory
								{
									Name:      "dshm",
									MountPath: "/dev/shm",
								},
							},
							Command: []string{"/bin/sh"},
							Args: []string{
								"-c",
								fmt.Sprintf("%s %s %s", launchCommand, launchArgs, pythonCommand),
							},
							Resources: corev1.ResourceRequirements{
								Limits: corev1.ResourceList{
									corev1.ResourceName("nvidia.com/gpu"): *resource.NewQuantity(1, resource.DecimalSI),
								},
							},
						},
					},
					NodeName: nodeName,
				},
			},
		},
	}

	if err := ctrl.SetControllerReference(&ujob, &statefulset, reconciler.Scheme); err != nil {
		return statefulset, err
	}

	return statefulset, nil
}

//GET OBJECTS	------------------------------------------------------

func (r TorchElasticJobController) getService(reconciler *UnifiedJobReconciler, ctx context.Context, name string, namespace string) (corev1.Service, error) {
	var svc corev1.Service
	key := types.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}

	if err := reconciler.Get(ctx, key, &svc); err != nil {
		return svc, err
	}

	return svc, nil
}

func (r TorchElasticJobController) getWorkers(reconciler *UnifiedJobReconciler, ctx context.Context, name string, namespace string) (appsv1.StatefulSet, error) {
	var ss appsv1.StatefulSet
	key := types.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}

	if err := reconciler.Get(ctx, key, &ss); err != nil {
		return ss, err
	}

	return ss, nil
}

func (r TorchElasticJobController) getPodGroup(reconciler *UnifiedJobReconciler, ctx context.Context, name string, namespace string) (sigsv1alpha1.PodGroup, error) {
	var pg sigsv1alpha1.PodGroup
	key := types.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}

	if err := reconciler.Get(ctx, key, &pg); err != nil {
		return pg, err
	}

	return pg, nil
}

//DELETE OBJECTS------------------------------------------------------

func (r TorchElasticJobController) deleteService(reconciler *UnifiedJobReconciler, ctx context.Context, name, namespace string, deleteOpts []client.DeleteOption) error {

	svc, err := r.getService(reconciler, ctx, name, namespace)
	if err != nil {
		return nil
	}

	return reconciler.Delete(ctx, &svc, deleteOpts...)
}

// func (r TorchElasticJobController) deleteJob(reconciler *UnifiedJobReconciler, ctx context.Context, name, namespace string, deleteOpts []client.DeleteOption) error {

// 	job, err := r.getJob(reconciler, ctx, name, namespace)
// 	if err != nil {
// 		return nil
// 	}

// 	return reconciler.Delete(ctx, &job, deleteOpts...)
// }

func (r TorchElasticJobController) deleteWorkers(reconciler *UnifiedJobReconciler, ctx context.Context, name, namespace string, deleteOpts []client.DeleteOption) error {

	workers, err := r.getWorkers(reconciler, ctx, name, namespace)
	if err != nil {
		return nil
	}

	return reconciler.Delete(ctx, &workers, deleteOpts...)
}

// func (r TorchElasticJobController) deletePodGroup(reconciler *UnifiedJobReconciler, ctx context.Context, name, namespace string, deleteOpts []client.DeleteOption) error {

// 	pg, err := r.getJob(reconciler, ctx, name, namespace)
// 	if err != nil {
// 		return nil
// 	}

// 	return reconciler.Delete(ctx, &pg, deleteOpts...)
// }
