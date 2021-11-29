package controllers

import (
	"context"
	aiv1alpha1 "elastictraining/api/v1alpha1"
	"fmt"
	"reflect"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

type TorchElasticJobController struct {
	etcdSvcName    string
	etcdServerName string
	jobName        string
	workerName     string
	workerSvcName  string
	launcherName   string
	pgName         string
	jobID          string
}

func (r TorchElasticJobController) Test() string {
	return "TorchElasticJobController"
}

func (r TorchElasticJobController) UpdateStatus(reconciler *UnifiedJobReconciler, ctx context.Context, ujob aiv1alpha1.UnifiedJob, applyOpts []client.PatchOption) (bool, error) {
	jobName := fmt.Sprintf(r.jobName, ujob.Name)
	oldStatus := ujob.Status.UnifiedJobStatus
	var newStatus aiv1alpha1.UnifiedJobStatusType

	if ujob.Spec.ReplicaSpec.TargetReplicas == nil {
		newStatus = aiv1alpha1.JobWaiting
	} else {
		workers, err := r.getWorkers(reconciler, ctx, jobName, ujob.Namespace)
		nodeMap := getCurrentNodeConfig(workers)
		if err != nil {
			if errors.IsNotFound(err) {
				newStatus = aiv1alpha1.JobPending
				reconciler.Log.Info("Workers not created yet")
			} else {
				reconciler.Log.Info(fmt.Sprintf("Error in querying workers: %s", err.Error()))
			}
		} else if reflect.DeepEqual(nodeMap, ujob.Spec.ReplicaSpec.TargetReplicas) {
			newStatus = aiv1alpha1.JobMigrating
		} else if len(workers) < getTotalGPU(nodeMap) {
			newStatus = aiv1alpha1.JobPending
		} else {

			launcher, err := r.getJob(reconciler, ctx, ujob.Name, ujob.Namespace)
			if err != nil {
				if errors.IsNotFound(err) {
					newStatus = aiv1alpha1.JobPending
					reconciler.Log.Info("Workers created, launcher not created yet")
				} else {
					reconciler.Log.Info(fmt.Sprintf("Error in querying launcher: %s", err.Error()))
				}
			} else {
				if launcher.Status.Active == 1 {
					newStatus = aiv1alpha1.JobRunning
				} else if launcher.Status.Failed == 1 {
					newStatus = aiv1alpha1.JobFailed
				} else if launcher.Status.Succeeded == 1 {
					newStatus = aiv1alpha1.JobCompleted
				}
			}

			// numPods := len(workers)

			// //create map of status: num pods
			// statusCount := map[corev1.PodPhase]int{
			// 	corev1.PodPending:   0,
			// 	corev1.PodRunning:   0,
			// 	corev1.PodSucceeded: 0,
			// 	corev1.PodFailed:    0,
			// 	corev1.PodUnknown:   0,
			// }

			// for _, pod := range workers {
			// 	statusCount[pod.Status.Phase] += 1
			// }

			// // TODO: less naive status updating
			// if statusCount[corev1.PodSucceeded] == numPods {
			// 	newStatus = aiv1alpha1.JobCompleted
			// } else if statusCount[corev1.PodPending] != 0 {
			// 	newStatus = aiv1alpha1.JobPending
			// } else if statusCount[corev1.PodFailed] != 0 {
			// 	newStatus = aiv1alpha1.JobFailed
			// } else if statusCount[corev1.PodRunning] == numPods {
			// 	newStatus = aiv1alpha1.JobRunning
			// }

		}

	}

	changed := newStatus == oldStatus
	ujob.Status.UnifiedJobStatus = newStatus

	return changed, reconciler.Status().Update(ctx, &ujob)

}

func (r TorchElasticJobController) ReleaseResources(reconciler *UnifiedJobReconciler, ctx context.Context, ujob aiv1alpha1.UnifiedJob, deleteOpts []client.DeleteOption) error {
	//only workers to delete

	//deleteworkers includes the services
	if err := r.deleteWorkers(reconciler, ctx, ujob.Name, ujob.Namespace, deleteOpts); err != nil {
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
	// these are the etcd server and services; different from the headless services that the pods require
	var svc corev1.Service
	svcName := r.etcdSvcName
	key := types.NamespacedName{
		Namespace: ujob.Namespace,
		Name:      svcName,
	}

	if err := reconciler.Get(ctx, key, &svc); err != nil {
		return false
	}

	reconciler.Log.Info("Service exists.")

	var pod corev1.Pod
	podName := r.etcdServerName

	key = types.NamespacedName{
		Namespace: ujob.Namespace,
		Name:      podName,
	}

	if err := reconciler.Get(ctx, key, &pod); err != nil {
		return false
	}

	reconciler.Log.Info("Service pod exists.")
	return true
}

func (r TorchElasticJobController) CreateService(reconciler *UnifiedJobReconciler, ctx context.Context, applyOpts []client.PatchOption, ujob aiv1alpha1.UnifiedJob) error {
	// also corresponsds to the etcd server and not the headless services
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

	//no podgroup

	return nil
}

func (r TorchElasticJobController) StuckInPending(reconciler *UnifiedJobReconciler, ctx context.Context, ujob aiv1alpha1.UnifiedJob) bool {
	//TODO: check if any pod is stuck in pending (race condition)
	//may use podgroup, may not use

	passed := false

	for i := 1; i <= 2; i++ {
		podList, err := r.getWorkers(reconciler, ctx, ujob.Name, ujob.Namespace)

		if err != nil {
			if errors.IsNotFound(err) {
				return false
			}
			reconciler.Log.Info(fmt.Sprintf("Could not find workers: %s", err.Error()))
			return false
		}

		for _, pod := range podList {
			if pod.Status.Phase == corev1.PodPending {
				if !passed {
					time.Sleep(10 * time.Second)
					passed = true
					break
				}
				return true
			}
		}

		if !passed {
			return false
		}

	}

	return false //unreachable

}

func (r TorchElasticJobController) PatchAll(reconciler *UnifiedJobReconciler, ctx context.Context, ujob aiv1alpha1.UnifiedJob, applyOpts []client.PatchOption) error {
	//create and patch the job
	m := ujob.Spec.ReplicaSpec.TargetReplicas

	podList, svcList, err := r.desiredWorkers(reconciler, ctx, ujob, m)
	if err != nil {
		reconciler.Log.Info(fmt.Sprintf("Error in creating workers: %s", err.Error()))
		return err
	}

	for index := range podList {
		if err := reconciler.Patch(ctx, &podList[index], client.Apply, applyOpts...); err != nil {
			reconciler.Log.Info(fmt.Sprintf("Error in patching workers: %s", err.Error()))
			return err
		}

		if err := reconciler.Patch(ctx, &svcList[index], client.Apply, applyOpts...); err != nil {
			reconciler.Log.Info(fmt.Sprintf("Error in patching services: %s", err.Error()))
			return err
		}
	}

	ready, err := r.waitUntilWorkersReady(reconciler, ctx, ujob.Name, ujob.Namespace, ujob.Spec.ReplicaSpec.TargetReplicas)
	if err != nil {
		return err
	}
	if !ready {
		reconciler.Log.Info("Workers are unable to be allocated; not creating launcher")
	}

	reconciler.Log.Info("Workers are all running; creating launcher")

	nodeMap := getCurrentNodeConfig(podList)
	launcher, err := r.desiredLauncher(reconciler, ctx, ujob, nodeMap)
	if err != nil {
		reconciler.Log.Info(fmt.Sprintf("Error in creating launcher: %s", err.Error()))
	}

	if err := reconciler.Patch(ctx, &launcher, client.Apply, applyOpts...); err != nil {
		reconciler.Log.Info(fmt.Sprintf("Error in patching launcher: %s", err.Error()))
		return err
	}

	time.Sleep(1 * time.Second) //sleep to allow API server to update

	return nil
}

//DESIRED OBJECTS ------------------------------------------------------

func (r TorchElasticJobController) desiredService(reconciler *UnifiedJobReconciler, ctx context.Context, ujob aiv1alpha1.UnifiedJob) (corev1.Service, corev1.Pod, error) {
	// creates service for etcd server
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

	// labels := map[string]string{
	// 	"UnifiedEPTJob": ujob.Name,
	// 	"role":          "worker",
	// }

	// //do not setControllerReference as we do not want server to be deleted when epjob is
	// workerService := corev1.Service{
	// 	TypeMeta: metav1.TypeMeta{APIVersion: corev1.SchemeGroupVersion.String(), Kind: "Service"},
	// 	ObjectMeta: metav1.ObjectMeta{
	// 		Name:      fmt.Sprintf(r.workerSvcName, ujob.Name),
	// 		Namespace: ujob.Namespace,
	// 	},
	// 	Spec: corev1.ServiceSpec{
	// 		ClusterIP: "None",
	// 		Selector:  labels,
	// 	},
	// }

	// if err := ctrl.SetControllerReference(&ujob, &workerService, reconciler.Scheme); err != nil {
	// 	return svc, svcPod, nil
	// }

	return svc, svcPod, nil
}

func (r TorchElasticJobController) desiredWorkers(reconciler *UnifiedJobReconciler, ctx context.Context, ujob aiv1alpha1.UnifiedJob, nodeMap map[string]int64) ([]corev1.Pod, []corev1.Service, error) {
	//returns workers as a list of pods that each take 1 GPU
	//currently, all the workers are running the command independently
	//TODO: create ssh system where launcher SSHs into workers

	svcName := r.etcdSvcName
	//svc, err := r.getService(reconciler, ctx, svcName, ujob.Namespace)
	_, err := r.getService(reconciler, ctx, svcName, ujob.Namespace)
	if err != nil {
		reconciler.Log.Info("Service not available for TorchElastic.")
		return []corev1.Pod{}, []corev1.Service{}, err
	}

	sshCommand := "/usr/sbin/sshd -p 12345; mkdir -p /root/.ssh; cp /etc/secrets/* /root/.ssh/; chmod 644 /root/.ssh/authorized_keys; sleep infinity"

	podNum := getTotalGPU(nodeMap)
	podList := make([]corev1.Pod, podNum)
	svcList := make([]corev1.Service, podNum)
	index := 0
	index1 := 0

	for nodeName, numGpu := range nodeMap {
		for index2 := 0; index2 < int(numGpu); index2++ {
			labels := map[string]string{
				"UnifiedEPTJob": ujob.Name,
				"role":          "worker",
				"workerID":      fmt.Sprintf("%d-%d", index1, index2),
			}

			currPod, currSvc, err := r.desiredPod(reconciler, ctx, ujob, sshCommand, labels, index1, index2, nodeName)
			if err != nil {
				reconciler.Log.Info(fmt.Sprintf("Pod on %s with %d GPU unable to be created: %s", nodeName, numGpu, err.Error()))
			}

			podList[index] = currPod
			svcList[index] = currSvc
			index += 1
		}

		index1 += 1

	}

	return podList, svcList, err
}

func (r TorchElasticJobController) desiredPod(reconciler *UnifiedJobReconciler, ctx context.Context, ujob aiv1alpha1.UnifiedJob, podCommand string,
	labels map[string]string, index1 int, index2 int, nodeName string) (corev1.Pod, corev1.Service, error) {

	//returns pod and headless service for pod
	defaultMode := int32(0600)

	pod := corev1.Pod{
		TypeMeta: metav1.TypeMeta{APIVersion: corev1.SchemeGroupVersion.String(), Kind: "Pod"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf(r.workerName, ujob.Name, index1, index2),
			Namespace: ujob.Namespace,
			Labels:    labels,
		},
		Spec: corev1.PodSpec{
			SchedulerName: "default-scheduler", //change if adding other schedulers
			//may want shared memory volume + container if large dataloaders
			Containers: []corev1.Container{
				{
					Name:    "worker",
					Image:   ujob.Spec.JobSpec.Image,
					Command: []string{"/bin/sh"},
					Args: []string{
						"-c",
						podCommand,
					},
					Resources: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							corev1.ResourceName("nvidia.com/gpu"): *resource.NewQuantity(1, resource.DecimalSI),
						},
					},
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "sshkeys",
							MountPath: "/etc/secrets",
						},
					},
					Ports: []corev1.ContainerPort{
						{
							ContainerPort: 22,
							Protocol:      "TCP",
						},
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "sshkeys",
					VolumeSource: corev1.VolumeSource{
						Secret: &corev1.SecretVolumeSource{
							SecretName:  "horovod-sshkeys",
							DefaultMode: &defaultMode,
						},
					},
				},
			},
			NodeName: nodeName,
		},
	}

	//create headless service bound to node as well
	svc := corev1.Service{
		TypeMeta: metav1.TypeMeta{APIVersion: corev1.SchemeGroupVersion.String(), Kind: "Service"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf(r.workerName, ujob.Name, index1, index2),
			Namespace: ujob.Namespace,
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: "None",
			Selector:  labels,
			Ports: []corev1.ServicePort{
				{
					Name:     "ssh-port",
					Port:     22,
					Protocol: "TCP",
				},
			},
		},
	}

	if err := ctrl.SetControllerReference(&ujob, &pod, reconciler.Scheme); err != nil {
		return pod, svc, err
	}

	if err := ctrl.SetControllerReference(&ujob, &svc, reconciler.Scheme); err != nil {
		return pod, svc, err
	}

	return pod, svc, nil

}

func (r TorchElasticJobController) desiredLauncher(reconciler *UnifiedJobReconciler, ctx context.Context, ujob aiv1alpha1.UnifiedJob, nodeMap map[string]int64) (batchv1.Job, error) {
	workerList, err := r.getWorkers(reconciler, ctx, ujob.Name, ujob.Namespace)
	if err != nil {
		return batchv1.Job{}, err
	}

	sshSetupCommand := "mkdir -p /root/.ssh; cp /etc/secrets/* /root/.ssh/; chmod 644 /root/.ssh/authorized_keys;"
	podCommand := r.genPodCommand(ujob)
	sshCommand := ""
	n := len(workerList)

	for i, pod := range workerList {
		sshCommand += fmt.Sprintf("ssh %s %s", string(pod.Status.PodIP), podCommand)
		if i == n-1 {
			sshCommand += ";"
		} else {
			sshCommand += " & "
		}
	}

	//TODO: need to modify podcommand
	sshkeysMode := int32(0600)
	one := int32(1)

	job := batchv1.Job{
		TypeMeta: metav1.TypeMeta{APIVersion: batchv1.SchemeGroupVersion.String(), Kind: "Job"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf(r.launcherName, ujob.Name),
			Namespace: ujob.Namespace,
			Labels: map[string]string{
				"UnifiedEPTJob": ujob.Name,
				"role":          "launcher",
			},
		},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					SchedulerName: "default-scheduler", //change if adding additional schedulers
					Volumes: []corev1.Volume{
						{
							Name: "sshkeys",
							VolumeSource: corev1.VolumeSource{
								Secret: &corev1.SecretVolumeSource{
									SecretName:  "horovod-sshkeys",
									DefaultMode: &sshkeysMode,
								},
							},
						},
					},
					Containers: []corev1.Container{
						{
							Name:  "launcher",
							Image: "davidluzhu/ubuntu-ssh:latest", //ubuntu image for ssh command
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "sshkeys",
									MountPath: "/etc/secrets",
								},
							},
							Command: []string{"/bin/sh"},
							Args: []string{
								"-c",
								fmt.Sprintf("%s %s", sshSetupCommand, sshCommand),
							},
						},
					},
					RestartPolicy: corev1.RestartPolicyNever,
				},
			},
			BackoffLimit: &one,
		},
	}

	if err := ctrl.SetControllerReference(&ujob, &job, reconciler.Scheme); err != nil {
		return job, err
	}

	return job, nil
}

//GET OBJECTS	------------------------------------------------------

func (r TorchElasticJobController) getService(reconciler *UnifiedJobReconciler, ctx context.Context, name string, namespace string) (corev1.Service, error) {
	//generic get service (can be used for etcd service and pod service)
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

func (r TorchElasticJobController) getWorkers(reconciler *UnifiedJobReconciler, ctx context.Context, name string, namespace string) ([]corev1.Pod, error) {
	labels := map[string]string{
		"UnifiedEPTJob": name,
		"role":          "worker",
	}

	return reconciler.getPodsByLabel(ctx, namespace, labels)
}

func (r TorchElasticJobController) getJob(reconciler *UnifiedJobReconciler, ctx context.Context, name string, namespace string) (batchv1.Job, error) {
	var job batchv1.Job
	key := types.NamespacedName{
		Namespace: namespace,
		Name:      fmt.Sprintf(r.launcherName, name),
	}

	if err := reconciler.Get(ctx, key, &job); err != nil {
		return job, err
	}

	return job, nil

}

//DELETE OBJECTS------------------------------------------------------

func (r TorchElasticJobController) deleteWorkers(reconciler *UnifiedJobReconciler, ctx context.Context, name, namespace string, deleteOpts []client.DeleteOption) error {
	//deletes workers and all the headless services for the workers
	workers, err := r.getWorkers(reconciler, ctx, name, namespace)
	if err != nil {
		reconciler.Log.Info("Unable to obtain Workers (Pod List)")
		return nil
	}

	for _, pod := range workers {
		if err := reconciler.Delete(ctx, &pod, deleteOpts...); err != nil {
			reconciler.Log.Info(fmt.Sprintf("Unable to delete pod %s", pod.Name))
			return err
		}

		podSvc, err := r.getService(reconciler, ctx, pod.Name, pod.Namespace)
		if err != nil {
			reconciler.Log.Info(fmt.Sprintf("Unable to find Pod Service for pod %s", pod.Name))
			return err
		}

		if err := reconciler.Delete(ctx, &podSvc, deleteOpts...); err != nil {
			reconciler.Log.Info(fmt.Sprintf("Unable to delete pod service %s", podSvc.Name))
			return err
		}
	}

	return nil
}

//HELPER FUNCTIONS---------------------------------------------------

func (r TorchElasticJobController) genPodCommand(ujob aiv1alpha1.UnifiedJob) string {
	//generate individual pod command for each pod; launcher will ssh into every pod and launch
	jobID := fmt.Sprintf(r.jobID, ujob.Name)
	port := 2379
	rdzv_id := jobID
	rdzv_backend := "etcd"
	//rdzv_endpoint := fmt.Sprintf("%s:%s", svc.Spec.ClusterIP, port)
	rdzv_endpoint := fmt.Sprintf("torchelastic-etcd-service:%d", port)
	nproc_per_node := 1

	launchCommand := "python -m torchelastic.distributed.launch"
	launchArgs := fmt.Sprintf("--nnodes %d:%d --nproc_per_node %d --rdzv_id %s --rdzv_backend %s --rdzv_endpoint %s",
		*ujob.Spec.ReplicaSpec.MinReplicas, *ujob.Spec.ReplicaSpec.MaxReplicas, nproc_per_node, rdzv_id, rdzv_backend, rdzv_endpoint)
	pythonCommand := strings.Join(ujob.Spec.JobSpec.PythonCommand, " ")
	podCommand := fmt.Sprintf("%s %s %s", launchCommand, launchArgs, pythonCommand)

	return podCommand
}

func (r TorchElasticJobController) areWorkersReady(podList []corev1.Pod, nodeConfig map[string]int64) bool {
	if len(podList) != getTotalGPU(nodeConfig) {
		return false
	}
	for _, pod := range podList {
		if pod.Status.Phase != corev1.PodRunning {
			return false
		}
	}
	return true
}

func (r TorchElasticJobController) waitUntilWorkersReady(reconciler *UnifiedJobReconciler, ctx context.Context, name string, namespace string, nodeConfig map[string]int64) (bool, error) {
	time.Sleep(1 * time.Second)
	startTime := time.Now()
	for {
		if time.Since(startTime) > 30*time.Second {
			break
		}
		podList, err := r.getWorkers(reconciler, ctx, name, namespace)
		if err != nil {
			if errors.IsNotFound(err) {
				time.Sleep(1 * time.Second)
				continue
			}
			reconciler.Log.Info(fmt.Sprintf("Error in fetching workers: %s", err.Error()))
			return false, err
		}
		if r.areWorkersReady(podList, nodeConfig) {
			return true, nil
		}
		time.Sleep(1 * time.Second)
	}

	return false, nil
}
