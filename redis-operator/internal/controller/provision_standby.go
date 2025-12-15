package controller

import (
	"context"
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	appv1 "github.com/myuser/redis-operator/api/v1"
)

// checkProvisioningStatus adds new standby pods to the Redis cluster.
// It waits for the new standby pods to be ready, then creates a job to join them to the cluster.
func (r *RedisClusterReconciler) checkProvisioningStatus(ctx context.Context, cluster *appv1.RedisCluster) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Calculate the new standby indices
	newStandbyIndex := cluster.Spec.Masters * (1 + cluster.Spec.ReplicasPerMaster)
	newStandbyPod := fmt.Sprintf("%s-%d", cluster.Name, newStandbyIndex)

	// Check if new standby pod is ready
	standbyPod := &corev1.Pod{}
	err := r.Get(ctx, client.ObjectKey{Name: newStandbyPod, Namespace: cluster.Namespace}, standbyPod)
	if err != nil {
		if errors.IsNotFound(err) {
			logger.Info("New standby pod not yet created, waiting",
				"standbyPod", newStandbyPod)
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
		return ctrl.Result{}, err
	}

	if standbyPod.Status.Phase != corev1.PodRunning {
		logger.Info("New standby pod not yet running, waiting",
			"standbyPod", newStandbyPod,
			"phase", standbyPod.Status.Phase)
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// Check if pod is ready
	podReady := false
	for _, condition := range standbyPod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			podReady = true
			break
		}
	}

	if !podReady {
		logger.Info("New standby pod not yet ready, waiting",
			"standbyPod", newStandbyPod)
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	logger.Info("New standby pod is ready, checking for join-nodes job",
		"standbyPod", newStandbyPod,
		"podIP", standbyPod.Status.PodIP)

	// Check for join-nodes job
	joinJob := &batchv1.Job{}
	jobName := cluster.Name + "-join-nodes"
	err = r.Get(ctx, client.ObjectKey{Name: jobName, Namespace: cluster.Namespace}, joinJob)

	if err != nil && errors.IsNotFound(err) {
		logger.Info("Creating join-nodes job to add new pods to cluster")

		job := r.joinNodesJobForRedisCluster(cluster)
		if err := controllerutil.SetControllerReference(cluster, job, r.Scheme); err != nil {
			logger.Error(err, "Failed to set owner reference on join-nodes job")
			return ctrl.Result{}, err
		}
		if err := r.Create(ctx, job); err != nil {
			logger.Error(err, "Failed to create join-nodes job")
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil

	} else if err != nil {
		logger.Error(err, "Failed to get join-nodes job")
		return ctrl.Result{}, err
	}

	// Check job status
	if joinJob.Status.Succeeded > 0 {
		logger.Info("Join-nodes job succeeded, finalizing provisioning")

		// Update standby pod in status and clear provisioning flag
		cluster.Status.StandbyPod = newStandbyPod
		cluster.Status.IsProvisioningStandby = false

		if err := r.Status().Update(ctx, cluster); err != nil {
			logger.Error(err, "Failed to update status after provisioning")
			return ctrl.Result{}, err
		}

		_ = r.Delete(ctx, joinJob, client.PropagationPolicy(metav1.DeletePropagationBackground))
		logger.Info("Successfully provisioned new standby",
			"standbyPod", newStandbyPod)

		return ctrl.Result{Requeue: true}, nil
	}

	if joinJob.Status.Failed > 0 {
		logger.Error(fmt.Errorf("join-nodes job %s failed", jobName), "Failed to join nodes")
		// Clean up the failed job to allow a retry
		_ = r.Delete(ctx, joinJob, client.PropagationPolicy(metav1.DeletePropagationBackground))
		cluster.Status.IsProvisioningStandby = false
		if err := r.Status().Update(ctx, cluster); err != nil {
			logger.Error(err, "Failed to update status after failed join")
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	logger.Info("Join-nodes job is still running...")
	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
}
