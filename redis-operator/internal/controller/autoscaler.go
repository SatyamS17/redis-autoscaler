package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/api"
	prometheusv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	appv1 "github.com/myuser/redis-operator/api/v1" // Update this import path!
)

// PromServiceURL is the hardcoded location of your Prometheus server.
// TODO: Make this configurable!
const PromServiceURL = "http://prometheus-operated.monitoring.svc:9090"

// handleAutoScaling is the main entry point for the autoscaler logic.
// It's a "state machine" that checks if we are monitoring, scaling, or resharding.
func (r *RedisClusterReconciler) handleAutoScaling(ctx context.Context, cluster *appv1.RedisCluster) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// --- STATE 1: Resharding ---
	// We are currently in the middle of a scaling operation.
	if cluster.Status.IsResharding {
		logger.Info("Cluster is currently resharding. Checking status.")
		return r.checkReshardingStatus(ctx, cluster)
	}

	// --- STATE 2: Monitoring ---
	// We are stable and monitoring Prometheus for CPU usage.
	logger.Info("Cluster is stable. Monitoring CPU usage.")
	return r.monitorCPUUsage(ctx, cluster)
}

// checkReshardingStatus checks if the new pods are ready and if the reshard job is complete.
func (r *RedisClusterReconciler) checkReshardingStatus(ctx context.Context, cluster *appv1.RedisCluster) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// 1. Check if the StatefulSet has been updated and all new pods are ready.
	sts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, sts); err != nil {
		logger.Error(err, "Failed to get StatefulSet for reshard check")
		return ctrl.Result{}, err
	}

	desiredTotalReplicas := cluster.Spec.Masters * (1 + cluster.Spec.ReplicasPerMaster)
	if sts.Status.ReadyReplicas != desiredTotalReplicas {
		logger.Info("Resharding: Waiting for new pods to be ready", "Ready", sts.Status.ReadyReplicas, "Desired", desiredTotalReplicas)
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	// 2. Pods are ready. Now check the resharding Job.
	reshardJob := &batchv1.Job{}
	jobName := cluster.Name + "-reshard"
	err := r.Get(ctx, client.ObjectKey{Name: jobName, Namespace: cluster.Namespace}, reshardJob)

	if err != nil && errors.IsNotFound(err) {
		// Job not found, create it
		logger.Info("New pods are ready. Creating reshard Job.")
		job := r.reshardJobForRedisCluster(cluster)
		if err := controllerutil.SetControllerReference(cluster, job, r.Scheme); err != nil {
			logger.Error(err, "Failed to set owner ref on reshard Job")
			return ctrl.Result{}, err
		}
		if err := r.Create(ctx, job); err != nil {
			logger.Error(err, "Failed to create reshard Job")
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil // Requeue to check job status

	} else if err != nil {
		logger.Error(err, "Failed to get reshard Job")
		return ctrl.Result{}, err
	}

	// 3. Job exists. Check its status.
	if reshardJob.Status.Succeeded > 0 {
		logger.Info("Reshard Job succeeded. Unlocking autoscaler.")
		cluster.Status.IsResharding = false // Unlock the state machine
		if err := r.Status().Update(ctx, cluster); err != nil {
			logger.Error(err, "Failed to update status after reshard")
			return ctrl.Result{}, err
		}
		// Clean up the successful job
		_ = r.Delete(ctx, reshardJob, client.PropagationPolicy(metav1.DeletePropagationBackground))
		return ctrl.Result{}, nil
	}

	if reshardJob.Status.Failed > 0 {
		logger.Error(fmt.Errorf("reshard job %s failed", jobName), "Resharding failed")
		// Clean up the failed job to allow a retry
		_ = r.Delete(ctx, reshardJob, client.PropagationPolicy(metav1.DeletePropagationBackground))
		cluster.Status.IsResharding = false // Unlock
		if err := r.Status().Update(ctx, cluster); err != nil {
			logger.Error(err, "Failed to update status after failed reshard")
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 1 * time.Minute}, nil // Wait before retrying
	}

	logger.Info("Reshard job is still running...")
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

// monitorCPUUsage queries Prometheus and triggers scaling if needed.
func (r *RedisClusterReconciler) monitorCPUUsage(ctx context.Context, cluster *appv1.RedisCluster) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// 1. Set up Prometheus client
	client, err := api.NewClient(api.Config{Address: PromServiceURL})
	if err != nil {
		logger.Error(err, "Error creating Prometheus client")
		return ctrl.Result{}, err
	}
	v1api := prometheusv1.NewAPI(client)

	// 2. Build and run the query.
	// This query gets the average CPU usage *percentage* across all "redis" containers
	// in our StatefulSet over the last 2 minutes.
	query := fmt.Sprintf(
		`avg(rate(container_cpu_usage_seconds_total{container="redis", pod=~"^%s-.*", namespace="%s"}[2m])) * 100`,
		cluster.Name,
		cluster.Namespace,
	)

	result, warnings, err := v1api.Query(ctx, query, time.Now())
	if err != nil {
		logger.Error(err, "Error querying Prometheus", "query", query)
		return ctrl.Result{RequeueAfter: 1 * time.Minute}, err
	}
	if len(warnings) > 0 {
		logger.Info("Prometheus warnings", "warnings", warnings)
	}

	// 3. Parse the result
	vec, ok := result.(model.Vector)
	if !ok || vec.Len() == 0 {
		logger.Info("Prometheus query returned no data. Skipping check.")
		return ctrl.Result{RequeueAfter: 1 * time.Minute}, nil
	}

	avgCPU := float64(vec[0].Value)
	threshold := float64(cluster.Spec.CpuThreshold)
	logger.Info("Current cluster CPU usage", "avgCPU", avgCPU, "threshold", threshold)

	// 4. Take action if threshold is breached
	if avgCPU > threshold {
		logger.Info("CPU threshold breached! Scaling up cluster.", "AvgCPU", avgCPU)

		// Set the lock
		cluster.Status.IsResharding = true
		if err := r.Status().Update(ctx, cluster); err != nil {
			logger.Error(err, "Failed to update status to IsResharding")
			return ctrl.Result{}, err
		}

		// Increase the master count
		cluster.Spec.Masters++
		if err := r.Update(ctx, cluster); err != nil {
			logger.Error(err, "Failed to update spec to increase masters")
			// Try to unlock if spec update fails
			cluster.Status.IsResharding = false
			_ = r.Status().Update(ctx, cluster)
			return ctrl.Result{}, err
		}

		// Success! The Reconcile loop will be triggered by the Spec update.
		// The main controller will update the StatefulSet.
		// The next loop will see IsResharding=true and go to checkReshardingStatus.
		return ctrl.Result{}, nil
	}

	// All good, check again later.
	return ctrl.Result{RequeueAfter: 1 * time.Minute}, nil
}

// reshardJobForRedisCluster defines the Job to rebalance the cluster.
func (r *RedisClusterReconciler) reshardJobForRedisCluster(cluster *appv1.RedisCluster) *batchv1.Job {
	// We just need one pod to act as the entrypoint for the rebalance command.
	anyPod := fmt.Sprintf("%s-0.%s.%s.svc.cluster.local:6379", cluster.Name, cluster.Name+"-headless", cluster.Namespace)

	// This command tells redis-cli to rebalance the cluster and assign
	// slots to any new, empty master nodes.
	cliCmd := fmt.Sprintf("redis-cli --cluster rebalance %s --cluster-use-empty-masters --cluster-yes", anyPod)

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name + "-reshard",
			Namespace: cluster.Namespace,
			Labels:    getLabels(cluster), // getLabels() is from your other controller file
		},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyOnFailure,
					Containers: []corev1.Container{
						{
							Name:    "reshard",
							Image:   fmt.Sprintf("redis:%s", cluster.Spec.RedisVersion),
							Command: []string{"sh", "-c"},
							Args:    []string{cliCmd},
						},
					},
				},
			},
			BackoffLimit: new(int32), // 0 retries, we handle failure in the controller
		},
	}
}
