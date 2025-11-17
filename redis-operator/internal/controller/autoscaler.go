package controller

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/prometheus/client_golang/api"
	prometheusv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	appv1 "github.com/myuser/redis-operator/api/v1" // Update this import path!
)

// PromServiceURL is the hardcoded location of your Prometheus server.
// TODO: Make this configurable!
const PromServiceURL = "http://prometheus-operated.monitoring.svc:9090"

// ClusterHealthStatus represents the health state of the Redis cluster
type ClusterHealthStatus struct {
	IsHealthy    bool
	Reason       string
	RequeueAfter time.Duration
}

type PodLoad struct {
	PodName  string
	CPUUsage float64
}

// --- UPDATED ---
// handleAutoScaling is the main entry point for the autoscaler logic.
// It's a "state machine" that checks if we are draining, resharding (scaling up), or monitoring.
func (r *RedisClusterReconciler) handleAutoScaling(ctx context.Context, cluster *appv1.RedisCluster) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// --- NEW STATE 1: Draining (Scale-Down) ---
	// Highest priority: If we are draining a pod, check that job.
	if cluster.Status.IsDraining {
		logger.Info("Cluster is locked for draining. Checking drain job status.")
		return r.checkDrainStatus(ctx, cluster)
	}

	// --- STATE 2: Resharding (Scale-Up) ---
	// We are in the middle of a scale-up operation.
	if cluster.Status.IsResharding {
		logger.Info("Cluster is locked for resharding. Checking job status, NOT monitoring CPU.")
		return r.checkReshardingStatus(ctx, cluster)
	}

	// --- STATE 3: Monitoring ---
	// We are stable and monitoring Prometheus for CPU usage.
	logger.Info("Cluster is stable. Monitoring CPU usage for scale-up or scale-down.")
	// --- UPDATED ---
	return r.monitorCPUUsage(ctx, cluster)
}

// monitorCPUUsage with simple underutilized pod detection
func (r *RedisClusterReconciler) monitorCPUUsage(ctx context.Context, cluster *appv1.RedisCluster) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// CRITICAL: Check cluster health before monitoring CPU
	healthStatus := r.isClusterHealthyForScaling(ctx, cluster)
	if !healthStatus.IsHealthy {
		logger.Info("Cluster not ready for scaling", "reason", healthStatus.Reason)
		return ctrl.Result{RequeueAfter: healthStatus.RequeueAfter}, nil
	}

	// Check if there's a reshard/drain job still running
	reshardJob := &batchv1.Job{}
	err := r.Get(ctx, client.ObjectKey{Name: cluster.Name + "-reshard", Namespace: cluster.Namespace}, reshardJob)
	if err == nil && reshardJob.Status.Succeeded == 0 && reshardJob.Status.Failed == 0 {
		logger.Info("Reshard job still running, skipping autoscale check")
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	// Query Prometheus
	promClient, err := api.NewClient(api.Config{Address: PromServiceURL})
	if err != nil {
		logger.Error(err, "Error creating Prometheus client")
		return ctrl.Result{}, err
	}
	v1api := prometheusv1.NewAPI(promClient)

	query := fmt.Sprintf(
		`rate(container_cpu_usage_seconds_total{container="redis", pod=~"^%s-.*", namespace="%s", service="kps-kube-prometheus-stack-kubelet"}[1m]) * 100
	 and on(pod) redis_instance_info{role="master"}`,
		cluster.Name,
		cluster.Namespace,
	)

	result, warnings, err := v1api.Query(ctx, query, time.Now())
	if err != nil {
		logger.Error(err, "Error querying Prometheus", "query", query)
		return ctrl.Result{RequeueAfter: 15 * time.Second}, err
	}
	if len(warnings) > 0 {
		logger.Info("Prometheus warnings", "warnings", warnings)
	}

	vec, ok := result.(model.Vector)
	if !ok || vec.Len() == 0 {
		logger.Info("Prometheus query returned no data. Skipping check.")
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	// Collect all pod loads
	var podLoads []PodLoad
	for _, sample := range vec {
		podName := string(sample.Metric["pod"])
		cpuUsage := float64(sample.Value)
		podLoads = append(podLoads, PodLoad{
			PodName:  podName,
			CPUUsage: cpuUsage,
		})
	}

	if len(podLoads) == 0 {
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	// Find max loaded pod
	maxLoad := podLoads[0]
	for _, pod := range podLoads {
		if pod.CPUUsage > maxLoad.CPUUsage {
			maxLoad = pod
		}
	}

	highThreshold := float64(cluster.Spec.CpuThreshold)
	lowThreshold := float64(cluster.Spec.CpuThresholdLow)
	minMasters := int32(3)

	// ========== SCALE-UP DECISION ==========
	if maxLoad.CPUUsage > highThreshold {
		logger.Info("Triggering scale-up due to overloaded pod",
			"pod", maxLoad.PodName,
			"cpu", maxLoad.CPUUsage,
			"threshold", highThreshold,
		)

		cluster.Status.IsResharding = true
		cluster.Status.OverloadedPod = maxLoad.PodName
		if err := r.Status().Update(ctx, cluster); err != nil {
			logger.Error(err, "Failed to update status to IsResharding")
			return ctrl.Result{}, err
		}

		cluster.Spec.Masters++
		if err := r.Update(ctx, cluster); err != nil {
			logger.Error(err, "Failed to update spec to increase masters")
			cluster.Status.IsResharding = false
			cluster.Status.OverloadedPod = ""
			_ = r.Status().Update(ctx, cluster)
			return ctrl.Result{}, err
		}

		logger.Info("Successfully triggered scale-up", "newMasterCount", cluster.Spec.Masters)
		return ctrl.Result{RequeueAfter: 1 * time.Second}, nil
	}

	// ========== SCALE-DOWN DECISION ==========
	if lowThreshold > 0 && cluster.Spec.Masters > minMasters {

		// Count underutilized pods
		underutilizedCount := 0
		for _, pod := range podLoads {
			if pod.CPUUsage < lowThreshold {
				underutilizedCount++
			}
		}

		// Scale down if we have 2+ underutilized pods
		if underutilizedCount >= 2 {
			logger.Info("Scale-down triggered: multiple underutilized pods detected",
				"underutilizedCount", underutilizedCount,
				"lowThreshold", lowThreshold,
			)

			// Calculate highest index master pod (the one that will be deleted)
			highestMasterIndex := (cluster.Spec.Masters - 1) * (1 + cluster.Spec.ReplicasPerMaster)
			highestIndexPod := fmt.Sprintf("%s-%d", cluster.Name, highestMasterIndex)

			// Sort pods by CPU usage to find lowest utilization masters
			sortedLoads := make([]PodLoad, len(podLoads))
			copy(sortedLoads, podLoads)
			sort.Slice(sortedLoads, func(i, j int) bool {
				return sortedLoads[i].CPUUsage < sortedLoads[j].CPUUsage
			})

			// Get first two lowest util pods as destinations
			lowestUtil1 := sortedLoads[0].PodName
			lowestUtil2 := ""
			if len(sortedLoads) > 1 {
				lowestUtil2 = sortedLoads[1].PodName
			}

			logger.Info("Scale-down candidates identified",
				"highestIndexPod", highestIndexPod,
				"lowestUtil1", lowestUtil1,
				"lowestUtil1CPU", sortedLoads[0].CPUUsage,
				"lowestUtil2", lowestUtil2,
				"lowestUtil2CPU", func() float64 {
					if len(sortedLoads) > 1 {
						return sortedLoads[1].CPUUsage
					}
					return 0
				}(),
			)

			// Determine drain strategy
			var destPod1, destPod2 string
			if highestIndexPod != lowestUtil1 && highestIndexPod != lowestUtil2 {
				// Highest index is NOT one of the low util pods
				// Split load between the two lowest util pods
				destPod1 = lowestUtil1
				destPod2 = lowestUtil2
				logger.Info("Strategy: Split load from highest index to two low-util pods",
					"from", highestIndexPod,
					"to1", destPod1,
					"to2", destPod2,
				)
			} else {
				// Highest index IS one of the low util pods
				// Give all load to the other low util pod
				if highestIndexPod == lowestUtil1 {
					destPod1 = lowestUtil2
					destPod2 = ""
				} else {
					destPod1 = lowestUtil1
					destPod2 = ""
				}
				logger.Info("Strategy: Highest index is low-util. Moving all load to single pod",
					"from", highestIndexPod,
					"to", destPod1,
				)
			}

			// Set drain state
			cluster.Status.IsDraining = true
			cluster.Status.PodToDrain = highestIndexPod
			cluster.Status.DrainDestPod1 = destPod1
			cluster.Status.DrainDestPod2 = destPod2
			if err := r.Status().Update(ctx, cluster); err != nil {
				logger.Error(err, "Failed to update status to IsDraining")
				return ctrl.Result{}, err
			}

			logger.Info("Successfully triggered scale-down")
			return ctrl.Result{RequeueAfter: 1 * time.Second}, nil
		}

		logger.Info("Not enough underutilized pods for scale-down",
			"underutilizedCount", underutilizedCount,
			"required", 2,
		)
	}

	logger.Info("All pods within acceptable CPU range")
	return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
}

// isClusterHealthyForScaling performs comprehensive health checks before allowing any scale operation.
// It checks: cooldown period, pod readiness, job completion, and cluster state consistency.
func (r *RedisClusterReconciler) isClusterHealthyForScaling(ctx context.Context, cluster *appv1.RedisCluster) ClusterHealthStatus {
	logger := log.FromContext(ctx)

	// Check 1: Enforce cooldown period after last scale operation
	minCooldown := 1 * time.Minute
	if cluster.Status.LastScaleTime != nil {
		timeSinceLastScale := time.Since(cluster.Status.LastScaleTime.Time)
		if timeSinceLastScale < minCooldown {
			return ClusterHealthStatus{
				IsHealthy:    false,
				Reason:       fmt.Sprintf("Scale cooldown active (%s remaining)", (minCooldown - timeSinceLastScale).String()),
				RequeueAfter: 15 * time.Second,
			}
		}
	}

	// Check 2: Verify expected pod count matches spec
	expectedPods := cluster.Spec.Masters * (1 + cluster.Spec.ReplicasPerMaster)
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList,
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels(getLabels(cluster))); err != nil {
		logger.Error(err, "Failed to list pods during health check")
		return ClusterHealthStatus{
			IsHealthy:    false,
			Reason:       "Failed to list pods",
			RequeueAfter: 15 * time.Second,
		}
	}

	// Check 3: Count running and ready pods
	runningPods := 0
	readyPods := 0
	for _, pod := range podList.Items {
		if pod.Status.Phase == corev1.PodRunning {
			runningPods++

			// Check if all containers are ready
			allReady := true
			if len(pod.Status.ContainerStatuses) > 0 {
				for _, cs := range pod.Status.ContainerStatuses {
					if !cs.Ready {
						allReady = false
						break
					}
				}
			} else {
				allReady = false
			}

			if allReady {
				readyPods++
			}
		}
	}

	if runningPods != int(expectedPods) {
		return ClusterHealthStatus{
			IsHealthy: false,
			Reason: fmt.Sprintf("Pod count mismatch (expected: %d, running: %d)",
				expectedPods, runningPods),
			RequeueAfter: 15 * time.Second,
		}
	}

	if readyPods != int(expectedPods) {
		return ClusterHealthStatus{
			IsHealthy: false,
			Reason: fmt.Sprintf("Not all pods ready (expected: %d, ready: %d)",
				expectedPods, readyPods),
			RequeueAfter: 15 * time.Second,
		}
	}

	// Check 4: Ensure no reshard job is in progress
	reshardJob := &batchv1.Job{}
	reshardJobName := cluster.Name + "-reshard"
	err := r.Get(ctx, client.ObjectKey{
		Name:      reshardJobName,
		Namespace: cluster.Namespace,
	}, reshardJob)

	if err == nil {
		// Job exists - check if it's still running
		if reshardJob.Status.Succeeded == 0 && reshardJob.Status.Failed == 0 {
			return ClusterHealthStatus{
				IsHealthy:    false,
				Reason:       "Reshard job in progress",
				RequeueAfter: 15 * time.Second,
			}
		}

		// If job failed, we should wait before attempting another scale
		if reshardJob.Status.Failed > 0 {
			return ClusterHealthStatus{
				IsHealthy:    false,
				Reason:       "Recent reshard job failed - waiting for cleanup",
				RequeueAfter: 15 * time.Second,
			}
		}
	} else if !errors.IsNotFound(err) {
		logger.Error(err, "Failed to check reshard job status")
		return ClusterHealthStatus{
			IsHealthy:    false,
			Reason:       "Failed to query reshard job",
			RequeueAfter: 15 * time.Second,
		}
	}

	// Check 5: Ensure no drain job is in progress
	drainJob := &batchv1.Job{}
	drainJobName := cluster.Name + "-drain"
	err = r.Get(ctx, client.ObjectKey{
		Name:      drainJobName,
		Namespace: cluster.Namespace,
	}, drainJob)

	if err == nil {
		// Job exists - check if it's still running
		if drainJob.Status.Succeeded == 0 && drainJob.Status.Failed == 0 {
			return ClusterHealthStatus{
				IsHealthy:    false,
				Reason:       "Drain job in progress",
				RequeueAfter: 15 * time.Second,
			}
		}

		// If job failed, we should wait before attempting another scale
		if drainJob.Status.Failed > 0 {
			return ClusterHealthStatus{
				IsHealthy:    false,
				Reason:       "Recent drain job failed - waiting for cleanup",
				RequeueAfter: 15 * time.Second,
			}
		}
	} else if !errors.IsNotFound(err) {
		logger.Error(err, "Failed to check drain job status")
		return ClusterHealthStatus{
			IsHealthy:    false,
			Reason:       "Failed to query drain job",
			RequeueAfter: 15 * time.Second,
		}
	}

	// Check 6: Verify state machine flags are consistent
	if cluster.Status.IsDraining {
		return ClusterHealthStatus{
			IsHealthy:    false,
			Reason:       "Cluster is locked in draining state",
			RequeueAfter: 15 * time.Second,
		}
	}

	if cluster.Status.IsResharding {
		return ClusterHealthStatus{
			IsHealthy:    false,
			Reason:       "Cluster is locked in resharding state",
			RequeueAfter: 15 * time.Second,
		}
	}

	// All checks passed!
	logger.Info("Cluster health check passed - safe to scale",
		"pods", readyPods,
		"timeSinceLastScale", func() string {
			if cluster.Status.LastScaleTime != nil {
				return time.Since(cluster.Status.LastScaleTime.Time).String()
			}
			return "never"
		}(),
	)

	return ClusterHealthStatus{
		IsHealthy:    true,
		Reason:       "All health checks passed",
		RequeueAfter: 0,
	}
}
