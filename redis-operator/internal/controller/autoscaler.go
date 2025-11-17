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
	PodName     string
	CPUUsage    float64
	MemoryUsage float64
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

	// Query Prometheus for CPU
	promClient, err := api.NewClient(api.Config{Address: PromServiceURL})
	if err != nil {
		logger.Error(err, "Error creating Prometheus client")
		return ctrl.Result{}, err
	}
	v1api := prometheusv1.NewAPI(promClient)

	cpuQuery := fmt.Sprintf(
		`rate(container_cpu_usage_seconds_total{container="redis", pod=~"^%s-.*", namespace="%s", service="kps-kube-prometheus-stack-kubelet"}[1m]) * 100
	 and on(pod) redis_instance_info{role="master"}`,
		cluster.Name,
		cluster.Namespace,
	)

	cpuResult, warnings, err := v1api.Query(ctx, cpuQuery, time.Now())
	if err != nil {
		logger.Error(err, "Error querying Prometheus for CPU", "query", cpuQuery)
		return ctrl.Result{RequeueAfter: 15 * time.Second}, err
	}
	if len(warnings) > 0 {
		logger.Info("Prometheus CPU warnings", "warnings", warnings)
	}

	cpuVec, ok := cpuResult.(model.Vector)
	if !ok || cpuVec.Len() == 0 {
		logger.Info("Prometheus CPU query returned no data. Skipping check.")
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	// Query Prometheus for Memory
	memoryQuery := fmt.Sprintf(
		`(
		  sum(container_memory_usage_bytes{container="redis", pod=~"^%s-.*", namespace="%s"}) by (pod)
		  /
		  sum(kube_pod_container_resource_limits{resource="memory", pod=~"^%s-.*", namespace="%s"}) by (pod)
		) * 100
		and on(pod) redis_instance_info{role="master"}`,
		cluster.Name,
		cluster.Namespace,
		cluster.Name,
		cluster.Namespace,
	)

	memoryResult, warnings, err := v1api.Query(ctx, memoryQuery, time.Now())
	if err != nil {
		logger.Error(err, "Error querying Prometheus for Memory", "query", memoryQuery)
		return ctrl.Result{RequeueAfter: 15 * time.Second}, err
	}
	if len(warnings) > 0 {
		logger.Info("Prometheus Memory warnings", "warnings", warnings)
	}

	memoryVec, ok := memoryResult.(model.Vector)
	if !ok || memoryVec.Len() == 0 {
		logger.Info("Prometheus Memory query returned no data. Skipping check.")
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	// Build a map of pod -> memory usage
	memoryMap := make(map[string]float64)
	for _, sample := range memoryVec {
		podName := string(sample.Metric["pod"])
		memoryUsage := float64(sample.Value)
		memoryMap[podName] = memoryUsage
	}

	// Collect all pod loads (CPU + Memory)
	var podLoads []PodLoad
	for _, sample := range cpuVec {
		podName := string(sample.Metric["pod"])
		cpuUsage := float64(sample.Value)

		// Get corresponding memory usage
		memoryUsage, ok := memoryMap[podName]
		if !ok {
			logger.Info("No memory data for pod, skipping", "pod", podName)
			continue
		}

		podLoads = append(podLoads, PodLoad{
			PodName:     podName,
			CPUUsage:    cpuUsage,
			MemoryUsage: memoryUsage,
		})
		logger.Info("Pod metrics",
			"pod", podName,
			"cpu", fmt.Sprintf("%.2f%%", cpuUsage),
			"memory", fmt.Sprintf("%.2f%%", memoryUsage),
		)
	}

	if len(podLoads) == 0 {
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	// Find pod with highest CPU OR Memory (pick highest memory as tiebreaker)
	var triggerPod PodLoad
	triggered := false

	highThreshold := float64(cluster.Spec.CpuThreshold)
	memoryThreshold := float64(cluster.Spec.MemoryThreshold)

	for _, pod := range podLoads {
		if pod.CPUUsage > highThreshold || pod.MemoryUsage > memoryThreshold {
			triggered = true
			// First trigger or this pod has higher memory
			if triggerPod.PodName == "" || pod.MemoryUsage > triggerPod.MemoryUsage {
				triggerPod = pod
			}
		}
	}

	// ========== SCALE-UP DECISION ==========
	if triggered {
		reason := ""
		if triggerPod.CPUUsage > highThreshold && triggerPod.MemoryUsage > memoryThreshold {
			reason = fmt.Sprintf("CPU and Memory overloaded (CPU: %.2f%%, Memory: %.2f%%)",
				triggerPod.CPUUsage, triggerPod.MemoryUsage)
		} else if triggerPod.CPUUsage > highThreshold {
			reason = fmt.Sprintf("CPU overloaded (CPU: %.2f%%, Memory: %.2f%%)",
				triggerPod.CPUUsage, triggerPod.MemoryUsage)
		} else {
			reason = fmt.Sprintf("Memory overloaded (CPU: %.2f%%, Memory: %.2f%%)",
				triggerPod.CPUUsage, triggerPod.MemoryUsage)
		}

		logger.Info("Triggering scale-up",
			"pod", triggerPod.PodName,
			"reason", reason,
			"cpuThreshold", highThreshold,
			"memoryThreshold", memoryThreshold,
		)

		cluster.Status.IsResharding = true
		cluster.Status.OverloadedPod = triggerPod.PodName
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
	lowThreshold := float64(cluster.Spec.CpuThresholdLow)
	memoryLowThreshold := float64(cluster.Spec.MemoryThresholdLow)
	minMasters := int32(3)

	if lowThreshold > 0 && memoryLowThreshold > 0 && cluster.Spec.Masters > minMasters {

		// Count underutilized pods (BOTH CPU AND Memory must be low)
		underutilizedCount := 0
		for _, pod := range podLoads {
			if pod.CPUUsage < lowThreshold && pod.MemoryUsage < memoryLowThreshold {
				underutilizedCount++
			}
		}

		// Scale down if we have 2+ underutilized pods
		if underutilizedCount >= 2 {
			logger.Info("Scale-down triggered: multiple underutilized pods detected",
				"underutilizedCount", underutilizedCount,
				"cpuLowThreshold", lowThreshold,
				"memoryLowThreshold", memoryLowThreshold,
			)

			// Calculate highest index master pod (the one that will be deleted)
			highestMasterIndex := (cluster.Spec.Masters - 1) * (1 + cluster.Spec.ReplicasPerMaster)
			highestIndexPod := fmt.Sprintf("%s-%d", cluster.Name, highestMasterIndex)

			// Sort pods by Memory usage (prioritize lowest memory for destinations)
			sortedLoads := make([]PodLoad, len(podLoads))
			copy(sortedLoads, podLoads)
			sort.Slice(sortedLoads, func(i, j int) bool {
				return sortedLoads[i].MemoryUsage < sortedLoads[j].MemoryUsage
			})

			// Get first two lowest memory pods as destinations
			lowestUtil1 := sortedLoads[0].PodName
			lowestUtil2 := ""
			if len(sortedLoads) > 1 {
				lowestUtil2 = sortedLoads[1].PodName
			}

			logger.Info("Scale-down candidates identified",
				"highestIndexPod", highestIndexPod,
				"lowestUtil1", lowestUtil1,
				"lowestUtil1CPU", sortedLoads[0].CPUUsage,
				"lowestUtil1Memory", sortedLoads[0].MemoryUsage,
				"lowestUtil2", lowestUtil2,
				"lowestUtil2CPU", func() float64 {
					if len(sortedLoads) > 1 {
						return sortedLoads[1].CPUUsage
					}
					return 0
				}(),
				"lowestUtil2Memory", func() float64 {
					if len(sortedLoads) > 1 {
						return sortedLoads[1].MemoryUsage
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

	logger.Info("All pods within acceptable CPU and memory ranges")
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
