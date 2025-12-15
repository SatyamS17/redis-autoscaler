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

	appv1 "github.com/myuser/redis-operator/api/v1"
)

// ClusterHealthStatus represents the health state of the cluster for scaling decisions.
type ClusterHealthStatus struct {
	IsHealthy    bool
	Reason       string
	RequeueAfter time.Duration
}

// PodLoad represents CPU and memory metrics for a single Redis pod.
type PodLoad struct {
	PodName     string
	CPUUsage    float64
	MemoryUsage float64
}

// handleAutoScaling is the main entry point for autoscaling logic.
// It implements a state machine with four states:
//   - IsDraining: Scale-down operation in progress
//   - IsResharding: Scale-up operation in progress
//   - IsProvisioningStandby: Adding new standby pods to cluster
//   - Monitoring: Normal operation, checking metrics for scaling decisions
func (r *RedisClusterReconciler) handleAutoScaling(ctx context.Context, cluster *appv1.RedisCluster) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if cluster.Status.IsDraining {
		logger.Info("Cluster is draining, checking drain job status")
		return r.checkDrainStatus(ctx, cluster)
	}

	if cluster.Status.IsResharding {
		logger.Info("Cluster is resharding, checking reshard job status")
		return r.checkReshardingStatus(ctx, cluster)
	}

	if cluster.Status.IsProvisioningStandby {
		logger.Info("Cluster is provisioning standby, adding new pods to cluster")
		return r.checkProvisioningStatus(ctx, cluster)
	}

	logger.Info("Cluster is stable, monitoring metrics for scaling decisions")
	return r.monitorMetrics(ctx, cluster)
}

// monitorMetrics queries Prometheus for CPU and memory metrics and makes scaling decisions.
// It excludes the standby pod from metrics analysis and checks both scale-up and scale-down conditions.
func (r *RedisClusterReconciler) monitorMetrics(ctx context.Context, cluster *appv1.RedisCluster) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	requeueInterval := time.Duration(cluster.Spec.MetricsQueryInterval) * time.Second

	healthStatus := r.isClusterHealthyForScaling(ctx, cluster)
	if !healthStatus.IsHealthy {
		logger.Info("Cluster not ready for scaling", "reason", healthStatus.Reason)
		return ctrl.Result{RequeueAfter: healthStatus.RequeueAfter}, nil
	}

	if r.isJobRunning(ctx, cluster.Name+"-reshard", cluster.Namespace) {
		logger.Info("Reshard job still running, skipping autoscale check")
		return ctrl.Result{RequeueAfter: requeueInterval}, nil
	}

	podLoads, err := r.queryPodMetrics(ctx, cluster)
	if err != nil {
		logger.Error(err, "Failed to query pod metrics")
		return ctrl.Result{RequeueAfter: requeueInterval}, err
	}

	if len(podLoads) == 0 {
		logger.Info("No pod metrics available, skipping scaling check")
		return ctrl.Result{RequeueAfter: requeueInterval}, nil
	}

	if shouldScaleUp, triggerPod, reason := r.checkScaleUpCondition(cluster, podLoads); shouldScaleUp {
		return r.triggerScaleUp(ctx, cluster, triggerPod, reason)
	}

	if shouldScaleDown, reason := r.checkScaleDownCondition(cluster, podLoads); shouldScaleDown {
		return r.triggerScaleDown(ctx, cluster, podLoads, reason)
	}

	logger.Info("All pods within acceptable CPU and memory ranges")
	return ctrl.Result{RequeueAfter: requeueInterval}, nil
}

// queryPodMetrics queries Prometheus for CPU and memory usage of all active Redis master pods.
// It returns a slice of PodLoad structs, excluding the standby pod.
func (r *RedisClusterReconciler) queryPodMetrics(ctx context.Context, cluster *appv1.RedisCluster) ([]PodLoad, error) {
	logger := log.FromContext(ctx)

	promClient, err := api.NewClient(api.Config{Address: cluster.Spec.PrometheusURL})
	if err != nil {
		return nil, fmt.Errorf("failed to create Prometheus client: %w", err)
	}
	v1api := prometheusv1.NewAPI(promClient)

	cpuMap, err := r.queryCPUMetrics(ctx, v1api, cluster)
	if err != nil {
		return nil, err
	}

	memoryMap, err := r.queryMemoryMetrics(ctx, v1api, cluster)
	if err != nil {
		return nil, err
	}

	var podLoads []PodLoad
	for podName, cpuUsage := range cpuMap {
		if podName == cluster.Status.StandbyPod {
			logger.Info("Skipping standby pod from metrics", "standbyPod", podName)
			continue
		}

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

	return podLoads, nil
}

// queryCPUMetrics queries Prometheus for CPU usage percentage of Redis master pods.
// Returns a map of pod name to CPU usage percentage.
func (r *RedisClusterReconciler) queryCPUMetrics(ctx context.Context, v1api prometheusv1.API, cluster *appv1.RedisCluster) (map[string]float64, error) {
	logger := log.FromContext(ctx)

	cpuQuery := fmt.Sprintf(
		`rate(container_cpu_usage_seconds_total{container="redis", pod=~"^%s-.*", namespace="%s", service="kps-kube-prometheus-stack-kubelet"}[1m]) * 100
		 and on(pod) redis_instance_info{role="master"}`,
		cluster.Name,
		cluster.Namespace,
	)

	cpuResult, warnings, err := v1api.Query(ctx, cpuQuery, time.Now())
	if err != nil {
		return nil, fmt.Errorf("Prometheus CPU query failed: %w", err)
	}
	if len(warnings) > 0 {
		logger.Info("Prometheus CPU warnings", "warnings", warnings)
	}

	cpuVec, ok := cpuResult.(model.Vector)
	if !ok || cpuVec.Len() == 0 {
		return nil, fmt.Errorf("no CPU metrics data available")
	}

	cpuMap := make(map[string]float64)
	for _, sample := range cpuVec {
		podName := string(sample.Metric["pod"])
		cpuMap[podName] = float64(sample.Value)
	}

	return cpuMap, nil
}

// queryMemoryMetrics queries Prometheus for memory usage percentage of Redis master pods.
// Returns a map of pod name to memory usage percentage.
func (r *RedisClusterReconciler) queryMemoryMetrics(ctx context.Context, v1api prometheusv1.API, cluster *appv1.RedisCluster) (map[string]float64, error) {
	logger := log.FromContext(ctx)

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
		return nil, fmt.Errorf("Prometheus memory query failed: %w", err)
	}
	if len(warnings) > 0 {
		logger.Info("Prometheus memory warnings", "warnings", warnings)
	}

	memoryVec, ok := memoryResult.(model.Vector)
	if !ok || memoryVec.Len() == 0 {
		return nil, fmt.Errorf("no memory metrics data available")
	}

	memoryMap := make(map[string]float64)
	for _, sample := range memoryVec {
		podName := string(sample.Metric["pod"])
		memoryMap[podName] = float64(sample.Value)
	}

	return memoryMap, nil
}

// checkScaleUpCondition determines if scale-up is needed.
// Returns true if any pod exceeds CPU or memory thresholds, along with the triggering pod and reason.
func (r *RedisClusterReconciler) checkScaleUpCondition(cluster *appv1.RedisCluster, podLoads []PodLoad) (bool, PodLoad, string) {
	highCPUThreshold := float64(cluster.Spec.CpuThreshold)
	highMemoryThreshold := float64(cluster.Spec.MemoryThreshold)

	var triggerPod PodLoad
	triggered := false

	for _, pod := range podLoads {
		if pod.CPUUsage > highCPUThreshold || pod.MemoryUsage > highMemoryThreshold {
			triggered = true
			if triggerPod.PodName == "" || pod.MemoryUsage > triggerPod.MemoryUsage {
				triggerPod = pod
			}
		}
	}

	if !triggered {
		return false, PodLoad{}, ""
	}

	var reason string
	if triggerPod.CPUUsage > highCPUThreshold && triggerPod.MemoryUsage > highMemoryThreshold {
		reason = fmt.Sprintf("CPU and Memory overloaded (CPU: %.2f%%, Memory: %.2f%%)",
			triggerPod.CPUUsage, triggerPod.MemoryUsage)
	} else if triggerPod.CPUUsage > highCPUThreshold {
		reason = fmt.Sprintf("CPU overloaded (CPU: %.2f%%, Memory: %.2f%%)",
			triggerPod.CPUUsage, triggerPod.MemoryUsage)
	} else {
		reason = fmt.Sprintf("Memory overloaded (CPU: %.2f%%, Memory: %.2f%%)",
			triggerPod.CPUUsage, triggerPod.MemoryUsage)
	}

	return true, triggerPod, reason
}

// checkScaleDownCondition determines if scale-down is needed.
// Returns true if there are at least 2 underutilized pods and we're above minimum masters.
func (r *RedisClusterReconciler) checkScaleDownCondition(cluster *appv1.RedisCluster, podLoads []PodLoad) (bool, string) {
	lowCPUThreshold := float64(cluster.Spec.CpuThresholdLow)
	lowMemoryThreshold := float64(cluster.Spec.MemoryThresholdLow)

	if cluster.Spec.Masters <= cluster.Spec.MinMasters {
		return false, ""
	}

	underutilizedCount := 0
	for _, pod := range podLoads {
		if pod.CPUUsage < lowCPUThreshold && pod.MemoryUsage < lowMemoryThreshold {
			underutilizedCount++
		}
	}

	if underutilizedCount >= 2 {
		reason := fmt.Sprintf("Scale-down triggered: %d underutilized pods (CPU < %.0f%%, Memory < %.0f%%)",
			underutilizedCount, lowCPUThreshold, lowMemoryThreshold)
		return true, reason
	}

	return false, ""
}

// triggerScaleUp initiates a scale-up operation by activating the standby pod.
func (r *RedisClusterReconciler) triggerScaleUp(ctx context.Context, cluster *appv1.RedisCluster, triggerPod PodLoad, reason string) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	logger.Info("Triggering scale-up using standby pod",
		"pod", triggerPod.PodName,
		"reason", reason,
		"standbyPod", cluster.Status.StandbyPod,
	)

	cluster.Status.IsResharding = true
	cluster.Status.OverloadedPod = triggerPod.PodName

	if err := r.Status().Update(ctx, cluster); err != nil {
		logger.Error(err, "Failed to update status to IsResharding")
		return ctrl.Result{}, err
	}

	logger.Info("Successfully triggered scale-up", "currentMasters", cluster.Spec.Masters)
	return ctrl.Result{RequeueAfter: 1 * time.Second}, nil
}

// triggerScaleDown initiates a scale-down operation by draining the highest-index active master.
func (r *RedisClusterReconciler) triggerScaleDown(ctx context.Context, cluster *appv1.RedisCluster, podLoads []PodLoad, reason string) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	logger.Info("Scale-down triggered", "reason", reason)

	highestActiveMasterIndex := (cluster.Spec.Masters - 1) * (1 + cluster.Spec.ReplicasPerMaster)
	highestIndexPod := fmt.Sprintf("%s-%d", cluster.Name, highestActiveMasterIndex)

	// Filter out replica pods - only select master pods as drain destinations
	// Master pods are at indices: 0, (1+R), 2*(1+R), 3*(1+R), etc.
	var masterLoads []PodLoad
	for _, load := range podLoads {
		// Extract pod index from pod name (e.g., "redis-cluster-6" -> 6)
		var podIndex int
		_, err := fmt.Sscanf(load.PodName, cluster.Name+"-%d", &podIndex)
		if err != nil {
			logger.Error(err, "Failed to parse pod index", "podName", load.PodName)
			continue
		}

		// Check if this is a master pod (index divisible by 1+ReplicasPerMaster)
		replicasPerMaster := int(cluster.Spec.ReplicasPerMaster)
		if podIndex%(1+replicasPerMaster) == 0 {
			masterLoads = append(masterLoads, load)
		}
	}

	if len(masterLoads) < 2 {
		logger.Error(fmt.Errorf("not enough master pods for scale-down"), "Only have master pods",
			"count", len(masterLoads))
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	sortedLoads := make([]PodLoad, len(masterLoads))
	copy(sortedLoads, masterLoads)
	sort.Slice(sortedLoads, func(i, j int) bool {
		return sortedLoads[i].MemoryUsage < sortedLoads[j].MemoryUsage
	})

	lowestUtil1 := sortedLoads[0].PodName
	lowestUtil2 := ""
	if len(sortedLoads) > 1 {
		lowestUtil2 = sortedLoads[1].PodName
	}

	logger.Info("Scale-down candidates identified",
		"highestIndexPod", highestIndexPod,
		"standbyPod", cluster.Status.StandbyPod,
		"lowestUtil1", lowestUtil1,
		"lowestUtil2", lowestUtil2,
	)

	var destPod1, destPod2 string
	if highestIndexPod != lowestUtil1 && highestIndexPod != lowestUtil2 {
		destPod1 = lowestUtil1
		destPod2 = lowestUtil2
		logger.Info("Strategy: Split load from highest index to two low-util pods",
			"from", highestIndexPod, "to1", destPod1, "to2", destPod2)
	} else {
		if highestIndexPod == lowestUtil1 {
			destPod1 = lowestUtil2
		} else {
			destPod1 = lowestUtil1
		}
		logger.Info("Strategy: Highest index is low-util. Moving all load to single pod",
			"from", highestIndexPod, "to", destPod1)
	}

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

// isClusterHealthyForScaling performs comprehensive health checks before allowing scaling operations.
// It checks cooldown period, pod count, pod readiness, standby detection, and job status.
func (r *RedisClusterReconciler) isClusterHealthyForScaling(ctx context.Context, cluster *appv1.RedisCluster) ClusterHealthStatus {
	logger := log.FromContext(ctx)
	requeueInterval := time.Duration(cluster.Spec.MetricsQueryInterval) * time.Second

	if err := r.checkCooldownPeriod(cluster); err != nil {
		return ClusterHealthStatus{
			IsHealthy:    false,
			Reason:       err.Error(),
			RequeueAfter: requeueInterval,
		}
	}

	if err := r.checkPodCount(ctx, cluster); err != nil {
		return ClusterHealthStatus{
			IsHealthy:    false,
			Reason:       err.Error(),
			RequeueAfter: requeueInterval,
		}
	}

	if err := r.checkAndUpdateStandbyPod(ctx, cluster); err != nil {
		return ClusterHealthStatus{
			IsHealthy:    false,
			Reason:       err.Error(),
			RequeueAfter: 10 * time.Second,
		}
	}

	if err := r.checkNoJobsRunning(ctx, cluster); err != nil {
		return ClusterHealthStatus{
			IsHealthy:    false,
			Reason:       err.Error(),
			RequeueAfter: requeueInterval,
		}
	}

	if cluster.Status.IsDraining || cluster.Status.IsResharding {
		return ClusterHealthStatus{
			IsHealthy:    false,
			Reason:       "Cluster is locked in scaling state",
			RequeueAfter: requeueInterval,
		}
	}

	logger.Info("Cluster health check passed - safe to scale",
		"pods", (cluster.Spec.Masters+1)*(1+cluster.Spec.ReplicasPerMaster),
		"standbyPod", cluster.Status.StandbyPod,
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

// checkCooldownPeriod verifies that enough time has passed since the last scaling operation.
func (r *RedisClusterReconciler) checkCooldownPeriod(cluster *appv1.RedisCluster) error {
	if cluster.Status.LastScaleTime == nil {
		return nil
	}

	cooldown := time.Duration(cluster.Spec.ScaleCooldownSeconds) * time.Second
	timeSinceLastScale := time.Since(cluster.Status.LastScaleTime.Time)

	if timeSinceLastScale < cooldown {
		return fmt.Errorf("scale cooldown active (%s remaining)", (cooldown - timeSinceLastScale).String())
	}

	return nil
}

// checkPodCount verifies that all expected pods are running and ready.
func (r *RedisClusterReconciler) checkPodCount(ctx context.Context, cluster *appv1.RedisCluster) error {
	logger := log.FromContext(ctx)

	expectedPods := (cluster.Spec.Masters + 1) * (1 + cluster.Spec.ReplicasPerMaster)
	podList := &corev1.PodList{}

	if err := r.List(ctx, podList,
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels(getLabels(cluster))); err != nil {
		logger.Error(err, "Failed to list pods during health check")
		return fmt.Errorf("failed to list pods")
	}

	runningPods := 0
	readyPods := 0

	for _, pod := range podList.Items {
		if pod.Status.Phase == corev1.PodRunning {
			runningPods++

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
		return fmt.Errorf("pod count mismatch (expected: %d, running: %d)", expectedPods, runningPods)
	}

	if readyPods != int(expectedPods) {
		return fmt.Errorf("not all pods ready (expected: %d, ready: %d)", expectedPods, readyPods)
	}

	return nil
}

// checkAndUpdateStandbyPod detects the standby pod and updates the status if it has changed.
func (r *RedisClusterReconciler) checkAndUpdateStandbyPod(ctx context.Context, cluster *appv1.RedisCluster) error {
	logger := log.FromContext(ctx)

	oldStandby := cluster.Status.StandbyPod
	if err := r.detectAndSetStandbyPod(ctx, cluster); err != nil {
		logger.Error(err, "Failed to detect standby pod")
		return fmt.Errorf("cannot detect standby pod: %w", err)
	}

	if cluster.Status.StandbyPod != oldStandby {
		logger.Info("Standby pod updated",
			"oldStandby", oldStandby,
			"newStandby", cluster.Status.StandbyPod)

		if err := r.Status().Update(ctx, cluster); err != nil {
			logger.Error(err, "Failed to update status with new standby pod")
			return fmt.Errorf("failed to save standby pod reference")
		}

		return fmt.Errorf("standby pod reference updated, stabilizing")
	}

	return nil
}

// checkNoJobsRunning verifies that no reshard or drain jobs are currently active.
func (r *RedisClusterReconciler) checkNoJobsRunning(ctx context.Context, cluster *appv1.RedisCluster) error {
	if err := r.checkJobStatus(ctx, cluster.Name+"-reshard", cluster.Namespace); err != nil {
		return err
	}

	if err := r.checkJobStatus(ctx, cluster.Name+"-drain", cluster.Namespace); err != nil {
		return err
	}

	return nil
}

// checkJobStatus checks if a job is running or recently failed.
func (r *RedisClusterReconciler) checkJobStatus(ctx context.Context, jobName, namespace string) error {
	logger := log.FromContext(ctx)

	job := &batchv1.Job{}
	err := r.Get(ctx, client.ObjectKey{Name: jobName, Namespace: namespace}, job)

	if err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		logger.Error(err, "Failed to check job status", "job", jobName)
		return fmt.Errorf("failed to query %s job", jobName)
	}

	if job.Status.Succeeded == 0 && job.Status.Failed == 0 {
		return fmt.Errorf("%s job in progress", jobName)
	}

	if job.Status.Failed > 0 {
		return fmt.Errorf("recent %s job failed - waiting for cleanup", jobName)
	}

	return nil
}

// isJobRunning checks if a specific job is currently running.
func (r *RedisClusterReconciler) isJobRunning(ctx context.Context, jobName, namespace string) bool {
	job := &batchv1.Job{}
	err := r.Get(ctx, client.ObjectKey{Name: jobName, Namespace: namespace}, job)
	if err != nil {
		return false
	}
	return job.Status.Succeeded == 0 && job.Status.Failed == 0
}
