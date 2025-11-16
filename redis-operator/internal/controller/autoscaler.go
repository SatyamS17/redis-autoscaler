package controller

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/prometheus/client_golang/api"
	prometheusv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"

	appv1 "github.com/myuser/redis-operator/api/v1" // Update this import path!
)

// PromServiceURL is the hardcoded location of your Prometheus server.
// TODO: Make this configurable!
const PromServiceURL = "http://prometheus-operated.monitoring.svc:9090"

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

// monitorCPUUsage identifies the most/least loaded pods and triggers smart scaling
func (r *RedisClusterReconciler) monitorCPUUsage(ctx context.Context, cluster *appv1.RedisCluster) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// 1. Query Prometheus
	client, err := api.NewClient(api.Config{Address: PromServiceURL})
	if err != nil {
		logger.Error(err, "Error creating Prometheus client")
		return ctrl.Result{}, err
	}
	v1api := prometheusv1.NewAPI(client)

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

	// 2. Collect all pod loads
	var podLoads []PodLoad
	for _, sample := range vec {
		podName := string(sample.Metric["pod"])
		cpuUsage := float64(sample.Value)
		podLoads = append(podLoads, PodLoad{
			PodName:  podName,
			CPUUsage: cpuUsage,
		})
		logger.Info("Pod CPU usage", "pod", podName, "cpu", cpuUsage)
	}

	if len(podLoads) == 0 {
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	// 3. Find max loaded pod
	maxLoad := podLoads[0]
	for _, pod := range podLoads {
		if pod.CPUUsage > maxLoad.CPUUsage {
			maxLoad = pod
		}
	}

	highThreshold := float64(cluster.Spec.CpuThreshold)
	lowThreshold := float64(cluster.Spec.CpuThresholdLow)
	minMasters := int32(3)

	// 4. SCALE-UP DECISION: if max pod exceeds threshold
	if maxLoad.CPUUsage > highThreshold {
		logger.Info("Triggering scale-up due to overloaded pod",
			"pod", maxLoad.PodName,
			"cpu", maxLoad.CPUUsage,
			"threshold", highThreshold,
		)

		// Set the lock and store which pod is overloaded
		cluster.Status.IsResharding = true
		cluster.Status.OverloadedPod = maxLoad.PodName
		if err := r.Status().Update(ctx, cluster); err != nil {
			logger.Error(err, "Failed to update status to IsResharding")
			return ctrl.Result{}, err
		}

		// Increment master count
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

	// 5. SCALE-DOWN DECISION: if ALL pods are under low threshold
	if lowThreshold > 0 && cluster.Spec.Masters > minMasters {
		allUnderThreshold := true
		for _, pod := range podLoads {
			if pod.CPUUsage > lowThreshold {
				allUnderThreshold = false
				break
			}
		}

		if allUnderThreshold {
			// Calculate highest index master pod (the one that will be deleted)
			highestMasterIndex := (cluster.Spec.Masters - 1) * (1 + cluster.Spec.ReplicasPerMaster)
			highestIndexPod := fmt.Sprintf("%s-%d", cluster.Name, highestMasterIndex)

			// Find the two lowest utilization pods
			sortedLoads := make([]PodLoad, len(podLoads))
			copy(sortedLoads, podLoads)
			sort.Slice(sortedLoads, func(i, j int) bool {
				return sortedLoads[i].CPUUsage < sortedLoads[j].CPUUsage
			})

			// Get first two lowest util pods
			lowestUtil1 := sortedLoads[0].PodName
			lowestUtil2 := ""
			if len(sortedLoads) > 1 {
				lowestUtil2 = sortedLoads[1].PodName
			}

			logger.Info("Scale-down candidates identified",
				"highestIndexPod", highestIndexPod,
				"lowestUtil1", lowestUtil1,
				"lowestUtil2", lowestUtil2,
			)

			// Determine drain strategy:
			// - If highest index pod is NOT in the two lowest util pods:
			//   -> drain highest index pod and split its load to lowestUtil1 and lowestUtil2
			// - If highest index pod IS one of the two lowest util pods:
			//   -> drain highest index pod and give all load to the OTHER lowest util pod

			var destPod1, destPod2 string
			if highestIndexPod != lowestUtil1 && highestIndexPod != lowestUtil2 {
				// Highest index is NOT one of the low util pods
				// Split between the two low util pods
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
					destPod2 = "" // No second destination
				} else {
					destPod1 = lowestUtil1
					destPod2 = ""
				}
				logger.Info("Strategy: Highest index is low-util. Moving all load to single pod",
					"from", highestIndexPod,
					"to", destPod1,
				)
			}

			// Set drain state with all necessary info
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
	}

	logger.Info("All pods within acceptable CPU range")
	return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
}
