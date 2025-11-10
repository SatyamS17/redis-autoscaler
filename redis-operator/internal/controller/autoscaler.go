package controller

import (
	"context"
	"fmt"
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

// --- UPDATED FUNCTION ---
// monitorCPUUsage queries Prometheus for individual pod CPU and triggers scaling (up or down) if needed.
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
	query := fmt.Sprintf(
		`rate(container_cpu_usage_seconds_total{container="redis", pod=~"^%s-.*", namespace="%s"}[1m]) * 100`,
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

	// 3. Parse the result
	vec, ok := result.(model.Vector)
	if !ok || vec.Len() == 0 {
		logger.Info("Prometheus query returned no data. Skipping check.")
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	highThreshold := float64(cluster.Spec.CpuThreshold)
	lowThreshold := float64(cluster.Spec.CpuThresholdLow)
	var scaleUp bool = false
	var scaleDown bool = true // Assume true, set to false if any pod is *not* low

	// Safety check: Don't check for scale-down if no low threshold is set
	if lowThreshold <= 0 {
		scaleDown = false
	}

	// --- THIS IS THE MINIMUM MASTER CHECK ---
	// You should use a real Spec field, e.g., cluster.Spec.MinMasters
	// Using '3' as a placeholder.
	minMasters := int32(3)
	if cluster.Spec.Masters <= minMasters {
		scaleDown = false
	}

	// 4. Iterate over each pod's CPU usage
	for _, sample := range vec {
		podName := string(sample.Metric["pod"])
		cpuUsage := float64(sample.Value)
		logger.Info("Checking pod CPU usage", "pod", podName, "cpuUsage", cpuUsage, "highThreshold", highThreshold, "lowThreshold", lowThreshold)

		// Check for scale-up
		if cpuUsage > highThreshold {
			logger.Info("CPU threshold breached by a pod! Scaling up cluster.", "pod", podName, "cpuUsage", cpuUsage)
			scaleUp = true
			break // Found a pod that needs scaling, no need to check others.
		}

		// --- UPDATED SCALE-DOWN CHECK ---
		// We only care if *any* pod is *above* the low threshold.
		if scaleDown && cpuUsage > lowThreshold {
			// This pod is *not* underutilized, so we can't scale down.
			scaleDown = false
		}
		// We no longer save the 'podToDrain' name here.
	}

	// 5. Take action
	if scaleUp {
		// --- SCALE-UP LOGIC (UNCHANGED) ---
		logger.Info("Triggering scale-up...")
		cluster.Status.IsResharding = true
		if err := r.Status().Update(ctx, cluster); err != nil {
			logger.Error(err, "Failed to update status to IsResharding")
			return ctrl.Result{}, err
		}

		cluster.Spec.Masters++
		if err := r.Update(ctx, cluster); err != nil {
			logger.Error(err, "Failed to update spec to increase masters")
			cluster.Status.IsResharding = false
			_ = r.Status().Update(ctx, cluster)
			return ctrl.Result{}, err
		}

		logger.Info("Successfully triggered scale-up by increasing master count", "newMasterCount", cluster.Spec.Masters)
		return ctrl.Result{RequeueAfter: 1 * time.Second}, nil
	}

	// --- CORRECTED SCALE-DOWN LOGIC ---
	if scaleDown {
		// All pods are under threshold. Time to scale down.
		// We MUST drain the highest-index master.

		// This calculation finds the pod index for the highest master.
		// Example: 4 Masters, 1 Replica -> (4-1)*(1+1) = 6. Pod: "cluster-6"
		// Example: 3 Masters, 1 Replica -> (3-1)*(1+1) = 4. Pod: "cluster-4"
		podIndexToDrain := (cluster.Spec.Masters - 1) * (1 + cluster.Spec.ReplicasPerMaster)
		podToDrain := fmt.Sprintf("%s-%d", cluster.Name, podIndexToDrain)

		logger.Info("Triggering scale-down. All pods below low threshold.", "podToDrain", podToDrain)

		// Set the lock and the *correct* pod to drain
		cluster.Status.IsDraining = true
		cluster.Status.PodToDrain = podToDrain // <-- This is now correct
		if err := r.Status().Update(ctx, cluster); err != nil {
			logger.Error(err, "Failed to update status to IsDraining")
			return ctrl.Result{}, err
		}

		// Success! The next reconcile loop will see IsDraining=true
		// and call checkDrainStatus, which will create the drain job.
		logger.Info("Successfully triggered scale-down.", "pod", podToDrain)
		return ctrl.Result{RequeueAfter: 1 * time.Second}, nil
	}

	// All good, check again later.
	logger.Info("All pods are within CPU thresholds.")
	return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
}
