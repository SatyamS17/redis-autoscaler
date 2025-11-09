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

// --- NEW FUNCTION ---
// checkDrainStatus checks the progress of the drain job.
func (r *RedisClusterReconciler) checkDrainStatus(ctx context.Context, cluster *appv1.RedisCluster) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	jobName := cluster.Name + "-drain"

	// 1. Check if the drain Job exists
	drainJob := &batchv1.Job{}
	err := r.Get(ctx, client.ObjectKey{Name: jobName, Namespace: cluster.Namespace}, drainJob)

	if err != nil && errors.IsNotFound(err) {
		// Job not found, so we need to create it.
		// We must first get the IP of the pod we want to drain.
		podToDrain := &corev1.Pod{}
		podName := cluster.Status.PodToDrain
		if podName == "" {
			// Safety check - this should not happen if IsDraining is true.
			logger.Error(fmt.Errorf("IsDraining is true but PodToDrain is empty"), "State error")
			cluster.Status.IsDraining = false // Unlock
			_ = r.Status().Update(ctx, cluster)
			return ctrl.Result{}, nil
		}

		if err := r.Get(ctx, types.NamespacedName{Name: podName, Namespace: cluster.Namespace}, podToDrain); err != nil {
			logger.Error(err, "Failed to get Pod object to find IP for draining", "pod", podName)
			// Pod might be gone? Unlock and retry.
			cluster.Status.IsDraining = false
			cluster.Status.PodToDrain = ""
			_ = r.Status().Update(ctx, cluster)
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}

		if podToDrain.Status.PodIP == "" {
			logger.Info("Pod to drain does not have an IP yet. Waiting...", "pod", podName)
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}

		podIP := podToDrain.Status.PodIP
		logger.Info("Creating pod drain Job", "pod", podName, "ip", podIP)
		job := r.drainJobForRedisCluster(cluster, podIP) // Pass the IP to the job constructor
		if err := controllerutil.SetControllerReference(cluster, job, r.Scheme); err != nil {
			logger.Error(err, "Failed to set owner ref on drain Job")
			return ctrl.Result{}, err
		}
		if err := r.Create(ctx, job); err != nil {
			logger.Error(err, "Failed to create drain Job")
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil // Requeue to check job status

	} else if err != nil {
		logger.Error(err, "Failed to get drain Job")
		return ctrl.Result{}, err
	}

	// 2. Job exists. Check its status.
	if drainJob.Status.Succeeded > 0 {
		logger.Info("Drain Job succeeded. Pod is empty. Reducing master count.")

		// Drain succeeded, now we can safely reduce the master count
		cluster.Spec.Masters--
		if err := r.Update(ctx, cluster); err != nil {
			logger.Error(err, "Failed to update spec to decrease masters after drain")
			return ctrl.Result{}, err
		}

		// Unlock the state machine and clear drain state
		cluster.Status.IsDraining = false
		cluster.Status.PodToDrain = ""

		if err := r.Status().Update(ctx, cluster); err != nil {
			logger.Error(err, "Failed to update status after drain")
			return ctrl.Result{}, err
		}

		// Clean up the successful job
		// _ = r.Delete(ctx, drainJob, client.PropagationPolicy(metav1.DeletePropagationBackground))
		// The main reconcile loop will now see the reduced Spec.Masters
		// and will terminate the drained pod (e.g., redis-cluster-5)
		logger.Info("Scale-down complete. Main reconciler will now remove the pod.")
		return ctrl.Result{}, nil
	}

	if drainJob.Status.Failed > 0 {
		logger.Error(fmt.Errorf("drain job %s failed", jobName), "Draining failed")
		// Clean up the failed job to allow a retry
		// _ = r.Delete(ctx, drainJob, client.PropagationPolicy(metav1.DeletePropagationBackground))
		cluster.Status.IsDraining = false // Unlock
		cluster.Status.PodToDrain = ""
		if err := r.Status().Update(ctx, cluster); err != nil {
			logger.Error(err, "Failed to update status after failed drain")
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 1 * time.Second}, nil // Wait before retrying
	}

	logger.Info("Drain job is still running...")
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

// checkReshardingStatus checks if the new pods are ready and if the reshard job is complete.
// --- THIS FUNCTION IS UNCHANGED ---
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

	logger.Info("All StatefulSet pods are ready. Checking for reshard job.")

	// 2. All pods are ready. Check for the resharding Job.
	reshardJob := &batchv1.Job{}
	jobName := cluster.Name + "-reshard"
	err := r.Get(ctx, client.ObjectKey{Name: jobName, Namespace: cluster.Namespace}, reshardJob)

	if err != nil && errors.IsNotFound(err) {
		// Job not found, create it
		logger.Info("Creating smart reshard Job.")
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
		return ctrl.Result{RequeueAfter: 1 * time.Second}, nil // Wait before retrying
	}

	logger.Info("Reshard job is still running...")
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
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

// --- THIS IS THE CORRECTED FUNCTION ---
// Replace the old one in your Go code with this.
// drainJobForRedisCluster defines the Job to drain and safely remove a pod.
func (r *RedisClusterReconciler) drainJobForRedisCluster(cluster *appv1.RedisCluster, podToDrainIP string) *batchv1.Job {
	anyPodHost := fmt.Sprintf("%s-0.%s.%s.svc.cluster.local", cluster.Name, cluster.Name+"-headless", cluster.Namespace)
	entrypointWithPort := fmt.Sprintf("%s:6379", anyPodHost) // Needed for reshard/del-node
	timeout := int64(600)                                    // Increased timeout for drain + delete
	backoff := int32(0)

	cliCmd := `
#!/bin/sh
set -ex

echo "--- Drain & Delete Job Started ---"
echo "Target Pod IP: $POD_TO_DRAIN_IP"
echo "Entrypoint: $ENTRYPOINT_WITH_PORT"

# 1. Find the Redis Node ID for the pod we want to drain
NODE_ID=$(redis-cli -h $ENTRYPOINT_HOST cluster nodes | grep "$POD_TO_DRAIN_IP:6379" | awk '{ print $1 }')

if [ -z "${NODE_ID}" ]; then
  echo "Could not find node with IP $POD_TO_DRAIN_IP. Assuming already gone. Exiting gracefully."
  exit 0
fi
echo "Found node ID ${NODE_ID} for IP $POD_TO_DRAIN_IP"

# 2. Check if the node is a master and has slots
SLOTS=$(redis-cli -h $ENTRYPOINT_HOST cluster nodes | grep "${NODE_ID}" | awk '{ print $9 }')
if [ -z "${SLOTS}" ]; then
  echo "Node ${NODE_ID} is not a master or has no slots. Draining not needed."
else
  # 3. Find a recipient node (any other master)
  TO_NODE_ID=$(redis-cli -h $ENTRYPOINT_HOST cluster nodes | grep master | grep -v "${NODE_ID}" | head -n 1 | awk '{ print $1 }')
  if [ -z "${TO_NODE_ID}" ]; then
    echo "FATAL: Could not find another master to migrate slots to."
    exit 1
  fi

  # 4. Migrate slots
  echo "Migrating all slots from ${NODE_ID} to ${TO_NODE_ID}"
  redis-cli --cluster reshard $ENTRYPOINT_WITH_PORT --cluster-from ${NODE_ID} --cluster-to ${TO_NODE_ID} --cluster-slots 16384 --cluster-yes
fi

# --- CORRECTED LOGIC ---

echo "Drain complete. Now safely removing nodes from cluster."

# 5. Find ALL replicas of the drained master
# Use awk to find slaves where field 4 (master_id) matches our NODE_ID
REPLICA_IDS=$(redis-cli -h $ENTRYPOINT_HOST cluster nodes | awk -v master_id="${NODE_ID}" '$3 == "slave" && $4 == master_id { print $1 }' || true)

if [ -n "${REPLICA_IDS}" ]; then
  echo "Found replicas: ${REPLICA_IDS}. Removing them from the cluster first."
  
  # Loop through each ID (it's a multi-line string)
  for rep_id in ${REPLICA_IDS}; do
    echo "Removing replica ${rep_id}..."
    # Send command, retry once if it fails
    redis-cli --cluster del-node $ENTRYPOINT_WITH_PORT ${rep_id} || \
      (sleep 5 && redis-cli --cluster del-node $ENTRYPOINT_WITH_PORT ${rep_id})
    echo "Replica ${rep_id} removal command sent."
    sleep 2 # Small pause
  done
  
  sleep 5 # Give cluster time to update after all replicas are gone
else
  echo "WARN: Could not find any replicas for master ${NODE_ID}. Proceeding to remove master."
fi

# 6. Remove the (now empty) master node
echo "Removing drained master ${NODE_ID} from the cluster."
redis-cli --cluster del-node $ENTRYPOINT_WITH_PORT ${NODE_ID} || \
  (sleep 5 && redis-cli --cluster del-node $ENTRYPOINT_WITH_PORT ${NODE_ID})
echo "Master removal command sent."

# 7. Final check
sleep 5
echo "Final cluster state:"
redis-cli -h $ENTRYPOINT_HOST cluster nodes

echo "--- Drain & Delete Job Successful ---"
`

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name + "-drain", // Job name
			Namespace: cluster.Namespace,
			Labels:    getLabels(cluster),
		},
		Spec: batchv1.JobSpec{
			ActiveDeadlineSeconds: &timeout,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:    "drain",
							Image:   fmt.Sprintf("redis:%s", cluster.Spec.RedisVersion),
							Command: []string{"sh", "-c"},
							Args:    []string{cliCmd},
							Env: []corev1.EnvVar{
								{
									Name:  "POD_TO_DRAIN_IP",
									Value: podToDrainIP,
								},
								{
									Name:  "ENTRYPOINT_HOST", // Hostname without port
									Value: anyPodHost,
								},
								{
									Name:  "ENTRYPOINT_WITH_PORT", // Hostname:port
									Value: entrypointWithPort,
								},
							},
						},
					},
				},
			},
			BackoffLimit: &backoff,
		},
	}
}

// reshardJobForRedisCluster defines the Job to rebalance the cluster.
// --- THIS FUNCTION IS UNCHANGED ---
func (r *RedisClusterReconciler) reshardJobForRedisCluster(cluster *appv1.RedisCluster) *batchv1.Job {
	anyPodHost := fmt.Sprintf("%s-0.%s.%s.svc.cluster.local", cluster.Name, cluster.Name+"-headless", cluster.Namespace)
	anyPodPort := "6379"
	anyPodEntrypoint := fmt.Sprintf("%s:%s", anyPodHost, anyPodPort)

	serviceName := cluster.Name + "-headless"
	expectedNodes := cluster.Spec.Masters * (1 + cluster.Spec.ReplicasPerMaster)
	timeout := 300
	if cluster.Spec.ReshardTimeoutSeconds > 0 {
		timeout = int(cluster.Spec.ReshardTimeoutSeconds)
	}

	cliCmd := fmt.Sprintf(
		`
set -x

echo "--- Smart Reshard Job Started ---"
wait_until=$(($(date +%%s) + %d))
rebalance_timeout=$((%d - 30))
[ $rebalance_timeout -lt 60 ] && rebalance_timeout=60

EXPECTED_NODES=%d
ANY_POD_HOST="%s"
ANY_POD_PORT="%s"
ANY_POD_ENTRYPOINT="%s"
CLUSTER_NAME="%s"
SERVICE_NAME="%s"
NAMESPACE="%s"

echo "Running 'redis-cli --cluster fix' to clean up any stuck slots..."
yes | redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT --cluster fix $ANY_POD_ENTRYPOINT
echo "Cluster fix complete."

echo "Checking for orphaned nodes..."
cluster_nodes_output=$(redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT cluster nodes)
actual_nodes_count=$(echo "$cluster_nodes_output" | grep -v fail | wc -l)
known_node_ips=$(echo "$cluster_nodes_output" | grep -v fail | awk '{ print $2 }' | cut -d'@' -f1 | cut -d':' -f1)
echo "$known_node_ips"

if [ "$actual_nodes_count" -lt "$EXPECTED_NODES" ]; then
  echo "Finding orphan nodes to add..."
  orphan_nodes_dns=""
  for i in $(seq 0 $(($EXPECTED_NODES - 1))); do
    pod_dns="${CLUSTER_NAME}-${i}.${SERVICE_NAME}.${NAMESPACE}.svc.cluster.local"
    pod_ip=$(getent hosts $pod_dns | awk '{ print $1 }' || true)
    if [ -z "$pod_ip" ]; then
      echo "Skipping unresolved DNS: $pod_dns"
      continue
    fi
    if ! echo "$known_node_ips" | grep -q "$pod_ip"; then
      echo "Found orphan: $pod_dns (IP: $pod_ip)"
      orphan_nodes_dns="$orphan_nodes_dns $pod_dns"
    fi
  done

  master_dns=""
  for orphan in $orphan_nodes_dns; do
    if [ -z "$master_dns" ]; then
      master_dns=$orphan
    else
      replica_dns=$orphan
      echo "Adding MASTER=$master_dns, REPLICA=$replica_dns"
      redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT --cluster add-node ${master_dns}:6379 $ANY_POD_ENTRYPOINT || true
      sleep 5
      new_master_ip=$(getent hosts $master_dns | awk '{ print $1 }')
      new_master_id=$(redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT cluster nodes | grep $new_master_ip | grep master | awk '{ print $1 }')
      redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT --cluster add-node ${replica_dns}:6379 $ANY_POD_ENTRYPOINT --cluster-slave --cluster-master-id $new_master_id || true
      master_dns=""
    fi
  done
fi

echo "Waiting for all $EXPECTED_NODES nodes to join..."
while true; do
  joined=$(redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT cluster nodes | grep -v fail | wc -l)
  [ "$joined" -eq "$EXPECTED_NODES" ] && break
  [ $(date +%%s) -gt $wait_until ] && echo "Timeout waiting for nodes." && exit 1
  echo "Waiting... $joined/$EXPECTED_NODES nodes joined."
  sleep 5
done

echo "Waiting for an empty master..."
while true; do
  empty_masters=$(redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT cluster nodes | grep master | grep -v fail | awk '$9==""' | wc -l)
  [ "$empty_masters" -gt 0 ] && echo "Found $empty_masters empty master(s)." && break
  [ $(date +%%s) -gt $wait_until ] && echo "Timeout: no empty masters found." && exit 1
  echo "Waiting... still none."
  sleep 5
done

# --- Disable full coverage on all nodes ---
echo "Disabling full coverage check on all nodes..."
node_ips=$(redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT cluster nodes | awk '{ print $2 }' | cut -d'@' -f1 | cut -d':' -f1 | sort -u)
for ip in $node_ips; do
  if redis-cli -h $ip -p 6379 ping >/dev/null 2%1; then
    redis-cli -h $ip -p 6379 config set cluster-require-full-coverage no || echo "WARN: Failed config set on $ip"
  else
    echo "WARN: Node $ip not reachable for config set"
  fi
done
sleep 3

# --- Run rebalance with retry ---
echo "Starting rebalance attempt #1..."
if timeout $rebalance_timeout redis-cli --cluster rebalance $ANY_POD_ENTRYPOINT --cluster-use-empty-masters --cluster-yes; then
  echo "Rebalance succeeded on attempt #1"
else
  echo "Rebalance failed, running 'redis-cli --cluster fix' and retrying..."
  yes | redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT --cluster fix $ANY_POD_ENTRYPOINT
  sleep 5
  echo "Starting rebalance attempt #2..."
  if timeout $rebalance_timeout redis-cli --cluster rebalance $ANY_POD_ENTRYPOINT --cluster-use-empty-masters --cluster-yes; then
    echo "Rebalance succeeded on attempt #2"
  else
    echo "ERROR: Rebalance failed twice. Dumping state..."
    redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT cluster nodes
    redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT cluster info
    exit 1
  fi
fi

# --- Re-enable full coverage on all nodes ---
echo "Re-enabling full coverage on all nodes..."
for ip in $node_ips; do
  if redis-cli -h $ip -p 6379 ping >/dev/null 2%1; then
    redis-cli -h $ip -p 6379 config set cluster-require-full-coverage yes || echo "WARN: Failed re-enable on $ip"
  fi
done
sleep 3

echo "--- Smart Reshard Job Finished ---"
`,
		timeout,
		timeout,
		expectedNodes,
		anyPodHost,
		anyPodPort,
		anyPodEntrypoint,
		cluster.Name,
		serviceName,
		cluster.Namespace,
	)

	ads := int64(timeout + 120)
	backoff := int32(0)

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name + "-reshard",
			Namespace: cluster.Namespace,
			Labels:    getLabels(cluster),
		},
		Spec: batchv1.JobSpec{
			ActiveDeadlineSeconds: &ads,
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
			BackoffLimit: &backoff,
		},
	}
}
