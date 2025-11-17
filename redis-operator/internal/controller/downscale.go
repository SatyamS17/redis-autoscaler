package controller

import (
	"context"
	"fmt"
	"time"

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

// checkDrainStatus checks the progress of the drain job.
func (r *RedisClusterReconciler) checkDrainStatus(ctx context.Context, cluster *appv1.RedisCluster) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	jobName := cluster.Name + "-drain"

	// 1. Check if the drain Job exists
	drainJob := &batchv1.Job{}
	err := r.Get(ctx, client.ObjectKey{Name: jobName, Namespace: cluster.Namespace}, drainJob)

	if err != nil && errors.IsNotFound(err) {
		// Job not found, so we need to create it.
		podName := cluster.Status.PodToDrain
		destPod1 := cluster.Status.DrainDestPod1
		destPod2 := cluster.Status.DrainDestPod2

		if podName == "" || destPod1 == "" {
			logger.Error(fmt.Errorf("IsDraining is true but drain info is incomplete"), "State error")
			cluster.Status.IsDraining = false
			cluster.Status.PodToDrain = ""
			cluster.Status.DrainDestPod1 = ""
			cluster.Status.DrainDestPod2 = ""
			_ = r.Status().Update(ctx, cluster)
			return ctrl.Result{}, nil
		}

		// Get the pod to find its IP
		podToDrain := &corev1.Pod{}
		if err := r.Get(ctx, types.NamespacedName{Name: podName, Namespace: cluster.Namespace}, podToDrain); err != nil {
			logger.Error(err, "Failed to get Pod object to find IP for draining", "pod", podName)
			cluster.Status.IsDraining = false
			cluster.Status.PodToDrain = ""
			cluster.Status.DrainDestPod1 = ""
			cluster.Status.DrainDestPod2 = ""
			_ = r.Status().Update(ctx, cluster)
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}

		logger.Info("Creating smart drain Job",
			"pod", podName,
			"dest1", destPod1,
			"dest2", destPod2,
		)

		job := r.drainJobForRedisCluster(cluster, podName, destPod1, destPod2)
		if err := controllerutil.SetControllerReference(cluster, job, r.Scheme); err != nil {
			logger.Error(err, "Failed to set owner ref on drain Job")
			return ctrl.Result{}, err
		}
		if err := r.Create(ctx, job); err != nil {
			logger.Error(err, "Failed to create drain Job")
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil

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
		cluster.Status.DrainDestPod1 = ""
		cluster.Status.DrainDestPod2 = ""
		now := metav1.Now()
		cluster.Status.LastScaleTime = &now
		if err := r.Status().Update(ctx, cluster); err != nil {
			logger.Error(err, "Failed to update status after drain")
			return ctrl.Result{}, err
		}

		// Clean up the successful job
		_ = r.Delete(ctx, drainJob, client.PropagationPolicy(metav1.DeletePropagationBackground))

		logger.Info("Scale-down complete. Main reconciler will now remove the pod.")
		return ctrl.Result{}, nil
	}

	if drainJob.Status.Failed > 0 {
		logger.Error(fmt.Errorf("drain job %s failed", jobName), "Draining failed")
		// Clean up the failed job to allow a retry
		_ = r.Delete(ctx, drainJob, client.PropagationPolicy(metav1.DeletePropagationBackground))
		cluster.Status.IsDraining = false
		cluster.Status.PodToDrain = ""
		cluster.Status.DrainDestPod1 = ""
		cluster.Status.DrainDestPod2 = ""
		if err := r.Status().Update(ctx, cluster); err != nil {
			logger.Error(err, "Failed to update status after failed drain")
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 1 * time.Second}, nil
	}

	logger.Info("Drain job is still running...")
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

// smartDrainJobForRedisCluster creates a job that:
// 1. Drains the highest-index master (podToDrain)
// 2. Splits load to destPod1 and destPod2 (if provided)
// 3. Uses replica pre-seeding for fast data copy
// 4. Removes the drained pod safely
//
// destPod2 can be empty string if highest index is one of the low-util pods
func (r *RedisClusterReconciler) drainJobForRedisCluster(
	cluster *appv1.RedisCluster,
	podToDrain string,
	destPod1 string,
	destPod2 string,
) *batchv1.Job {
	anyPodHost := fmt.Sprintf("%s-0.%s.%s.svc.cluster.local",
		cluster.Name, cluster.Name+"-headless", cluster.Namespace)
	entrypoint := fmt.Sprintf("%s:6379", anyPodHost)

	timeout := int64(600)
	backoff := int32(0)

	cliCmd := `
#!/bin/sh
set -ex

echo "=== Smart Scale-Down with Two-Way Split ==="
POD_TO_DRAIN="$POD_TO_DRAIN"
DEST_POD_1="$DEST_POD_1"
DEST_POD_2="$DEST_POD_2"
SERVICE_NAME="$SERVICE_NAME"
NAMESPACE="$NAMESPACE"
ENTRYPOINT_HOST="$ENTRYPOINT_HOST"
ENTRYPOINT="$ENTRYPOINT_WITH_PORT"

# ========== CLUSTER FIX (ADDED) ==========
echo "=== Step 0: Running cluster fix to ensure consistency ==="
redis-cli --cluster fix $ENTRYPOINT --cluster-fix-with-unreachable-masters || {
  echo "WARNING: Cluster fix encountered issues, but continuing..."
}

# Verify cluster state after fix
CLUSTER_STATE=$(redis-cli -h $ENTRYPOINT_HOST cluster info | grep cluster_state | cut -d: -f2 | tr -d '\r')
if [ "$CLUSTER_STATE" != "ok" ]; then
  echo "ERROR: Cluster state is '$CLUSTER_STATE' after fix (expected: ok)"
  redis-cli -h $ENTRYPOINT_HOST cluster info
  redis-cli -h $ENTRYPOINT_HOST cluster nodes
  exit 1
fi

echo "Cluster fix complete. State: $CLUSTER_STATE"
# ========================================

# 1. Resolve IPs
echo "Resolving pod IPs..."
POD_TO_DRAIN_FQDN="${POD_TO_DRAIN}.${SERVICE_NAME}.${NAMESPACE}.svc.cluster.local"
POD_IP=$(getent hosts $POD_TO_DRAIN_FQDN | awk '{print $1}')

DEST1_FQDN="${DEST_POD_1}.${SERVICE_NAME}.${NAMESPACE}.svc.cluster.local"
DEST1_IP=$(getent hosts $DEST1_FQDN | awk '{print $1}')

if [ -z "$POD_IP" ]; then
  echo "ERROR: Could not resolve IP for $POD_TO_DRAIN"
  exit 1
fi

if [ -z "$DEST1_IP" ]; then
  echo "ERROR: Could not resolve IP for $DEST_POD_1"
  exit 1
fi

echo "Pod to drain: $POD_TO_DRAIN (IP: $POD_IP)"
echo "Destination 1: $DEST_POD_1 (IP: $DEST1_IP)"

# Check if we have a second destination
DEST2_IP=""
if [ -n "$DEST_POD_2" ]; then
  DEST2_FQDN="${DEST_POD_2}.${SERVICE_NAME}.${NAMESPACE}.svc.cluster.local"
  DEST2_IP=$(getent hosts $DEST2_FQDN | awk '{print $1}')
  echo "Destination 2: $DEST_POD_2 (IP: $DEST2_IP)"
fi

# 2. Find node IDs
echo "Finding Redis node IDs..."
NODE_TO_DRAIN=$(redis-cli -h $ENTRYPOINT_HOST cluster nodes | \
  grep "$POD_IP:6379" | awk '{print $1}')

if [ -z "$NODE_TO_DRAIN" ]; then
  echo "Node with IP $POD_IP not found. Assuming already removed."
  exit 0
fi
echo "Node to drain: $NODE_TO_DRAIN"

DEST1_ID=$(redis-cli -h $ENTRYPOINT_HOST cluster nodes | \
  grep "$DEST1_IP:6379" | grep master | awk '{print $1}')

if [ -z "$DEST1_ID" ]; then
  echo "ERROR: Could not find master node for $DEST_POD_1"
  exit 1
fi
echo "Destination 1 node ID: $DEST1_ID"

DEST2_ID=""
if [ -n "$DEST2_IP" ]; then
  DEST2_ID=$(redis-cli -h $ENTRYPOINT_HOST cluster nodes | \
    grep "$DEST2_IP:6379" | grep master | awk '{print $1}')
  
  if [ -z "$DEST2_ID" ]; then
    echo "ERROR: Could not find master node for $DEST_POD_2"
    exit 1
  fi
  echo "Destination 2 node ID: $DEST2_ID"
fi

# 3. Check if node has slots
SLOT_COUNT=$(redis-cli -h $ENTRYPOINT_HOST cluster nodes | \
  grep "^$NODE_TO_DRAIN" | awk '{
    slots=0
    for(i=9; i<=NF; i++) {
      if($i ~ /^[0-9]+-[0-9]+$/) {
        split($i, range, "-")
        slots += (range[2] - range[1] + 1)
      } else if($i ~ /^[0-9]+$/) {
        slots += 1
      }
    }
    print slots
  }')

echo "Node has $SLOT_COUNT slots"

if [ "$SLOT_COUNT" -eq 0 ]; then
  echo "Node has no slots. Skipping migration, going straight to removal."
else
  # 4. Pre-seed destinations via replication
  echo "Pre-seeding data via replication..."
  
  # Find the replica of DEST1
  DEST1_REPLICA=$(redis-cli -h $ENTRYPOINT_HOST cluster nodes | \
    awk -v master="$DEST1_ID" '$3=="slave" && $4==master {print $2}' | \
    head -n1 | cut -d':' -f1 | cut -d'@' -f1)

  # Make DEST1's replica temporarily replicate from NODE_TO_DRAIN
  if [ -n "$DEST1_REPLICA" ]; then
    echo "Making $DEST1_REPLICA a replica of $NODE_TO_DRAIN for pre-seed..."
    redis-cli -h $DEST1_REPLICA -p 6379 CLUSTER REPLICATE $NODE_TO_DRAIN || true
  fi

  # If we have a second destination, do the same
  DEST2_REPLICA=""
  if [ -n "$DEST2_ID" ]; then
    DEST2_REPLICA=$(redis-cli -h $ENTRYPOINT_HOST cluster nodes | \
      awk -v master="$DEST2_ID" '$3=="slave" && $4==master {print $2}' | \
      head -n1 | cut -d':' -f1 | cut -d'@' -f1)
    
    if [ -n "$DEST2_REPLICA" ]; then
      echo "Making $DEST2_REPLICA a replica of $NODE_TO_DRAIN for pre-seed..."
      redis-cli -h $DEST2_REPLICA -p 6379 CLUSTER REPLICATE $NODE_TO_DRAIN || true
    fi
  fi

  # Wait for replication sync
  echo "Waiting for replication to sync..."
  sleep 10
  
  max_wait=120
  elapsed=0
  while true; do
    drain_offset=$(redis-cli -h $POD_IP -p 6379 INFO replication | \
      grep master_repl_offset | cut -d':' -f2 | tr -d '\r')
    
    synced=1
    
    if [ -n "$DEST1_REPLICA" ]; then
      rep1_offset=$(redis-cli -h $DEST1_REPLICA -p 6379 INFO replication | \
        grep master_repl_offset | cut -d':' -f2 | tr -d '\r')
      lag1=$((drain_offset - rep1_offset))
      echo "Replica 1 lag: $lag1 bytes"
      if [ "$lag1" -gt 1000 ] || [ "$lag1" -lt 0 ]; then
        synced=0
      fi
    fi
    
    if [ -n "$DEST2_REPLICA" ]; then
      rep2_offset=$(redis-cli -h $DEST2_REPLICA -p 6379 INFO replication | \
        grep master_repl_offset | cut -d':' -f2 | tr -d '\r')
      lag2=$((drain_offset - rep2_offset))
      echo "Replica 2 lag: $lag2 bytes"
      if [ "$lag2" -gt 1000 ] || [ "$lag2" -lt 0 ]; then
        synced=0
      fi
    fi
    
    if [ "$synced" -eq 1 ]; then
      echo "All replicas synced!"
      break
    fi
    
    if [ $elapsed -gt $max_wait ]; then
      echo "WARNING: Replication sync timeout. Proceeding anyway..."
      break
    fi
    
    sleep 5
    elapsed=$((elapsed + 5))
  done

  # 5. Restore original replication relationships
  echo "Restoring original replication relationships..."
  if [ -n "$DEST1_REPLICA" ]; then
    redis-cli -h $DEST1_REPLICA -p 6379 CLUSTER REPLICATE $DEST1_ID || true
  fi
  if [ -n "$DEST2_REPLICA" ]; then
    redis-cli -h $DEST2_REPLICA -p 6379 CLUSTER REPLICATE $DEST2_ID || true
  fi
  sleep 3

  # 6. Disable full coverage
  echo "Disabling full coverage requirement..."
  node_ips=$(redis-cli -h $ENTRYPOINT_HOST cluster nodes | \
    awk '{print $2}' | cut -d'@' -f1 | cut -d':' -f1 | sort -u)
  for ip in $node_ips; do
    redis-cli -h $ip -p 6379 CONFIG SET cluster-require-full-coverage no || true
  done
  sleep 2

  # 7. Migrate slots
  if [ -n "$DEST2_ID" ]; then
    # Split between two destinations
    HALF_SLOTS=$((SLOT_COUNT / 2))
    REMAINING_SLOTS=$((SLOT_COUNT - HALF_SLOTS))
    
    echo "Migrating $HALF_SLOTS slots to $DEST1_ID..."
    echo "$HALF_SLOTS" | redis-cli --cluster reshard $ENTRYPOINT \
      --cluster-from $NODE_TO_DRAIN \
      --cluster-to $DEST1_ID \
      --cluster-yes \
      --cluster-timeout 10000 || true

    sleep 5

    echo "Migrating remaining $REMAINING_SLOTS slots to $DEST2_ID..."
    echo "$REMAINING_SLOTS" | redis-cli --cluster reshard $ENTRYPOINT \
      --cluster-from $NODE_TO_DRAIN \
      --cluster-to $DEST2_ID \
      --cluster-yes \
      --cluster-timeout 10000 || true
  else
    # All slots go to single destination
    echo "Migrating all $SLOT_COUNT slots to $DEST1_ID..."
    echo "$SLOT_COUNT" | redis-cli --cluster reshard $ENTRYPOINT \
      --cluster-from $NODE_TO_DRAIN \
      --cluster-to $DEST1_ID \
      --cluster-yes \
      --cluster-timeout 10000 || true
  fi

  sleep 5

  # Re-enable full coverage
  echo "Re-enabling full coverage requirement..."
  for ip in $node_ips; do
    redis-cli -h $ip -p 6379 CONFIG SET cluster-require-full-coverage yes || true
  done
  sleep 2
fi

# 8. Remove all replicas of the drained master
echo "Removing replicas of drained master..."
REPLICA_IDS=$(redis-cli -h $ENTRYPOINT_HOST cluster nodes | \
  awk -v master="$NODE_TO_DRAIN" '$3=="slave" && $4==master {print $1}')

for rep_id in $REPLICA_IDS; do
  echo "Removing replica $rep_id..."
  redis-cli --cluster del-node $ENTRYPOINT $rep_id || \
    (sleep 5 && redis-cli --cluster del-node $ENTRYPOINT $rep_id)
  sleep 2
done

# 9. Remove the drained master
echo "Removing drained master $NODE_TO_DRAIN..."
redis-cli --cluster del-node $ENTRYPOINT $NODE_TO_DRAIN || \
  (sleep 5 && redis-cli --cluster del-node $ENTRYPOINT $NODE_TO_DRAIN)

# 10. Verify
echo "Final cluster state:"
redis-cli -h $ENTRYPOINT_HOST cluster nodes
redis-cli -h $ENTRYPOINT_HOST cluster info

echo "=== Smart Scale-Down Complete ==="
`

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name + "-drain",
			Namespace: cluster.Namespace,
			Labels:    getLabels(cluster),
		},
		Spec: batchv1.JobSpec{
			ActiveDeadlineSeconds: &timeout,
			BackoffLimit:          &backoff,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:    "smart-drain",
							Image:   fmt.Sprintf("redis:%s", cluster.Spec.RedisVersion),
							Command: []string{"sh", "-c"},
							Args:    []string{cliCmd},
							Env: []corev1.EnvVar{
								{Name: "POD_TO_DRAIN", Value: podToDrain},
								{Name: "DEST_POD_1", Value: destPod1},
								{Name: "DEST_POD_2", Value: destPod2},
								{Name: "SERVICE_NAME", Value: cluster.Name + "-headless"},
								{Name: "NAMESPACE", Value: cluster.Namespace},
								{Name: "ENTRYPOINT_HOST", Value: anyPodHost},
								{Name: "ENTRYPOINT_WITH_PORT", Value: entrypoint},
							},
						},
					},
				},
			},
		},
	}
}
