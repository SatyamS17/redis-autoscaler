package controller

import (
	"context"
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	appv1 "github.com/myuser/redis-operator/api/v1"
)

// checkDrainStatus monitors the scale-down operation progress.
// It creates the drain job if needed, monitors its progress, and finalizes the scale-down
// by decrementing the master count and updating the standby pod reference.
func (r *RedisClusterReconciler) checkDrainStatus(ctx context.Context, cluster *appv1.RedisCluster) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	jobName := cluster.Name + "-drain"

	drainJob := &batchv1.Job{}
	err := r.Get(ctx, client.ObjectKey{Name: jobName, Namespace: cluster.Namespace}, drainJob)

	if err != nil && errors.IsNotFound(err) {
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

		if podName == cluster.Status.StandbyPod {
			logger.Error(fmt.Errorf("Attempted to drain standby pod"), "Invalid operation",
				"standbyPod", cluster.Status.StandbyPod)
			cluster.Status.IsDraining = false
			cluster.Status.PodToDrain = ""
			cluster.Status.DrainDestPod1 = ""
			cluster.Status.DrainDestPod2 = ""
			_ = r.Status().Update(ctx, cluster)
			return ctrl.Result{}, nil
		}

		logger.Info("Creating drain job",
			"pod", podName,
			"dest1", destPod1,
			"dest2", destPod2)

		job := r.drainJobForRedisCluster(cluster, podName, destPod1, destPod2)
		if err := controllerutil.SetControllerReference(cluster, job, r.Scheme); err != nil {
			logger.Error(err, "Failed to set owner reference on drain job")
			return ctrl.Result{}, err
		}
		if err := r.Create(ctx, job); err != nil {
			logger.Error(err, "Failed to create drain job")
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil

	} else if err != nil {
		logger.Error(err, "Failed to get drain job")
		return ctrl.Result{}, err
	}

	if drainJob.Status.Succeeded > 0 {
		logger.Info("Drain job succeeded, pod is empty, removing old standby from cluster")

		drainedPod := cluster.Status.PodToDrain
		oldStandby := cluster.Status.StandbyPod

		// Check if cleanup job exists to remove old standby from cluster
		cleanupJobName := cluster.Name + "-cleanup-standby"
		cleanupJob := &batchv1.Job{}
		err := r.Get(ctx, client.ObjectKey{Name: cleanupJobName, Namespace: cluster.Namespace}, cleanupJob)

		if err != nil && errors.IsNotFound(err) {
			logger.Info("Creating cleanup job to remove old standby from cluster",
				"oldStandby", oldStandby,
				"drainedPod", drainedPod)

			job := r.cleanupStandbyJobForRedisCluster(cluster, oldStandby, drainedPod)
			if err := controllerutil.SetControllerReference(cluster, job, r.Scheme); err != nil {
				logger.Error(err, "Failed to set owner reference on cleanup job")
				return ctrl.Result{}, err
			}
			if err := r.Create(ctx, job); err != nil {
				logger.Error(err, "Failed to create cleanup job")
				return ctrl.Result{}, err
			}
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil

		} else if err != nil {
			logger.Error(err, "Failed to get cleanup job")
			return ctrl.Result{}, err
		}

		// Wait for cleanup job to complete
		if cleanupJob.Status.Succeeded == 0 && cleanupJob.Status.Failed == 0 {
			logger.Info("Cleanup job is still running, waiting...")
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}

		if cleanupJob.Status.Failed > 0 {
			logger.Error(fmt.Errorf("cleanup job failed"), "Failed to remove old standby from cluster")
			// Clean up the failed job to allow retry
			_ = r.Delete(ctx, cleanupJob, client.PropagationPolicy(metav1.DeletePropagationBackground))
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}

		// Cleanup job succeeded - now safe to scale down StatefulSet
		logger.Info("Cleanup job succeeded, old standby removed from cluster, scaling down StatefulSet")

		// Decrement masters count
		cluster.Spec.Masters--
		if err := r.Update(ctx, cluster); err != nil {
			logger.Error(err, "Failed to update spec to decrease masters after drain")
			return ctrl.Result{}, err
		}

		// Scale down StatefulSet to remove old standby pod
		// The StatefulSet will delete the highest-index pod (the old standby)
		sts := r.statefulSetForRedisCluster(cluster)
		logger.Info("Scaling down StatefulSet to remove old standby",
			"oldStandby", oldStandby,
			"newStandby", drainedPod)
		if err := r.reconcileStatefulSet(ctx, cluster, sts); err != nil {
			logger.Error(err, "Failed to reconcile StatefulSet after scale-down")
		}

		// Drained pod becomes the new standby
		cluster.Status.StandbyPod = drainedPod

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

		_ = r.Delete(ctx, drainJob, client.PropagationPolicy(metav1.DeletePropagationBackground))
		_ = r.Delete(ctx, cleanupJob, client.PropagationPolicy(metav1.DeletePropagationBackground))

		logger.Info("Scale-down complete, StatefulSet will delete old standby pods",
			"newMasters", cluster.Spec.Masters,
			"oldStandby", oldStandby,
			"newStandby", drainedPod)
		return ctrl.Result{}, nil
	}

	if drainJob.Status.Failed > 0 {
		logger.Error(fmt.Errorf("drain job %s failed", jobName), "Draining failed")
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

	logger.Info("Drain job is still running")
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

// drainJobForRedisCluster creates a Kubernetes Job that performs the scale-down draining.
// It uses pre-seeding via replication to speed up the migration, then moves slots from the
// drained pod to the destination pod(s). The drained pod becomes the new standby.
func (r *RedisClusterReconciler) drainJobForRedisCluster(
	cluster *appv1.RedisCluster,
	podToDrain string,
	destPod1 string,
	destPod2 string,
) *batchv1.Job {
	anyPodHost := fmt.Sprintf("%s-0.%s.%s.svc.cluster.local",
		cluster.Name, cluster.Name+"-headless", cluster.Namespace)
	entrypoint := fmt.Sprintf("%s:6379", anyPodHost)

	timeout := int64(cluster.Spec.ReshardTimeoutSeconds)
	backoff := int32(0)

	cliCmd := `
#!/bin/sh
set -ex

echo "=== Smart Scale-Down with Standby Preservation ==="
POD_TO_DRAIN="$POD_TO_DRAIN"
DEST_POD_1="$DEST_POD_1"
DEST_POD_2="$DEST_POD_2"
STANDBY_POD="$STANDBY_POD"
SERVICE_NAME="$SERVICE_NAME"
NAMESPACE="$NAMESPACE"
ENTRYPOINT_HOST="$ENTRYPOINT_HOST"
ENTRYPOINT="$ENTRYPOINT_WITH_PORT"

echo "Pod to drain: $POD_TO_DRAIN (will become new standby)"
echo "Current standby: $STANDBY_POD (will become active master)"
echo "Destinations: $DEST_POD_1, $DEST_POD_2"

# ========== CLUSTER FIX ==========
echo "=== Step 0: Quick cluster health check ==="
CLUSTER_STATE=$(redis-cli -h $ENTRYPOINT_HOST cluster info | grep cluster_state | cut -d: -f2 | tr -d '\r')

if [ "$CLUSTER_STATE" = "ok" ]; then
  echo "Cluster state is OK, skipping cluster fix"
else
  echo "Cluster state is '$CLUSTER_STATE', running fix..."
  timeout 30 redis-cli --cluster fix $ENTRYPOINT --cluster-fix-with-unreachable-masters || {
    echo "WARNING: Cluster fix failed or timed out, continuing anyway..."
  }
fi

CLUSTER_STATE=$(redis-cli -h $ENTRYPOINT_HOST cluster info | grep cluster_state | cut -d: -f2 | tr -d '\r')
if [ "$CLUSTER_STATE" != "ok" ]; then
  echo "ERROR: Cluster state is '$CLUSTER_STATE' after fix (expected: ok)"
  redis-cli -h $ENTRYPOINT_HOST cluster info
  redis-cli -h $ENTRYPOINT_HOST cluster nodes
  exit 1
fi

echo "Cluster fix complete. State: $CLUSTER_STATE"

# ========== GHOST NODE CLEANUP ==========
echo "=== Step 0.5: Cleanup failed/disconnected nodes ==="
FAILED_NODES=$(redis-cli -h $ENTRYPOINT_HOST -p 6379 cluster nodes | grep -E 'fail|disconnected|noaddr' | awk '{print $1}')

if [ -n "$FAILED_NODES" ]; then
  FAILED_COUNT=$(echo "$FAILED_NODES" | wc -w)
  echo "Found $FAILED_COUNT failed/ghost nodes to clean up"
  
  HEALTHY_IPS=$(redis-cli -h $ENTRYPOINT_HOST -p 6379 cluster nodes | \
    grep -v -E 'fail|disconnected|noaddr' | \
    awk '{print $2}' | cut -d'@' -f1 | cut -d':' -f1 | sort -u)
  
  for failed_id in $FAILED_NODES; do
    echo "Forgetting failed node: $failed_id"
    for ip in $HEALTHY_IPS; do
      redis-cli -h $ip -p 6379 CLUSTER FORGET $failed_id 2>/dev/null || true
    done
  done
  
  sleep 2
  echo "Ghost node cleanup complete"
else
  echo "No failed nodes found - cluster is clean"
fi

# ========== VERIFY STANDBY ==========
echo "=== Step 1: Verify standby node ==="
STANDBY_FQDN="${STANDBY_POD}.${SERVICE_NAME}.${NAMESPACE}.svc.cluster.local"
STANDBY_IP=$(getent hosts $STANDBY_FQDN | awk '{print $1}')

if [ -z "$STANDBY_IP" ]; then
  echo "ERROR: Could not resolve standby pod $STANDBY_POD"
  exit 1
fi

STANDBY_NODE_ID=$(redis-cli -h $ENTRYPOINT_HOST cluster nodes | grep "$STANDBY_IP:6379" | grep master | awk '{print $1}')

if [ -z "$STANDBY_NODE_ID" ]; then
  echo "ERROR: Standby node not found in cluster"
  exit 1
fi

# Verify standby has no slots
STANDBY_SLOTS=$(redis-cli -h $ENTRYPOINT_HOST cluster nodes | grep "^$STANDBY_NODE_ID " | awk '{
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

if [ "$STANDBY_SLOTS" -ne 0 ]; then
  echo "WARNING: Standby node has $STANDBY_SLOTS slots (expected 0)"
  echo "This is unusual but we'll continue..."
fi

echo "Standby verified: $STANDBY_POD (ID: $STANDBY_NODE_ID)"

# ========== RESOLVE IPs ==========
echo "=== Step 2: Resolving pod IPs ==="
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

# ========== FIND NODE IDs ==========
echo "=== Step 3: Finding Redis node IDs ==="
NODE_TO_DRAIN=$(redis-cli -h $ENTRYPOINT_HOST cluster nodes | \
  grep "$POD_IP:6379" | grep master | awk '{print $1}')

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

# ========== CHECK SLOT COUNT ==========
echo "=== Step 4: Checking slot count ==="
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
  echo "Node has no slots. Skipping migration."
else
  # ========== DISABLE FULL COVERAGE ==========
  echo "=== Step 5: Disabling full coverage requirement ==="
  node_ips=$(redis-cli -h $ENTRYPOINT_HOST cluster nodes | \
    awk '{print $2}' | cut -d'@' -f1 | cut -d':' -f1 | sort -u)
  for ip in $node_ips; do
    timeout 5 redis-cli -h $ip -p 6379 CONFIG SET cluster-require-full-coverage no || true  
  done
  sleep 2

  # ========== MIGRATE SLOTS ==========
  echo "=== Step 6: Migrating slots ==="
  if [ -n "$DEST2_ID" ]; then
    # Split between two destinations
    HALF_SLOTS=$((SLOT_COUNT / 2))
    REMAINING_SLOTS=$((SLOT_COUNT - HALF_SLOTS))
    
    echo "Migrating $HALF_SLOTS slots to $DEST1_ID..."
    echo "$HALF_SLOTS" | redis-cli --cluster reshard $ENTRYPOINT \
      --cluster-from $NODE_TO_DRAIN \
      --cluster-to $DEST1_ID \
      --cluster-yes \
      --cluster-timeout 10000 \
      --cluster-pipeline 100

    sleep 5

    echo "Migrating remaining $REMAINING_SLOTS slots to $DEST2_ID..."
    echo "$REMAINING_SLOTS" | redis-cli --cluster reshard $ENTRYPOINT \
      --cluster-from $NODE_TO_DRAIN \
      --cluster-to $DEST2_ID \
      --cluster-yes \
      --cluster-timeout 10000 \
      --cluster-pipeline 100
  else
    # All slots go to single destination
    echo "Migrating all $SLOT_COUNT slots to $DEST1_ID..."
    echo "$SLOT_COUNT" | redis-cli --cluster reshard $ENTRYPOINT \
      --cluster-from $NODE_TO_DRAIN \
      --cluster-to $DEST1_ID \
      --cluster-yes \
      --cluster-timeout 10000 \
      --cluster-pipeline 100
  fi

  sleep 5

  # ========== RE-ENABLE FULL COVERAGE ==========
  echo "=== Step 8: Re-enabling full coverage requirement ==="
  for ip in $node_ips; do
    timeout 5 redis-cli -h $ip -p 6379 CONFIG SET cluster-require-full-coverage yes || true
  done
  sleep 2
fi

# ========== VERIFY ==========
echo "=== Step 9: Final verification ==="
redis-cli -h $ENTRYPOINT_HOST cluster nodes
redis-cli -h $ENTRYPOINT_HOST cluster info

echo "=== Smart Scale-Down Complete ==="
echo "Drained pod $POD_TO_DRAIN now has 0 slots and will become new standby (along with its replica)"
echo "Its slots have been migrated away - StatefulSet will handle deletion of old standby pods"
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
								{Name: "STANDBY_POD", Value: cluster.Status.StandbyPod},
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

// cleanupStandbyJobForRedisCluster creates a Kubernetes Job that removes the old standby pods from the Redis cluster.
// This job removes both the new standby (drained pod + replicas) and old standby (previous standby + replicas),
// then re-adds the new standby pods fresh to the cluster.
func (r *RedisClusterReconciler) cleanupStandbyJobForRedisCluster(cluster *appv1.RedisCluster, standbyPod string, drainedPod string) *batchv1.Job {
	anyPodHost := fmt.Sprintf("%s-0.%s.%s.svc.cluster.local",
		cluster.Name, cluster.Name+"-headless", cluster.Namespace)
	entrypoint := fmt.Sprintf("%s:6379", anyPodHost)

	// Calculate indices
	newStandbyIndex := (cluster.Spec.Masters - 1) * (1 + cluster.Spec.ReplicasPerMaster)
	oldStandbyIndex := cluster.Spec.Masters * (1 + cluster.Spec.ReplicasPerMaster)

	timeout := int64(300) // 5 minutes should be enough
	backoff := int32(3)

	cliCmd := `
#!/bin/bash
set -ex

echo "=== Cleanup and Re-add New Standby Pods ==="
SERVICE_NAME="$SERVICE_NAME"
NAMESPACE="$NAMESPACE"
ENTRYPOINT_HOST="$ENTRYPOINT_HOST"
ENTRYPOINT="$ENTRYPOINT_WITH_PORT"
REPLICAS_PER_MASTER="$REPLICAS_PER_MASTER"
NEW_STANDBY_INDEX="$NEW_STANDBY_INDEX"
OLD_STANDBY_INDEX="$OLD_STANDBY_INDEX"
CLUSTER_NAME="$CLUSTER_NAME"

echo "New standby index: $NEW_STANDBY_INDEX (will be re-added)"
echo "Old standby index: $OLD_STANDBY_INDEX (will be deleted)"

# Get cluster nodes
cluster_nodes_output=$(redis-cli -h $ENTRYPOINT_HOST cluster nodes)

# ========== STEP 1: Delete new standby pods and their replicas ==========
echo "=== Step 1: Deleting new standby pods (index $NEW_STANDBY_INDEX + replicas) ==="

for i in $(seq 0 $REPLICAS_PER_MASTER); do
  pod_index=$((NEW_STANDBY_INDEX + i))
  POD_NAME="${CLUSTER_NAME}-${pod_index}"
  POD_FQDN="${POD_NAME}.${SERVICE_NAME}.${NAMESPACE}.svc.cluster.local"
  POD_IP=$(getent hosts $POD_FQDN | awk '{print $1}')

  if [ -z "$POD_IP" ]; then
    echo "Pod $POD_NAME not found in DNS, skipping"
    continue
  fi

  NODE_ID=$(echo "$cluster_nodes_output" | grep "$POD_IP:6379" | awk '{print $1}')

  if [ -z "$NODE_ID" ]; then
    echo "Pod $POD_NAME ($POD_IP) not found in cluster, skipping"
    continue
  fi

  echo "Deleting pod $POD_NAME (ID: $NODE_ID, IP: $POD_IP)"
  redis-cli --cluster del-node $ENTRYPOINT $NODE_ID || \
    (sleep 5 && redis-cli --cluster del-node $ENTRYPOINT $NODE_ID)
  sleep 2
done

# ========== STEP 2: Delete old standby pods and their replicas ==========
echo "=== Step 2: Deleting old standby pods (index $OLD_STANDBY_INDEX + replicas) ==="

for i in $(seq 0 $REPLICAS_PER_MASTER); do
  pod_index=$((OLD_STANDBY_INDEX + i))
  POD_NAME="${CLUSTER_NAME}-${pod_index}"
  POD_FQDN="${POD_NAME}.${SERVICE_NAME}.${NAMESPACE}.svc.cluster.local"
  POD_IP=$(getent hosts $POD_FQDN | awk '{print $1}')

  if [ -z "$POD_IP" ]; then
    echo "Pod $POD_NAME not found in DNS, skipping"
    continue
  fi

  NODE_ID=$(echo "$cluster_nodes_output" | grep "$POD_IP:6379" | awk '{print $1}')

  if [ -z "$NODE_ID" ]; then
    echo "Pod $POD_NAME ($POD_IP) not found in cluster, skipping"
    continue
  fi

  echo "Deleting pod $POD_NAME (ID: $NODE_ID, IP: $POD_IP)"
  redis-cli --cluster del-node $ENTRYPOINT $NODE_ID || \
    (sleep 5 && redis-cli --cluster del-node $ENTRYPOINT $NODE_ID)
  sleep 2
done

echo "Finished deleting old pods from cluster"
sleep 3

# ========== STEP 3: Reset new standby pods to clean state ==========
echo "=== Step 3: Resetting new standby pods to clean state ==="

# Reset the new standby master and replicas
for i in $(seq 0 $REPLICAS_PER_MASTER); do
  pod_index=$((NEW_STANDBY_INDEX + i))
  POD_NAME="${CLUSTER_NAME}-${pod_index}"
  POD_FQDN="${POD_NAME}.${SERVICE_NAME}.${NAMESPACE}.svc.cluster.local"
  POD_IP=$(getent hosts $POD_FQDN | awk '{print $1}')

  if [ -z "$POD_IP" ]; then
    echo "WARNING: Pod $POD_NAME not found in DNS, skipping reset"
    continue
  fi

  echo "Resetting pod $POD_NAME ($POD_IP)..."

  # Reset the node - this clears cluster state and data
  redis-cli -h $POD_IP -p 6379 FLUSHALL
  redis-cli -h $POD_IP -p 6379 CLUSTER RESET HARD

  sleep 2
done

echo "Reset complete, nodes are now clean"
sleep 3

# ========== STEP 4: Add new standby pods fresh to cluster ==========
echo "=== Step 4: Adding new standby pods to cluster ==="

# Calculate new standby pod indices
NEW_STANDBY_POD="${CLUSTER_NAME}-${NEW_STANDBY_INDEX}"
NEW_STANDBY_FQDN="${NEW_STANDBY_POD}.${SERVICE_NAME}.${NAMESPACE}.svc.cluster.local"
NEW_STANDBY_IP=$(getent hosts $NEW_STANDBY_FQDN | awk '{print $1}')

if [ -z "$NEW_STANDBY_IP" ]; then
  echo "ERROR: Could not resolve new standby pod $NEW_STANDBY_POD"
  exit 1
fi

echo "Adding new standby master: $NEW_STANDBY_POD ($NEW_STANDBY_IP:6379)"

# Add as fresh node (should work now after CLUSTER RESET)
redis-cli --cluster add-node ${NEW_STANDBY_IP}:6379 $ENTRYPOINT
sleep 5

cluster_nodes_output=$(redis-cli -h $ENTRYPOINT_HOST cluster nodes)
NEW_STANDBY_NODE_ID=$(echo "$cluster_nodes_output" | grep "$NEW_STANDBY_IP:6379" | awk '{print $1}')

if [ -z "$NEW_STANDBY_NODE_ID" ]; then
  echo "ERROR: Failed to get new standby node ID after adding"
  exit 1
fi
echo "New standby master added with ID: $NEW_STANDBY_NODE_ID"

# ========== STEP 5: Add replicas for new standby ==========
if [ "$REPLICAS_PER_MASTER" -gt 0 ]; then
  echo "=== Step 5: Adding replicas for new standby master ==="

  for i in $(seq 1 $REPLICAS_PER_MASTER); do
    REPLICA_INDEX=$((NEW_STANDBY_INDEX + i))
    REPLICA_POD="${CLUSTER_NAME}-${REPLICA_INDEX}"
    REPLICA_FQDN="${REPLICA_POD}.${SERVICE_NAME}.${NAMESPACE}.svc.cluster.local"
    REPLICA_IP=$(getent hosts $REPLICA_FQDN | awk '{print $1}')

    if [ -z "$REPLICA_IP" ]; then
      echo "WARNING: Could not resolve replica pod $REPLICA_POD, skipping"
      continue
    fi

    echo "Adding replica: $REPLICA_POD ($REPLICA_IP:6379) as slave of $NEW_STANDBY_NODE_ID"

    # Check if replica is already in cluster
    if echo "$cluster_nodes_output" | grep -q "$REPLICA_IP:6379"; then
      echo "Replica $REPLICA_POD already in cluster"
    else
      redis-cli --cluster add-node ${REPLICA_IP}:6379 $ENTRYPOINT --cluster-slave --cluster-master-id $NEW_STANDBY_NODE_ID
      sleep 3
      echo "Replica $REPLICA_POD added"
    fi
  done
fi

echo "=== Cleanup and Re-add Complete ==="
redis-cli -h $ENTRYPOINT_HOST cluster nodes
`

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name + "-cleanup-standby",
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
							Name:    "cleanup-standby",
							Image:   fmt.Sprintf("redis:%s", cluster.Spec.RedisVersion),
							Command: []string{"sh", "-c"},
							Args:    []string{cliCmd},
							Env: []corev1.EnvVar{
								{Name: "CLUSTER_NAME", Value: cluster.Name},
								{Name: "SERVICE_NAME", Value: cluster.Name + "-headless"},
								{Name: "NAMESPACE", Value: cluster.Namespace},
								{Name: "ENTRYPOINT_HOST", Value: anyPodHost},
								{Name: "ENTRYPOINT_WITH_PORT", Value: entrypoint},
								{Name: "REPLICAS_PER_MASTER", Value: fmt.Sprintf("%d", cluster.Spec.ReplicasPerMaster)},
								{Name: "NEW_STANDBY_INDEX", Value: fmt.Sprintf("%d", newStandbyIndex)},
								{Name: "OLD_STANDBY_INDEX", Value: fmt.Sprintf("%d", oldStandbyIndex)},
							},
						},
					},
				},
			},
		},
	}
}
