package controller

import (
	"context"
	"fmt"
	"time"

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
	if *sts.Spec.Replicas != desiredTotalReplicas {
		logger.Info("StatefulSet spec not updated yet", "Current", *sts.Spec.Replicas, "Desired", desiredTotalReplicas)
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

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
		// Job not found, but double-check pods are ACTUALLY ready before creating
		if sts.Status.ReadyReplicas != desiredTotalReplicas {
			logger.Info("Pods not ready yet, waiting before creating reshard job",
				"Ready", sts.Status.ReadyReplicas,
				"Desired", desiredTotalReplicas)
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		// Job not found, create it with the overloaded pod info
		logger.Info("Creating smart reshard Job.", "overloadedPod", cluster.Status.OverloadedPod)

		if cluster.Status.OverloadedPod == "" {
			logger.Error(fmt.Errorf("OverloadedPod is empty"), "Cannot create reshard job without knowing which pod to drain")
			cluster.Status.IsResharding = false
			_ = r.Status().Update(ctx, cluster)
			return ctrl.Result{}, nil
		}

		job := r.reshardJobForRedisCluster(cluster, cluster.Status.OverloadedPod)
		if err := controllerutil.SetControllerReference(cluster, job, r.Scheme); err != nil {
			logger.Error(err, "Failed to set owner ref on reshard Job")
			return ctrl.Result{}, err
		}
		if err := r.Create(ctx, job); err != nil {
			logger.Error(err, "Failed to create reshard Job")
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil

	} else if err != nil {
		logger.Error(err, "Failed to get reshard Job")
		return ctrl.Result{}, err
	}

	// 3. Job exists. Check its status.
	if reshardJob.Status.Succeeded > 0 {
		logger.Info("Reshard Job succeeded. Unlocking autoscaler.")
		cluster.Status.IsResharding = false
		cluster.Status.OverloadedPod = "" // Clear the overloaded pod info
		now := metav1.Now()
		cluster.Status.LastScaleTime = &now
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
		cluster.Status.IsResharding = false
		cluster.Status.OverloadedPod = ""
		if err := r.Status().Update(ctx, cluster); err != nil {
			logger.Error(err, "Failed to update status after failed reshard")
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 1 * time.Second}, nil
	}

	logger.Info("Reshard job is still running...")
	return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
}

// smartReshardJobForRedisCluster creates a job that:
// 1. Uses the OVERLOADED_POD passed in (identified by monitoring)
// 2. Makes the new empty master a replica of the overloaded master
// 3. Waits for replication to sync
// 4. Splits approximately half the slots from overloaded to new master
// 5. Converts the new node back to a master
// func (r *RedisClusterReconciler) reshardJobForRedisCluster(cluster *appv1.RedisCluster, overloadedPod string) *batchv1.Job {
// 	anyPodHost := fmt.Sprintf("%s-0.%s.%s.svc.cluster.local",
// 		cluster.Name, cluster.Name+"-headless", cluster.Namespace)
// 	anyPodPort := "6379"
// 	entrypoint := fmt.Sprintf("%s:%s", anyPodHost, anyPodPort)

// 	timeout := int64(600)
// 	backoff := int32(0)

// 	// This script implements the replica-based smart reshard
// 	cliCmd := `
// #!/bin/sh
// set -x # Print commands as they run
// # DO NOT 'set -e' while debugging with trap, we want the trap to be the last command.

// # --- START: DEBUGGING TRAP ---
// trap 'exit_code=$?; echo "--- SCRIPT EXITED (Status: $exit_code) ---"; echo "Pod will stay alive for 10 minutes."; sleep 600; exit $exit_code' EXIT
// # --- END: DEBUGGING TRAP ---

// echo "=== Smart Scale-Up with Replica Pre-seeding ==="
// ENTRYPOINT="$ANY_POD_ENTRYPOINT"
// EXPECTED_MASTERS="$EXPECTED_MASTERS"
// EXPECTED_NODES=$(($EXPECTED_MASTERS * 2))
// OVERLOADED_POD="$OVERLOADED_POD"

// # 1. Add orphaned nodes to cluster (only the new MASTER, not replica yet)
// echo "Checking for orphaned nodes..."
// cluster_nodes_output=$(redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT cluster nodes)
// actual_nodes_count=$(echo "$cluster_nodes_output" | grep -v fail | wc -l)
// known_node_ips=$(echo "$cluster_nodes_output" | grep -v fail | awk '{ print $2 }' | cut -d'@' -f1 | cut -d':' -f1)
// echo "$known_node_ips"

// if [ "$actual_nodes_count" -lt "$EXPECTED_NODES" ]; then
//   echo "Finding orphan nodes to add..."
//   orphan_nodes_dns=""
//   for i in $(seq 0 $(($EXPECTED_NODES - 1))); do
//     pod_dns="${CLUSTER_NAME}-${i}.${SERVICE_NAME}.${NAMESPACE}.svc.cluster.local"
//     pod_ip=$(getent hosts $pod_dns | awk '{ print $1 }' || true)
//     if [ -z "$pod_ip" ]; then
//       echo "Skipping unresolved DNS: $pod_dns"
//       continue
//     fi
//     if ! echo "$known_node_ips" | grep -q "$pod_ip"; then
//       echo "Found orphan: $pod_dns (IP: $pod_ip)"
//       orphan_nodes_dns="$orphan_nodes_dns $pod_dns"
//     fi
//   done

//   # Add orphan nodes in pairs (master + replica)
//   # BUT: only add the MASTER now, save replica for later
//   master_dns=""
//   replica_dns_to_add_later=""
//   for orphan in $orphan_nodes_dns; do
//     if [ -z "$master_dns" ]; then
//       master_dns=$orphan
//     else
//       replica_dns=$orphan
//       echo "Found pair: MASTER=$master_dns, REPLICA=$replica_dns (will add replica later)"

//       # Add only the master for now
//       echo "Adding MASTER=$master_dns to cluster..."
//       redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT --cluster add-node ${master_dns}:6379 $ENTRYPOINT || true
//       sleep 5

//       # Save the replica DNS for later (after slot migration)
//       replica_dns_to_add_later="$replica_dns"

//       # Reset for next pair
//       master_dns=""
//     fi
//   done
// fi

// # 2. Wait for the new master to join (not the full count yet, since replica not added)
// echo "Waiting for new master to join cluster..."
// max_wait=120
// elapsed=0
// expected_nodes_without_replica=$(($EXPECTED_NODES - 1))
// while true; do
//   joined=$(redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT cluster nodes | grep -v fail | wc -l)

//   if [ "$joined" -ge "$expected_nodes_without_replica" ]; then
//     echo "$joined nodes joined (expected at least $expected_nodes_without_replica)."
//     break
//   fi

//   if [ $elapsed -gt $max_wait ]; then
//     echo "Timeout waiting for nodes to join"
//     exit 1
//   fi

//   echo "Waiting... $joined nodes joined"
//   sleep 5
//   elapsed=$((elapsed + 5))
// done

// # 2. Find the empty master (the new node we just added)
// echo "Finding empty master node..."
// EMPTY_MASTER=$(redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT cluster nodes | \
//   grep master | grep -v fail | awk '$9==""' | head -n1 | awk '{print $1}')

// if [ -z "$EMPTY_MASTER" ]; then
//   echo "ERROR: No empty master found!"
//   exit 1
// fi

// EMPTY_MASTER_IP=$(redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT cluster nodes | \
//   grep "$EMPTY_MASTER" | awk '{print $2}' | cut -d':' -f1 | cut -d'@' -f1)

// echo "Empty master node ID: $EMPTY_MASTER (IP: $EMPTY_MASTER_IP)"

// # 3. Find the overloaded master by matching pod name
// # Convert pod name to IP via DNS lookup
// echo "Resolving overloaded pod: $OVERLOADED_POD"
// OVERLOADED_POD_FQDN="${OVERLOADED_POD}.${SERVICE_NAME}.${NAMESPACE}.svc.cluster.local"
// LOADED_MASTER_IP=$(getent hosts $OVERLOADED_POD_FQDN | awk '{print $1}')

// if [ -z "$LOADED_MASTER_IP" ]; then
//   echo "ERROR: Could not resolve IP for $OVERLOADED_POD"
//   exit 1
// fi

// echo "Overloaded pod IP: $LOADED_MASTER_IP"

// # Find the node ID for this IP
// LOADED_MASTER=$(redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT cluster nodes | \
//   grep "$LOADED_MASTER_IP:6379" | grep master | awk '{print $1}')

// if [ -z "$LOADED_MASTER" ]; then
//   echo "ERROR: Could not find master node for IP $LOADED_MASTER_IP"
//   exit 1
// fi

// echo "Overloaded master node ID: $LOADED_MASTER"

// # 4. Make empty master a REPLICA of the loaded master (fast data copy)
// echo "Converting empty master to replica of $LOADED_MASTER for fast sync..."
// redis-cli -h $EMPTY_MASTER_IP -p 6379 CLUSTER REPLICATE $LOADED_MASTER

// echo "Replication started. Waiting for sync..."

// # 5. Wait for replication to complete
// max_wait=300
// elapsed=0
// while true; do
//   # Check replication offset
//   master_offset=$(redis-cli -h $LOADED_MASTER_IP -p 6379 INFO replication | \
//     grep master_repl_offset | cut -d':' -f2 | tr -d '\r')

//   replica_offset=$(redis-cli -h $EMPTY_MASTER_IP -p 6379 INFO replication | \
//     grep master_repl_offset | cut -d':' -f2 | tr -d '\r')

//   lag=$((master_offset - replica_offset))

//   echo "Replication lag: $lag bytes (master: $master_offset, replica: $replica_offset)"

//   # Consider synced if lag < 5000 bytes
//   if [ "$lag" -lt 5000 ] && [ "$lag" -ge 0 ]; then
//     echo "Replication synced!"
//     break
//   fi

//   if [ $elapsed -gt $max_wait ]; then
//     echo "Timeout waiting for replication sync"
//     exit 1
//   fi

//   sleep 3
//   elapsed=$((elapsed + 3))
// done

// # 6. Calculate which slots to move (approximately half)
// echo "Calculating slots to move..."
// TOTAL_SLOTS=$(redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT cluster nodes | \
//   grep "^$LOADED_MASTER " | \
//   awk '{
//     slots=0
//     for(i=9; i<=NF; i++) {
//       if($i ~ /^[0-9]+-[0-9]+$/) {
//         split($i, range, "-")
//         slots += (range[2] - range[1] + 1)
//       } else if($i ~ /^[0-9]+$/) {
//         slots += 1
//       }
//     }
//     print slots
//   }')

// SLOTS_TO_MOVE=$((TOTAL_SLOTS / 2))
// echo "Will move $SLOTS_TO_MOVE out of $TOTAL_SLOTS slots from overloaded master"

// # 7. Disable cluster-require-full-coverage temporarily
// echo "Disabling full coverage requirement..."
// node_ips=$(redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT cluster nodes | \
//   awk '{print $2}' | cut -d'@' -f1 | cut -d':' -f1 | sort -u)
// for ip in $node_ips; do
//   redis-cli -h $ip -p 6379 CONFIG SET cluster-require-full-coverage no || true
// done
// sleep 2

// # 9. Perform the slot migration (keys already present via replication!)
// echo "Migrating $SLOTS_TO_MOVE slots from $LOADED_MASTER to $EMPTY_MASTER..."
// redis-cli --cluster reshard $ENTRYPOINT \
//   --cluster-from $LOADED_MASTER \
//   --cluster-to $EMPTY_MASTER \
//   --cluster-slots $SLOTS_TO_MOVE \
//   --cluster-yes \
//   --cluster-timeout 10000

// # Verify EMPTY_MASTER actually got slots and became master
// EMPTY_MASTER_ROLE=$(redis-cli -h $EMPTY_MASTER_IP -p 6379 ROLE | head -1)
// if [ "$EMPTY_MASTER_ROLE" != "master" ]; then
//   echo "ERROR: Empty master is still a replica after reshard!"
//   redis-cli -h $ANY_POD_HOST -p 6379 cluster nodes
//   exit 1
// fi

// # 10. Re-enable full coverage
// echo "Re-enabling full coverage requirement..."
// for ip in $node_ips; do
//   redis-cli -h $ip -p 6379 CONFIG SET cluster-require-full-coverage yes || true
// done

// # 11. Verify cluster health
// echo "Verifying cluster state..."
// redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT cluster info
// redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT cluster nodes

// # 12. Now add the replica we saved earlier
// if [ -n "$replica_dns_to_add_later" ]; then
//   echo "Now adding replica $replica_dns_to_add_later as follower of empty master..."

//   # Get the empty master's node ID (it may have changed after FAILOVER)
//   EMPTY_MASTER_ID=$(redis-cli -h $EMPTY_MASTER_IP -p 6379 CLUSTER MYID)

//   echo "Adding replica ${replica_dns_to_add_later} to cluster..."
//   redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT --cluster add-node ${replica_dns_to_add_later}:6379 $ENTRYPOINT --cluster-slave --cluster-master-id $EMPTY_MASTER_ID || true

//   sleep 5

//   # Wait for final node count
//   echo "Waiting for all $EXPECTED_NODES nodes to join..."
//   max_wait=60
//   elapsed=0
//   while true; do
//     joined=$(redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT cluster nodes | grep -v fail | wc -l)
//     [ "$joined" -eq "$EXPECTED_NODES" ] && break
//     [ $elapsed -gt $max_wait ] && echo "Timeout waiting for replica to join." && exit 1
//     echo "Waiting... $joined/$EXPECTED_NODES nodes joined."
//     sleep 5
//     elapsed=$((elapsed + 5))
//   done

//   echo "Replica successfully added!"
// else
//   echo "No replica to add (unexpected - this should not happen)"
// fi

// echo "Final cluster state:"
// redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT cluster nodes

// echo "=== Smart Scale-Up Complete ==="
// `

// 	return &batchv1.Job{
// 		ObjectMeta: metav1.ObjectMeta{
// 			Name:      cluster.Name + "-reshard",
// 			Namespace: cluster.Namespace,
// 			Labels:    getLabels(cluster),
// 		},
// 		Spec: batchv1.JobSpec{
// 			ActiveDeadlineSeconds: &timeout,
// 			BackoffLimit:          &backoff,
// 			Template: corev1.PodTemplateSpec{
// 				Spec: corev1.PodSpec{
// 					RestartPolicy: corev1.RestartPolicyNever,
// 					Containers: []corev1.Container{
// 						{
// 							Name:    "smart-reshard",
// 							Image:   fmt.Sprintf("redis:%s", cluster.Spec.RedisVersion),
// 							Command: []string{"sh", "-c"},
// 							Args:    []string{cliCmd},
// 							Env: []corev1.EnvVar{
// 								{Name: "ANY_POD_HOST", Value: anyPodHost},
// 								{Name: "ANY_POD_PORT", Value: anyPodPort},
// 								{Name: "ANY_POD_ENTRYPOINT", Value: entrypoint},
// 								{Name: "EXPECTED_MASTERS", Value: fmt.Sprintf("%d", cluster.Spec.Masters)},
// 								{Name: "OVERLOADED_POD", Value: overloadedPod},
// 								{Name: "SERVICE_NAME", Value: cluster.Name + "-headless"},
// 								{Name: "NAMESPACE", Value: cluster.Namespace},
// 								{Name: "CLUSTER_NAME", Value: cluster.Name},
// 							},
// 						},
// 					},
// 				},
// 			},
// 		},
// 	}
// }

// smartReshardJobForRedisCluster creates a job that:
// 1. Adds the new master AND its new replica to the cluster.
// 2. Waits for all nodes to join.
// 3. Reshards (migrates) slots from the OVERLOADED_POD to the new empty master.
// 4. Verifies the reshard was successful.
func (r *RedisClusterReconciler) reshardJobForRedisCluster(cluster *appv1.RedisCluster, overloadedPod string) *batchv1.Job {
	anyPodHost := fmt.Sprintf("%s-0.%s.%s.svc.cluster.local",
		cluster.Name, cluster.Name+"-headless", cluster.Namespace)
	anyPodPort := "6379"
	entrypoint := fmt.Sprintf("%s:%s", anyPodHost, anyPodPort)

	// Get the StatefulSet to find the CURRENT desired replicas
	sts := &appsv1.StatefulSet{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, sts); err != nil {
		// If we can't get STS, fall back to calculated value
		sts = nil
	}

	var desiredTotalReplicas int32
	if sts != nil && sts.Spec.Replicas != nil {
		desiredTotalReplicas = *sts.Spec.Replicas
	} else {
		desiredTotalReplicas = cluster.Spec.Masters * (1 + cluster.Spec.ReplicasPerMaster)
	}

	timeout := int64(600)
	backoff := int32(0)

	// This script adds both new nodes, then does a "cold" reshard
	cliCmd := `
#!/bin/bash
set -ex

echo "=== Smart Scale-Up (Targeted Reshard) ==="
ENTRYPOINT="$ANY_POD_ENTRYPOINT"
EXPECTED_NODES="$EXPECTED_NODES"
OVERLOADED_POD="$OVERLOADED_POD"
ANY_POD_HOST="$ANY_POD_HOST"
ANY_POD_PORT="$ANY_POD_PORT"
CLUSTER_NAME="$CLUSTER_NAME"
SERVICE_NAME="$SERVICE_NAME"
NAMESPACE="$NAMESPACE"

wait_until=$(($(date +%s) + 600))

echo "Running 'redis-cli --cluster fix' to clean up any stuck slots..."
yes | redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT --cluster fix $ENTRYPOINT
echo "Cluster fix complete."

echo "Checking for orphaned nodes..."
cluster_nodes_output=$(redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT cluster nodes)
actual_nodes_count=$(echo "$cluster_nodes_output" | grep -v fail | wc -l)
known_node_ips=$(echo "$cluster_nodes_output" | grep -v fail | awk '{ print $2 }' | cut -d'@' -f1 | cut -d':' -f1)
echo "$known_node_ips"

NEW_MASTER_DNS=""

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
      
      # SAVE the new master DNS
      NEW_MASTER_DNS=$master_dns
      
      redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT --cluster add-node ${master_dns}:6379 $ENTRYPOINT || true
      sleep 5
      new_master_ip=$(getent hosts $master_dns | awk '{ print $1 }')
      new_master_id=$(redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT cluster nodes | grep $new_master_ip | grep master | awk '{ print $1 }')
      redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT --cluster add-node ${replica_dns}:6379 $ENTRYPOINT --cluster-slave --cluster-master-id $new_master_id || true
      master_dns=""
    fi
  done
fi

echo "Waiting for all $EXPECTED_NODES nodes to join..."
while true; do
  joined=$(redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT cluster nodes | grep -v fail | wc -l)
  [ "$joined" -eq "$EXPECTED_NODES" ] && break
  [ $(date +%s) -gt $wait_until ] && echo "Timeout waiting for nodes." && exit 1
  echo "Waiting... $joined/$EXPECTED_NODES nodes joined."
  sleep 5
done

# Use the NEW_MASTER_DNS we tracked to find the empty master
if [ -z "$NEW_MASTER_DNS" ]; then
  echo "ERROR: No new master was added (NEW_MASTER_DNS is empty)"
  exit 1
fi

echo "Finding the new master we added: $NEW_MASTER_DNS"
NEW_MASTER_IP=$(getent hosts $NEW_MASTER_DNS | awk '{print $1}')
EMPTY_MASTER=$(redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT cluster nodes | grep "$NEW_MASTER_IP:6379" | grep master | awk '{print $1}')

if [ -z "$EMPTY_MASTER" ]; then
  echo "ERROR: Could not find new master with IP $NEW_MASTER_IP"
  redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT cluster nodes
  exit 1
fi

echo "Empty master ID: $EMPTY_MASTER (DNS: $NEW_MASTER_DNS, IP: $NEW_MASTER_IP)"

# Find the overloaded master
OVERLOADED_POD_FQDN="${OVERLOADED_POD}.${SERVICE_NAME}.${NAMESPACE}.svc.cluster.local"
LOADED_MASTER_IP=$(getent hosts $OVERLOADED_POD_FQDN | awk '{print $1}')
LOADED_MASTER=$(redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT cluster nodes | grep "$LOADED_MASTER_IP:6379" | grep master | awk '{print $1}')
echo "Overloaded master ID: $LOADED_MASTER"

# Calculate slots to move (half)
TOTAL_SLOTS=$(redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT cluster nodes | grep "^$LOADED_MASTER " | awk '{
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
SLOTS_TO_MOVE=$((TOTAL_SLOTS / 2))
echo "Will move $SLOTS_TO_MOVE out of $TOTAL_SLOTS slots"

# Disable full coverage
echo "Disabling full coverage check on all nodes..."
node_ips=$(redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT cluster nodes | awk '{ print $2 }' | cut -d'@' -f1 | cut -d':' -f1 | sort -u)
for ip in $node_ips; do
  redis-cli -h $ip -p 6379 config set cluster-require-full-coverage no || echo "WARN: Failed config set on $ip"
done
sleep 3

# Reshard
echo "Resharding $SLOTS_TO_MOVE slots from $LOADED_MASTER to $EMPTY_MASTER..."
redis-cli --cluster reshard $ENTRYPOINT \
  --cluster-from $LOADED_MASTER \
  --cluster-to $EMPTY_MASTER \
  --cluster-slots $SLOTS_TO_MOVE \
  --cluster-yes \
  --cluster-timeout 10000

# Re-enable full coverage
echo "Re-enabling full coverage on all nodes..."
for ip in $node_ips; do
  redis-cli -h $ip -p 6379 config set cluster-require-full-coverage yes || echo "WARN: Failed re-enable on $ip"
done
sleep 3

echo "=== Smart Scale-Up Complete ==="
`

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name + "-reshard",
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
							Name:    "smart-reshard",
							Image:   fmt.Sprintf("redis:%s", cluster.Spec.RedisVersion),
							Command: []string{"sh", "-c"},
							Args:    []string{cliCmd},
							Env: []corev1.EnvVar{
								{Name: "ANY_POD_HOST", Value: anyPodHost},
								{Name: "ANY_POD_PORT", Value: anyPodPort},
								{Name: "ANY_POD_ENTRYPOINT", Value: entrypoint},
								{Name: "EXPECTED_NODES", Value: fmt.Sprintf("%d", desiredTotalReplicas)},
								{Name: "OVERLOADED_POD", Value: overloadedPod},
								{Name: "CLUSTER_NAME", Value: cluster.Name},
								{Name: "SERVICE_NAME", Value: cluster.Name + "-headless"},
								{Name: "NAMESPACE", Value: cluster.Namespace},
							},
						},
					},
				},
			},
		},
	}
}
