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
