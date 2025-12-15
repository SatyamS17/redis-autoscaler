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

	appv1 "github.com/myuser/redis-operator/api/v1"
)

// checkReshardingStatus monitors the scale-up operation progress.
// It waits for pods to be ready, creates the reshard job if needed, and finalizes the scale-up
// by incrementing the master count and provisioning the next standby node.
func (r *RedisClusterReconciler) checkReshardingStatus(ctx context.Context, cluster *appv1.RedisCluster) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	sts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, sts); err != nil {
		logger.Error(err, "Failed to get StatefulSet for reshard check")
		return ctrl.Result{}, err
	}

	desiredTotalReplicas := (cluster.Spec.Masters + 1) * (1 + cluster.Spec.ReplicasPerMaster)

	if *sts.Spec.Replicas != desiredTotalReplicas {
		logger.Info("Waiting for StatefulSet spec update",
			"current", *sts.Spec.Replicas,
			"desired", desiredTotalReplicas)
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	if sts.Status.ReadyReplicas != desiredTotalReplicas {
		logger.Info("Waiting for new pods to be ready",
			"ready", sts.Status.ReadyReplicas,
			"desired", desiredTotalReplicas)
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	logger.Info("All StatefulSet pods are ready, checking for reshard job")

	reshardJob := &batchv1.Job{}
	jobName := cluster.Name + "-reshard"
	err := r.Get(ctx, client.ObjectKey{Name: jobName, Namespace: cluster.Namespace}, reshardJob)

	if err != nil && errors.IsNotFound(err) {
		if sts.Status.ReadyReplicas != desiredTotalReplicas {
			logger.Info("Pods not ready yet, waiting before creating reshard job",
				"ready", sts.Status.ReadyReplicas,
				"desired", desiredTotalReplicas)
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}

		logger.Info("Creating reshard job to activate standby",
			"overloadedPod", cluster.Status.OverloadedPod,
			"standbyPod", cluster.Status.StandbyPod)

		if cluster.Status.OverloadedPod == "" {
			logger.Error(fmt.Errorf("OverloadedPod is empty"), "Cannot create reshard job without overloaded pod")
			cluster.Status.IsResharding = false
			_ = r.Status().Update(ctx, cluster)
			return ctrl.Result{}, nil
		}

		if cluster.Status.StandbyPod == "" {
			logger.Error(fmt.Errorf("StandbyPod is empty"), "Cannot create reshard job without standby pod")
			cluster.Status.IsResharding = false
			_ = r.Status().Update(ctx, cluster)
			return ctrl.Result{}, nil
		}

		job := r.reshardJobForRedisCluster(cluster, cluster.Status.OverloadedPod, cluster.Status.StandbyPod)
		if err := controllerutil.SetControllerReference(cluster, job, r.Scheme); err != nil {
			logger.Error(err, "Failed to set owner reference on reshard job")
			return ctrl.Result{}, err
		}
		if err := r.Create(ctx, job); err != nil {
			logger.Error(err, "Failed to create reshard job")
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil

	} else if err != nil {
		logger.Error(err, "Failed to get reshard job")
		return ctrl.Result{}, err
	}

	if reshardJob.Status.Succeeded > 0 {
		logger.Info("Reshard job succeeded, provisioning next standby pods")

		cluster.Spec.Masters++
		if err := r.Update(ctx, cluster); err != nil {
			logger.Error(err, "Failed to update spec to increment masters")
			return ctrl.Result{}, err
		}

		sts := r.statefulSetForRedisCluster(cluster)
		logger.Info("Triggering StatefulSet update to provision next standby")
		if err := r.reconcileStatefulSet(ctx, cluster, sts); err != nil {
			logger.Error(err, "Failed to reconcile StatefulSet to provision next standby")
		}

		// Transition to provisioning state - need to add new pods to cluster
		cluster.Status.IsResharding = false
		cluster.Status.IsProvisioningStandby = true
		cluster.Status.OverloadedPod = ""
		now := metav1.Now()
		cluster.Status.LastScaleTime = &now

		if err := r.Status().Update(ctx, cluster); err != nil {
			logger.Error(err, "Failed to update status after reshard")
			return ctrl.Result{}, err
		}

		_ = r.Delete(ctx, reshardJob, client.PropagationPolicy(metav1.DeletePropagationBackground))
		logger.Info("Transitioning to standby provisioning phase",
			"newMasters", cluster.Spec.Masters)

		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
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

// reshardJobForRedisCluster creates a Kubernetes Job that performs the scale-up resharding.
// It activates the standby pod by moving half the slots from the overloaded pod to it.
// The job uses redis-cli to fix cluster health, verify the standby, and perform the slot migration.
func (r *RedisClusterReconciler) reshardJobForRedisCluster(cluster *appv1.RedisCluster, overloadedPod string, standbyPod string) *batchv1.Job {
	anyPodHost := fmt.Sprintf("%s-0.%s.%s.svc.cluster.local",
		cluster.Name, cluster.Name+"-headless", cluster.Namespace)
	anyPodPort := "6379"
	entrypoint := fmt.Sprintf("%s:%s", anyPodHost, anyPodPort)

	timeout := int64(cluster.Spec.ReshardTimeoutSeconds)
	backoff := int32(0)

	cliCmd := `
#!/bin/bash
set -ex

echo "=== Smart Scale-Up: Standby Activation (use existing standby) ==="
ENTRYPOINT="$ANY_POD_ENTRYPOINT"
OVERLOADED_POD="$OVERLOADED_POD"
STANDBY_POD="$STANDBY_POD"
ANY_POD_HOST="$ANY_POD_HOST"
ANY_POD_PORT="$ANY_POD_PORT"
CLUSTER_NAME="$CLUSTER_NAME"
SERVICE_NAME="$SERVICE_NAME"
NAMESPACE="$NAMESPACE"

wait_until=$(($(date +%s) + 600))

echo "Standby to activate: $STANDBY_POD"
echo "Overloaded pod to relieve: $OVERLOADED_POD"

# Step 0: Try to fix cluster inconsistencies first (best-effort)
echo "=== Step 0: Running cluster fix to ensure consistency ==="
redis-cli --cluster fix $ENTRYPOINT --cluster-fix-with-unreachable-masters || {
  echo "WARNING: Cluster fix encountered issues, but continuing..."
}

# Verify cluster state after fix
CLUSTER_STATE=$(redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT cluster info | grep cluster_state | cut -d: -f2 | tr -d '\r')
if [ "$CLUSTER_STATE" != "ok" ]; then
  echo "ERROR: Cluster state is '$CLUSTER_STATE' after fix (expected: ok)"
  redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT cluster info || true
  redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT cluster nodes || true
  exit 1
fi
echo "Cluster state: $CLUSTER_STATE"

# Resolve standby pod
STANDBY_FQDN="${STANDBY_POD}.${SERVICE_NAME}.${NAMESPACE}.svc.cluster.local"
STANDBY_IP=$(getent hosts $STANDBY_FQDN | awk '{print $1}')
if [ -z "$STANDBY_IP" ]; then
  echo "ERROR: Could not resolve standby pod $STANDBY_POD"
  exit 1
fi

cluster_nodes_output=$(redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT cluster nodes)
STANDBY_NODE_ID=$(echo "$cluster_nodes_output" | grep "$STANDBY_IP:6379" | grep master | awk '{print $1}')
if [ -z "$STANDBY_NODE_ID" ]; then
  echo "ERROR: Standby node not found in cluster nodes output"
  echo "$cluster_nodes_output"
  exit 1
fi

# Verify standby has zero slots
STANDBY_SLOTS=$(echo "$cluster_nodes_output" | grep "^$STANDBY_NODE_ID" | awk '{
  slots=0
  for(i=9;i<=NF;i++){
    if($i ~ /^[0-9]+-[0-9]+$/){
      split($i,range,"-")
      slots += (range[2]-range[1]+1)
    } else if($i ~ /^[0-9]+$/){
      slots += 1
    }
  }
  print slots
}')
if [ "$STANDBY_SLOTS" -ne 0 ]; then
  echo "ERROR: Standby node has $STANDBY_SLOTS slots (expected 0)"
  exit 1
fi
echo "Standby verified: $STANDBY_POD (ID: $STANDBY_NODE_ID, IP: $STANDBY_IP, Slots: 0)"

# Find overloaded master
OVERLOADED_FQDN="${OVERLOADED_POD}.${SERVICE_NAME}.${NAMESPACE}.svc.cluster.local"
OVERLOADED_IP=$(getent hosts $OVERLOADED_FQDN | awk '{print $1}')
if [ -z "$OVERLOADED_IP" ]; then
  echo "ERROR: Could not resolve overloaded pod $OVERLOADED_POD"
  exit 1
fi
OVERLOADED_MASTER_ID=$(echo "$cluster_nodes_output" | grep "$OVERLOADED_IP:6379" | grep master | awk '{print $1}')
if [ -z "$OVERLOADED_MASTER_ID" ]; then
  echo "ERROR: Overloaded master not found in cluster nodes output"
  exit 1
fi
echo "Overloaded master: $OVERLOADED_POD (ID: $OVERLOADED_MASTER_ID)"

# Calculate slots to move (half)
TOTAL_SLOTS=$(echo "$cluster_nodes_output" | grep "^$OVERLOADED_MASTER_ID " | awk '{
  slots=0
  for(i=9;i<=NF;i++){
    if($i ~ /^[0-9]+-[0-9]+$/){
      split($i,range,"-")
      slots += (range[2]-range[1]+1)
    } else if($i ~ /^[0-9]+$/){
      slots += 1
    }
  }
  print slots
}')
SLOTS_TO_MOVE=$((TOTAL_SLOTS / 2))
if [ "$SLOTS_TO_MOVE" -le 0 ]; then
  echo "Nothing to move (TOTAL_SLOTS=$TOTAL_SLOTS)"
  exit 0
fi
echo "Will move $SLOTS_TO_MOVE out of $TOTAL_SLOTS slots from overloaded master to standby"

# Disable full coverage temporarily on all nodes
echo "=== Disabling full coverage check on all nodes ==="
node_ips=$(echo "$cluster_nodes_output" | awk '{print $2}' | cut -d'@' -f1 | cut -d':' -f1 | sort -u)
for ip in $node_ips; do
  timeout 5 redis-cli -h $ip -p 6379 CONFIG SET cluster-require-full-coverage no || true
done
sleep 2

# Reshard using the standby node (use pipeline to speed up but not too large)
echo "=== Resharding $SLOTS_TO_MOVE slots ==="
redis-cli --cluster reshard $ENTRYPOINT \
  --cluster-from $OVERLOADED_MASTER_ID \
  --cluster-to $STANDBY_NODE_ID \
  --cluster-slots $SLOTS_TO_MOVE \
  --cluster-yes \
  --cluster-timeout 10000 \
  --cluster-pipeline 100

# Re-enable full coverage
echo "=== Re-enabling full coverage ==="
for ip in $node_ips; do
  timeout 5 redis-cli -h $ip -p 6379 CONFIG SET cluster-require-full-coverage yes || true
done
sleep 2

echo "=== Smart Scale-Up Complete: Standby Activated ==="
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
								{Name: "OVERLOADED_POD", Value: overloadedPod},
								{Name: "STANDBY_POD", Value: standbyPod},
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
