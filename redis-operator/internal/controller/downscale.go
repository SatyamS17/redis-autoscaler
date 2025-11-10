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
