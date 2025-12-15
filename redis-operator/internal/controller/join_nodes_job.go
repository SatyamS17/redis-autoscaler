package controller

import (
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	appv1 "github.com/myuser/redis-operator/api/v1"
)

// joinNodesJobForRedisCluster creates a Kubernetes Job that joins new standby pods to the cluster.
// The job adds the standby master and its replicas to the cluster.
func (r *RedisClusterReconciler) joinNodesJobForRedisCluster(cluster *appv1.RedisCluster) *batchv1.Job {
	anyPodHost := fmt.Sprintf("%s-0.%s.%s.svc.cluster.local",
		cluster.Name, cluster.Name+"-headless", cluster.Namespace)
	anyPodPort := "6379"
	entrypoint := fmt.Sprintf("%s:%s", anyPodHost, anyPodPort)

	// Calculate new standby indices
	newStandbyIndex := cluster.Spec.Masters * (1 + cluster.Spec.ReplicasPerMaster)

	timeout := int64(300) // 5 minutes should be enough to join nodes
	backoff := int32(3)   // Retry up to 3 times

	cliCmd := `
#!/bin/bash
set -ex

echo "=== Joining New Standby Pods to Cluster ==="
ENTRYPOINT="$ANY_POD_ENTRYPOINT"
ANY_POD_HOST="$ANY_POD_HOST"
ANY_POD_PORT="$ANY_POD_PORT"
CLUSTER_NAME="$CLUSTER_NAME"
SERVICE_NAME="$SERVICE_NAME"
NAMESPACE="$NAMESPACE"
NEW_STANDBY_INDEX="$NEW_STANDBY_INDEX"
REPLICAS_PER_MASTER="$REPLICAS_PER_MASTER"

# Step 1: Add standby master
STANDBY_POD="${CLUSTER_NAME}-${NEW_STANDBY_INDEX}"
STANDBY_FQDN="${STANDBY_POD}.${SERVICE_NAME}.${NAMESPACE}.svc.cluster.local"
STANDBY_IP=$(getent hosts $STANDBY_FQDN | awk '{print $1}')

if [ -z "$STANDBY_IP" ]; then
  echo "ERROR: Could not resolve standby pod $STANDBY_POD"
  exit 1
fi

echo "Adding standby master: $STANDBY_POD ($STANDBY_IP:6379)"

# Check if node is already in cluster
cluster_nodes_output=$(redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT cluster nodes)
if echo "$cluster_nodes_output" | grep -q "$STANDBY_IP:6379"; then
  echo "Standby master already in cluster"
  STANDBY_NODE_ID=$(echo "$cluster_nodes_output" | grep "$STANDBY_IP:6379" | awk '{print $1}')
else
  echo "Adding standby master to cluster"
  redis-cli --cluster add-node ${STANDBY_IP}:6379 $ENTRYPOINT
  sleep 5

  # Get the node ID of the newly added standby
  cluster_nodes_output=$(redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT cluster nodes)
  STANDBY_NODE_ID=$(echo "$cluster_nodes_output" | grep "$STANDBY_IP:6379" | awk '{print $1}')

  if [ -z "$STANDBY_NODE_ID" ]; then
    echo "ERROR: Failed to get standby node ID after adding"
    exit 1
  fi
  echo "Standby master added with ID: $STANDBY_NODE_ID"
fi

# Step 2: Add replicas for the standby master
if [ "$REPLICAS_PER_MASTER" -gt 0 ]; then
  echo "Adding $REPLICAS_PER_MASTER replica(s) for standby master"

  for i in $(seq 1 $REPLICAS_PER_MASTER); do
    REPLICA_INDEX=$((NEW_STANDBY_INDEX + i))
    REPLICA_POD="${CLUSTER_NAME}-${REPLICA_INDEX}"
    REPLICA_FQDN="${REPLICA_POD}.${SERVICE_NAME}.${NAMESPACE}.svc.cluster.local"
    REPLICA_IP=$(getent hosts $REPLICA_FQDN | awk '{print $1}')

    if [ -z "$REPLICA_IP" ]; then
      echo "WARNING: Could not resolve replica pod $REPLICA_POD, skipping"
      continue
    fi

    echo "Adding replica: $REPLICA_POD ($REPLICA_IP:6379) as slave of $STANDBY_NODE_ID"

    # Check if replica is already in cluster
    if echo "$cluster_nodes_output" | grep -q "$REPLICA_IP:6379"; then
      echo "Replica $REPLICA_POD already in cluster"
    else
      redis-cli --cluster add-node ${REPLICA_IP}:6379 $ENTRYPOINT --cluster-slave --cluster-master-id $STANDBY_NODE_ID
      sleep 3
      echo "Replica $REPLICA_POD added"
    fi
  done
fi

echo "=== Successfully Joined All New Pods to Cluster ==="
redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT cluster nodes | grep -E "(${STANDBY_IP}|master|slave)"
`

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name + "-join-nodes",
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
							Name:    "join-nodes",
							Image:   fmt.Sprintf("redis:%s", cluster.Spec.RedisVersion),
							Command: []string{"sh", "-c"},
							Args:    []string{cliCmd},
							Env: []corev1.EnvVar{
								{Name: "ANY_POD_HOST", Value: anyPodHost},
								{Name: "ANY_POD_PORT", Value: anyPodPort},
								{Name: "ANY_POD_ENTRYPOINT", Value: entrypoint},
								{Name: "CLUSTER_NAME", Value: cluster.Name},
								{Name: "SERVICE_NAME", Value: cluster.Name + "-headless"},
								{Name: "NAMESPACE", Value: cluster.Namespace},
								{Name: "NEW_STANDBY_INDEX", Value: fmt.Sprintf("%d", newStandbyIndex)},
								{Name: "REPLICAS_PER_MASTER", Value: fmt.Sprintf("%d", cluster.Spec.ReplicasPerMaster)},
							},
						},
					},
				},
			},
		},
	}
}
