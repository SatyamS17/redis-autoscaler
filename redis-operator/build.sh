#!/bin/bash

# ---
# This script automates the build, push, and redeployment of the redis-operator.
# It also cleans up the old cluster and tails the logs of the new operator pod.
#
# Usage:
# ./build.sh <version>
#
# Example:
# ./build.sh v0.0.10
# ---

# Exit immediately if any command fails
set -e

# 1. Check for version argument
VERSION=$1
if [ -z "$VERSION" ]; then
  echo "Error: No version argument provided."
  echo "Usage: ./build_and_deploy.sh <version>"
  echo "Example: ./build_and_deploy.sh v0.0.10"
  exit 1
fi

IMAGE_URL="docker.io/satyams17/redis-operator:$VERSION"
OPERATOR_NAMESPACE="redis-operator-system"
REDIS_NAMESPACE="default"
LOG_FILE="redis-operator.log"

# 2. Build and Deploy Operator
echo "--- [1/4] Building and Deploying Operator Version: $VERSION ---"
echo "Image: $IMAGE_URL"

echo "Running 'make install'..."
make install

echo "Running 'make docker-build'..."
make docker-build IMG=$IMAGE_URL

echo "Running 'make docker-push'..."
make docker-push IMG=$IMAGE_URL

echo "Running 'make deploy'..."
make deploy IMG=$IMAGE_URL

echo "Operator deployment updated. Waiting 15s for new pod to be scheduled..."
sleep 10

# 3. Recreate Redis Cluster
echo "--- [2/4] Recreating Redis Cluster ---"

echo "Deleting old RedisCluster CR (if it exists)..."
kubectl delete rediscluster redis-cluster -n $REDIS_NAMESPACE --ignore-not-found=true

echo "Waiting 10s for cluster pods to begin termination..."
sleep 5

echo "Deleting old PVCs with a 10s timeout (if they exist)..."
# Use --timeout and '|| true' to continue even if it fails or times out
kubectl delete pvc -l app=redis-cluster -n $REDIS_NAMESPACE --ignore-not-found=true --timeout=10s


echo "Waiting 5s for resources to fully terminate..."
sleep 5

echo "Applying new cluster.yaml..."
kubectl apply -f cluster.yaml

echo "Waiting 60s for Redis pods to be created..."
sleep 60

# 4. Check Redis Cluster Slots


# 5. Tail Operator Logs
echo "--- [4/4] Tailing Operator Logs ---"
echo "Waiting 10s for operator pod to stabilize..."
sleep 5

echo "Finding running operator pod in namespace '$OPERATOR_NAMESPACE'..."

# Find the name of the new, running operator pod.
# We look for the label 'control-plane=controller-manager' and 'Running' status.
POD_NAME=$(kubectl get pods -n $OPERATOR_NAMESPACE \
  -l control-plane=controller-manager \
  --field-selector=status.phase=Running \
  -o jsonpath='{.items[0].metadata.name}')

if [ -z "$POD_NAME" ]; then
  echo "Error: Could not find a running operator pod in namespace '$OPERATOR_NAMESPACE'."
  echo "Please check the deployment status manually:"
  echo "kubectl get pods -n $OPERATOR_NAMESPACE"
  exit 1
fi

echo "Found pod: $POD_NAME"
echo "Tailing logs to console and saving to '$LOG_FILE'. (Press Ctrl+C to stop)"
echo "----------------------------------------------"

# Tail logs and pipe to tee
kubectl logs -f $POD_NAME -n $OPERATOR_NAMESPACE | tee redis-operator.log

