#!/bin/bash

# === Redis Operator Build & Deploy Script ===
# Cleans old resources, rebuilds image, pushes, redeploys, and tails logs

set -e

# --- Config ---
VERSION=${1:-"v0.0.33-$(date +%s)"}  # Add timestamp if none given
IMAGE_URL="docker.io/satyams17/redis-operator:$VERSION"
OPERATOR_NAMESPACE="redis-operator-system"
REDIS_NAMESPACE="default"
LOG_FILE="redis-operator.log"

echo "=============================================================="
echo " Building and Deploying Redis Operator: $VERSION"
echo " Image: $IMAGE_URL"
echo "=============================================================="

# --- Step 1: Cleanup old operator ---
echo "--- [1/5] Cleaning old operator deployment ---"

if kubectl get deploy -n $OPERATOR_NAMESPACE | grep -q redis-operator-controller-manager; then
  echo "Deleting old operator deployment..."
  kubectl delete deploy -n $OPERATOR_NAMESPACE redis-operator-controller-manager --ignore-not-found
  echo "Waiting for old operator pod termination..."
  kubectl wait --for=delete pod -l control-plane=controller-manager -n $OPERATOR_NAMESPACE --timeout=30s || true
else
  echo "No old operator deployment found, continuing..."
fi

# Optional: Clean up RedisCluster CR and PVCs
echo "Cleaning old RedisCluster resources..."
kubectl delete rediscluster redis-cluster -n $REDIS_NAMESPACE --ignore-not-found=true

# DELETE ServiceMonitor to force Prometheus refresh
echo "Deleting old ServiceMonitor..."
kubectl delete servicemonitor redis-cluster -n $REDIS_NAMESPACE --ignore-not-found=true

# Wait for pods to terminate before deleting PVCs
kubectl wait --for=delete pod -l app=redis-cluster -n $REDIS_NAMESPACE --timeout=60s || true

kubectl delete pvc -l app=redis-cluster -n $REDIS_NAMESPACE --ignore-not-found=true --timeout=10s || true

# --- Step 2: Build + Push Operator Image ---
echo "--- [2/5] Building and pushing operator image ---"

echo "Running 'make install'..."
make install

echo "Building fresh image (no cache)..."
make docker-build IMG=$IMAGE_URL

echo "Pushing image..."
make docker-push IMG=$IMAGE_URL

# --- Step 3: Deploy Operator ---
echo "--- [3/5] Deploying new operator image ---"
make deploy IMG=$IMAGE_URL

echo "Waiting for new operator pod to start..."
sleep 10

# --- Step 4: Deploy Redis Cluster ---
echo "--- [4/5] Deploying Redis Cluster ---"
kubectl apply -f cluster.yaml
sleep 30

# --- Step 5: Tail Operator Logs ---
echo "--- [5/5] Tailing operator logs ---"

POD_NAME=$(kubectl get pods -n $OPERATOR_NAMESPACE \
  -l control-plane=controller-manager \
  --field-selector=status.phase=Running \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)

if [ -z "$POD_NAME" ]; then
  echo "Error: Could not find running operator pod. Listing pods:"
  kubectl get pods -n $OPERATOR_NAMESPACE
  exit 1
fi

echo "Found pod: $POD_NAME"
kubectl get deploy -n $OPERATOR_NAMESPACE redis-operator-controller-manager -o=jsonpath='{.spec.template.spec.containers[0].image}'
echo
echo "----------------------------------------------"
echo "Tailing logs (Ctrl+C to stop)..."
kubectl logs -f $POD_NAME -n $OPERATOR_NAMESPACE | tee $LOG_FILE
