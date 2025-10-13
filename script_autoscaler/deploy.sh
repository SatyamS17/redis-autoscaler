#!/bin/bash
# deploy.sh
set -e

echo "=== Deploying Redis Autoscaler ==="

# Create/update ConfigMap from source files
kubectl create configmap simple-redis-autoscaler \
  --from-file=autoscaler.py=./autoscaler.py \
  --from-file=install.sh=./install.sh \
  -n default \
  -o yaml --dry-run=client | kubectl apply -f -

# Apply Deployment spec
kubectl apply -f autoscaler-deployment.yaml

# Restart deployment so it picks up new ConfigMap
kubectl rollout restart deployment/redis-autoscaler -n default

echo "✅ Deployment updated"
