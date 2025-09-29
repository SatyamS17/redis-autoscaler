#!/bin/bash

echo "=== Checking Prerequisites ==="

# Check kubectl
if ! command -v kubectl &> /dev/null; then
    echo "❌ kubectl not found. Please install kubectl first."
    exit 1
fi
echo "✅ kubectl found"

# Check Docker
if ! command -v docker &> /dev/null; then
    echo "❌ docker not found. Please install Docker first."
    exit 1
fi
echo "✅ docker found"

# Check cluster connection
if ! kubectl cluster-info &> /dev/null; then
    echo "❌ Not connected to Kubernetes cluster"
    echo "Please run: kubectl config current-context"
    exit 1
fi
echo "✅ Connected to cluster: $(kubectl config current-context)"

# Check for metrics server (needed for CPU monitoring)
if ! kubectl get apiservice v1beta1.metrics.k8s.io &> /dev/null; then
    echo "❌ Metrics server not found"
    echo "The autoscaler needs metrics server to monitor CPU usage."
    echo "Please install it with:"
    echo "  kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml"
    exit 1
fi
echo "✅ Metrics server available"

# Check Redis deployment
echo ""
echo "Redis deployments found:"
kubectl get statefulset | grep -i redis || echo "No Redis StatefulSets found"
kubectl get deployment | grep -i redis || echo "No Redis Deployments found"

echo ""
echo "Redis services:"
kubectl get svc | grep -i redis

echo ""
echo "Redis pods:"
kubectl get pods | grep -i redis

echo ""
echo "✅ Prerequisites check complete"
echo ""
echo "If you see Redis resources above, you're ready to deploy the autoscaler!"