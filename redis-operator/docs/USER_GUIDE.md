# User Guide

This guide walks you through installing, configuring, and using the Redis Cluster Autoscaler.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Basic Configuration](#basic-configuration)
- [Common Use Cases](#common-use-cases)
- [Monitoring Your Cluster](#monitoring-your-cluster)
- [Next Steps](#next-steps)

## Prerequisites

Before installing the Redis Cluster Autoscaler, ensure you have:

### Required

1. **Kubernetes Cluster** (v1.19 or later)
   ```bash
   kubectl version --short
   ```

2. **kubectl** configured to access your cluster
   ```bash
   kubectl cluster-info
   ```

3. **Prometheus Operator** installed (for metrics collection)
   ```bash
   kubectl get servicemonitors -A
   ```

   If not installed:
   ```bash
   # Using Helm
   helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
   helm install prometheus prometheus-community/kube-prometheus-stack
   ```

4. **Cluster Admin Access** (to install CRDs)

### Recommended

- **Helm** (v3+) - For easier dependency management
- **Storage Class** - For Redis persistent volumes (if using persistence)
- **Metrics Server** - For HPA integration (optional)

### Resource Requirements

**Per Redis Pod:**
- CPU: 100m minimum, 1000m recommended
- Memory: 256Mi minimum, 2Gi recommended

**Operator:**
- CPU: 100m
- Memory: 50Mi

**Minimum Cluster:**
- 3 master nodes + 1 standby + replicas
- Example: 3 masters, 1 replica each = 8 pods total
- Total: ~800m CPU, ~20Gi memory (with 2Gi per pod)

## Installation

### Option 1: Using Make (Development)

```bash
# Clone the repository
git clone https://github.com/yourorg/redis-operator.git
cd redis-operator

# Install CRDs
make install

# Deploy operator to cluster
make deploy

# Verify operator is running
kubectl get pods -n redis-operator-system
```

### Option 2: Using Manifests (Production)

```bash
# Install CRDs
kubectl apply -f config/crd/bases/cache.example.com_redisclusters.yaml

# Create namespace
kubectl create namespace redis-operator-system

# Deploy operator
kubectl apply -f config/manager/manager.yaml

# Verify
kubectl get deployment -n redis-operator-system
```

### Option 3: Using Kustomize

```bash
# Deploy everything
kubectl apply -k config/default

# Verify
kubectl get all -n redis-operator-system
```

### Verify Installation

```bash
# Check CRD is installed
kubectl get crd redisclusters.cache.example.com

# Check operator is running
kubectl get pods -n redis-operator-system

# Expected output:
# NAME                                           READY   STATUS    RESTARTS   AGE
# redis-operator-controller-manager-xxxx-yyyy    2/2     Running   0          1m
```

## Quick Start

### 1. Create Your First Redis Cluster

Create a file `my-cluster.yaml`:

```yaml
apiVersion: cache.example.com/v1
kind: RedisCluster
metadata:
  name: my-redis
  namespace: default
spec:
  # Start with 3 masters
  masters: 3
  minMasters: 3
  replicasPerMaster: 1

  # Basic autoscaling
  autoScaleEnabled: true
  cpuThreshold: 70
  cpuThresholdLow: 20
  memoryThreshold: 70
  memoryThresholdLow: 30

  # Redis version
  redisVersion: "7.2"
```

Apply it:

```bash
kubectl apply -f my-cluster.yaml
```

### 2. Watch the Bootstrap Process

```bash
# Watch pods being created
kubectl get pods -w

# Expected sequence:
# my-redis-0 through my-redis-7 (3 masters + replicas + 1 standby + replicas)

# Watch the bootstrap job
kubectl get jobs

# Check cluster status
kubectl get rediscluster my-redis -o yaml
```

### 3. Verify Cluster is Ready

```bash
# Check cluster status
kubectl get rediscluster my-redis

# Output should show:
# NAME        MASTERS   INITIALIZED   AUTOSCALE   AGE
# my-redis    3         true          true        5m

# Connect to a pod and verify cluster
kubectl exec -it my-redis-0 -- redis-cli cluster info

# Should see:
# cluster_state:ok
# cluster_slots_assigned:16384
# cluster_known_nodes:8
```

### 4. Test Autoscaling

Generate load to trigger scale-up:

```bash
# Port-forward to access Redis
kubectl port-forward svc/my-redis-headless 6379:6379

# In another terminal, generate load with redis-benchmark
redis-benchmark -h localhost -p 6379 -c 50 -n 100000 -t get,set

# Watch autoscaling in action
kubectl get rediscluster my-redis -w

# You should see masters increase when load is high
```

## Basic Configuration

### Minimal Configuration

The absolute minimum required fields:

```yaml
apiVersion: cache.example.com/v1
kind: RedisCluster
metadata:
  name: minimal-redis
spec:
  masters: 3
  autoScaleEnabled: true
  cpuThreshold: 70
```

All other fields have defaults. See [Configuration Reference](./CONFIGURATION.md) for complete list.

### Recommended Production Configuration

```yaml
apiVersion: cache.example.com/v1
kind: RedisCluster
metadata:
  name: prod-redis
  namespace: production
spec:
  # Cluster sizing
  masters: 5                    # Start with 5 active masters
  minMasters: 3                 # Never scale below 3
  replicasPerMaster: 2          # 2 replicas for HA

  # Autoscaling thresholds
  autoScaleEnabled: true
  cpuThreshold: 75              # Scale up at 75% CPU
  cpuThresholdLow: 25           # Scale down at 25% CPU
  memoryThreshold: 80           # Scale up at 80% memory
  memoryThresholdLow: 35        # Scale down at 35% memory

  # Timing and limits
  scaleCooldownSeconds: 120     # Wait 2 minutes between scales
  reshardTimeoutSeconds: 900    # 15 minute timeout for operations
  metricsQueryInterval: 30      # Check metrics every 30s

  # Infrastructure
  redisVersion: "7.2"
  prometheusURL: "http://prometheus-operated.monitoring.svc:9090"
```

### Configuration for Different Workloads

**High-Traffic, Memory-Intensive:**
```yaml
spec:
  masters: 10
  minMasters: 5
  replicasPerMaster: 2
  cpuThreshold: 80
  memoryThreshold: 75           # More aggressive memory scaling
  memoryThresholdLow: 40
  scaleCooldownSeconds: 180     # Longer cooldown for stability
```

**Development/Testing:**
```yaml
spec:
  masters: 3
  minMasters: 3
  replicasPerMaster: 0          # No replicas for dev
  cpuThreshold: 90              # Less aggressive scaling
  scaleCooldownSeconds: 30      # Faster testing cycles
```

**Cost-Optimized:**
```yaml
spec:
  masters: 3
  minMasters: 3
  replicasPerMaster: 1
  cpuThreshold: 85              # Scale up less frequently
  cpuThresholdLow: 15           # Scale down more aggressively
  memoryThreshold: 85
  memoryThresholdLow: 25
```

## Common Use Cases

### Use Case 1: New Redis Cluster with Autoscaling

**Scenario:** You need a new Redis cluster that automatically scales with load.

**Solution:**

```yaml
apiVersion: cache.example.com/v1
kind: RedisCluster
metadata:
  name: auto-redis
  namespace: app-namespace
spec:
  masters: 3
  minMasters: 3
  replicasPerMaster: 1
  autoScaleEnabled: true
  cpuThreshold: 70
  cpuThresholdLow: 20
  memoryThreshold: 70
  memoryThresholdLow: 30
  redisVersion: "7.2"
```

Apply and verify:

```bash
kubectl apply -f auto-redis.yaml
kubectl wait --for=condition=initialized rediscluster/auto-redis --timeout=5m
```

Connect your application:

```python
# Python example
import redis

r = redis.RedisCluster(
    host='auto-redis-headless.app-namespace.svc.cluster.local',
    port=6379,
    decode_responses=True
)

r.set('key', 'value')
```

### Use Case 2: Existing Redis Cluster with Autoscaling Only

**Scenario:** You have an existing Redis cluster and want to add autoscaling without changing the infrastructure.

**Solution:**

```yaml
apiVersion: cache.example.com/v1
kind: RedisCluster
metadata:
  name: existing-redis
  namespace: default
spec:
  # Mark as existing cluster
  existingCluster: true
  manageStatefulSet: false      # Don't manage the StatefulSet

  # Your existing cluster's labels
  podSelector:
    app: redis
    cluster: my-existing-cluster

  # Your existing service name
  serviceName: redis-headless

  # Match your current cluster size
  masters: 5
  minMasters: 3
  replicasPerMaster: 2

  # Enable autoscaling
  autoScaleEnabled: true
  cpuThreshold: 70
  memoryThreshold: 70
```

See [EXISTING_CLUSTER_SUPPORT.md](../EXISTING_CLUSTER_SUPPORT.md) for detailed guide.

### Use Case 3: High-Availability Production Cluster

**Scenario:** Production cluster with high availability and conservative scaling.

**Solution:**

```yaml
apiVersion: cache.example.com/v1
kind: RedisCluster
metadata:
  name: prod-ha-redis
  namespace: production
spec:
  # High availability
  masters: 6
  minMasters: 4
  replicasPerMaster: 2          # 2 replicas for redundancy

  # Conservative autoscaling
  autoScaleEnabled: true
  cpuThreshold: 80              # Higher threshold
  cpuThresholdLow: 25
  memoryThreshold: 85
  memoryThresholdLow: 30

  # Longer cooldown for stability
  scaleCooldownSeconds: 300     # 5 minutes
  reshardTimeoutSeconds: 1200   # 20 minutes

  redisVersion: "7.2"
```

### Use Case 4: Temporarily Disable Autoscaling

**Scenario:** You need to perform maintenance and don't want autoscaling to interfere.

**Solution:**

```bash
# Edit the cluster
kubectl edit rediscluster my-redis

# Change autoScaleEnabled to false
spec:
  autoScaleEnabled: false  # Set this to false

# Or using patch
kubectl patch rediscluster my-redis -p '{"spec":{"autoScaleEnabled":false}}'

# Perform your maintenance...

# Re-enable when done
kubectl patch rediscluster my-redis -p '{"spec":{"autoScaleEnabled":true}}'
```

### Use Case 5: Manual Scaling

**Scenario:** You know you need more capacity and want to scale manually.

**Solution:**

```bash
# Increase masters from 3 to 5
kubectl patch rediscluster my-redis -p '{"spec":{"masters":5}}'

# Watch the scale-up
kubectl get rediscluster my-redis -w

# Decrease masters from 5 to 3
kubectl patch rediscluster my-redis -p '{"spec":{"masters":3}}'
```

Note: Manual scaling still respects `minMasters` limit.

## Monitoring Your Cluster

### View Cluster Status

```bash
# Basic status
kubectl get rediscluster

# Detailed status
kubectl get rediscluster my-redis -o yaml | grep -A 20 status:

# Watch for changes
kubectl get rediscluster my-redis -w
```

### Check Pod Metrics

```bash
# CPU and memory usage
kubectl top pods -l app=redis-cluster

# Prometheus queries (if you have prometheus UI)
# CPU: rate(process_cpu_seconds_total{pod=~"my-redis-.*"}[1m]) * 100
# Memory: (redis_memory_used_bytes / redis_memory_max_bytes) * 100
```

### View Scaling Events

```bash
# Operator logs
kubectl logs -n redis-operator-system deployment/redis-operator-controller-manager -f

# Look for lines like:
# "Triggering scale-up" pod="my-redis-2" reason="CPU"
# "Reshard job succeeded"
# "Scale-up completed" newMasters=4
```

### Check Redis Cluster Health

```bash
# Cluster info
kubectl exec -it my-redis-0 -- redis-cli cluster info

# Cluster nodes
kubectl exec -it my-redis-0 -- redis-cli cluster nodes

# Check slot distribution
kubectl exec -it my-redis-0 -- redis-cli cluster slots
```

### Grafana Dashboard (Optional)

If you have Grafana, import this dashboard:

```json
{
  "dashboard": {
    "title": "Redis Cluster Autoscaler",
    "panels": [
      {
        "title": "Active Masters",
        "targets": [{
          "expr": "redis_cluster_active_masters"
        }]
      },
      {
        "title": "CPU Usage by Pod",
        "targets": [{
          "expr": "rate(process_cpu_seconds_total{pod=~\"my-redis-.*\"}[1m]) * 100"
        }]
      },
      {
        "title": "Memory Usage by Pod",
        "targets": [{
          "expr": "(redis_memory_used_bytes / redis_memory_max_bytes) * 100"
        }]
      }
    ]
  }
}
```

## Next Steps

### Learn More

- **[Configuration Reference](./CONFIGURATION.md)** - All configuration options explained
- **[Operations Guide](./OPERATIONS.md)** - Day-2 operations and maintenance
- **[Architecture](./ARCHITECTURE.md)** - How the autoscaler works internally
- **[Troubleshooting](./TROUBLESHOOTING.md)** - Common issues and solutions

### Advanced Topics

- **Resource Limits**: Set CPU/memory limits on Redis pods
- **Persistent Storage**: Configure PVCs for data persistence
- **Network Policies**: Secure Redis cluster communication
- **RBAC**: Fine-tune operator permissions
- **Multi-tenancy**: Run multiple clusters in one namespace

### Community

- **Report Issues**: GitHub Issues
- **Feature Requests**: GitHub Discussions
- **Contributing**: See CONTRIBUTING.md

## Appendix

### Useful Commands

```bash
# Delete a cluster (WARNING: deletes data)
kubectl delete rediscluster my-redis

# Force delete stuck cluster
kubectl patch rediscluster my-redis -p '{"metadata":{"finalizers":[]}}' --type=merge
kubectl delete rediscluster my-redis --force --grace-period=0

# View all clusters
kubectl get redisclusters -A

# Describe cluster (shows events)
kubectl describe rediscluster my-redis

# Get operator version
kubectl get deployment -n redis-operator-system redis-operator-controller-manager -o jsonpath='{.spec.template.spec.containers[0].image}'
```

### Default Values Reference

| Field | Default | Description |
|-------|---------|-------------|
| `redisVersion` | `"7.2"` | Redis Docker image version |
| `minMasters` | `3` | Minimum masters |
| `replicasPerMaster` | `1` | Replicas per master |
| `cpuThresholdLow` | `20` | Scale-down CPU threshold (%) |
| `memoryThreshold` | `70` | Scale-up memory threshold (%) |
| `memoryThresholdLow` | `30` | Scale-down memory threshold (%) |
| `reshardTimeoutSeconds` | `600` | Job timeout (10 minutes) |
| `scaleCooldownSeconds` | `60` | Cooldown period (1 minute) |
| `prometheusURL` | `http://prometheus-operated.monitoring.svc:9090` | Prometheus endpoint |
| `metricsQueryInterval` | `15` | Metrics check interval (seconds) |
| `manageStatefulSet` | `true` | Manage StatefulSet |
