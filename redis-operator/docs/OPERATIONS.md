# Operations Guide

This guide covers day-to-day operations, monitoring, and maintenance of the Redis Cluster Autoscaler.

## Table of Contents

- [Monitoring](#monitoring)
- [Scaling Operations](#scaling-operations)
- [Backup and Recovery](#backup-and-recovery)
- [Upgrades and Maintenance](#upgrades-and-maintenance)
- [Performance Tuning](#performance-tuning)
- [Security](#security)

## Monitoring

### Key Metrics to Watch

#### 1. Cluster Health Metrics

**Active Masters:**
```bash
kubectl get rediscluster <name> -o jsonpath='{.status.currentMasters}'
```

**Expected:** Should match `spec.masters` when stable

**Alerts:**
- Masters < spec.masters → Scale-up in progress or failure
- Masters > spec.masters → Unexpected state

---

**Cluster Initialization:**
```bash
kubectl get rediscluster <name> -o jsonpath='{.status.initialized}'
```

**Expected:** `true` after bootstrap completes

---

**Scaling Operations:**
```bash
kubectl get rediscluster <name> -o jsonpath='{.status.isResharding}'
kubectl get rediscluster <name> -o jsonpath='{.status.isDraining}'
```

**Expected:** `false` when cluster is stable

**Alerts:**
- True for > `reshardTimeoutSeconds` → Scaling job may be stuck

---

#### 2. Pod Metrics

**CPU Usage:**
```bash
kubectl top pods -l app=redis-cluster

# Via Prometheus
curl 'http://prometheus:9090/api/v1/query?query=rate(process_cpu_seconds_total{pod=~"redis-.*"}[1m])*100'
```

**Memory Usage:**
```bash
# Via Prometheus
curl 'http://prometheus:9090/api/v1/query?query=(redis_memory_used_bytes/redis_memory_max_bytes)*100'
```

---

#### 3. Redis Cluster Metrics

**Cluster State:**
```bash
kubectl exec -it <pod-name> -- redis-cli cluster info | grep cluster_state
```

**Expected:** `cluster_state:ok`

**Alerts:**
- `cluster_state:fail` → Cluster has issues

---

**Slot Coverage:**
```bash
kubectl exec -it <pod-name> -- redis-cli cluster info | grep cluster_slots
```

**Expected:** `cluster_slots_assigned:16384` (all slots assigned)

---

**Node Count:**
```bash
kubectl exec -it <pod-name> -- redis-cli cluster info | grep cluster_known_nodes
```

**Expected:** `(masters + 1) * (replicasPerMaster + 1)`

---

### Prometheus Queries

Add these to your Prometheus or Grafana:

```promql
# CPU usage by pod (%)
rate(process_cpu_seconds_total{pod=~"redis-cluster-.*"}[1m]) * 100

# Memory usage by pod (%)
(redis_memory_used_bytes / redis_memory_max_bytes) * 100

# Number of active masters
redis_cluster_active_masters

# Scaling operations in progress
redis_cluster_is_resharding
redis_cluster_is_draining

# Commands per second
rate(redis_commands_processed_total[1m])

# Connected clients
redis_connected_clients

# Keyspace hits/misses
rate(redis_keyspace_hits_total[1m])
rate(redis_keyspace_misses_total[1m])
```

---

### Recommended Alerts

#### Critical Alerts

```yaml
# Cluster is down
- alert: RedisClusterDown
  expr: up{job="redis"} == 0
  for: 1m
  severity: critical

# Cluster state is not OK
- alert: RedisClusterStateFailure
  expr: redis_cluster_state != 1
  for: 2m
  severity: critical

# Not all slots assigned
  expr: redis_cluster_slots_assigned < 16384
  for: 5m
  severity: critical
```

#### Warning Alerts

```yaml
# High CPU usage
- alert: RedisHighCPU
  expr: rate(process_cpu_seconds_total{pod=~"redis-.*"}[1m]) * 100 > 80
  for: 5m
  severity: warning

# High memory usage
- alert: RedisHighMemory
  expr: (redis_memory_used_bytes / redis_memory_max_bytes) * 100 > 85
  for: 5m
  severity: warning

# Scaling operation taking too long
- alert: RedisScalingStuck
  expr: (redis_cluster_is_resharding == 1 or redis_cluster_is_draining == 1)
  for: 15m
  severity: warning
```

---

### Logging

#### Operator Logs

```bash
# Follow operator logs
kubectl logs -n redis-operator-system deployment/redis-operator-controller-manager -f

# Search for scaling events
kubectl logs -n redis-operator-system deployment/redis-operator-controller-manager | grep -i "scale"

# Search for errors
kubectl logs -n redis-operator-system deployment/redis-operator-controller-manager | grep -i "error"
```

**Important log lines:**
- `Triggering scale-up` - Scale-up initiated
- `Reshard job succeeded` - Scale-up completed
- `Triggering scale-down` - Scale-down initiated
- `Drain job succeeded` - Scale-down completed
- `ERROR` - Any errors

---

#### Redis Logs

```bash
# View Redis pod logs
kubectl logs <pod-name>

# Follow logs
kubectl logs <pod-name> -f

# Specific container (if multiple)
kubectl logs <pod-name> -c redis
```

---

#### Job Logs

```bash
# List jobs
kubectl get jobs

# View bootstrap job logs
kubectl logs job/<cluster-name>-bootstrap

# View reshard job logs
kubectl logs job/<cluster-name>-reshard

# View drain job logs
kubectl logs job/<cluster-name>-drain
```

**Note:** Jobs are deleted after completion by default. Set `backoffLimit: 1` to keep failed jobs.

---

## Scaling Operations

### Manual Scale-Up

**When to use:**
- You know traffic will increase soon
- Preparing for a marketing campaign
- Load testing

**How to:**

```bash
# Current state
kubectl get rediscluster my-redis

# Scale from 3 to 5 masters
kubectl patch rediscluster my-redis -p '{"spec":{"masters":5}}'

# Watch progress
kubectl get rediscluster my-redis -w

# Expected output:
# NAME       MASTERS   CURRENT   RESHARDING   AGE
# my-redis   5         3         false        10m
# my-redis   5         3         true         10m  <- Resharding started
# my-redis   5         4         true         11m  <- First scale-up done
# my-redis   5         5         false        12m  <- Second scale-up done
```

---

### Manual Scale-Down

**When to use:**
- Traffic has decreased permanently
- Cost optimization
- Over-provisioned

**How to:**

```bash
# Scale from 5 to 3 masters
kubectl patch rediscluster my-redis -p '{"spec":{"masters":3}}'

# Watch progress
kubectl get rediscluster my-redis -w
```

**Important:**
- Must respect `minMasters` limit
- Cannot scale below 3 masters
- Data is preserved (moved to remaining nodes)

---

### Temporarily Disable Autoscaling

**When to use:**
- During maintenance windows
- Debugging issues
- Preventing unexpected scaling

**How to:**

```bash
# Disable
kubectl patch rediscluster my-redis -p '{"spec":{"autoScaleEnabled":false}}'

# Verify
kubectl get rediscluster my-redis -o jsonpath='{.spec.autoScaleEnabled}'
# Output: false

# Re-enable
kubectl patch rediscluster my-redis -p '{"spec":{"autoScaleEnabled":true}}'
```

---

### Monitor Ongoing Scaling

```bash
# Check if scaling is in progress
kubectl get rediscluster my-redis -o yaml | grep -E "isResharding|isDraining"

# If resharding:
kubectl get job <cluster-name>-reshard
kubectl logs job/<cluster-name>-reshard -f

# If draining:
kubectl get job <cluster-name>-drain
kubectl logs job/<cluster-name>-drain -f

# Check pod status
kubectl get pods -l cluster=my-redis -w
```

---

## Backup and Recovery

### Backup Strategies

#### 1. RDB Snapshots (Recommended)

**Configure in ConfigMap:**

```yaml
# redis.conf
save 900 1     # Save after 900s if ≥1 key changed
save 300 10    # Save after 300s if ≥10 keys changed
save 60 10000  # Save after 60s if ≥10000 keys changed
```

**Backup RDB files:**

```bash
# Copy RDB from pod to local
kubectl exec <pod-name> -- cat /data/dump.rdb > backup-$(date +%Y%m%d).rdb

# Or use persistent volumes and backup the PV
```

---

#### 2. AOF (Append-Only File)

**Configure in ConfigMap:**

```yaml
# redis.conf
appendonly yes
appendfsync everysec  # or 'always' for strict durability
```

**Backup AOF files:**

```bash
kubectl exec <pod-name> -- cat /data/appendonly.aof > backup-aof-$(date +%Y%m%d).aof
```

---

#### 3. Full Cluster Backup

**Automated script:**

```bash
#!/bin/bash
CLUSTER_NAME="my-redis"
BACKUP_DIR="./backups/$(date +%Y%m%d)"
mkdir -p $BACKUP_DIR

# Get all master pods
PODS=$(kubectl get pods -l cluster=$CLUSTER_NAME,role=master -o name)

for pod in $PODS; do
  POD_NAME=${pod#pod/}
  echo "Backing up $POD_NAME..."
  kubectl exec $POD_NAME -- redis-cli BGSAVE
  sleep 5
  kubectl exec $POD_NAME -- cat /data/dump.rdb > $BACKUP_DIR/$POD_NAME.rdb
done

echo "Backup complete: $BACKUP_DIR"
```

---

### Recovery

#### Restore from RDB

```bash
# 1. Stop writes to cluster
kubectl exec <pod-name> -- redis-cli CONFIG SET min-slaves-to-write 999

# 2. Copy RDB file to pod
kubectl cp backup.rdb <pod-name>:/data/dump.rdb

# 3. Restart Redis
kubectl delete pod <pod-name>

# 4. Verify data
kubectl exec <pod-name> -- redis-cli DBSIZE

# 5. Re-enable writes
kubectl exec <pod-name> -- redis-cli CONFIG SET min-slaves-to-write 0
```

---

#### Disaster Recovery

**If cluster is completely lost:**

```bash
# 1. Delete the RedisCluster CR
kubectl delete rediscluster my-redis

# 2. Restore from backups to persistent volumes

# 3. Create new RedisCluster with same name
kubectl apply -f cluster.yaml

# 4. Wait for bootstrap
kubectl wait --for=condition=initialized rediscluster/my-redis

# 5. Verify data
kubectl exec -it my-redis-0 -- redis-cli DBSIZE
```

---

## Upgrades and Maintenance

### Upgrade Redis Version

```bash
# Update redisVersion
kubectl patch rediscluster my-redis -p '{"spec":{"redisVersion":"7.2.4"}}'

# Operator will perform rolling update
kubectl rollout status statefulset/<cluster-name>

# Verify new version
kubectl exec -it my-redis-0 -- redis-cli INFO server | grep redis_version
```

**Best practices:**
- Test in non-prod first
- Check Redis release notes for breaking changes
- Monitor during rollout
- Have backups ready

---

### Upgrade Operator

```bash
# Pull latest code
git pull origin main

# Regenerate manifests
make manifests

# Update CRDs
make install

# Redeploy operator
make deploy

# Verify
kubectl get deployment -n redis-operator-system
```

**Best practices:**
- Review CHANGELOG
- Test in non-prod
- Check for API changes
- Backup all RedisCluster CRs before upgrade

---

### Maintenance Windows

**Plan:**

1. **Notify users** of maintenance window
2. **Disable autoscaling:**
   ```bash
   kubectl patch rediscluster my-redis -p '{"spec":{"autoScaleEnabled":false}}'
   ```
3. **Perform maintenance** (upgrades, config changes, etc.)
4. **Verify cluster health:**
   ```bash
   kubectl exec -it my-redis-0 -- redis-cli cluster info
   ```
5. **Re-enable autoscaling:**
   ```bash
   kubectl patch rediscluster my-redis -p '{"spec":{"autoScaleEnabled":true}}'
   ```

---

## Performance Tuning

### Optimize for Throughput

```yaml
spec:
  # More aggressive scaling
  cpuThreshold: 60
  memoryThreshold: 65

  # Faster response
  metricsQueryInterval: 10
  scaleCooldownSeconds: 45
```

**Redis config:**
```
# redis.conf
tcp-backlog 511
timeout 0
tcp-keepalive 300
```

---

### Optimize for Latency

```yaml
spec:
  # More capacity headroom
  cpuThreshold: 50
  memoryThreshold: 55

  # Prevent scaling during peak
  scaleCooldownSeconds: 300
```

**Redis config:**
```
# redis.conf
latency-monitor-threshold 100
slowlog-log-slower-than 10000
```

---

### Optimize for Cost

```yaml
spec:
  # Higher thresholds
  cpuThreshold: 85
  cpuThresholdLow: 15
  memoryThreshold: 90
  memoryThresholdLow: 20

  # Minimal resources
  masters: 3
  minMasters: 3
  replicasPerMaster: 1
```

---

### Resource Limits

**Set pod resource limits:**

Edit the StatefulSet template (or operator code):

```yaml
resources:
  requests:
    cpu: 100m
    memory: 256Mi
  limits:
    cpu: 1000m
    memory: 2Gi
```

**Configure Redis max memory:**

```
# redis.conf
maxmemory 2gb
maxmemory-policy allkeys-lru
```

---

## Security

### Network Policies

**Restrict Redis access:**

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: redis-network-policy
spec:
  podSelector:
    matchLabels:
      app: redis-cluster
  policyTypes:
  - Ingress
  ingress:
  - from:
    - podSelector:
        matchLabels:
          app: my-application
    ports:
    - protocol: TCP
      port: 6379
```

---

### Enable Authentication

**1. Create secret:**

```bash
kubectl create secret generic redis-auth --from-literal=password='your-strong-password'
```

**2. Update Redis config:**

```yaml
# ConfigMap
requirepass ${REDIS_PASSWORD}
masterauth ${REDIS_PASSWORD}
```

**3. Mount secret in pods**

---

### TLS/SSL

**1. Generate certificates**

**2. Create secret:**

```bash
kubectl create secret tls redis-tls \
  --cert=redis.crt \
  --key=redis.key
```

**3. Configure Redis:**

```
# redis.conf
tls-port 6380
port 0
tls-cert-file /tls/redis.crt
tls-key-file /tls/redis.key
tls-ca-cert-file /tls/ca.crt
```

---

### RBAC

**Operator needs these permissions:**

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: redis-operator
rules:
- apiGroups: [""]
  resources: ["pods", "services", "configmaps"]
  verbs: ["get", "list", "watch", "create", "update", "delete"]
- apiGroups: ["apps"]
  resources: ["statefulsets"]
  verbs: ["get", "list", "watch", "create", "update", "delete"]
- apiGroups: ["batch"]
  resources: ["jobs"]
  verbs: ["get", "list", "watch", "create", "update", "delete"]
- apiGroups: ["cache.example.com"]
  resources: ["redisclusters", "redisclusters/status"]
  verbs: ["get", "list", "watch", "update", "patch"]
```

---

## Next Steps

- **[Troubleshooting Guide](./TROUBLESHOOTING.md)** - Resolve common issues
- **[Configuration Reference](./CONFIGURATION.md)** - Tune settings
- **[Architecture](./ARCHITECTURE.md)** - Understand internals
