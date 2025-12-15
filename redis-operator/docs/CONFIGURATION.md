# Configuration Reference

Complete reference for all RedisCluster configuration options.

## Table of Contents

- [RedisCluster Spec](#rediscluster-spec)
- [RedisCluster Status](#rediscluster-status)
- [Configuration Examples](#configuration-examples)
- [Tuning Guidelines](#tuning-guidelines)
- [Validation Rules](#validation-rules)

## RedisCluster Spec

The `spec` section defines the desired state of your Redis cluster.

### Cluster Size Configuration

#### `masters` (required)

**Type:** `int32`
**Minimum:** `1`
**Description:** Number of **active** master nodes in the cluster (excluding the standby master).

```yaml
spec:
  masters: 3  # 3 active masters + 1 standby = 4 master pods total
```

**Notes:**
- The operator always provisions one extra master as a hot standby (with 0 hash slots)
- Total master pods = `masters` + 1
- Cannot be less than `minMasters`

---

#### `minMasters`

**Type:** `int32`
**Minimum:** `3`
**Default:** `3`
**Description:** Minimum number of masters the cluster can scale down to. Prevents excessive scale-down.

```yaml
spec:
  minMasters: 3  # Never scale below 3 masters
```

**Notes:**
- Redis cluster requires minimum 3 masters to function properly
- Acts as a safety guard for autoscaling
- Must be ≥ 3 for cluster mode

---

#### `replicasPerMaster`

**Type:** `int32`
**Minimum:** `0`
**Default:** `1`
**Description:** Number of replica nodes per master for high availability.

```yaml
spec:
  replicasPerMaster: 2  # Each master has 2 replicas
```

**Total Pod Count Calculation:**
```
Total Pods = (masters + 1) * (replicasPerMaster + 1)

Example: 3 masters, 1 replica per master
= (3 + 1) * (1 + 1)
= 4 * 2
= 8 pods total
```

**Notes:**
- `0` = No replicas (not recommended for production)
- `1` = Standard HA configuration
- `2+` = Higher availability, more resources

---

#### `redisVersion`

**Type:** `string`
**Default:** `"7.2"`
**Description:** Redis Docker image version to use.

```yaml
spec:
  redisVersion: "7.2"  # Uses redis:7.2 image
```

**Supported Versions:**
- `"7.2"` - Latest stable (recommended)
- `"7.0"` - Previous stable
- `"6.2"` - Older stable

**Notes:**
- Changes to this field trigger a rolling update
- Use specific tags for production (e.g., `"7.2.4"`)

---

### Autoscaling Configuration

#### `autoScaleEnabled`

**Type:** `bool`
**Required:** Yes
**Description:** Enable or disable autoscaling.

```yaml
spec:
  autoScaleEnabled: true  # Enable autoscaling
```

**When `false`:**
- Operator still manages cluster infrastructure
- No automatic scaling operations
- Manual scaling still works (change `masters` field)

---

#### `cpuThreshold`

**Type:** `int32`
**Required:** Yes
**Range:** `1-100`
**Description:** CPU usage percentage that triggers scale-up.

```yaml
spec:
  cpuThreshold: 70  # Scale up when any pod exceeds 70% CPU
```

**How it works:**
- Operator queries Prometheus for CPU usage of each pod
- If **any** pod exceeds this threshold, scale-up is triggered
- Measured as percentage of CPU requests/limits

**Tuning:**
- Lower (50-60): More aggressive scaling, higher cost
- Medium (70-75): Balanced approach
- Higher (80-90): Conservative scaling, lower cost, higher latency risk

---

#### `cpuThresholdLow`

**Type:** `int32`
**Range:** `1-100`
**Default:** `20`
**Description:** CPU usage percentage below which scale-down is considered.

```yaml
spec:
  cpuThresholdLow: 20  # Scale down when ALL pods are below 20% CPU
```

**How it works:**
- If **all** active pods are below this threshold, scale-down is triggered
- Combined with `memoryThresholdLow` (both must be satisfied)

**Tuning:**
- Higher (30-40): Scale down less aggressively, more stable
- Lower (10-20): Scale down more aggressively, cost savings

**Important:** Must be < `cpuThreshold`

---

#### `memoryThreshold`

**Type:** `int32`
**Range:** `1-100`
**Default:** `70`
**Description:** Memory usage percentage that triggers scale-up.

```yaml
spec:
  memoryThreshold: 80  # Scale up when any pod exceeds 80% memory
```

**How it works:**
- Measured as `(used_memory / max_memory) * 100`
- Triggers scale-up if **any** pod exceeds this value

**Tuning:**
- Redis eviction policies: Set lower (60-70) if using `volatile-*` policies
- No eviction: Set higher (80-85)
- Critical workloads: Set lower (65-75)

---

#### `memoryThresholdLow`

**Type:** `int32`
**Range:** `1-100`
**Default:** `30`
**Description:** Memory usage percentage below which scale-down is considered.

```yaml
spec:
  memoryThresholdLow: 35  # Scale down when ALL pods are below 35% memory
```

**How it works:**
- Combined with `cpuThresholdLow` (both must be satisfied)
- All active pods must be below both thresholds

**Important:** Must be < `memoryThreshold`

---

### Timing and Limits

#### `reshardTimeoutSeconds`

**Type:** `int32`
**Range:** `60-3600`
**Default:** `600` (10 minutes)
**Description:** Timeout for reshard and drain jobs.

```yaml
spec:
  reshardTimeoutSeconds: 900  # 15 minutes
```

**When to increase:**
- Large datasets (multi-GB per master)
- Slow network
- Many hash slots per pod

**When to decrease:**
- Small datasets
- Fast SSD storage
- Dev/test environments

---

#### `scaleCooldownSeconds`

**Type:** `int32`
**Range:** `30-3600`
**Default:** `60` (1 minute)
**Description:** Minimum time between scaling operations.

```yaml
spec:
  scaleCooldownSeconds: 120  # Wait 2 minutes between scales
```

**Purpose:**
- Prevent rapid scaling oscillations
- Allow cluster to stabilize after scaling
- Give metrics time to reflect changes

**Tuning:**
- Production: 120-300 seconds (stable)
- Development: 30-60 seconds (fast iteration)
- Critical workloads: 300-600 seconds (very stable)

---

#### `metricsQueryInterval`

**Type:** `int32`
**Range:** `5-300`
**Default:** `15` (15 seconds)
**Description:** How often to query Prometheus for metrics.

```yaml
spec:
  metricsQueryInterval: 30  # Check metrics every 30 seconds
```

**Trade-offs:**
- Lower (5-10): Faster response to load, more Prometheus queries
- Higher (30-60): Less load on Prometheus, slower response

---

### Infrastructure Configuration

#### `prometheusURL`

**Type:** `string`
**Default:** `"http://prometheus-operated.monitoring.svc:9090"`
**Description:** URL to Prometheus server for metrics queries.

```yaml
spec:
  prometheusURL: "http://prometheus-server.monitoring.svc:9090"
```

**Common values:**
- Prometheus Operator: `http://prometheus-operated.monitoring.svc:9090`
- Helm chart: `http://prometheus-server.monitoring.svc:9090`
- External: `http://prometheus.example.com:9090`

---

### Existing Cluster Support

These fields are used when managing existing Redis clusters. See [EXISTING_CLUSTER_SUPPORT.md](../EXISTING_CLUSTER_SUPPORT.md) for details.

#### `existingCluster`

**Type:** `bool`
**Default:** `false`
**Description:** Indicates this CR is managing an existing Redis cluster.

```yaml
spec:
  existingCluster: true
```

---

#### `podSelector`

**Type:** `map[string]string`
**Required when:** `existingCluster: true`
**Description:** Label selector to identify Redis pods in an existing cluster.

```yaml
spec:
  podSelector:
    app: redis
    cluster: my-cluster
    role: server
```

---

#### `serviceName`

**Type:** `string`
**Default:** `"<cluster-name>-headless"`
**Description:** Name of the headless service for the cluster.

```yaml
spec:
  serviceName: my-redis-headless
```

---

#### `manageStatefulSet`

**Type:** `bool`
**Default:** `true`
**Description:** Whether the operator should manage the StatefulSet.

```yaml
spec:
  manageStatefulSet: false  # Only manage autoscaling, not infrastructure
```

**When `false`:**
- Operator does not create/update StatefulSet
- Only manages autoscaling operations
- Useful for existing deployments

---

#### `statefulSetName`

**Type:** `string`
**Default:** `"<cluster-name>"`
**Description:** Name of the StatefulSet to manage.

```yaml
spec:
  statefulSetName: my-existing-statefulset
```

---

## RedisCluster Status

The `status` section shows the current state of the cluster. These fields are managed by the operator.

### `currentMasters`

**Type:** `int32`
**Description:** Actual number of active master nodes currently running.

```yaml
status:
  currentMasters: 3
```

---

### `currentReplicas`

**Type:** `int32`
**Description:** Actual number of replica nodes currently running.

```yaml
status:
  currentReplicas: 3
```

---

### `initialized`

**Type:** `bool`
**Description:** Whether the cluster has completed bootstrap.

```yaml
status:
  initialized: true
```

**States:**
- `false`: Bootstrap in progress or cluster not ready
- `true`: Cluster is initialized and ready for use

---

### `isResharding`

**Type:** `bool`
**Description:** Scale-up operation is in progress.

```yaml
status:
  isResharding: true
```

---

### `isDraining`

**Type:** `bool`
**Description:** Scale-down operation is in progress.

```yaml
status:
  isDraining: true
```

---

### `standbyPod`

**Type:** `string`
**Description:** Name of the pod serving as the hot standby (0 hash slots).

```yaml
status:
  standbyPod: redis-cluster-6
```

---

### `overloadedPod`

**Type:** `string`
**Description:** Pod that triggered the current scale-up operation.

```yaml
status:
  overloadedPod: redis-cluster-2
```

---

### `podToDrain`

**Type:** `string`
**Description:** Pod being drained during the current scale-down operation.

```yaml
status:
  podToDrain: redis-cluster-4
```

---

### `lastScaleTime`

**Type:** `metav1.Time`
**Description:** When the last scaling operation started (used for cooldown).

```yaml
status:
  lastScaleTime: "2025-01-15T10:30:00Z"
```

---

## Configuration Examples

### Production High-Availability

```yaml
apiVersion: cache.example.com/v1
kind: RedisCluster
metadata:
  name: prod-redis
  namespace: production
spec:
  # Large cluster with HA
  masters: 10
  minMasters: 5
  replicasPerMaster: 2

  # Conservative autoscaling
  autoScaleEnabled: true
  cpuThreshold: 80
  cpuThresholdLow: 25
  memoryThreshold: 85
  memoryThresholdLow: 30

  # Longer timeouts for safety
  scaleCooldownSeconds: 300
  reshardTimeoutSeconds: 1200
  metricsQueryInterval: 30

  redisVersion: "7.2"
```

---

### Development/Testing

```yaml
apiVersion: cache.example.com/v1
kind: RedisCluster
metadata:
  name: dev-redis
  namespace: development
spec:
  # Minimal cluster
  masters: 3
  minMasters: 3
  replicasPerMaster: 0  # No replicas for dev

  # Aggressive autoscaling for testing
  autoScaleEnabled: true
  cpuThreshold: 60
  cpuThresholdLow: 15
  memoryThreshold: 65
  memoryThresholdLow: 20

  # Fast iteration
  scaleCooldownSeconds: 30
  reshardTimeoutSeconds: 300
  metricsQueryInterval: 10

  redisVersion: "7.2"
```

---

### Cost-Optimized

```yaml
apiVersion: cache.example.com/v1
kind: RedisCluster
metadata:
  name: cost-redis
  namespace: default
spec:
  # Start small
  masters: 3
  minMasters: 3
  replicasPerMaster: 1

  # Scale up slowly, down quickly
  autoScaleEnabled: true
  cpuThreshold: 85        # High threshold
  cpuThresholdLow: 15     # Low threshold
  memoryThreshold: 90
  memoryThresholdLow: 20

  scaleCooldownSeconds: 60
  metricsQueryInterval: 20

  redisVersion: "7.2"
```

---

### Memory-Intensive Workload

```yaml
apiVersion: cache.example.com/v1
kind: RedisCluster
metadata:
  name: cache-redis
  namespace: default
spec:
  masters: 5
  minMasters: 3
  replicasPerMaster: 1

  # Prioritize memory scaling
  autoScaleEnabled: true
  cpuThreshold: 90        # Less sensitive to CPU
  cpuThresholdLow: 20
  memoryThreshold: 75     # More sensitive to memory
  memoryThresholdLow: 35

  scaleCooldownSeconds: 90
  redisVersion: "7.2"
```

---

## Tuning Guidelines

### Choosing CPU Thresholds

**Formula:** `cpuThresholdLow` should be ~25-30% of `cpuThreshold`

**Examples:**
- `cpuThreshold: 70`, `cpuThresholdLow: 20` (default)
- `cpuThreshold: 80`, `cpuThresholdLow: 25`
- `cpuThreshold: 60`, `cpuThresholdLow: 15`

**Why:** Provides hysteresis to prevent oscillation.

---

### Choosing Memory Thresholds

**Consider:**
1. Redis eviction policy
2. Dataset growth rate
3. Availability requirements

**Guidelines:**

| Eviction Policy | Recommended memoryThreshold | Reason |
|----------------|----------------------------|---------|
| `noeviction` | 75-80% | Must scale before OOM |
| `volatile-*` | 80-85% | Can evict to free space |
| `allkeys-*` | 80-90% | More aggressive eviction |

---

### Cooldown Period

**Choose based on:**
- Cluster size (larger = longer cooldown)
- Load patterns (spiky = longer cooldown)
- Cost sensitivity (higher = longer cooldown)

**Recommendations:**

| Cluster Size | Load Pattern | Cooldown |
|--------------|--------------|----------|
| Small (3-5 masters) | Steady | 60s |
| Small (3-5 masters) | Spiky | 120s |
| Medium (6-10 masters) | Steady | 120s |
| Medium (6-10 masters) | Spiky | 180-300s |
| Large (10+ masters) | Any | 300-600s |

---

### Metrics Query Interval

**Trade-off:** Response time vs Prometheus load

**Recommendations:**
- **5-10s**: Very responsive, high Prometheus load
- **15s** (default): Balanced
- **30-60s**: Low Prometheus load, slower response

---

## Validation Rules

The operator validates your configuration. These rules must be satisfied:

### Required Rules

1. **CPU Thresholds:**
   ```
   cpuThreshold > cpuThresholdLow
   ```
   Error: `cpuThreshold (70) must be greater than cpuThresholdLow (20)`

2. **Memory Thresholds:**
   ```
   memoryThreshold > memoryThresholdLow
   ```
   Error: `memoryThreshold (70) must be greater than memoryThresholdLow (30)`

3. **Master Limits:**
   ```
   masters >= minMasters
   ```
   Error: `masters (2) cannot be less than minMasters (3)`

4. **Minimum Masters:**
   ```
   minMasters >= 3
   ```
   Kubebuilder validation enforces this.

5. **Existing Cluster:**
   ```
   If existingCluster == true:
     - podSelector must not be empty
     - serviceName must be specified
   ```

### Range Validations

| Field | Min | Max |
|-------|-----|-----|
| `masters` | 1 | N/A |
| `minMasters` | 3 | N/A |
| `replicasPerMaster` | 0 | N/A |
| `cpuThreshold` | 1 | 100 |
| `cpuThresholdLow` | 1 | 100 |
| `memoryThreshold` | 1 | 100 |
| `memoryThresholdLow` | 1 | 100 |
| `reshardTimeoutSeconds` | 60 | 3600 |
| `scaleCooldownSeconds` | 30 | 3600 |
| `metricsQueryInterval` | 5 | 300 |

---

## Next Steps

- **[User Guide](./USER_GUIDE.md)** - Learn how to use these configurations
- **[Operations Guide](./OPERATIONS.md)** - Monitor and tune running clusters
- **[Troubleshooting](./TROUBLESHOOTING.md)** - Fix configuration issues
