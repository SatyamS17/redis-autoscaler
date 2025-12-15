# Architecture Overview

This document explains how the Redis Cluster Autoscaler works internally.

## Table of Contents

- [System Components](#system-components)
- [Hot Standby Strategy](#hot-standby-strategy)
- [Reconciliation Loop](#reconciliation-loop)
- [State Machine](#state-machine)
- [Scaling Algorithms](#scaling-algorithms)
- [Data Flow](#data-flow)

## System Components

### Operator Controller

The main controller reconciles the `RedisCluster` custom resource and manages the cluster lifecycle.

**Key Files:**
- [internal/controller/rediscluster_controller.go](../internal/controller/rediscluster_controller.go) - Main reconciliation logic
- [internal/controller/autoscaler.go](../internal/controller/autoscaler.go) - Autoscaling decision engine
- [internal/controller/upscale.go](../internal/controller/upscale.go) - Scale-up operations
- [internal/controller/downscale.go](../internal/controller/downscale.go) - Scale-down operations

### Custom Resources

**RedisCluster CR** ([api/v1/rediscluster_types.go](../api/v1/rediscluster_types.go))

Defines the desired state of a Redis cluster with autoscaling configuration.

```go
type RedisClusterSpec struct {
    Masters              int32   // Active masters (excluding standby)
    MinMasters           int32   // Minimum masters for scale-down limit
    ReplicasPerMaster    int32   // Replicas per master for HA
    AutoScaleEnabled     bool    // Enable/disable autoscaling
    CpuThreshold         int32   // Scale-up CPU threshold
    CpuThresholdLow      int32   // Scale-down CPU threshold
    MemoryThreshold      int32   // Scale-up memory threshold
    MemoryThresholdLow   int32   // Scale-down memory threshold
    // ... more fields
}

type RedisClusterStatus struct {
    CurrentMasters       int32        // Actual number of active masters
    Initialized          bool         // Cluster bootstrap complete
    IsResharding         bool         // Scale-up in progress
    IsDraining           bool         // Scale-down in progress
    StandbyPod           string       // Hot standby pod name
    OverloadedPod        string       // Pod triggering scale-up
    PodToDrain           string       // Pod being drained
    LastScaleTime        *metav1.Time // For cooldown calculation
    // ... more fields
}
```

### Managed Resources

The operator creates and manages:

1. **StatefulSet**: Manages Redis pods with stable network identities
2. **Headless Service**: Enables direct pod-to-pod communication
3. **ConfigMap**: Contains Redis configuration (`redis.conf`)
4. **ServiceMonitor**: Prometheus scraping configuration
5. **Jobs**: Temporary jobs for bootstrap, reshard, and drain operations

## Hot Standby Strategy

### Concept

The operator always maintains **N active masters + 1 standby master**:

- **Active Masters**: Masters with hash slots serving traffic
- **Standby Master**: Master with 0 hash slots, ready for instant activation

### Why Standby?

Traditional Redis scaling requires:
1. Add new master
2. Wait for pod to start (~30-60 seconds)
3. Join to cluster
4. Migrate slots

With standby:
1. Standby is already running and joined
2. Just migrate slots (~5-10 seconds)
3. Provision next standby in background

**Result**: ~80% faster scale-up with no client disruption

### Standby Lifecycle

```
┌─────────────────────────────────────────────────────────┐
│  Cluster State: 3 Active Masters + 1 Standby           │
│                                                         │
│  Pod-0 (M) ◄──┐                                        │
│  Pod-1 (R)    │                                        │
│  Pod-2 (M) ◄──┼── Active masters with slots           │
│  Pod-3 (R)    │                                        │
│  Pod-4 (M) ◄──┘                                        │
│  Pod-5 (R)                                             │
│  Pod-6 (M) ◄──── Standby (0 slots, ready)             │
│  Pod-7 (R)                                             │
└─────────────────────────────────────────────────────────┘

          ▼ SCALE-UP (overload detected on Pod-2)

┌─────────────────────────────────────────────────────────┐
│  1. Migrate half of Pod-2's slots to Pod-6              │
│  2. Pod-6 becomes active (4 masters total)              │
│  3. Provision new standby at Pod-8                      │
└─────────────────────────────────────────────────────────┘

          ▼ Result

┌─────────────────────────────────────────────────────────┐
│  Cluster State: 4 Active Masters + 1 Standby           │
│                                                         │
│  Pod-0 (M)    Pod-4 (M)    Pod-8 (M) ◄── New standby  │
│  Pod-1 (R)    Pod-5 (R)    Pod-9 (R)                   │
│  Pod-2 (M)    Pod-6 (M) ◄─ Activated standby           │
│  Pod-3 (R)    Pod-7 (R)                                │
└─────────────────────────────────────────────────────────┘
```

### Standby Detection

**For Managed Clusters:**
- Standby is always at index `Masters * (1 + ReplicasPerMaster)`
- Example: 3 masters, 1 replica/master → standby at index 6
  - Pods 0,2,4 are active masters
  - Pods 1,3,5 are replicas
  - Pod 6 is standby master

**For Existing Clusters:**
- Uses label selector to find all Redis pods
- Queries Redis cluster to find master with 0 slots
- (Currently requires manual configuration)

## Reconciliation Loop

The controller uses Kubernetes controller-runtime reconciliation pattern.

### Main Reconcile Flow

```go
func (r *RedisClusterReconciler) Reconcile(ctx, req) (Result, error) {
    1. Get RedisCluster CR
    2. Set defaults and validate spec
    3. Reconcile infrastructure (StatefulSet, Service, etc.)
    4. Handle bootstrap (or discovery for existing clusters)
    5. If initialized && autoscale enabled:
       → handleAutoScaling()
    6. Requeue after MetricsQueryInterval
}
```

**Defined in:** [internal/controller/rediscluster_controller.go:73-95](../internal/controller/rediscluster_controller.go#L73-L95)

### Auto-Scaling Flow

```go
func (r *RedisClusterReconciler) handleAutoScaling(ctx, cluster) (Result, error) {
    1. If IsResharding:
       → checkReshardingStatus()  // Monitor scale-up job
    2. Else if IsDraining:
       → checkDrainStatus()        // Monitor scale-down job
    3. Else:
       → monitorMetrics()          // Check if scaling needed
}
```

**Defined in:** [internal/controller/autoscaler.go:16-36](../internal/controller/autoscaler.go#L16-L36)

## State Machine

The cluster operates in one of three states:

### 1. Monitoring State

**Condition:** `!IsResharding && !IsDraining && Initialized`

**Actions:**
- Query Prometheus for pod metrics every `MetricsQueryInterval` seconds
- Check if any pod exceeds scale-up thresholds
- Check if all pods are below scale-down thresholds
- Verify cluster health before scaling
- Enforce cooldown period

**Transitions:**
- To Resharding: Pod exceeds thresholds → `triggerScaleUp()`
- To Draining: All pods underutilized → `triggerScaleDown()`

### 2. Resharding State (Scale-Up)

**Condition:** `IsResharding == true`

**Actions:**
1. Wait for new standby pod to be ready
2. Create reshard job to activate standby
3. Monitor job progress
4. On success:
   - Increment `Spec.Masters`
   - Provision next standby
   - Clear `IsResharding` and `OverloadedPod`
   - Set `LastScaleTime`

**Job:** Migrates slots from overloaded pod to standby using `redis-cli --cluster reshard`

**Defined in:** [internal/controller/upscale.go:25-146](../internal/controller/upscale.go#L25-L146)

### 3. Draining State (Scale-Down)

**Condition:** `IsDraining == true`

**Actions:**
1. Create drain job to empty a pod
2. Monitor job progress
3. On success:
   - Decrement `Spec.Masters`
   - Drained pod becomes new standby
   - Clear `IsDraining`, `PodToDrain`, etc.
   - Set `LastScaleTime`

**Job:** Uses pre-seeding (replication) then migrates slots to destination pods

**Defined in:** [internal/controller/downscale.go:23-128](../internal/controller/downscale.go#L23-L128)

### State Diagram

```
                    ┌────────────────┐
                    │  MONITORING    │
                    │                │
                    │ - Query metrics│
                    │ - Check health │
                    │ - Check cooldown│
                    └───┬────────┬───┘
                        │        │
          CPU/Mem high  │        │  All pods low utilization
          + health OK   │        │  + health OK
          + cooldown OK │        │  + cooldown OK
                        │        │  + masters > minMasters
                        ▼        ▼
              ┌──────────────┐ ┌──────────────┐
              │ RESHARDING   │ │  DRAINING    │
              │              │ │              │
              │ - Wait pods  │ │ - Create job │
              │ - Run job    │ │ - Monitor    │
              │ - Provision  │ │ - Update     │
              └─────┬────────┘ └──────┬───────┘
                    │                 │
         Job success│                 │Job success
         Increment M│                 │Decrement M
                    │                 │
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │  MONITORING     │
                    │ (wait cooldown) │
                    └─────────────────┘
```

## Scaling Algorithms

### Scale-Up Decision

Located in [internal/controller/autoscaler.go:214-254](../internal/controller/autoscaler.go#L214-L254)

```
For each pod:
    cpuUsage = query Prometheus for redis_cpu_usage
    memUsage = query Prometheus for redis_memory_usage

    if cpuUsage > CpuThreshold:
        trigger scale-up (reason: CPU)

    if memUsage > MemoryThreshold:
        trigger scale-up (reason: Memory)
```

**Pre-conditions:**
1. Cluster is healthy (no jobs running, all pods ready)
2. Cooldown period has elapsed
3. Standby pod exists and is ready

### Scale-Down Decision

Located in [internal/controller/autoscaler.go:256-281](../internal/controller/autoscaler.go#L256-L281)

```
if ALL pods have:
    cpuUsage < CpuThresholdLow AND
    memUsage < MemoryThresholdLow
then:
    if Masters > MinMasters:
        trigger scale-down
```

**Pre-conditions:**
1. Cluster is healthy
2. Cooldown period has elapsed
3. Current masters > minimum masters
4. ALL active pods are underutilized (not just one)

### Pod Selection for Draining

Located in [internal/controller/autoscaler.go:296-360](../internal/controller/autoscaler.go#L296-L360)

**Algorithm:**
1. List all active master pods (exclude standby)
2. Find pod with minimum slot count (least data to migrate)
3. Select 1-2 destination pods to receive the slots
4. Set `PodToDrain`, `DrainDestPod1`, `DrainDestPod2` in status

### Slot Migration Strategy

**Scale-Up (Reshard):**
- Source: Overloaded pod
- Destination: Standby pod
- Amount: 50% of source's slots
- Method: `redis-cli --cluster reshard --cluster-pipeline 100`

**Scale-Down (Drain):**
- Phase 1: Pre-seed data using `CLUSTER REPLICATE` (speeds up migration)
- Phase 2: Migrate all slots to destination(s) using reshard
- Phase 3: Forget drained node and convert to new standby

## Data Flow

### Metrics Collection

```
┌────────────┐
│ Redis Pods │
│   :6379    │
└─────┬──────┘
      │ Expose metrics
      │
      ▼
┌────────────────┐
│  Prometheus    │
│                │
│ Scrapes every  │
│ 15s via        │
│ ServiceMonitor │
└────────┬───────┘
         │
         │ PromQL queries
         │ every MetricsQueryInterval
         │
         ▼
┌────────────────────┐
│  Operator          │
│                    │
│ queryPodMetrics()  │
│ queryCPUMetrics()  │
│ queryMemoryMetrics()│
└────────────────────┘
```

**Queries Used:**

CPU Usage:
```promql
rate(process_cpu_seconds_total{pod=~"redis-cluster-.*"}[1m]) * 100
```

Memory Usage:
```promql
(redis_memory_used_bytes / redis_memory_max_bytes) * 100
```

### Scaling Operation Flow

**Scale-Up:**

```
1. Detect: monitorMetrics() → pod exceeds threshold
           ↓
2. Trigger: triggerScaleUp() → set IsResharding, OverloadedPod
           ↓
3. Reconcile: checkReshardingStatus() → create reshard Job
           ↓
4. Job: Bash script in pod executes redis-cli commands
        - Fix cluster inconsistencies
        - Verify standby has 0 slots
        - Migrate 50% slots from overloaded → standby
           ↓
5. Complete: Job succeeds → increment Masters, provision new standby
           ↓
6. Reset: Clear IsResharding, set LastScaleTime
```

**Scale-Down:**

```
1. Detect: monitorMetrics() → all pods underutilized
           ↓
2. Select: findPodToDrain() → choose pod with min slots
           ↓
3. Trigger: triggerScaleDown() → set IsDraining, PodToDrain, destinations
           ↓
4. Reconcile: checkDrainStatus() → create drain Job
           ↓
5. Job: Bash script in pod executes:
        a. Pre-seed: Make destinations replicate from source
        b. Wait for sync
        c. Migrate all slots
        d. Forget drained node
           ↓
6. Complete: Job succeeds → decrement Masters, drained pod = new standby
           ↓
7. Reset: Clear IsDraining, set LastScaleTime
```

## Key Design Decisions

### Why Jobs for Scaling?

**Pros:**
- Isolated execution environment
- Built-in retry and timeout
- Easy to debug (logs persist)
- Cluster state is atomic (job either succeeds or fails)

**Cons:**
- Higher latency than direct redis-cli from operator
- Requires RBAC permissions for job creation

**Decision:** Chosen for reliability and debuggability

### Why Pre-Seeding in Scale-Down?

**Traditional approach:** Migrate slots directly
- Slow: Must transfer all data over network during migration

**Pre-seeding approach:** Make destination replicate from source first
- Fast: Data is already synced, only need to reassign slots
- ~50% faster for large datasets

**Defined in:** [internal/controller/downscale.go:146-539](../internal/controller/downscale.go#L146-L539)

### Why Cooldown Period?

Prevents oscillation:
```
Without cooldown:
  CPU high → scale up → CPU drops → scale down → CPU high → ...

With cooldown:
  CPU high → scale up → wait 60s → re-evaluate → ...
```

Default: 60 seconds (configurable via `scaleCooldownSeconds`)

## Performance Characteristics

### Scale-Up Latency

- **Standby ready:** ~5-10 seconds (just slot migration)
- **Standby provisioning:** ~30-60 seconds (pod startup + join cluster)
- **Total:** ~35-70 seconds for full scale-up cycle

### Scale-Down Latency

- **Pre-seeding:** ~10-30 seconds (depends on data size)
- **Slot migration:** ~5-10 seconds
- **Total:** ~15-40 seconds

### Memory Overhead

- **Standby pod:** Uses same resources as active master
- **Operator:** ~50MB for controller process
- **Jobs:** Minimal (Redis image, run once)

## Next Steps

- See [User Guide](./USER_GUIDE.md) for deployment instructions
- See [Configuration Reference](./CONFIGURATION.md) for tuning parameters
- See [Operations Guide](./OPERATIONS.md) for monitoring and maintenance
