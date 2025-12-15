# Existing Cluster Support

This document explains how to use the Redis Operator to manage autoscaling for existing Redis clusters.

## Overview

The Redis Operator now supports two modes of operation:

1. **Managed Mode** (default): The operator creates and manages the entire Redis cluster infrastructure including StatefulSet, Service, and ConfigMap.

2. **Existing Cluster Mode** (new): The operator discovers and manages autoscaling for already-deployed Redis clusters, with flexible support for custom naming schemes and label selectors.

## Configuration for Existing Clusters

To use the operator with an existing Redis cluster, set the following fields in your RedisCluster CR:

### Required Fields

- **`existingCluster: true`**: Enables existing cluster mode
- **`podSelector`**: Label selector to identify your Redis pods (e.g., `{app: redis, cluster: my-cluster}`)
- **`serviceName`**: Name of your existing headless service

### Optional Fields

- **`manageStatefulSet`**: (default: `true`)
  - Set to `false` if you want the operator to only manage autoscaling without touching the StatefulSet
  - Set to `true` if you want the operator to also manage scaling the StatefulSet

- **`statefulSetName`**: Name of the existing StatefulSet (required if `manageStatefulSet: true` and the StatefulSet name differs from the cluster name)

## Example Configuration

See [cluster-existing.yaml](./cluster-existing.yaml) for a complete example:

```yaml
apiVersion: cache.example.com/v1
kind: RedisCluster
metadata:
  name: redis-cluster-existing
  namespace: default
spec:
  existingCluster: true
  podSelector:
    app: redis
    cluster: my-existing-cluster
  serviceName: my-existing-cluster-headless
  manageStatefulSet: false
  masters: 3
  minMasters: 3
  replicasPerMaster: 1
  autoScaleEnabled: true
  cpuThreshold: 70
  memoryThreshold: 70
  # ... other configuration
```

## How It Works

### Discovery Phase

When `existingCluster: true`:

1. The operator skips creating Service and StatefulSet (if `manageStatefulSet: false`)
2. Uses `podSelector` to find all Redis pods in the cluster
3. Queries a running pod to discover the cluster topology
4. Marks the cluster as initialized and ready for autoscaling

### Autoscaling Phase

Once discovered, autoscaling works the same as managed clusters:

1. Monitors CPU and memory metrics from Prometheus
2. Triggers scale-up when thresholds are exceeded
3. Triggers scale-down when usage is low
4. Manages resharding and draining operations

## Current Limitations

### Standby Pod Detection

For existing clusters, the operator currently requires manual configuration for the standby pod detection. This is planned for a future enhancement. The limitation exists because:

- Detecting which master has 0 slots requires executing `redis-cli` commands in the pod
- This adds complexity to the discovery phase

**Workaround**: For now, the operator will attempt to use the existing `StandbyPod` value from the status if it exists.

### Cluster Requirements

Your existing Redis cluster must meet these requirements:

1. **Redis Cluster Mode**: Must be running in cluster mode (not standalone or sentinel)
2. **Prometheus Metrics**: Pods must expose metrics that Prometheus can scrape
3. **Headless Service**: Must have a headless service for pod discovery
4. **Labels**: All Redis pods must have consistent labels that can be selected with `podSelector`

## Migration from Managed to Existing Mode

If you have a cluster created by this operator and want to switch to existing mode:

1. Note your current labels (usually `app: redis-cluster`, `cluster: <name>`)
2. Note your service name (usually `<name>-headless`)
3. Update your CR to set:
   ```yaml
   existingCluster: true
   podSelector:
     app: redis-cluster
     cluster: <your-cluster-name>
   serviceName: <your-cluster-name>-headless
   manageStatefulSet: true  # Keep managing the StatefulSet
   ```

## Validation

The operator validates that:

- When `existingCluster: true`, `podSelector` must not be empty
- When `existingCluster: true`, `serviceName` must be specified
- CPU and memory thresholds are properly configured
- Masters >= MinMasters

If validation fails, the operator will log an error and not proceed with reconciliation.

## Troubleshooting

### "No pods found matching selector"

Check that:
1. Your `podSelector` labels match your actual pod labels
2. The pods are in the same namespace as the RedisCluster CR
3. Use `kubectl get pods -n <namespace> -l <selector>` to verify

### "Cannot auto-detect standby pod for existing cluster"

This is expected in the current version. Future enhancements will add automatic standby detection. For now:
1. Ensure your cluster has a standby master with 0 hash slots
2. The operator will attempt to use any previously set standby pod value

### "Failed to discover cluster topology"

Ensure:
1. At least one Redis pod is running
2. The pods are accessible from the operator pod
3. The Redis cluster is properly initialized and healthy

## Future Enhancements

Planned improvements for existing cluster support:

1. **Automatic Standby Detection**: Execute redis-cli commands to find the master with 0 slots
2. **Topology Validation**: Verify cluster topology matches the CR specification
3. **Migration Tools**: Helper scripts to migrate from external Redis clusters to operator-managed
4. **Multi-namespace Support**: Support for clusters spanning multiple namespaces
