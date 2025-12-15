# Redis Cluster Autoscaler Documentation

Welcome to the Redis Cluster Autoscaler documentation. This Kubernetes operator provides intelligent autoscaling for Redis clusters with zero-downtime operations.

## Overview

The Redis Cluster Autoscaler is a Kubernetes operator that automatically scales Redis clusters up and down based on CPU and memory metrics. It uses a **hot standby strategy** to enable zero-downtime scaling operations.

### Key Features

- **Zero-Downtime Scaling**: Uses a hot standby master node to enable instant scale-up
- **Intelligent Metrics Monitoring**: Monitors both CPU and memory usage via Prometheus
- **Automatic Resharding**: Manages Redis cluster slot distribution during scaling
- **Pre-seeded Scale-Down**: Uses Redis replication to speed up slot migration
- **Flexible Deployment**: Supports both managed clusters and existing clusters
- **Cooldown Protection**: Prevents rapid scaling oscillations

## Documentation Structure

This documentation is organized into the following sections:

### 1. [Architecture Overview](./ARCHITECTURE.md)
Learn how the autoscaler works internally, including:
- System architecture and components
- Hot standby strategy
- Scaling algorithms
- State machine and reconciliation loop

### 2. [User Guide](./USER_GUIDE.md)
Get started with deploying and using the autoscaler:
- Installation instructions
- Quick start guide
- Basic configuration examples
- Common use cases

### 3. [Configuration Reference](./CONFIGURATION.md)
Complete reference for all configuration options:
- RedisCluster CR specification
- Autoscaling parameters
- Threshold tuning
- Resource limits

### 4. [Operations Guide](./OPERATIONS.md)
Operational procedures for running the autoscaler:
- Monitoring and observability
- Scaling operations
- Backup and recovery
- Upgrades and maintenance

### 5. [Troubleshooting Guide](./TROUBLESHOOTING.md)
Common issues and how to resolve them:
- Pod issues
- Scaling failures
- Performance problems
- Debugging techniques

### 6. [Existing Cluster Support](../EXISTING_CLUSTER_SUPPORT.md)
Special guide for integrating with existing Redis deployments:
- Configuration for existing clusters
- Label selectors and pod discovery
- Migration from external clusters
- Limitations and workarounds

## Quick Links

- **Installation**: See [User Guide - Installation](./USER_GUIDE.md#installation)
- **Quick Start**: See [User Guide - Quick Start](./USER_GUIDE.md#quick-start)
- **Configuration Examples**: See [Configuration Reference - Examples](./CONFIGURATION.md#examples)
- **Common Issues**: See [Troubleshooting Guide](./TROUBLESHOOTING.md)

## Architecture at a Glance

```
┌─────────────────────────────────────────────────────────────┐
│                     Kubernetes Cluster                       │
│                                                              │
│  ┌──────────────┐        ┌─────────────────────────────┐   │
│  │  Prometheus  │◄───────│  Redis Cluster Pods         │   │
│  │              │        │  ┌────┐ ┌────┐ ┌────┐      │   │
│  └──────┬───────┘        │  │M+R │ │M+R │ │M+R │      │   │
│         │                │  └────┘ └────┘ └────┘      │   │
│         │ metrics        │  ┌────┐ (Standby - 0 slots)│   │
│         ▼                │  │ M  │                     │   │
│  ┌──────────────┐        │  └────┘                     │   │
│  │   Operator   │────────┴─────────────────────────────┘   │
│  │  Controller  │                                           │
│  │              │        Manages                            │
│  │  - Monitor   │────────►  StatefulSet                     │
│  │  - Scale     │────────►  Services                        │
│  │  - Reshard   │────────►  ConfigMaps                      │
│  └──────────────┘────────►  Jobs (bootstrap/reshard/drain)  │
│                                                              │
└─────────────────────────────────────────────────────────────┘

Legend: M = Master, R = Replicas
```

## How It Works (Brief)

1. **Monitoring**: The operator continuously queries Prometheus for CPU and memory metrics of each Redis pod

2. **Decision**: When a pod exceeds thresholds (e.g., CPU > 70% or Memory > 70%), the operator triggers a scale-up

3. **Scale-Up**: The hot standby master is activated by migrating half the slots from the overloaded pod to it. A new standby is then provisioned.

4. **Scale-Down**: When all pods are underutilized (e.g., CPU < 20% and Memory < 30%), a pod is drained and becomes the new standby.

5. **Cooldown**: After any scaling operation, the operator waits for a cooldown period before allowing another scale operation.

For detailed information, see the [Architecture Overview](./ARCHITECTURE.md).

## Getting Started

### Prerequisites

- Kubernetes cluster (v1.19+)
- Prometheus with ServiceMonitor support
- kubectl configured for your cluster

### Quick Installation

```bash
# Install CRDs
make install

# Deploy the operator
make deploy

# Create a Redis cluster
kubectl apply -f cluster.yaml
```

See the [User Guide](./USER_GUIDE.md) for detailed installation instructions.

## Configuration Example

```yaml
apiVersion: cache.example.com/v1
kind: RedisCluster
metadata:
  name: redis-cluster
  namespace: default
spec:
  # Cluster size
  masters: 3
  minMasters: 3
  replicasPerMaster: 1

  # Autoscaling
  autoScaleEnabled: true
  cpuThreshold: 70        # Scale up when CPU > 70%
  cpuThresholdLow: 20     # Scale down when CPU < 20%
  memoryThreshold: 70     # Scale up when Memory > 70%
  memoryThresholdLow: 30  # Scale down when Memory < 30%

  # Timing
  scaleCooldownSeconds: 60
  metricsQueryInterval: 15
```

See [Configuration Reference](./CONFIGURATION.md) for all options.

## Support and Contributing

- **Issues**: Report bugs and request features via GitHub issues
- **Questions**: Check the [Troubleshooting Guide](./TROUBLESHOOTING.md) first
- **Contributing**: Follow standard GitHub pull request workflow

## License

Copyright 2025. Licensed under the Apache License, Version 2.0.
