# Redis Cluster Autoscaler

A Kubernetes operator that provides intelligent autoscaling for Redis clusters with zero-downtime operations.

## Overview

The Redis Cluster Autoscaler automatically scales your Redis cluster up and down based on CPU and memory metrics. It uses a **hot standby strategy** to enable instant scale-up operations without waiting for pod startup times.

### Key Features

- **Zero-Downtime Scaling**: Hot standby master node enables instant scale-up (~5-10 seconds)
- **Intelligent Monitoring**: Automatically scales based on CPU and memory usage via Prometheus
- **Automatic Resharding**: Manages Redis cluster slot distribution during scaling operations
- **Pre-seeded Scale-Down**: Uses Redis replication to speed up slot migration
- **Flexible Deployment**: Supports both operator-managed and existing Redis clusters
- **Cooldown Protection**: Prevents rapid scaling oscillations

## Quick Start

### Prerequisites

- Kubernetes cluster (v1.19+)
- Prometheus Operator (for metrics collection)
- kubectl configured for your cluster

### Installation

```bash
# Install CRDs
make install

# Deploy operator
make deploy

# Create a Redis cluster
kubectl apply -f cluster.yaml
```

### Basic Configuration

```yaml
apiVersion: cache.example.com/v1
kind: RedisCluster
metadata:
  name: my-redis
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

  # Redis version
  redisVersion: "7.2"
```

## Documentation

Comprehensive documentation is available in the [docs](./docs) directory:

### 📚 [Complete Documentation](./docs/README.md)

| Document | Description |
|----------|-------------|
| **[User Guide](./docs/USER_GUIDE.md)** | Installation, configuration, and basic usage |
| **[Architecture](./docs/ARCHITECTURE.md)** | How the autoscaler works internally |
| **[Configuration Reference](./docs/CONFIGURATION.md)** | Complete configuration options reference |
| **[Operations Guide](./docs/OPERATIONS.md)** | Monitoring, maintenance, and best practices |
| **[Troubleshooting](./docs/TROUBLESHOOTING.md)** | Common issues and solutions |
| **[Existing Cluster Support](./docs/EXISTING_CLUSTER_SUPPORT.md)** | Integrate with existing Redis deployments |

## How It Works

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

**Process:**

1. **Monitor**: Continuously query Prometheus for CPU and memory metrics
2. **Decide**: Trigger scale-up/down based on configured thresholds
3. **Scale-Up**: Activate hot standby by migrating slots, provision new standby
4. **Scale-Down**: Drain underutilized pod, it becomes the new standby
5. **Cooldown**: Wait before allowing next scaling operation

See [Architecture Overview](./docs/ARCHITECTURE.md) for detailed explanation.

## Examples

### Production High-Availability Cluster

```yaml
apiVersion: cache.example.com/v1
kind: RedisCluster
metadata:
  name: prod-redis
  namespace: production
spec:
  masters: 10
  minMasters: 5
  replicasPerMaster: 2
  autoScaleEnabled: true
  cpuThreshold: 80
  memoryThreshold: 85
  scaleCooldownSeconds: 300
  redisVersion: "7.2"
```

### Existing Cluster with Autoscaling Only

```yaml
apiVersion: cache.example.com/v1
kind: RedisCluster
metadata:
  name: existing-redis
  namespace: default
spec:
  existingCluster: true
  manageStatefulSet: false
  podSelector:
    app: redis
    cluster: my-existing-cluster
  serviceName: redis-headless
  masters: 5
  autoScaleEnabled: true
  cpuThreshold: 70
  memoryThreshold: 70
```

See [Configuration Reference](./docs/CONFIGURATION.md) for all options.

## Development

### Prerequisites

- Go 1.24+
- Docker 17.03+
- kubectl
- Access to Kubernetes cluster

### Building from Source

```bash
# Clone the repository
git clone https://github.com/yourorg/redis-operator.git
cd redis-operator

# Install dependencies
go mod download

# Build the operator
make build

# Run tests
make test

# Build and push Docker image
make docker-build docker-push IMG=<your-registry>/redis-operator:tag

# Deploy to cluster
make deploy IMG=<your-registry>/redis-operator:tag
```

### Project Structure

```
redis-operator/
├── api/v1/                      # API definitions
│   └── rediscluster_types.go    # RedisCluster CRD
├── internal/controller/         # Controller logic
│   ├── rediscluster_controller.go  # Main reconciler
│   ├── autoscaler.go            # Autoscaling decisions
│   ├── upscale.go               # Scale-up operations
│   └── downscale.go             # Scale-down operations
├── config/                      # Kubernetes manifests
│   ├── crd/                     # CRD definitions
│   ├── manager/                 # Operator deployment
│   └── rbac/                    # RBAC permissions
├── docs/                        # Documentation
└── cluster.yaml                 # Example cluster config
```

## Contributing

We welcome contributions! Please:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## Troubleshooting

If you encounter issues:

1. Check the [Troubleshooting Guide](./docs/TROUBLESHOOTING.md)
2. View operator logs: `kubectl logs -n redis-operator-system deployment/redis-operator-controller-manager`
3. Check cluster status: `kubectl describe rediscluster <name>`
4. Create a GitHub issue with diagnostic information

## Support

- **Documentation**: [docs](./docs)
- **Issues**: [GitHub Issues](https://github.com/yourorg/redis-operator/issues)
- **Discussions**: [GitHub Discussions](https://github.com/yourorg/redis-operator/discussions)

## License

Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

---

## Acknowledgments

Built with [Kubebuilder](https://book.kubebuilder.io/) and [controller-runtime](https://github.com/kubernetes-sigs/controller-runtime).
