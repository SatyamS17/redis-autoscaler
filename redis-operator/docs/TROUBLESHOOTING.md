# Troubleshooting Guide

Common issues and how to resolve them when using the Redis Cluster Autoscaler.

## Table of Contents

- [General Debugging](#general-debugging)
- [Installation Issues](#installation-issues)
- [Bootstrap Issues](#bootstrap-issues)
- [Scaling Issues](#scaling-issues)
- [Performance Issues](#performance-issues)
- [Pod Issues](#pod-issues)
- [Metrics and Monitoring Issues](#metrics-and-monitoring-issues)

## General Debugging

### Check Operator Status

```bash
# Is operator running?
kubectl get pods -n redis-operator-system

# Check operator logs
kubectl logs -n redis-operator-system deployment/redis-operator-controller-manager -f

# Recent errors
kubectl logs -n redis-operator-system deployment/redis-operator-controller-manager --tail=100 | grep ERROR
```

---

### Check Cluster Status

```bash
# Overall status
kubectl get rediscluster <name>

# Detailed status
kubectl describe rediscluster <name>

# Full YAML
kubectl get rediscluster <name> -o yaml

# Watch for changes
kubectl get rediscluster <name> -w
```

---

### Check Events

```bash
# Cluster events
kubectl describe rediscluster <name> | grep -A 20 Events

# Pod events
kubectl get events --sort-by='.lastTimestamp' | grep redis

# Namespace events
kubectl get events -n <namespace>
```

---

## Installation Issues

### Issue: CRD Installation Fails

**Symptoms:**
```
Error: unable to recognize "config/crd/bases/cache.example.com_redisclusters.yaml": no matches for kind "CustomResourceDefinition"
```

**Cause:** Kubernetes version too old or CRD API not available

**Solution:**
```bash
# Check Kubernetes version (need v1.16+)
kubectl version

# Ensure apiextensions.k8s.io/v1 is available
kubectl api-resources | grep customresourcedefinitions

# Try installing with older API version
# Edit CRD file and change apiVersion from v1 to v1beta1
```

---

### Issue: Operator Pod CrashLoopBackOff

**Symptoms:**
```bash
kubectl get pods -n redis-operator-system
# NAME                                         READY   STATUS             RESTARTS   AGE
# redis-operator-controller-manager-xxx-yyy    0/2     CrashLoopBackOff   5          3m
```

**Diagnosis:**
```bash
# Check logs
kubectl logs -n redis-operator-system deployment/redis-operator-controller-manager

# Common errors:
# - "Failed to get API Group": CRDs not installed
# - "Permission denied": RBAC issues
# - "Port already in use": Port conflict
```

**Solutions:**

**RBAC issues:**
```bash
# Verify operator has correct permissions
kubectl auth can-i list pods --as=system:serviceaccount:redis-operator-system:redis-operator-controller-manager

# Re-apply RBAC
kubectl apply -f config/rbac/
```

**CRD issues:**
```bash
# Reinstall CRDs
make install
```

---

### Issue: Prometheus Not Found

**Symptoms:**
```
ERROR: Failed to query Prometheus: dial tcp: lookup prometheus-operated.monitoring.svc on 10.96.0.10:53: no such host
```

**Solution:**
```bash
# Check if Prometheus is installed
kubectl get svc -A | grep prometheus

# Update prometheusURL in cluster spec
kubectl patch rediscluster <name> -p '{"spec":{"prometheusURL":"http://<actual-prometheus-svc>:9090"}}'
```

---

## Bootstrap Issues

### Issue: Bootstrap Job Stuck in Pending

**Symptoms:**
```bash
kubectl get jobs
# NAME                    COMPLETIONS   DURATION   AGE
# redis-cluster-bootstrap 0/1           5m         5m
```

**Diagnosis:**
```bash
# Check pod status
kubectl get pods | grep bootstrap

# Check events
kubectl describe job redis-cluster-bootstrap
```

**Common Causes:**

**Insufficient resources:**
```bash
# Check node resources
kubectl describe nodes | grep -A 5 "Allocated resources"

# Solution: Add more nodes or reduce resource requests
```

**Image pull errors:**
```bash
kubectl describe pod <bootstrap-pod> | grep -A 5 "Events"

# Solution: Fix image name or registry credentials
kubectl patch rediscluster <name> -p '{"spec":{"redisVersion":"7.2"}}'
```

---

### Issue: Bootstrap Job Fails

**Symptoms:**
```bash
kubectl get jobs
# NAME                    COMPLETIONS   DURATION   AGE
# redis-cluster-bootstrap 0/1           10m        10m

kubectl get pods | grep bootstrap
# redis-cluster-bootstrap-xxx   0/1     Error    0          10m
```

**Diagnosis:**
```bash
# Check job logs
kubectl logs job/redis-cluster-bootstrap

# Common errors:
# - "ERR This instance has cluster support disabled": Wrong Redis image
# - "Waiting for the cluster to join": Pod networking issues
# - "Node is not empty": Pods have stale data
```

**Solutions:**

**Cluster support disabled:**
```yaml
# Ensure redis.conf has:
cluster-enabled yes
```

**Pod networking:**
```bash
# Test pod-to-pod connectivity
kubectl exec -it redis-cluster-0 -- ping redis-cluster-1.redis-cluster-headless
```

**Stale data:**
```bash
# Delete all pods to start fresh
kubectl delete pods -l cluster=redis-cluster

# Or delete and recreate the cluster
kubectl delete rediscluster redis-cluster
kubectl apply -f cluster.yaml
```

---

### Issue: Cluster Not Initializing

**Symptoms:**
```bash
kubectl get rediscluster <name> -o jsonpath='{.status.initialized}'
# false  (stays false for > 5 minutes)
```

**Diagnosis:**
```bash
# Check pod status
kubectl get pods -l cluster=<name>

# Check if all pods are ready
kubectl get pods -l cluster=<name> -o jsonpath='{.items[*].status.conditions[?(@.type=="Ready")].status}'

# Check bootstrap job
kubectl get jobs | grep bootstrap
```

**Solution:**
```bash
# If pods are not all ready, wait for them
# If pods are ready but bootstrap job hasn't run:

# Delete and recreate bootstrap job
kubectl delete job <name>-bootstrap

# Operator will recreate it on next reconcile
```

---

## Scaling Issues

### Issue: Scale-Up Not Triggering

**Symptoms:**
- High CPU/memory usage
- `autoScaleEnabled: true`
- No scale-up happening

**Diagnosis:**
```bash
# Check current metrics
kubectl exec -it <pod> -- redis-cli INFO stats | grep cpu
kubectl exec -it <pod> -- redis-cli INFO memory | grep used_memory

# Check operator logs for decision
kubectl logs -n redis-operator-system deployment/redis-operator-controller-manager | grep -i "scale"

# Possible log messages:
# - "Cluster not healthy for scaling"
# - "Cooldown period not elapsed"
# - "Standby pod not ready"
# - "No pod exceeds thresholds"
```

**Common Causes:**

**1. Cooldown period active:**
```bash
# Check last scale time
kubectl get rediscluster <name> -o jsonpath='{.status.lastScaleTime}'

# If within cooldownSeconds, wait
# Or force by resetting:
kubectl patch rediscluster <name> --type=json -p='[{"op":"remove","path":"/status/lastScaleTime"}]'
```

**2. Standby pod not ready:**
```bash
# Check standby
kubectl get rediscluster <name> -o jsonpath='{.status.standbyPod}'

# Check if pod exists and is running
kubectl get pod <standby-pod>

# If not, manually set or wait for provisioning
```

**3. Thresholds not exceeded:**
```bash
# Verify Prometheus has metrics
curl 'http://prometheus:9090/api/v1/query?query=process_cpu_seconds_total{pod=~"redis-.*"}'

# Lower thresholds temporarily
kubectl patch rediscluster <name> -p '{"spec":{"cpuThreshold":50}}'
```

---

### Issue: Scale-Up Job Fails

**Symptoms:**
```bash
kubectl get jobs | grep reshard
# redis-cluster-reshard   0/1    10m    10m

kubectl get pods | grep reshard
# redis-cluster-reshard-xxx   0/1   Error   0   10m
```

**Diagnosis:**
```bash
# Check job logs
kubectl logs job/redis-cluster-reshard

# Common errors:
# - "ERR Invalid node address": DNS issues
# - "Timeout waiting for cluster join": Slow network
# - "ERR target node is not empty": Standby has slots
```

**Solutions:**

**DNS issues:**
```bash
# Test DNS resolution from pod
kubectl exec -it redis-cluster-0 -- nslookup redis-cluster-6.redis-cluster-headless

# If fails, check service and DNS
kubectl get svc redis-cluster-headless
```

**Standby has slots:**
```bash
# Manually check standby slots
kubectl exec -it <standby-pod> -- redis-cli cluster nodes | grep <standby-ip>

# If standby has slots, something went wrong
# Reset by deleting cluster and starting over
```

**Timeout:**
```bash
# Increase timeout
kubectl patch rediscluster <name> -p '{"spec":{"reshardTimeoutSeconds":1200}}'
```

---

### Issue: Scale-Down Not Triggering

**Symptoms:**
- Low CPU/memory usage on all pods
- No scale-down happening

**Diagnosis:**
```bash
# Check metrics
# ALL pods must be below both thresholds

# Check operator logs
kubectl logs -n redis-operator-system deployment/redis-operator-controller-manager | grep -i "scale-down"

# Possible messages:
# - "Cannot scale below minMasters"
# - "Cooldown period not elapsed"
# - "Not all pods are underutilized"
```

**Solutions:**

**At minimum masters:**
```bash
# Check current vs minimum
kubectl get rediscluster <name> -o jsonpath='{.spec.masters} {.spec.minMasters}'

# Cannot scale down if already at minimum
# Decrease minMasters if appropriate (min 3)
kubectl patch rediscluster <name> -p '{"spec":{"minMasters":3}}'
```

**Not all pods underutilized:**
```bash
# Check if ANY pod is above thresholds
# Even one pod above prevents scale-down

# This is intentional to prevent premature scale-down
```

---

## Performance Issues

### Issue: High Latency

**Symptoms:**
- Application sees slow Redis response times

**Diagnosis:**
```bash
# Check Redis slowlog
kubectl exec -it <pod> -- redis-cli SLOWLOG GET 10

# Check CPU usage
kubectl top pods -l cluster=<name>

# Check network latency
kubectl exec -it <pod> -- redis-cli --latency

# Check if resharding is happening
kubectl get rediscluster <name> -o jsonpath='{.status.isResharding}'
```

**Solutions:**

**During resharding:**
```
# Resharding causes temporary latency spikes
# Wait for it to complete or increase timeout
kubectl patch rediscluster <name> -p '{"spec":{"reshardTimeoutSeconds":900}}'
```

**Under high load:**
```bash
# Scale up manually
kubectl patch rediscluster <name> -p '{"spec":{"masters":5}}'

# Or lower autoscale threshold
kubectl patch rediscluster <name> -p '{"spec":{"cpuThreshold":60}}'
```

**Network issues:**
```bash
# Check pod network policies
kubectl get networkpolicies

# Test pod-to-pod latency
kubectl exec -it redis-cluster-0 -- ping redis-cluster-2.redis-cluster-headless
```

---

### Issue: Memory Pressure

**Symptoms:**
- Pods hitting memory limits
- Redis evicting keys unexpectedly
- OOMKilled pods

**Diagnosis:**
```bash
# Check memory usage
kubectl exec -it <pod> -- redis-cli INFO memory

# Check eviction stats
kubectl exec -it <pod> -- redis-cli INFO stats | grep evicted

# Check OOM events
kubectl get events | grep OOMKilled
```

**Solutions:**

**Increase memory threshold:**
```bash
# Trigger scale-up earlier
kubectl patch rediscluster <name> -p '{"spec":{"memoryThreshold":65}}'
```

**Increase pod memory limits:**
```yaml
# Edit StatefulSet (or operator code)
resources:
  limits:
    memory: 4Gi  # Increase from 2Gi
```

**Configure Redis max memory:**
```
# redis.conf
maxmemory 3gb  # Leave headroom for OS
maxmemory-policy allkeys-lru
```

---

### Issue: Frequent Scaling Oscillations

**Symptoms:**
- Cluster scales up and down repeatedly
- Wasteful resource usage

**Diagnosis:**
```bash
# Watch cluster size
kubectl get rediscluster <name> -w

# Check logs for rapid scaling
kubectl logs -n redis-operator-system deployment/redis-operator-controller-manager | grep -E "scale-up|scale-down"
```

**Solutions:**

**Increase cooldown period:**
```bash
kubectl patch rediscluster <name> -p '{"spec":{"scaleCooldownSeconds":300}}'
```

**Widen threshold gap:**
```bash
# Current: cpuThreshold: 70, cpuThresholdLow: 20 (gap: 50)
# Increase gap:
kubectl patch rediscluster <name> -p '{"spec":{"cpuThreshold":80,"cpuThresholdLow":25}}'
```

**Add manual capacity:**
```bash
# Increase base capacity to absorb spikes
kubectl patch rediscluster <name> -p '{"spec":{"masters":5}}'
```

---

## Pod Issues

### Issue: Pods Not Starting

**Symptoms:**
```bash
kubectl get pods
# NAME              READY   STATUS             RESTARTS   AGE
# redis-cluster-0   0/1     ImagePullBackOff   0          2m
```

**Common Causes:**

**Image pull errors:**
```bash
kubectl describe pod redis-cluster-0 | grep -A 5 "Events"

# Solutions:
# - Fix image name
kubectl patch rediscluster <name> -p '{"spec":{"redisVersion":"7.2"}}'

# - Add image pull secret
kubectl create secret docker-registry regcred --docker-server=<server> --docker-username=<user> --docker-password=<pass>
```

**Resource constraints:**
```bash
kubectl describe pod redis-cluster-0 | grep -A 5 "Events"
# "Insufficient cpu" or "Insufficient memory"

# Solution: Add more nodes or reduce requests
```

**Storage issues:**
```bash
# If using PVCs
kubectl get pvc

# Check if PVC is bound
# If not, check StorageClass exists
kubectl get storageclass
```

---

### Issue: Pods Restarting Frequently

**Symptoms:**
```bash
kubectl get pods
# NAME              READY   STATUS    RESTARTS   AGE
# redis-cluster-0   1/1     Running   15         10m
```

**Diagnosis:**
```bash
# Check logs
kubectl logs redis-cluster-0 --previous

# Common errors:
# - "OOMKilled": Memory limit too low
# - "Error binding to port": Port already in use
# - "Fatal error": Configuration error
```

**Solutions:**

**OOMKilled:**
```bash
# Increase memory limit
# Edit StatefulSet or operator code
```

**Configuration error:**
```bash
# Check ConfigMap
kubectl get configmap <name>-config -o yaml

# Verify redis.conf syntax
```

---

### Issue: Pod Not Joining Cluster

**Symptoms:**
- Pod is running but not in `CLUSTER NODES`

**Diagnosis:**
```bash
# Check cluster nodes
kubectl exec -it redis-cluster-0 -- redis-cli cluster nodes

# Check if pod is reachable
kubectl exec -it redis-cluster-0 -- redis-cli -h <pod-ip> PING
```

**Solution:**
```bash
# Manually add node (emergency only)
kubectl exec -it redis-cluster-0 -- redis-cli --cluster add-node <new-pod-ip>:6379 <existing-pod-ip>:6379

# Better: Delete pod and let operator recreate
kubectl delete pod <pod-name>
```

---

## Metrics and Monitoring Issues

### Issue: Prometheus Not Scraping Redis

**Symptoms:**
- No metrics in Prometheus
- Autoscaling not working

**Diagnosis:**
```bash
# Check ServiceMonitor
kubectl get servicemonitor <name>

# Check Prometheus targets
# Access Prometheus UI → Status → Targets
# Look for redis-cluster endpoints

# Check pod annotations
kubectl get pods redis-cluster-0 -o yaml | grep -A 5 annotations
```

**Solutions:**

**ServiceMonitor not found:**
```bash
# Recreate cluster (operator should create it)
# Or manually create ServiceMonitor
```

**Prometheus not discovering ServiceMonitor:**
```bash
# Check Prometheus ServiceMonitor selector
kubectl get prometheus -o yaml | grep serviceMonitorSelector

# Ensure ServiceMonitor labels match
```

**Metrics endpoint not exposed:**
```bash
# Check if Redis exporter is running
kubectl exec -it redis-cluster-0 -- curl localhost:9121/metrics

# If not, add Redis exporter sidecar to pods
```

---

### Issue: Incorrect Metrics

**Symptoms:**
- Autoscaling triggers incorrectly
- Metrics don't match actual usage

**Diagnosis:**
```bash
# Query Prometheus directly
curl 'http://prometheus:9090/api/v1/query?query=process_cpu_seconds_total{pod="redis-cluster-0"}'

# Compare with actual
kubectl exec -it redis-cluster-0 -- top
```

**Solution:**
```bash
# Verify metrics exporter is correct version
# Check for stale metrics (old time series)
# Restart Prometheus if needed
```

---

## Getting Help

If none of these solutions work:

1. **Gather diagnostic information:**
   ```bash
   # Cluster status
   kubectl get rediscluster <name> -o yaml > cluster-status.yaml

   # Operator logs
   kubectl logs -n redis-operator-system deployment/redis-operator-controller-manager > operator-logs.txt

   # Pod status
   kubectl get pods -o yaml > pods.yaml

   # Events
   kubectl get events > events.txt
   ```

2. **Create GitHub issue** with:
   - Description of problem
   - Expected vs actual behavior
   - Diagnostic files above
   - Kubernetes version
   - Operator version

3. **Check documentation:**
   - [User Guide](./USER_GUIDE.md)
   - [Configuration Reference](./CONFIGURATION.md)
   - [Architecture](./ARCHITECTURE.md)
   - [Operations Guide](./OPERATIONS.md)
