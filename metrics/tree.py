#!/usr/bin/env python3
"""
Redis Cluster Scaling Decision Engine - Enhanced with Read/Write Load Analysis

Queries Prometheus for per-pod CPU, memory, and read/write operation metrics.
Follows the decision tree logic to make scaling decisions.
Runs continuously and makes periodic scaling decisions.
"""

import requests
import json
import time
import sys
from datetime import datetime
from typing import Dict, List, Optional

# Configuration
PROMETHEUS_URL = "http://localhost:9090"
CHECK_INTERVAL_SECONDS = 10
DECISION_HISTORY_FILE = "scaling_decisions_history.json"

# Thresholds (from decision tree)
CPU_THRESHOLD = 12  # CPU > 12% for 1+ min
MEMORY_THRESHOLD_PERCENT = 80  # Memory > 80% for 1+ min
CPU_SCALE_THRESHOLD = 5  # CPU > 5% & Memory < 80% for 1+ min
MEMORY_SCALE_THRESHOLD_PERCENT = 80  # Memory > 80% & CPU < 5% for 1+ min
READ_HEAVY_THRESHOLD = 0.90  # read/(read+write) > 70%
WRITE_HEAVY_THRESHOLD = 0.40  # write/(read+write) > 70%
MIN_OPS_THRESHOLD = 100

class PodMetrics:
    def __init__(self, pod_name: str):
        self.pod_name = pod_name
        self.cpu_percent = 0.0
        self.memory_used_mb = 0.0
        self.read_ops_per_sec = 0.0
        self.write_ops_per_sec = 0.0
        self.commands_per_sec = 0.0
        self.connected_clients = 0
        self.network_input_bytes_per_sec = 0.0
        self.network_output_bytes_per_sec = 0.0
        
        # Calculated properties
        self.total_ops = 0.0
        self.read_ratio = 0.0
        self.write_ratio = 0.0
        self.workload_type = "UNKNOWN"  # READ-HEAVY, WRITE-HEAVY, BALANCED
        
        # Status flags
        self.is_high_load = False
        self.is_cpu_constrained = False
        self.is_memory_constrained = False

    def calculate_derived_metrics(self):
        """Calculate workload ratios and type"""
        self.total_ops = self.read_ops_per_sec + self.write_ops_per_sec
        
        if self.total_ops > MIN_OPS_THRESHOLD:
            self.read_ratio = self.read_ops_per_sec / self.total_ops
            self.write_ratio = self.write_ops_per_sec / self.total_ops
            
            # Determine workload type
            if self.read_ratio >= READ_HEAVY_THRESHOLD:
                self.workload_type = "READ-HEAVY"
            elif self.write_ratio >= WRITE_HEAVY_THRESHOLD:
                self.workload_type = "WRITE-HEAVY"
            else:
                self.workload_type = "BALANCED"
        else:
            # Low traffic - don't classify
            self.read_ratio = 0.0
            self.write_ratio = 0.0
            self.workload_type = "IDLE"  # or "LOW-TRAFFIC"
        
        # Set constraint flags
        self.is_cpu_constrained = self.cpu_percent > CPU_THRESHOLD
        self.is_memory_constrained = self.memory_used_mb > 800
        self.is_high_load = self.is_cpu_constrained or self.is_memory_constrained
    def __repr__(self):
        return f"PodMetrics({self.pod_name}, workload={self.workload_type})"

class ScalingDecision:
    def __init__(self, action: str, reason: str, target_pods: List[str], priority: int, details: Optional[Dict] = None):
        self.action = action  # "scale_horizontal_out", "scale_vertical_up", "scale_down", "no_action"
        self.reason = reason
        self.target_pods = target_pods
        self.priority = priority  # 1=critical, 2=important, 3=preventive
        self.details = details or {}
        self.timestamp = datetime.now()

    def __repr__(self):
        return f"ScalingDecision(action={self.action}, priority={self.priority}, targets={len(self.target_pods)} pods)"
    
    def to_dict(self):
        return {
            "action": self.action,
            "reason": self.reason,
            "target_pods": self.target_pods,
            "priority": self.priority,
            "details": self.details,
            "timestamp": self.timestamp.isoformat()
        }

def query_prometheus(query: str) -> List[Dict]:
    """Query Prometheus and return results"""
    try:
        response = requests.get(
            f"{PROMETHEUS_URL}/api/v1/query",
            params={"query": query},
            timeout=10
        )
        response.raise_for_status()
        data = response.json()
        
        if data["status"] == "success":
            return data["data"]["result"]
        else:
            print(f"Prometheus query failed: {data}")
            return []
    except Exception as e:
        print(f"Error querying Prometheus: {e}")
        return []

def get_pod_metrics() -> Dict[str, PodMetrics]:
    """Fetch all relevant metrics for each Redis pod"""
    pods = {}
    
    # Define queries - try recording rules first
    queries = {
        "cpu_percent": "redis:cpu_percent:by_pod",
        "memory_used_mb": "redis:memory_used_mb:by_pod",
        "read_ops_per_sec": "redis:read_ops_per_sec:by_pod",
        "write_ops_per_sec": "redis:write_ops_per_sec:by_pod",
        "commands_per_sec": "redis:commands_per_sec:by_pod",
        "connected_clients": "redis:connected_clients:by_pod",
        "network_input_bytes_per_sec": "redis:network_input_bytes_per_sec:by_pod",
        "network_output_bytes_per_sec": "redis:network_output_bytes_per_sec:by_pod",
    }
    
    # Fetch metrics
    for metric_name, query in queries.items():
        results = query_prometheus(query)
        
        if not results and metric_name in ["cpu_percent", "memory_used_mb"]:
            # Fallback to raw queries for basic metrics
            fallback_queries = {
                "cpu_percent": 'sum(rate(container_cpu_usage_seconds_total{pod=~"redis-redis-cluster-.*",container="redis-redis-cluster"}[1m])) by (pod) * 100',
                "memory_used_mb": 'sum(container_memory_working_set_bytes{pod=~"redis-redis-cluster-.*",container="redis-redis-cluster"}) by (pod) / 1024 / 1024',
            }
            if metric_name in fallback_queries:
                results = query_prometheus(fallback_queries[metric_name])
        
        for result in results:
            pod_name = result["metric"].get("pod", "unknown")
            value = float(result["value"][1])
            
            # Initialize pod if not exists
            if pod_name not in pods:
                pods[pod_name] = PodMetrics(pod_name)
            
            # Set metric value
            setattr(pods[pod_name], metric_name, value)
    
    # Calculate derived metrics for all pods
    for pod in pods.values():
        pod.calculate_derived_metrics()
    
    return pods

def decision_tree(pod_metrics: Dict[str, PodMetrics]) -> List[ScalingDecision]:
    """
    Enhanced decision tree following the provided flowchart
    
    Decision Flow (per pod):
    1. Memory > 80% for 1+ min? → YES: Can we scale up? → YES: Scale memory vertically | NO: Add shard and redistribute slots
    2. CPU > 12% for 1+ min? → YES: Can we scale up? → YES: Scale CPU vertically | NO: depends on workload
    3. CPU > 5% & Memory < 80% for 1+ min? → Check workload type
        - READ-HEAVY (reads > 70%): Add read replicas/follower
        - WRITE-HEAVY (writes > 40%): Add shard and redistribute slots
    4. Safe to scale down? Check if underutilized
    """
    decisions = []
    
    # Analyze each pod
    for pod_name, metrics in pod_metrics.items():
        
        # CRITICAL PATH 1: Memory > 80%
        if metrics.is_memory_constrained:
            decisions.append(ScalingDecision(
                action="scale_vertical_up",
                reason=f"Memory constraint: {metrics.memory_used_mb:.0f} MB (>{MEMORY_THRESHOLD_PERCENT}%)",
                target_pods=[pod_name],
                priority=1,
                details={
                    "memory_mb": metrics.memory_used_mb,
                    "cpu_percent": metrics.cpu_percent,
                    "scaling_type": "memory"
                }
            ))
            continue  # Skip other checks for this pod
        
        # CRITICAL PATH 2: CPU > 12%
        if metrics.is_cpu_constrained:
            # Check if we can scale vertically or need horizontal
            # Assumption: if memory is low, we can scale CPU vertically
            # Need horizontal scaling based on workload
            if metrics.workload_type == "READ-HEAVY":
                decisions.append(ScalingDecision(
                    action="scale_horizontal_add_replica",
                    reason=f"CPU {metrics.cpu_percent:.1f}% + READ-HEAVY workload ({metrics.read_ratio*100:.0f}% reads)",
                    target_pods=[pod_name],
                    priority=1,
                    details={
                        "cpu_percent": metrics.cpu_percent,
                        "workload": metrics.workload_type,
                        "read_ratio": metrics.read_ratio,
                        "read_ops": metrics.read_ops_per_sec
                    }
                ))
            else:  # WRITE-HEAVY or BALANCED
                decisions.append(ScalingDecision(
                    action="scale_horizontal_add_shard",
                    reason=f"CPU {metrics.cpu_percent:.1f}% + {metrics.workload_type} workload ({metrics.write_ratio*100:.0f}% writes)",
                    target_pods=[pod_name],
                    priority=1,
                    details={
                        "cpu_percent": metrics.cpu_percent,
                        "workload": metrics.workload_type,
                        "write_ratio": metrics.write_ratio,
                        "write_ops": metrics.write_ops_per_sec
                    }
                ))
            continue
        
        # IMPORTANT PATH: CPU > 5% but < 12%
        if metrics.cpu_percent > CPU_SCALE_THRESHOLD and metrics.cpu_percent <= CPU_THRESHOLD:
            if metrics.memory_used_mb < 600:  # Memory is OK
                # Analyze workload type
                if metrics.workload_type == "READ-HEAVY":
                    decisions.append(ScalingDecision(
                        action="scale_horizontal_add_replica",
                        reason=f"Elevated CPU {metrics.cpu_percent:.1f}% + READ-HEAVY ({metrics.read_ratio*100:.0f}% reads)",
                        target_pods=[pod_name],
                        priority=2,
                        details={
                            "cpu_percent": metrics.cpu_percent,
                            "workload": metrics.workload_type,
                            "read_ops": metrics.read_ops_per_sec,
                            "commands_per_sec": metrics.commands_per_sec
                        }
                    ))
                elif metrics.workload_type == "WRITE-HEAVY":
                    decisions.append(ScalingDecision(
                        action="scale_horizontal_add_shard",
                        reason=f"Elevated CPU {metrics.cpu_percent:.1f}% + WRITE-HEAVY ({metrics.write_ratio*100:.0f}% writes)",
                        target_pods=[pod_name],
                        priority=2,
                        details={
                            "cpu_percent": metrics.cpu_percent,
                            "workload": metrics.workload_type,
                            "write_ops": metrics.write_ops_per_sec,
                            "commands_per_sec": metrics.commands_per_sec
                        }
                    ))
        
        # SCALE DOWN PATH: Check if pod is significantly underutilized
        if metrics.cpu_percent < 2 and metrics.memory_used_mb < 200 and metrics.total_ops < 10:
            # Check if this is a read-heavy workload with replica
            if metrics.workload_type == "READ-HEAVY":
                decisions.append(ScalingDecision(
                    action="scale_down_remove_replica",
                    reason=f"Underutilized replica: CPU {metrics.cpu_percent:.1f}%, {metrics.total_ops:.0f} ops/sec",
                    target_pods=[pod_name],
                    priority=3,
                    details={
                        "cpu_percent": metrics.cpu_percent,
                        "memory_mb": metrics.memory_used_mb,
                        "total_ops": metrics.total_ops
                    }
                ))
            elif metrics.workload_type == "WRITE-HEAVY" or metrics.workload_type == "BALANCED":
                decisions.append(ScalingDecision(
                    action="scale_down_remove_shard",
                    reason=f"Underutilized: CPU {metrics.cpu_percent:.1f}%, {metrics.total_ops:.0f} ops/sec",
                    target_pods=[pod_name],
                    priority=3,
                    details={
                        "cpu_percent": metrics.cpu_percent,
                        "memory_mb": metrics.memory_used_mb,
                        "total_ops": metrics.total_ops
                    }
                ))
    
    # Cluster-wide analysis
    total_pods = len(pod_metrics)
    if total_pods > 0:
        high_load_pods = sum(1 for m in pod_metrics.values() if m.is_high_load)
        cpu_constrained_pods = sum(1 for m in pod_metrics.values() if m.is_cpu_constrained)
        avg_cpu = sum(m.cpu_percent for m in pod_metrics.values()) / total_pods
        avg_memory = sum(m.memory_used_mb for m in pod_metrics.values()) / total_pods
        total_read_ops = sum(m.read_ops_per_sec for m in pod_metrics.values())
        total_write_ops = sum(m.write_ops_per_sec for m in pod_metrics.values())
        
        # Cluster-wide workload type
        total_ops = total_read_ops + total_write_ops
        cluster_workload = "UNKNOWN"
        if total_ops > 0:
            cluster_read_ratio = total_read_ops / total_ops
            if cluster_read_ratio >= READ_HEAVY_THRESHOLD:
                cluster_workload = "READ-HEAVY"
            elif (total_write_ops / total_ops) >= WRITE_HEAVY_THRESHOLD:
                cluster_workload = "WRITE-HEAVY"
            else:
                cluster_workload = "BALANCED"
        
        # If majority of pods are constrained
        if cpu_constrained_pods >= total_pods * 0.5:
            if cluster_workload == "READ-HEAVY":
                decisions.append(ScalingDecision(
                    action="scale_horizontal_add_replica",
                    reason=f"Cluster-wide: {cpu_constrained_pods}/{total_pods} pods CPU constrained, {cluster_workload} workload",
                    target_pods=list(pod_metrics.keys()),
                    priority=1,
                    details={
                        "avg_cpu": avg_cpu,
                        "cluster_workload": cluster_workload,
                        "total_read_ops": total_read_ops,
                        "total_write_ops": total_write_ops
                    }
                ))
            else:
                decisions.append(ScalingDecision(
                    action="scale_horizontal_add_shard",
                    reason=f"Cluster-wide: {cpu_constrained_pods}/{total_pods} pods CPU constrained, {cluster_workload} workload",
                    target_pods=list(pod_metrics.keys()),
                    priority=1,
                    details={
                        "avg_cpu": avg_cpu,
                        "cluster_workload": cluster_workload,
                        "total_read_ops": total_read_ops,
                        "total_write_ops": total_write_ops
                    }
                ))
    
    # Sort by priority (critical first)
    decisions.sort(key=lambda d: d.priority)
    
    return decisions

def print_pod_summary(pod_metrics: Dict[str, PodMetrics], compact: bool = False):
    """Print a summary table of pod metrics"""
    if compact:
        total = len(pod_metrics)
        avg_cpu = sum(m.cpu_percent for m in pod_metrics.values()) / total if total > 0 else 0
        max_cpu = max((m.cpu_percent for m in pod_metrics.values()), default=0)
        high_load = sum(1 for m in pod_metrics.values() if m.is_high_load)
        total_ops = sum(m.total_ops for m in pod_metrics.values())
        print(f"Pods: {total} | Avg CPU: {avg_cpu:.1f}% | Max CPU: {max_cpu:.1f}% | High Load: {high_load}/{total} | Total Ops: {total_ops:.0f}/sec")
    else:
        print("\n" + "="*100)
        print("REDIS POD METRICS SUMMARY")
        print("="*100)
        print(f"{'Pod Name':<30} {'CPU%':<8} {'Mem(MB)':<10} {'Workload':<12} {'Read/s':<10} {'Write/s':<10} {'Status':<30}")
        print("-"*100)
        
        for pod_name, metrics in sorted(pod_metrics.items()):
            status = []
            if metrics.is_high_load:
                status.append("HIGH_LOAD")
            if metrics.is_cpu_constrained:
                status.append("CPU")
            if metrics.is_memory_constrained:
                status.append("MEM")
            
            status_str = " ".join(status) if status else "✅ OK"
            
            print(f"{pod_name:<30} {metrics.cpu_percent:<8.2f} {metrics.memory_used_mb:<10.0f} "
                  f"{metrics.workload_type:<12} {metrics.read_ops_per_sec:<10.0f} "
                  f"{metrics.write_ops_per_sec:<10.0f} {status_str:<30}")
        
        print("="*100 + "\n")

def print_decisions(decisions: List[ScalingDecision], compact: bool = False):
    """Print scaling decisions"""
    if not decisions:
        if not compact:
            print("No scaling actions recommended - cluster is healthy\n")
        return
    
    if compact:
        critical = sum(1 for d in decisions if d.priority == 1)
        important = sum(1 for d in decisions if d.priority == 2)
        preventive = sum(1 for d in decisions if d.priority == 3)
        print(f"Decisions: {len(decisions)} total ({critical} critical, {important} important, {preventive} preventive)")
    else:
        print("\n" + "="*100)
        print("SCALING RECOMMENDATIONS")
        print("="*100)
        
        priority_icons = {1: "CRITICAL", 2: "IMPORTANT", 3: "PREVENTIVE"}
        
        for decision in decisions:
            print(f"\n{priority_icons.get(decision.priority, 'UNKNOWN')}")
            print(f"  Action:  {decision.action.upper()}")
            print(f"  Reason:  {decision.reason}")
            print(f"  Targets: {', '.join(decision.target_pods[:3])}{'...' if len(decision.target_pods) > 3 else ''} ({len(decision.target_pods)} pods)")
            if decision.details:
                print(f"  Details: {json.dumps(decision.details, indent=11)}")
            print(f"  Time:    {decision.timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
        
        print("\n" + "="*100 + "\n")

def save_decision_to_history(pod_metrics: Dict[str, PodMetrics], decisions: List[ScalingDecision]):
    """Append current decision to history file"""
    entry = {
        "timestamp": datetime.now().isoformat(),
        "pod_metrics": {
            pod_name: {
                "cpu_percent": metrics.cpu_percent,
                "memory_used_mb": metrics.memory_used_mb,
                "read_ops_per_sec": metrics.read_ops_per_sec,
                "write_ops_per_sec": metrics.write_ops_per_sec,
                "workload_type": metrics.workload_type,
                "is_high_load": metrics.is_high_load,
                "is_cpu_constrained": metrics.is_cpu_constrained,
                "is_memory_constrained": metrics.is_memory_constrained,
            }
            for pod_name, metrics in pod_metrics.items()
        },
        "decisions": [d.to_dict() for d in decisions]
    }
    
    # Load existing history
    try:
        with open(DECISION_HISTORY_FILE, 'r') as f:
            history = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        history = []
    
    # Append new entry
    history.append(entry)
    
    # Keep only last 100 entries
    history = history[-100:]
    
    # Save
    with open(DECISION_HISTORY_FILE, 'w') as f:
        json.dump(history, f, indent=2)

def main():
    """Main execution function - continuous monitoring"""
    print("="*100)
    print("REDIS AUTOSCALER - CONTINUOUS MONITORING WITH READ/WRITE ANALYSIS")
    print("="*100)
    print(f"Prometheus: {PROMETHEUS_URL}")
    print(f"Check interval: {CHECK_INTERVAL_SECONDS} seconds")
    print(f"History file: {DECISION_HISTORY_FILE}")
    print(f"Thresholds: CPU={CPU_THRESHOLD}%, Memory={MEMORY_THRESHOLD_PERCENT}%, Read-Heavy={READ_HEAVY_THRESHOLD*100:.0f}%")
    print("="*100 + "\n")
    
    iteration = 0
    
    while True:
        iteration += 1
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        print(f"\n{'='*199}")
        print(f"Iteration #{iteration} - {timestamp}")
        print(f"{'='*100}")
        
        try:
            # Fetch metrics
            print("Fetching metrics from Prometheus...")
            pod_metrics = get_pod_metrics()
            
            if not pod_metrics:
                print("No metrics found. Retrying in next iteration...")
                time.sleep(CHECK_INTERVAL_SECONDS)
                continue
            
            # Print summary
            print_pod_summary(pod_metrics, compact=False)
            
            # Run decision tree
            decisions = decision_tree(pod_metrics)
            
            # Print decisions
            print_decisions(decisions, compact=True)
            
            # Save to history
            save_decision_to_history(pod_metrics, decisions)
            
            # If there are critical decisions, print full details
            if any(d.priority == 1 for d in decisions):
                print("\nCRITICAL DECISIONS DETECTED - Full details:")
                print_decisions(decisions, compact=False)
            
            # Next check countdown
            print(f"\nNext check in {CHECK_INTERVAL_SECONDS} seconds...")
            
            # Sleep
            time.sleep(CHECK_INTERVAL_SECONDS)
            
        except KeyboardInterrupt:
            raise
        except Exception as e:
            print(f"\nError in iteration: {e}")
            import traceback
            traceback.print_exc()
            print(f"\nRetrying in {CHECK_INTERVAL_SECONDS} seconds...")
            time.sleep(CHECK_INTERVAL_SECONDS)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nStopped by user")
        print(f"Decision history saved to {DECISION_HISTORY_FILE}\n")
    except Exception as e:
        print(f"\nError: {e}\n")
        import traceback
        traceback.print_exc()



# MEMTIER DOES NOT SUPPORT FIXED LOAD (ops/sec) TESTING (RATE-LIMITING NOT WORKING?) and READONLY DOESNT WORK
    # REDIS_BENCHMARK but you have to restart the test after scale up and scale down since not cluster aware and will stop as soon as resharding happens

# FOR HOT AND COLDSPOT DETECTION
    # PROMOTEHTUS DOES NOT SUPPORT PER_KEYSLOT MONITORING (ALSO NOT ADVISED)
        # COULD MANUALY PROCESS THE STREAM OF REQUEUSTS BUT WOULD SLOW DOWN LATENCY
        # maybe a limiting factor for the first iteration that we push out?

# just add amd remove REPLICA (very easy to add since I already have code doing this)

# PLANS: QUICKLY LOOK AT VALKEY TO SEE IF ALL THE ISSUES WITH REDIS CAN BE ADRESSES WITH THAT

# NOTES:
# FAULT TOLERCANE: use sdk to save state elsewhere 
    # restarting 

# YCSB for redis? <--------------- !!!

# correct set size

# -------------------------------------------------------------




# YCSB should work since it uses something called Jedis to handel MOVED/ASK commands which can keep track of key slot changes during resharding
    # RUNNING INTO NOAUTH ISSUE not sure why (despite setting auth to false)




# New cluster cant pull bitnami image? is there some issue with the config? it works with the old cluster
    # helm install redis oci://registry-1.docker.io/bitnamicharts/redis-cluster



# Started shifting over to using operator SDK and go but still in progress 
    # large learning curve and not as simple as script
        # should get faster after getting more familiar


# helm repo add bitnami https://charts.bitnami.com/bitnami
# helm repo update

# helm install redis bitnami/redis



# expose kubernetes netowrk using node prot --> tells aws is okay for outside traffic

# show all the commands in notion