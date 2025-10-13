#!/usr/bin/env python3
"""
Redis Cluster Scaling Decision Engine - Continuous Monitoring Mode

Queries Prometheus for per-pod CPU and memory metrics only.
Works without redis-exporter, using only container metrics.
Runs continuously and makes periodic scaling decisions.
"""

import requests
import json
import time
import sys
from datetime import datetime
from typing import Dict, List

# Configuration
PROMETHEUS_URL = "http://localhost:9090"
CHECK_INTERVAL_SECONDS = 10
DECISION_HISTORY_FILE = "scaling_decisions_history.json"

class PodMetrics:
    def __init__(self, pod_name: str):
        self.pod_name = pod_name
        self.cpu_percent = 0.0
        self.memory_used_mb = 0.0
        self.is_high_load = False
        self.is_cpu_constrained = False
        self.is_memory_constrained = False

    def __repr__(self):
        return f"PodMetrics({self.pod_name})"

class ScalingDecision:
    def __init__(self, action: str, reason: str, target_pods: List[str], priority: int):
        self.action = action  # "scale_horizontal", "scale_vertical", "no_action"
        self.reason = reason
        self.target_pods = target_pods
        self.priority = priority  # 1=critical, 2=important, 3=nice-to-have
        self.timestamp = datetime.now()

    def __repr__(self):
        return f"ScalingDecision(action={self.action}, priority={self.priority}, targets={self.target_pods})"
    
    def to_dict(self):
        return {
            "action": self.action,
            "reason": self.reason,
            "target_pods": self.target_pods,
            "priority": self.priority,
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
    
    # Try recording rules first, fall back to raw queries
    test_query = query_prometheus("redis:cpu_percent:by_pod")
    use_recording_rules = len(test_query) > 0
    
    if use_recording_rules:
        queries = {
            "cpu_percent": "redis:cpu_percent:by_pod",
            "memory_used_mb": "redis:memory_used_mb:by_pod",
            "is_high_load": "redis:is_high_load:by_pod",
            "is_cpu_constrained": "redis:is_cpu_constrained:by_pod",
            "is_memory_constrained": "redis:is_memory_constrained:by_pod",
        }
    else:
        queries = {
            "cpu_percent": 'sum(rate(container_cpu_usage_seconds_total{pod=~"redis-redis-cluster-.*",container="redis-redis-cluster"}[1m])) by (pod) * 100',
            "memory_used_mb": 'sum(container_memory_working_set_bytes{pod=~"redis-redis-cluster-.*",container="redis-redis-cluster"}) by (pod) / 1024 / 1024',
        }
    
    # Fetch metrics
    for metric_name, query in queries.items():
        results = query_prometheus(query)
        
        for result in results:
            pod_name = result["metric"].get("pod", "unknown")
            value = float(result["value"][1])
            
            # Initialize pod if not exists
            if pod_name not in pods:
                pods[pod_name] = PodMetrics(pod_name)
            
            # Set metric value
            if metric_name.startswith("is_"):
                setattr(pods[pod_name], metric_name, bool(value))
            else:
                setattr(pods[pod_name], metric_name, value)
    
    # Calculate derived metrics if using raw queries
    if not use_recording_rules:
        for pod_name, metrics in pods.items():
            metrics.is_cpu_constrained = metrics.cpu_percent > 12
            metrics.is_memory_constrained = metrics.memory_used_mb > 800
            metrics.is_high_load = metrics.is_cpu_constrained or metrics.is_memory_constrained
    
    return pods

def decision_tree(pod_metrics: Dict[str, PodMetrics]) -> List[ScalingDecision]:
    """
    Simplified decision tree based only on CPU and memory
    
    Decision Flow:
    1. Check for critical CPU constraint (> 12%)
    2. Check for critical memory constraint (> 80%)
    3. Check for sustained high load
    4. Cluster-wide analysis
    """
    decisions = []
    
    # Analyze each pod
    for pod_name, metrics in pod_metrics.items():
        
        # CRITICAL: CPU Constraint
        if metrics.is_cpu_constrained:
            decisions.append(ScalingDecision(
                action="scale_horizontal",
                reason=f"{pod_name}: CPU {metrics.cpu_percent:.1f}% exceeds threshold (12%)",
                target_pods=[pod_name],
                priority=1
            ))
        
        # CRITICAL: Memory Constraint
        elif metrics.is_memory_constrained:
            decisions.append(ScalingDecision(
                action="scale_vertical",
                reason=f"{pod_name}: Memory {metrics.memory_used_mb:.0f} MB near limit",
                target_pods=[pod_name],
                priority=1
            ))
        
        # HIGH LOAD: Both CPU and memory elevated (but not critical)
        elif metrics.is_high_load:
            if metrics.cpu_percent > 8:  # Approaching CPU threshold
                decisions.append(ScalingDecision(
                    action="scale_horizontal",
                    reason=f"{pod_name}: High load - CPU {metrics.cpu_percent:.1f}%",
                    target_pods=[pod_name],
                    priority=2
                ))
    
    # Cluster-wide analysis
    total_pods = len(pod_metrics)
    if total_pods > 0:
        high_load_pods = sum(1 for m in pod_metrics.values() if m.is_high_load)
        cpu_constrained_pods = sum(1 for m in pod_metrics.values() if m.is_cpu_constrained)
        avg_cpu = sum(m.cpu_percent for m in pod_metrics.values()) / total_pods
        
        # If majority of pods are CPU constrained
        if cpu_constrained_pods >= total_pods * 0.5:
            decisions.append(ScalingDecision(
                action="scale_horizontal",
                reason=f"Cluster-wide: {cpu_constrained_pods}/{total_pods} pods CPU constrained, avg CPU {avg_cpu:.1f}%",
                target_pods=list(pod_metrics.keys()),
                priority=1
            ))
        
        # If majority of pods under high load
        elif high_load_pods >= total_pods * 0.6:
            decisions.append(ScalingDecision(
                action="scale_horizontal",
                reason=f"Cluster-wide: {high_load_pods}/{total_pods} pods under high load, avg CPU {avg_cpu:.1f}%",
                target_pods=list(pod_metrics.keys()),
                priority=2
            ))
    
    # Sort by priority (critical first)
    decisions.sort(key=lambda d: d.priority)
    
    return decisions

def print_pod_summary(pod_metrics: Dict[str, PodMetrics], compact: bool = False):
    """Print a summary table of pod metrics"""
    if compact:
        # Compact one-line summary
        total = len(pod_metrics)
        avg_cpu = sum(m.cpu_percent for m in pod_metrics.values()) / total if total > 0 else 0
        max_cpu = max((m.cpu_percent for m in pod_metrics.values()), default=0)
        high_load = sum(1 for m in pod_metrics.values() if m.is_high_load)
        print(f"Pods: {total} | Avg CPU: {avg_cpu:.1f}% | Max CPU: {max_cpu:.1f}% | High Load: {high_load}/{total}")
    else:
        print("\n" + "="*100)
        print("REDIS POD METRICS SUMMARY")
        print("="*100)
        print(f"{'Pod Name':<30} {'CPU%':<10} {'Memory (MB)':<15} {'Status':<30}")
        print("-"*100)
        
        for pod_name, metrics in sorted(pod_metrics.items()):
            status = []
            if metrics.is_high_load:
                status.append("HIGH_LOAD")
            if metrics.is_cpu_constrained:
                status.append("CPU_CONSTRAINED")
            if metrics.is_memory_constrained:
                status.append("MEMORY_CONSTRAINED")
            
            status_str = ", ".join(status) if status else "OK"
            
            print(f"{pod_name:<30} {metrics.cpu_percent:<10.2f} {metrics.memory_used_mb:<15.0f} {status_str:<30}")
        
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
        print(f"Decisions: {len(decisions)} ({critical} critical, {important} important)")
    else:
        print("\n" + "="*100)
        print("SCALING RECOMMENDATIONS")
        print("="*100)
        
        priority_names = {1: "🔴 CRITICAL", 2: "🟡 IMPORTANT", 3: "🟢 PREVENTIVE"}
        
        for decision in decisions:
            print(f"\n{priority_names.get(decision.priority, '⚪ UNKNOWN')}")
            print(f"  Action:  {decision.action.upper()}")
            print(f"  Reason:  {decision.reason}")
            print(f"  Targets: {', '.join(decision.target_pods)}")
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
    print("REDIS AUTOSCALER - CONTINUOUS MONITORING")
    print("="*100)
    print(f"Prometheus: {PROMETHEUS_URL}")
    print(f"Check interval: {CHECK_INTERVAL_SECONDS} seconds")
    print(f"History file: {DECISION_HISTORY_FILE}")
    print("="*100 + "\n")
    
    iteration = 0
    
    while True:
        iteration += 1
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        print(f"\n{'='*100}")
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
            print(f"\n⏳ Next check in {CHECK_INTERVAL_SECONDS} seconds...")
            
            # Sleep
            time.sleep(CHECK_INTERVAL_SECONDS)
            
        except KeyboardInterrupt:
            raise
        except Exception as e:
            print(f"\nError in iteration: {e}")
            import traceback
            traceback.print_exc()
            print(f"\n⏳ Retrying in {CHECK_INTERVAL_SECONDS} seconds...")
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