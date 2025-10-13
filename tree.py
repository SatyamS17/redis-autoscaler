#!/usr/bin/env python3
"""
Redis Cluster Scaling Decision Engine

Queries Prometheus for per-pod metrics and makes scaling recommendations
based on a decision tree approach.
"""

import requests
import json
from datetime import datetime
from typing import Dict, List, Tuple

# Configuration
PROMETHEUS_URL = "http://localhost:9090"
# Then use: PROMETHEUS_URL = "http://localhost:9090"

class PodMetrics:
    def __init__(self, pod_name: str):
        self.pod_name = pod_name
        self.cpu_percent = 0.0
        self.memory_percent = 0.0
        self.commands_per_sec = 0.0
        self.read_commands_per_sec = 0.0
        self.write_commands_per_sec = 0.0
        self.read_write_ratio = 0.0
        self.connected_clients = 0
        self.is_high_load = False
        self.is_cpu_constrained = False
        self.is_memory_constrained = False
        self.is_read_heavy = False
        self.is_write_heavy = False

    def __repr__(self):
        return f"PodMetrics({self.pod_name})"

class ScalingDecision:
    def __init__(self, action: str, reason: str, target_pods: List[str], priority: int):
        self.action = action  # "scale_horizontal", "scale_vertical", "add_replica", "no_action"
        self.reason = reason
        self.target_pods = target_pods
        self.priority = priority  # 1=critical, 2=important, 3=nice-to-have
        self.timestamp = datetime.now()

    def __repr__(self):
        return f"ScalingDecision(action={self.action}, priority={self.priority}, targets={self.target_pods})"

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
    
    # Define queries
    queries = {
        "cpu": "redis:cpu_percent:by_pod",
        "memory": "redis:memory_percent:by_pod",
        "commands": "redis:commands_per_sec:by_pod",
        "read_commands": "redis:read_commands_per_sec:by_pod",
        "write_commands": "redis:write_commands_per_sec:by_pod",
        "read_write_ratio": "redis:read_write_ratio:by_pod",
        "connected_clients": "redis:connected_clients:by_pod",
        "is_high_load": "redis:is_high_load:by_pod",
        "is_cpu_constrained": "redis:is_cpu_constrained:by_pod",
        "is_memory_constrained": "redis:is_memory_constrained:by_pod",
        "is_read_heavy": "redis:is_read_heavy:by_pod",
        "is_write_heavy": "redis:is_write_heavy:by_pod",
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
            setattr(pods[pod_name], metric_name, value)
    
    return pods

def decision_tree(pod_metrics: Dict[str, PodMetrics]) -> List[ScalingDecision]:
    """
    Decision tree for scaling recommendations
    
    Decision Flow:
    1. Check for critical issues (CPU/Memory > 80%)
    2. Check for high load indicators
    3. Analyze read/write patterns
    4. Make recommendation
    """
    decisions = []
    
    # Analyze each pod
    for pod_name, metrics in pod_metrics.items():
        
        # CRITICAL: CPU Constraint
        if metrics.cpu_percent > 12:
            if metrics.is_write_heavy:
                decisions.append(ScalingDecision(
                    action="scale_horizontal",
                    reason=f"{pod_name}: CPU {metrics.cpu_percent:.1f}% (write-heavy workload)",
                    target_pods=[pod_name],
                    priority=1
                ))
            else:
                decisions.append(ScalingDecision(
                    action="scale_vertical",
                    reason=f"{pod_name}: CPU {metrics.cpu_percent:.1f}% (increase CPU limit)",
                    target_pods=[pod_name],
                    priority=1
                ))
        
        # CRITICAL: Memory Constraint
        elif metrics.memory_percent > 80:
            decisions.append(ScalingDecision(
                action="scale_vertical",
                reason=f"{pod_name}: Memory {metrics.memory_percent:.1f}% (increase memory limit)",
                target_pods=[pod_name],
                priority=1
            ))
        
        # HIGH LOAD: Read-Heavy
        elif metrics.is_read_heavy and metrics.cpu_percent > 12:
            decisions.append(ScalingDecision(
                action="add_replica",
                reason=f"{pod_name}: Read-heavy (R/W ratio {metrics.read_write_ratio:.2f}) + CPU {metrics.cpu_percent:.1f}%",
                target_pods=[pod_name],
                priority=2
            ))
        
        # HIGH LOAD: Write-Heavy
        elif metrics.is_write_heavy and metrics.cpu_percent > 12:
            decisions.append(ScalingDecision(
                action="scale_horizontal",
                reason=f"{pod_name}: Write-heavy (R/W ratio {metrics.read_write_ratio:.2f}) + CPU {metrics.cpu_percent:.1f}%",
                target_pods=[pod_name],
                priority=2
            ))
        
        # MODERATE LOAD: High Commands
        elif metrics.commands_per_sec > 5000 and metrics.cpu_percent > 10:
            decisions.append(ScalingDecision(
                action="scale_horizontal",
                reason=f"{pod_name}: High throughput ({metrics.commands_per_sec:.0f} cmd/s) + CPU {metrics.cpu_percent:.1f}%",
                target_pods=[pod_name],
                priority=2
            ))
    
    # Cluster-wide analysis
    total_pods = len(pod_metrics)
    high_load_pods = sum(1 for m in pod_metrics.values() if m.is_high_load)
    avg_cpu = sum(m.cpu_percent for m in pod_metrics.values()) / total_pods if total_pods > 0 else 0
    
    # If majority of pods are under load
    if high_load_pods >= total_pods * 0.6:
        decisions.append(ScalingDecision(
            action="scale_horizontal",
            reason=f"Cluster-wide: {high_load_pods}/{total_pods} pods under high load, avg CPU {avg_cpu:.1f}%",
            target_pods=list(pod_metrics.keys()),
            priority=1
        ))
    
    # Sort by priority
    decisions.sort(key=lambda d: d.priority)
    
    return decisions

def print_pod_summary(pod_metrics: Dict[str, PodMetrics]):
    """Print a summary table of pod metrics"""
    print("\n" + "="*120)
    print("REDIS POD METRICS SUMMARY")
    print("="*120)
    print(f"{'Pod Name':<25} {'CPU%':<8} {'Mem%':<8} {'Cmd/s':<10} {'R/W Ratio':<12} {'Clients':<10} {'Status':<20}")
    print("-"*120)
    
    for pod_name, metrics in sorted(pod_metrics.items()):
        status = []
        if metrics.is_high_load:
            status.append("HIGH_LOAD")
        if metrics.is_cpu_constrained:
            status.append("CPU_LIMIT")
        if metrics.is_memory_constrained:
            status.append("MEM_LIMIT")
        if metrics.is_read_heavy:
            status.append("READ_HEAVY")
        if metrics.is_write_heavy:
            status.append("WRITE_HEAVY")
        
        status_str = ",".join(status) if status else "OK"
        
        print(f"{pod_name:<25} {metrics.cpu_percent:<8.1f} {metrics.memory_percent:<8.1f} "
              f"{metrics.commands_per_sec:<10.0f} {metrics.read_write_ratio:<12.2f} "
              f"{metrics.connected_clients:<10.0f} {status_str:<20}")
    
    print("="*120 + "\n")

def print_decisions(decisions: List[ScalingDecision]):
    """Print scaling decisions"""
    if not decisions:
        print("No scaling actions recommended - cluster is healthy\n")
        return
    
    print("\n" + "="*120)
    print("SCALING RECOMMENDATIONS")
    print("="*120)
    
    priority_names = {1: "🔴 CRITICAL", 2: "🟡 IMPORTANT", 3: "🟢 PREVENTIVE"}
    
    for decision in decisions:
        print(f"\n{priority_names.get(decision.priority, '⚪ UNKNOWN')}")
        print(f"  Action:  {decision.action.upper()}")
        print(f"  Reason:  {decision.reason}")
        print(f"  Targets: {', '.join(decision.target_pods)}")
        print(f"  Time:    {decision.timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
    
    print("\n" + "="*120 + "\n")

def export_to_json(pod_metrics: Dict[str, PodMetrics], decisions: List[ScalingDecision], filename: str = "scaling_decision.json"):
    """Export metrics and decisions to JSON for further processing"""
    data = {
        "timestamp": datetime.now().isoformat(),
        "pod_metrics": {
            pod_name: {
                "cpu_percent": metrics.cpu_percent,
                "memory_percent": metrics.memory_percent,
                "commands_per_sec": metrics.commands_per_sec,
                "read_commands_per_sec": metrics.read_commands_per_sec,
                "write_commands_per_sec": metrics.write_commands_per_sec,
                "read_write_ratio": metrics.read_write_ratio,
                "connected_clients": metrics.connected_clients,
                "is_high_load": metrics.is_high_load,
                "is_read_heavy": metrics.is_read_heavy,
                "is_write_heavy": metrics.is_write_heavy,
            }
            for pod_name, metrics in pod_metrics.items()
        },
        "decisions": [
            {
                "action": d.action,
                "reason": d.reason,
                "target_pods": d.target_pods,
                "priority": d.priority,
                "timestamp": d.timestamp.isoformat()
            }
            for d in decisions
        ]
    }
    
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2)
    
    print(f"Exported data to {filename}\n")

def main():
    """Main execution function"""
    print("\nFetching Redis pod metrics from Prometheus...")
    
    # Get metrics
    pod_metrics = get_pod_metrics()
    
    if not pod_metrics:
        print("No pod metrics found. Check:")
        print("  1. Redis pods are running")
        print("  2. Redis exporter is deployed and working")
        print("  3. Prometheus is scraping metrics")
        print("  4. Recording rules are applied")
        return
    
    print(f"Found metrics for {len(pod_metrics)} pods\n")
    
    # Print summary
    print_pod_summary(pod_metrics)
    
    # Run decision tree
    print("Analyzing metrics and generating recommendations...")
    decisions = decision_tree(pod_metrics)
    
    # Print decisions
    print_decisions(decisions)
    
    # Export to JSON
    export_to_json(pod_metrics, decisions)
    
    # Summary
    if decisions:
        critical = sum(1 for d in decisions if d.priority == 1)
        important = sum(1 for d in decisions if d.priority == 2)
        preventive = sum(1 for d in decisions if d.priority == 3)
        
        print(f"Summary: {len(decisions)} recommendations")
        print(f"   - Critical: {critical}")
        print(f"   - Important: {important}")
        print(f"   - Preventive: {preventive}\n")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user\n")
    except Exception as e:
        print(f"\nError: {e}\n")
        import traceback
        traceback.print_exc()