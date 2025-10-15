#!/usr/bin/env python3
import os
import time
import redis
import threading
import statistics
from datetime import datetime
from collections import defaultdict
from redis.cluster import RedisCluster
from kubernetes import client, config

class RedisBenchmarkMonitor:
    def __init__(self):
        self.redis_password = os.getenv('REDIS_PASSWORD', 'PK1QCCFHGp')
        self.namespace = "default"

        # Metrics and state tracking
        self.lock = threading.Lock()
        self.latencies = defaultdict(list)
        self.throughput_counts = defaultdict(int)
        self.errors = defaultdict(int)
        self.redirects = defaultdict(int)
        self.running = True
        self.start_time = time.time()

        # Topology refresh parameters
        self.last_topology_refresh = time.time()
        self.topology_refresh_interval = 5

        # Initialize cluster connection
        self.rc = self.create_redis_client()
        print("✅ Connected to Redis Cluster")

        # Kubernetes metrics setup
        try:
            config.load_incluster_config()
            self.k8s_core = client.CoreV1Api()
            self.k8s_metrics = client.CustomObjectsApi()
            self.metrics_available = True
            print("✅ Connected to Kubernetes metrics API")
        except Exception:
            self.metrics_available = False
            print("⚠️ Kubernetes metrics not available — CPU monitoring disabled")

    # ---------------- REDIS & K8S HELPERS ---------------- #

    def create_redis_client(self):
        """Create a new RedisCluster client."""
        return RedisCluster(
            host="redis-redis-cluster-headless",
            port=6379,
            password=self.redis_password,
            decode_responses=True,
            socket_timeout=5,
            socket_connect_timeout=5,
            retry_on_timeout=True,
            cluster_error_retry_attempts=5
        )

    def reconnect_cluster(self):
        """Reconnect safely to Redis Cluster after topology change."""
        try:
            self.rc.close()
        except Exception:
            pass
        time.sleep(1)
        self.rc = self.create_redis_client()
        print("[Reconnect] Redis cluster client refreshed.")

    def get_redis_pods(self):
        """Get all running Redis cluster pods."""
        try:
            all_pods = self.k8s_core.list_namespaced_pod(namespace=self.namespace)
            redis_pods = [
                {'name': pod.metadata.name, 'ip': pod.status.pod_ip}
                for pod in all_pods.items
                if "redis-redis-cluster" in pod.metadata.name and pod.status.phase == "Running"
            ]
            return sorted(redis_pods, key=lambda x: x['name'])
        except Exception:
            return []

    def get_pod_cpu_usage(self, pod_name):
        """Get CPU usage for a pod."""
        if not self.metrics_available:
            return None
        try:
            metrics = self.k8s_metrics.get_namespaced_custom_object(
                group="metrics.k8s.io",
                version="v1beta1",
                namespace=self.namespace,
                plural="pods",
                name=pod_name
            )
            cpu_usage = metrics['containers'][0]['usage']['cpu']
            cpu_cores = int(cpu_usage.rstrip('n')) / 1_000_000_000
            return min(cpu_cores * 100, 100.0)
        except Exception:
            return None

    # ---------------- MONITORING THREADS ---------------- #

    def monitor_cpu(self):
        """Continuously monitor CPU usage of Redis pods."""
        while self.running:
            pods = self.get_redis_pods()
            timestamp = datetime.now().strftime('%H:%M:%S')

            cpu_readings = []
            for pod in pods:
                cpu = self.get_pod_cpu_usage(pod['name'])
                if cpu is not None:
                    cpu_readings.append(f"{pod['name']}: {cpu:.1f}%")

            if cpu_readings:
                print(f"[{timestamp}] CPU: {' | '.join(cpu_readings)}")

            time.sleep(5)

    def monitor_cluster_changes(self):
        """Detect cluster topology changes."""
        last_node_count = 0
        while self.running:
            try:
                nodes = self.rc.cluster_nodes()
                current_count = len([n for n in nodes.values() if 'master' in n.get('flags', '')])

                if current_count != last_node_count and last_node_count > 0:
                    ts = datetime.now().strftime('%H:%M:%S')
                    print(f"[{ts}] ⚙️ TOPOLOGY CHANGE: Masters {last_node_count} → {current_count}")
                    self.reconnect_cluster()

                last_node_count = current_count
            except Exception:
                time.sleep(2)
            time.sleep(5)

    # ---------------- BENCHMARK LOGIC ---------------- #

    def benchmark_operation(self, rc_client, operation, key, value=None):
        """Execute a single Redis operation and measure latency."""
        start = time.perf_counter()
        try:
            if operation == "SET":
                # heavier operation: store a large hash with many fields
                rc_client.hset(f"{key}:hash", mapping={f"field{i}": value for i in range(20)})
            elif operation == "GET":
                rc_client.hgetall(f"{key}:hash")

            latency_ms = (time.perf_counter() - start) * 1000
            with self.lock:
                self.latencies[operation].append(latency_ms)
                self.throughput_counts[operation] += 1
            return True

        except redis.exceptions.ClusterError as e:
            error_str = str(e)
            with self.lock:
                if "MOVED" in error_str:
                    self.redirects["MOVED"] += 1
                elif "ASK" in error_str:
                    self.redirects["ASK"] += 1
                else:
                    self.errors["ClusterError"] += 1
            time.sleep(0.05)
            return False

        except Exception as e:
            with self.lock:
                self.errors[type(e).__name__] += 1
            time.sleep(0.05)
            return False

    def benchmark_worker(self, operation, thread_id, operations_per_thread):
        """Worker thread that keeps sending requests even during scaling."""
        rc_local = self.create_redis_client()
        value = "x" * 8

        for i in range(operations_per_thread):
            key = f"bench:{operation.lower()}:{thread_id}:{i}"

            success = self.benchmark_operation(rc_local, operation, key, value)
            if not success and i % 50 == 0:
                # reconnect if too many consecutive failures
                try:
                    rc_local = self.create_redis_client()
                    print(f"[Thread-{thread_id}] Reconnected local client.")
                except Exception:
                    pass

            # Topology refresh trigger
            if time.time() - self.last_topology_refresh > self.topology_refresh_interval:
                try:
                    self.rc.refresh_table_asap()
                except Exception:
                    pass
                self.last_topology_refresh = time.time()

            # small controlled pause
            if i % 100 == 0:
                time.sleep(0.001)

    # ---------------- STATISTICS ---------------- #

    def print_statistics(self):
        elapsed = time.time() - self.start_time
        timestamp = datetime.now().strftime('%H:%M:%S')

        with self.lock:
            latencies_copy = {k: v.copy() for k, v in self.latencies.items()}
            throughput_copy = self.throughput_counts.copy()
            errors_copy = self.errors.copy()
            redirects_copy = self.redirects.copy()

        print(f"\n{'='*80}")
        print(f"[{timestamp}] Stats after {elapsed:.1f}s")
        print(f"{'='*80}")

        # Throughput
        total_ops = 0
        for op, count in throughput_copy.items():
            ops_per_sec = count / elapsed if elapsed > 0 else 0
            total_ops += count
            print(f"  {op}: {count:,} ops ({ops_per_sec:.0f} ops/sec)")
        print(f"  TOTAL: {total_ops:,} ops ({total_ops/elapsed:.0f} ops/sec)")

        # Latency
        print("\nLATENCY (ms):")
        for op, latencies in latencies_copy.items():
            if latencies:
                print(f"  {op}: min={min(latencies):.2f}, p50={statistics.median(latencies):.2f}, "
                      f"p95={statistics.quantiles(latencies, n=20)[18]:.2f}, "
                      f"p99={statistics.quantiles(latencies, n=100)[98]:.2f}, "
                      f"max={max(latencies):.2f}, avg={statistics.mean(latencies):.2f}")

        # Errors and Redirects
        if errors_copy or redirects_copy:
            print("\nERRORS & REDIRECTS:")
            for e, c in errors_copy.items():
                print(f"  {e}: {c}")
            for r, c in redirects_copy.items():
                print(f"  {r}: {c}")

        print(f"{'='*80}\n")

    # ---------------- BENCHMARK RUNNER ---------------- #

    def run_benchmark(self, duration_seconds=300, threads=20, ops_per_thread=100000):
        print(f"\n🚀 Starting Redis Cluster Benchmark")
        print(f"Duration: ~{duration_seconds}s, Threads: {threads}, Ops/thread: {ops_per_thread}")
        print(f"Total operations: {threads * ops_per_thread:,}")
        print(f"{'='*80}\n")

        # Start CPU and topology monitor threads
        if self.metrics_available:
            threading.Thread(target=self.monitor_cpu, daemon=True).start()
        threading.Thread(target=self.monitor_cluster_changes, daemon=True).start()

        # Spawn benchmark workers
        benchmark_threads = []
        for i in range(threads // 2):
            t = threading.Thread(target=self.benchmark_worker, args=("SET", i, ops_per_thread))
            benchmark_threads.append(t)
            t.start()

        for i in range(threads // 2, threads):
            t = threading.Thread(target=self.benchmark_worker, args=("GET", i, ops_per_thread))
            benchmark_threads.append(t)
            t.start()

        print(f"Started {len(benchmark_threads)} benchmark threads ({threads // 2} SET, {threads - threads // 2} GET)\n")

        stats_interval = 10
        next_stats = time.time() + stats_interval

        while any(t.is_alive() for t in benchmark_threads):
            if time.time() >= next_stats:
                self.print_statistics()
                next_stats = time.time() + stats_interval
            time.sleep(1)

        for t in benchmark_threads:
            t.join()

        self.running = False
        print("\nFINAL RESULTS")
        print("="*80)
        self.print_statistics()


def main():
    monitor = RedisBenchmarkMonitor()
    monitor.run_benchmark(duration_seconds=300, threads=20, ops_per_thread=50000)


if __name__ == "__main__":
    main()
