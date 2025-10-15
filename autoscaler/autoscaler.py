#!/usr/bin/env python3
import os
import time
import logging
import subprocess
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from kubernetes import client, config
import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RedisAutoscaler:
    def __init__(self):
        self.namespace = "default"
        self.redis_cluster_name = "redis-redis-cluster"
        self.redis_password = os.getenv('REDIS_PASSWORD', 'HIYCPE8NtO')
        self.upscale_threshold = float(os.getenv('UPSCALE_CPU_THRESHOLD', '10.0'))
        self.downscale_threshold = float(os.getenv('DOWNSCALE_CPU_THRESHOLD', '5.0'))
        self.cooldown_minutes = int(os.getenv('COOLDOWN_MINUTES', '1'))
        self.check_interval = int(os.getenv('CHECK_INTERVAL_SECONDS', '30'))
        self.prometheus_url = os.getenv(
            'PROMETHEUS_URL', 
            'http://monitoring-kube-prometheus-prometheus.monitoring.svc.cluster.local:9090'
        )

        # Ensure minimum 3 masters + 3 replicas = 6 nodes minimum
        self.min_masters = 3
        self.min_replicas = 3
        self.min_nodes = max(int(os.getenv('MIN_NODES', '6')), self.min_masters + self.min_replicas)
        self.max_nodes = int(os.getenv('MAX_NODES', '12'))
        
        # Ensure max_nodes is even (for master-replica pairs)
        if self.max_nodes % 2 != 0:
            self.max_nodes += 1
            
        self.last_scale_time = datetime.min
        self.node_cpu_history = {}
        
        # Log configuration
        logger.info(f"🔧 Redis Password configured: {'YES' if self.redis_password else 'NO'}")
        if self.redis_password:
            logger.info(f"🔧 Password length: {len(self.redis_password)} characters")
        
        logger.info(f"🔧 Minimum masters: {self.min_masters}, minimum replicas: {self.min_replicas}")
        logger.info(f"🔧 Total min nodes: {self.min_nodes}, max nodes: {self.max_nodes}")
        
        # Connect to Kubernetes
        try:
            config.load_incluster_config()
            logger.info("✅ Connected to Kubernetes")
        except Exception as e:
            logger.error(f"❌ Kubernetes connection failed: {e}")
            raise

        self.k8s_core = client.CoreV1Api()
        self.k8s_apps = client.AppsV1Api()
        
        # Try to connect to metrics API
        try:
            self.k8s_metrics = client.CustomObjectsApi()
            self.metrics_available = True
            logger.info("✅ Metrics API available")
        except Exception as e:
            logger.warning("⚠️ Metrics API not available - CPU monitoring disabled")
            self.metrics_available = False
        
        # Test Redis connection at startup
        self.test_redis_setup()

    # Add new method to query Prometheus
    def query_prometheus(self, query: str) -> list:
        """Query Prometheus and return results"""
        try:
            response = requests.get(
                f"{self.prometheus_url}/api/v1/query",
                params={"query": query},
                timeout=10
            )
            response.raise_for_status()
            data = response.json()
            
            if data["status"] == "success":
                return data["data"]["result"]
            else:
                logger.warning(f"Prometheus query failed: {data}")
                return []
        except Exception as e:
            logger.error(f"Error querying Prometheus: {e}")
            return []

    # Replace get_pod_cpu_usage method
    def get_pod_cpu_usage(self, pod_name: str) -> Optional[float]:
        """Get CPU usage percentage for a pod from Prometheus"""
        try:
            query = f'redis:cpu_percent:by_pod{{pod="{pod_name}"}}'
            results = self.query_prometheus(query)
            
            if results:
                cpu_value = float(results[0]["value"][1])
                return cpu_value
            else:
                logger.debug(f"No CPU metrics found for {pod_name} in Prometheus")
                return None
                
        except Exception as e:
            logger.debug(f"Could not get CPU metrics for {pod_name}: {e}")
            return None
    
    def get_redis_pods(self) -> List[Dict]:
        """Get all Redis pods by name pattern"""
        try:
            all_pods = self.k8s_core.list_namespaced_pod(namespace=self.namespace)
            redis_pods = []
            
            for pod in all_pods.items:
                if "redis-redis-cluster" in pod.metadata.name and pod.status.phase == "Running":
                    redis_pods.append({
                        'name': pod.metadata.name,
                        'ip': pod.status.pod_ip,
                        'ready': pod.status.phase == "Running"
                    })
            
            return sorted(redis_pods, key=lambda x: x['name'])
            
        except Exception as e:
            logger.error(f"Error getting Redis pods: {e}")
            return []

    def update_cpu_history(self, pod_name: str, cpu_usage: float):
        """Track CPU usage history for scaling decisions"""
        now = datetime.now()
        if pod_name not in self.node_cpu_history:
            self.node_cpu_history[pod_name] = []
        
        self.node_cpu_history[pod_name].append((now, cpu_usage))
        
        # Keep only recent history (5 minutes)
        cutoff = now - timedelta(minutes=5)
        self.node_cpu_history[pod_name] = [
            (timestamp, usage) for timestamp, usage in self.node_cpu_history[pod_name]
            if timestamp > cutoff
        ]

    def count_cluster_roles(self, cluster_nodes: Dict[str, Dict]) -> Tuple[int, int]:
        """Count masters and replicas in the cluster"""
        masters = sum(1 for info in cluster_nodes.values() if info['is_master'])
        replicas = sum(1 for info in cluster_nodes.values() if info['is_slave'])
        return masters, replicas

    def should_scale_up(self, pods: List[Dict]) -> bool:
        """Check if we should scale up - considering minimum requirements"""
        # First check if we're below minimum requirements
        cluster_nodes = self.get_cluster_nodes_info(pods[0]['name']) if pods else {}
        
        if cluster_nodes:
            masters, replicas = self.count_cluster_roles(cluster_nodes)
            
            # Always scale up if below minimum requirements
            if masters < self.min_masters or replicas < self.min_replicas:
                logger.info(f"🚨 Below minimum requirements: {masters} masters, {replicas} replicas")
                return True
        
        # Check if we're at maximum capacity
        if len(pods) >= self.max_nodes:
            return False
        
        # CHANGED: Only check CPU on master nodes
        high_cpu_masters = []
        for pod in pods:
            pod_ip = pod['ip']
            if pod_ip not in cluster_nodes:
                continue
            
            # Skip if this is a replica
            if cluster_nodes[pod_ip]['is_slave']:
                continue
            
            cpu_usage = self.get_pod_cpu_usage(pod['name'])
            if cpu_usage is None:
                continue
                
            self.update_cpu_history(pod['name'], cpu_usage)
            
            if cpu_usage >= self.upscale_threshold:
                high_cpu_masters.append(pod['name'])
        
        # Check for sustained high usage on masters
        now = datetime.now()
        cooldown_threshold = now - timedelta(minutes=self.cooldown_minutes)
        
        for pod_name in high_cpu_masters:
            if pod_name in self.node_cpu_history:
                recent_high = [
                    usage for timestamp, usage in self.node_cpu_history[pod_name]
                    if timestamp > cooldown_threshold and usage >= self.upscale_threshold
                ]
                
                expected_points = (self.cooldown_minutes * 60) // self.check_interval
                if len(recent_high) >= expected_points:
                    logger.info(f"🚀 Sustained high CPU on master {pod_name}: {len(recent_high)} high readings")
                    return True
        
        return False
    
    def reset_redis_node(self, pod_name: str) -> bool:
        """Reset a Redis node by flushing keys and resetting cluster metadata"""
        logger.info(f"🔄 Resetting Redis node: {pod_name}")
        try:
            # Flush all keys - just pass the command, not redis-cli
            success, output = self.execute_redis_command(pod_name, "FLUSHALL")
            if not success:
                logger.error(f"❌ Failed to FLUSHALL on {pod_name}: {output}")
                return False
            logger.info(f"✅ FLUSHALL successful on {pod_name}")

            # Reset cluster state
            success, output = self.execute_redis_command(pod_name, "CLUSTER RESET HARD")
            if not success:
                logger.error(f"❌ Failed to CLUSTER RESET HARD on {pod_name}: {output}")
                return False
            logger.info(f"✅ CLUSTER RESET HARD successful on {pod_name}")

            time.sleep(5)
            
            # Verify node is alive
            success, output = self.execute_redis_command(pod_name, "PING")
            if success and "PONG" in output:
                logger.info(f"✅ Node {pod_name} reset and healthy")
                return True

            logger.error(f"❌ Node {pod_name} did not respond after reset")
            return False

        except Exception as e:
            logger.error(f"Failed to reset {pod_name}: {e}")
            return False

        
    def should_scale_down(self, pods: List[Dict]) -> List[str]:
        """Check if we should scale down - respecting minimum requirements"""
        # Never scale below minimum nodes
        if len(pods) <= self.min_nodes:
            return []
        
        cluster_nodes = self.get_cluster_nodes_info(pods[0]['name'])
        if not cluster_nodes:
            return []
        
        masters, replicas = self.count_cluster_roles(cluster_nodes)
        
        # Protect minimum master/replica requirements
        if masters <= self.min_masters or replicas <= self.min_replicas:
            logger.info(f"🛑 Cannot scale down: at minimum requirements ({masters} masters, {replicas} replicas)")
            return []
        
        # CHANGED: Find master nodes with sustained low CPU
        low_cpu_masters = []
        now = datetime.now()
        cooldown_threshold = now - timedelta(minutes=self.cooldown_minutes)
        
        for pod in pods:
            pod_ip = pod['ip']
            if pod_ip not in cluster_nodes:
                continue
            
            # Skip if this is a replica
            if cluster_nodes[pod_ip]['is_slave']:
                continue
            
            cpu_usage = self.get_pod_cpu_usage(pod['name'])
            if cpu_usage is None:
                continue
                
            self.update_cpu_history(pod['name'], cpu_usage)
            
            if cpu_usage <= self.downscale_threshold:
                recent_low = [
                    usage for timestamp, usage in self.node_cpu_history.get(pod['name'], [])
                    if timestamp > cooldown_threshold and usage <= self.downscale_threshold
                ]
                
                expected_points = (self.cooldown_minutes * 60) // self.check_interval
                if len(recent_low) >= expected_points:
                    low_cpu_masters.append(pod['name'])
        
        if not low_cpu_masters:
            return []
        
        # Find master-replica pairs to remove
        # For each low CPU master, find its replica
        pods_to_remove = []
        all_pods_sorted = sorted([pod['name'] for pod in pods], reverse=True)
        
        remaining_masters = masters
        remaining_replicas = replicas
        
        for pod_name in all_pods_sorted:
            if pod_name not in low_cpu_masters:
                continue
                
            pod_ip = next((p['ip'] for p in pods if p['name'] == pod_name), None)
            if not pod_ip or pod_ip not in cluster_nodes:
                continue
            
            master_node = cluster_nodes[pod_ip]
            master_id = master_node['node_id']
            
            # Check if we can remove this master
            if remaining_masters <= self.min_masters:
                continue
            
            # Find this master's replica
            replica_to_remove = None
            for check_pod in pods:
                check_ip = check_pod['ip']
                if check_ip in cluster_nodes:
                    node = cluster_nodes[check_ip]
                    if node['is_slave'] and node['master_id'] == master_id:
                        replica_to_remove = check_pod['name']
                        break
            
            # Check if we can remove the replica
            if replica_to_remove and remaining_replicas <= self.min_replicas:
                continue
            
            # Add master and its replica
            pods_to_remove.append(pod_name)
            if replica_to_remove:
                pods_to_remove.append(replica_to_remove)
                remaining_replicas -= 1
            remaining_masters -= 1
            
            # Remove in pairs
            if len(pods_to_remove) >= 2:
                break
        
        if pods_to_remove:
            logger.info(f"🎯 Will remove master-replica pairs: {pods_to_remove}")
            logger.info(f"📊 After removal: {remaining_masters} masters, {remaining_replicas} replicas")
        else:
            logger.info("🛑 No master nodes with low CPU eligible for scale-down")
        
        return pods_to_remove

    def is_in_cooldown(self) -> bool:
        """Check if we're in cooldown period"""
        cooldown_period = timedelta(minutes=self.cooldown_minutes)
        return datetime.now() - self.last_scale_time < cooldown_period

    def execute_redis_command(self, pod_name: str, command: str) -> Tuple[bool, str]:
        """Execute a redis-cli command with proper authentication"""
        try:
            base_cmd = ["kubectl", "exec", pod_name, "-n", self.namespace, "--"]
            
            # Parse the command to handle authentication properly
            command_parts = command.split()
            
            if self.redis_password:
                # Build redis-cli command with authentication
                redis_cmd = ["redis-cli", "-a", self.redis_password, "--no-auth-warning"] + command_parts
            else:
                redis_cmd = ["redis-cli"] + command_parts
            
            full_cmd = base_cmd + redis_cmd
            
            logger.debug(f"Executing: kubectl exec {pod_name} -n {self.namespace} -- redis-cli [auth] {' '.join(command_parts)}")
            
            result = subprocess.run(full_cmd, capture_output=True, text=True, timeout=300)
            
            # Check for authentication errors specifically
            if "NOAUTH" in result.stdout or "NOAUTH" in result.stderr:
                logger.error(f"Authentication failed for pod {pod_name}")
                return False, "Authentication failed"
            
            return result.returncode == 0, result.stdout + result.stderr
            
        except Exception as e:
            logger.error(f"Redis command failed: {e}")
            return False, str(e)

    def get_cluster_nodes_info(self, pod_name: str) -> Dict[str, Dict]:
        """Get detailed cluster nodes information"""
        success, output = self.execute_redis_command(pod_name, "cluster nodes")
        if not success:
            logger.error(f"Failed to get cluster nodes: {output}")
            return {}
        
        cluster_nodes = {}
        
        # Parse the output line by line
        lines = output.strip().split('\n')
        logger.debug(f"Got {len(lines)} cluster nodes lines")
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            parts = line.split()
            if len(parts) < 8:
                logger.warning(f"Invalid cluster nodes line: {line}")
                continue
            
            node_id = parts[0]
            ip_port = parts[1].split('@')[0]  # Remove gossip port
            flags = parts[2]
            master_id = parts[3] if parts[3] != '-' else None
            ping_sent = parts[4]
            pong_recv = parts[5]
            config_epoch = parts[6]
            link_state = parts[7]
            
            # Extract IP address
            ip_address = ip_port.split(':')[0]
            
            # Parse slots (if any)
            slots = []
            slot_ranges = []
            if len(parts) > 8:
                for slot_info in parts[8:]:
                    # Skip importing/migrating slot indicators
                    if '[' in slot_info:
                        continue
                    
                    if '-' in slot_info:
                        # Slot range
                        try:
                            start, end = map(int, slot_info.split('-'))
                            slots.extend(range(start, end + 1))
                            slot_ranges.append(slot_info)
                        except ValueError:
                            pass
                    elif slot_info.isdigit():
                        # Single slot
                        slots.append(int(slot_info))
                        slot_ranges.append(slot_info)
            
            cluster_nodes[ip_address] = {
                'node_id': node_id,
                'ip_port': ip_port,
                'flags': flags,
                'master_id': master_id,
                'ping_sent': ping_sent,
                'pong_recv': pong_recv,
                'config_epoch': config_epoch,
                'link_state': link_state,
                'slots': slots,
                'slot_ranges': slot_ranges,
                'is_master': 'master' in flags,
                'is_slave': 'slave' in flags,
                'is_myself': 'myself' in flags
            }
        
        masters, replicas = self.count_cluster_roles(cluster_nodes)
        logger.info(f"📋 Parsed {len(cluster_nodes)} cluster nodes: {masters} masters, {replicas} replicas")
        
        return cluster_nodes

    def get_statefulset_replica_count(self) -> int:
        """Get current StatefulSet replica count"""
        try:
            sts = self.k8s_apps.read_namespaced_stateful_set(
                name=self.redis_cluster_name,
                namespace=self.namespace
            )
            return sts.spec.replicas
        except Exception as e:
            logger.error(f"Failed to get StatefulSet replica count: {e}")
            return 0

    def scale_statefulset(self, new_replica_count: int) -> bool:
        """Scale the StatefulSet to new replica count"""
        try:
            # Get current StatefulSet
            sts = self.k8s_apps.read_namespaced_stateful_set(
                name=self.redis_cluster_name,
                namespace=self.namespace
            )
            
            # Update replica count
            sts.spec.replicas = new_replica_count
            
            # Apply the change
            self.k8s_apps.patch_namespaced_stateful_set(
                name=self.redis_cluster_name,
                namespace=self.namespace,
                body=sts
            )
            
            logger.info(f"✅ StatefulSet scaled to {new_replica_count} replicas")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to scale StatefulSet: {e}")
            return False

    def add_nodes_to_cluster(self, pods: List[Dict], cluster_nodes: Dict[str, Dict]) -> bool:
        """Add new nodes to cluster as master-replica pairs"""
        existing_nodes = set(cluster_nodes.keys())
        new_pods = [pod for pod in pods if pod['ip'] not in existing_nodes]
        
        if not new_pods:
            logger.info("✅ No new nodes to add")
            return True
        
        # Find an existing master to use as cluster endpoint
        master_endpoint = None
        for ip, info in cluster_nodes.items():
            if info['is_master']:
                master_endpoint = info['ip_port']
                break
        
        if not master_endpoint:
            logger.error("❌ No existing master found for cluster operations")
            return False
        
        primary_pod = pods[0]['name']  # Use first pod to execute commands
        
        # RESET NEW NODES FIRST
        for new_pod in new_pods:
            if not self.reset_redis_node(new_pod['name']):
                logger.error(f"❌ Failed to reset {new_pod['name']}, skipping")
                return False
            time.sleep(2)

        # Add new nodes in pairs (master + replica)
        for i in range(0, len(new_pods), 2):
            # Add master node
            master_pod = new_pods[i]
            master_ip_port = f"{master_pod['ip']}:6379"
            
            logger.info(f"➕ Adding new master: {master_pod['name']} ({master_ip_port})")
            add_master_cmd = f"--cluster add-node {master_ip_port} {master_endpoint}"
            
            success, output = self.execute_redis_command(primary_pod, add_master_cmd)
            if not success:
                logger.error(f"❌ Failed to add master {master_pod['name']}: {output}")
                continue
            
            # Add master first and get its node_id
            logger.info(f"✅ Added master {master_pod['name']}")
            time.sleep(10)  # Wait for node to join

            # Get the node_id of the newly added master
            updated_cluster = self.get_cluster_nodes_info(primary_pod)
            new_master_id = None

            for ip, info in updated_cluster.items():
                if ip == master_pod['ip']:
                    new_master_id = info['node_id']
                    break

            if not new_master_id:
                logger.error(f"❌ Could not find node_id for newly added master {master_pod['name']}")
                continue

            logger.info(f"📝 New master node_id: {new_master_id[:8]}...")

            # Add replica node if available
            if i + 1 < len(new_pods):
                replica_pod = new_pods[i + 1]
                replica_ip_port = f"{replica_pod['ip']}:6379"
                
                logger.info(f"➕ Adding new replica: {replica_pod['name']} ({replica_ip_port})")
                
                # ✅ Specify the master_id to ensure replica is assigned to correct master
                add_replica_cmd = f"--cluster add-node {replica_ip_port} {master_endpoint} --cluster-slave --cluster-master-id {new_master_id}"
                
                success, output = self.execute_redis_command(primary_pod, add_replica_cmd)
                if not success:
                    logger.error(f"❌ Failed to add replica {replica_pod['name']}: {output}")
                else:
                    logger.info(f"✅ Added replica {replica_pod['name']}")
                    time.sleep(5)
        
        return True

    def wait_for_pods_ready(self, expected_count: int, timeout_seconds: int = 180) -> bool:
        """Wait for pods to be ready after scaling"""
        start_time = time.time()
        
        while time.time() - start_time < timeout_seconds:
            pods = self.get_redis_pods()
            ready_count = len([p for p in pods if p['ready']])
            
            logger.info(f"⏳ Waiting for pods: {ready_count}/{expected_count} ready")
            
            if ready_count >= expected_count:
                logger.info(f"✅ All {expected_count} pods are ready")
                return True
            
            time.sleep(10)
        
        logger.error(f"❌ Timeout waiting for pods to be ready")
        return False

    def reshard_slots(self, pod_name: str, source_id: str, target_id: str, 
                    total_slots: int) -> bool:
        """Reshard slots in a single request with large pipeline"""
        cluster_endpoint = f"{self.get_redis_pods()[0]['ip']}:6379"
        
        logger.info(f"📦 Resharding {total_slots} slots in single request with large pipeline")
        
        reshard_cmd = (f"--cluster reshard {cluster_endpoint} "
                    f"--cluster-from {source_id} "
                    f"--cluster-to {target_id} "
                    f"--cluster-slots {total_slots} "
                    f"--cluster-yes "
                    f"--cluster-timeout 60000 "
                    f"--cluster-pipeline 10000")
        
        success, output = self.execute_redis_command(pod_name, reshard_cmd)
        
        if not success:
            logger.error(f"❌ Reshard failed: {output}")
            return False
        
        logger.info(f"✅ Resharding completed: {total_slots} slots moved")
        return True

    def scale_up(self, current_count: int) -> bool:
        """Scale up the cluster by adding master-replica pairs"""
        logger.info("🚀 Starting scale-up operation...")
        
        # Always scale up by 2 nodes (1 master + 1 replica)
        new_count = min(current_count + 2, self.max_nodes)
        
        logger.info(f"📈 Scaling from {current_count} to {new_count} nodes")
        
        if not self.scale_statefulset(new_count):
            return False
        
        # Wait for new pods to be ready with proper verification
        logger.info("⏳ Waiting for new pods to be ready...")
        if not self.wait_for_pods_ready(new_count):
            logger.error("❌ New pods did not become ready in time")
            return False

        # Get updated pod list
        pods = self.get_redis_pods()
        if not pods or len(pods) < new_count:
            logger.error(f"Not enough pods found after scale-up: expected {new_count}, found {len(pods)}")
            return False
        
        # Get current cluster state
        cluster_nodes = self.get_cluster_nodes_info(pods[0]['name'])
        if not cluster_nodes:
            logger.error("Failed to get cluster state after scale-up")
            return False
        
        # Add new nodes to cluster
        logger.info("🔗 Adding new nodes to cluster...")
        if not self.add_nodes_to_cluster(pods, cluster_nodes):
            logger.error("Failed to add new nodes to cluster")
            return False
        
        # Wait a bit for cluster to stabilize
        time.sleep(10)
        
        # Get updated cluster state to find empty masters
        updated_cluster = self.get_cluster_nodes_info(pods[0]['name'])
        if not updated_cluster:
            logger.error("Failed to get updated cluster state")
            return False
        
        # Find empty masters (new nodes with no slots)
        empty_masters = []
        source_masters = []
        
        for ip, info in updated_cluster.items():
            if info['is_master']:
                if len(info['slots']) == 0:
                    empty_masters.append(info)
                    logger.info(f"🆕 Found empty master: {info['node_id'][:8]}... ({ip})")
                elif len(info['slots']) > 0:
                    source_masters.append(info)
        
        # Reshard slots to empty masters INCREMENTALLY
        if empty_masters and source_masters:
            # Calculate how many slots to move to each new master
            total_slots = sum(len(m['slots']) for m in source_masters)
            total_masters = len(source_masters) + len(empty_masters)
            target_slots_per_master = 16384 // total_masters
            
            logger.info(f"📊 Total slots: {total_slots}, Target per master: {target_slots_per_master}")
            
            for empty_master in empty_masters:
                slots_to_move = target_slots_per_master
                target_id = empty_master['node_id']
                
                logger.info(f"📤 Moving {slots_to_move} slots to new master {target_id[:8]}... (incremental)")
                
                # Collect all source master IDs
                source_ids = ','.join([m['node_id'] for m in source_masters if len(m['slots']) > 0])
                
                # Use incremental resharding with 100-slot batches
                success = self.reshard_slots(
                    pods[0]['name'], 
                    source_ids,
                    target_id,
                    slots_to_move
                )
                
                if success:
                    logger.info(f"✅ Successfully moved {slots_to_move} slots to {target_id[:8]}")
                else:
                    logger.error(f"❌ Failed to reshard slots incrementally")
                
                time.sleep(5)
        else:
            logger.info("✅ No empty masters found or no source masters available")
        
        # Verify minimum requirements are met
        final_cluster_nodes = self.get_cluster_nodes_info(pods[0]['name'])
        if final_cluster_nodes:
            masters, replicas = self.count_cluster_roles(final_cluster_nodes)
            logger.info(f"📊 Final cluster state: {masters} masters, {replicas} replicas")
            
            # Log slot distribution for verification
            for ip, info in final_cluster_nodes.items():
                if info['is_master']:
                    slot_count = len(info['slots'])
                    logger.info(f"   Master {info['node_id'][:8]}... ({ip}): {slot_count} slots")
        
        self.last_scale_time = datetime.now()
        return True


    def scale_down(self, nodes_to_remove: List[str], current_count: int) -> bool:
        """Scale down the cluster by removing master-replica pairs"""
        logger.info(f"🔽 Starting scale-down operation for {len(nodes_to_remove)} nodes...")
        
        pods = self.get_redis_pods()
        if not pods:
            logger.error("No pods found for scale-down")
            return False
        
        # Find a stable primary pod for executing commands (not being removed)
        primary_pod = None
        for pod in pods:
            if pod['name'] not in nodes_to_remove:
                primary_pod = pod
                break
        
        if not primary_pod:
            logger.error("Could not find a stable pod to execute commands")
            return False
        
        primary_pod_name = primary_pod['name']
        cluster_endpoint = f"{primary_pod['ip']}:6379"
        
        # Get detailed cluster information
        cluster_nodes = self.get_cluster_nodes_info(primary_pod_name)
        if not cluster_nodes:
            logger.error("Failed to get cluster nodes information")
            return False
        
        # Pre-check: Verify we won't go below minimum requirements
        current_masters, current_replicas = self.count_cluster_roles(cluster_nodes)
        
        masters_to_remove_count = 0
        replicas_to_remove_count = 0
        
        for pod_name in nodes_to_remove:
            pod_ip = next((p['ip'] for p in pods if p['name'] == pod_name), None)
            if pod_ip and pod_ip in cluster_nodes:
                if cluster_nodes[pod_ip]['is_master']:
                    masters_to_remove_count += 1
                elif cluster_nodes[pod_ip]['is_slave']:
                    replicas_to_remove_count += 1
        
        if (current_masters - masters_to_remove_count < self.min_masters or 
            current_replicas - replicas_to_remove_count < self.min_replicas):
            logger.error(f"❌ Cannot proceed: would violate minimum requirements")
            logger.error(f"   Current: {current_masters} masters, {current_replicas} replicas")
            logger.error(f"   After removal: {current_masters - masters_to_remove_count} masters, {current_replicas - replicas_to_remove_count} replicas")
            logger.error(f"   Required minimum: {self.min_masters} masters, {self.min_replicas} replicas")
            return False
        
        # Separate masters and replicas to remove
        masters_to_remove = []
        replicas_to_remove = []
        
        for pod_name in nodes_to_remove:
            pod_ip = None
            for pod in pods:
                if pod['name'] == pod_name:
                    pod_ip = pod['ip']
                    break
            
            if not pod_ip or pod_ip not in cluster_nodes:
                logger.warning(f"Could not find cluster info for {pod_name}")
                continue
            
            node_info = cluster_nodes[pod_ip]
            
            if node_info['is_master']:
                masters_to_remove.append((pod_name, pod_ip, node_info))
            elif node_info['is_slave']:
                replicas_to_remove.append((pod_name, pod_ip, node_info))
        
        logger.info(f"📋 Masters to remove: {[m[0] for m in masters_to_remove]}")
        logger.info(f"📋 Replicas to remove: {[r[0] for r in replicas_to_remove]}")
        
        removed_count = 0
        
        # Process masters FIRST (redistribute slots and remove)
        for pod_name, pod_ip, node_info in masters_to_remove:
            node_id = node_info['node_id']
            
            logger.info(f"🎯 Processing master: {pod_name} ({pod_ip})")
            
            # If this master has slots, redistribute them INCREMENTALLY
            if node_info['slots']:
                slot_count = len(node_info['slots'])
                logger.info(f"📦 Master {pod_name} has {slot_count} slots to redistribute")
                
                # Fix any open slots first
                logger.info("🔧 Running cluster fix before redistribution...")
                fix_cmd = f"--cluster fix {cluster_endpoint} --cluster-timeout 5000"
                success, output = self.execute_redis_command(primary_pod_name, fix_cmd)
                time.sleep(10)
                
                # Find available target masters (not being removed)
                target_masters = []
                for ip, info in cluster_nodes.items():
                    if (info['is_master'] and 
                        info['node_id'] != node_id and 
                        ip not in [m[1] for m in masters_to_remove]):
                        target_masters.append(info)
                
                if not target_masters:
                    logger.error(f"❌ No target masters available for slot redistribution")
                    continue
                
                logger.info(f"🔄 Redistributing {slot_count} slots from {pod_name} (incremental)")
                
                slots_per_master = slot_count // len(target_masters)
                remaining_slots = slot_count % len(target_masters)
                
                for i, target_master in enumerate(target_masters):
                    slots_to_move = slots_per_master + (1 if i < remaining_slots else 0)
                    
                    if slots_to_move <= 0:
                        continue
                    
                    target_id = target_master['node_id']
                    target_ip_port = target_master['ip_port']
                    
                    logger.info(f"  📤 Moving {slots_to_move} slots to {target_id[:8]}... ({target_ip_port}) incrementally")
                    
                    # Use incremental resharding
                    success = self.reshard_slots(
                        primary_pod_name,
                        node_id,
                        target_id,
                        slots_to_move
                    )
                    
                    if not success:
                        logger.error(f"❌ Failed to reshard slots to {target_id[:8]}")
                        
                        # Retry with fix
                        logger.info("🔧 Fixing cluster and retrying...")
                        fix_cmd = f"--cluster fix {cluster_endpoint}"
                        self.execute_redis_command(primary_pod_name, fix_cmd)
                        time.sleep(15)
                        
                        success = self.reshard_slots(
                            primary_pod_name,
                            node_id,
                            target_id,
                            slots_to_move
                        )
                        
                        if not success:
                            logger.error(f"❌ Failed to reshard slots to {target_id[:8]} on retry")
                            return False
                    
                    logger.info(f"  ✅ Successfully moved {slots_to_move} slots")
                    time.sleep(5)
                
                logger.info(f"✅ Completed slot redistribution for {pod_name}")
                
                # Verify slots are gone
                time.sleep(5)
                updated_cluster_nodes = self.get_cluster_nodes_info(primary_pod_name)
                if updated_cluster_nodes and pod_ip in updated_cluster_nodes:
                    remaining_slots = len(updated_cluster_nodes[pod_ip]['slots'])
                    if remaining_slots > 0:
                        logger.warning(f"⚠️ Master {pod_name} still has {remaining_slots} slots")
                    else:
                        logger.info(f"✅ Verified: Master {pod_name} has no slots remaining")
            
            # Remove the master from cluster
            logger.info(f"🗑️ Removing master {pod_name} from cluster...")
            del_cmd = f"--cluster del-node {cluster_endpoint} {node_id}"
            success, output = self.execute_redis_command(primary_pod_name, del_cmd)
            
            if success:
                logger.info(f"✅ Successfully removed master {pod_name} from cluster")
                removed_count += 1
            else:
                logger.error(f"❌ Failed to remove master {pod_name} from cluster: {output}")
        
        # Now remove replicas AFTER masters are cleaned up
        logger.info("🔄 Now removing replicas after masters are cleaned up...")
        time.sleep(5)
        
        for pod_name, pod_ip, node_info in replicas_to_remove:
            logger.info(f"🗑️ Removing replica: {pod_name} ({pod_ip})")
            
            del_cmd = f"--cluster del-node {cluster_endpoint} {node_info['node_id']}"
            success, output = self.execute_redis_command(primary_pod_name, del_cmd)
            
            if success:
                logger.info(f"✅ Successfully removed replica {pod_name}")
                removed_count += 1
            else:
                logger.error(f"❌ Failed to remove replica {pod_name}: {output}")
        
        if removed_count > 0:
            # Scale down StatefulSet
            new_count = max(current_count - removed_count, self.min_nodes)
            logger.info(f"📉 Scaling StatefulSet from {current_count} to {new_count}")
            
            if self.scale_statefulset(new_count):
                # Verify final state
                time.sleep(10)
                final_cluster_nodes = self.get_cluster_nodes_info(primary_pod_name)
                
                if final_cluster_nodes:
                    final_masters, final_replicas = self.count_cluster_roles(final_cluster_nodes)
                    logger.info(f"📊 Final cluster state: {final_masters} masters, {final_replicas} replicas")
                    
                    if final_masters >= self.min_masters and final_replicas >= self.min_replicas:
                        logger.info("✅ Minimum requirements maintained after scale-down")
                    else:
                        logger.warning(f"⚠️ Below minimum: {final_masters} masters, {final_replicas} replicas")
                
                self.last_scale_time = datetime.now()
                logger.info(f"✅ Scale-down completed: removed {removed_count} nodes")
                
                return True
            else:
                logger.error("❌ Failed to scale down StatefulSet")
                return False
        
        logger.warning("❌ No nodes were successfully removed")
        return False

    def cleanup_orphan_pods(self):
        """Delete Redis pods that are not part of the active cluster."""
        try:
            all_pods = self.get_redis_pods()
            active_pods = self.get_active_cluster_pods()
            
            if not all_pods:
                logger.info("⚠️ No Redis pods found for cleanup")
                return

            active_names = {pod['name'] for pod in active_pods}

            for pod in all_pods:
                if pod['name'] not in active_names:
                    try:
                        logger.info(f"🗑️ Deleting orphan pod {pod['name']} (IP: {pod['ip']})")
                        
                        self.k8s_core.delete_namespaced_pod(
                            name=pod['name'],
                            namespace=self.namespace,
                            body=client.V1DeleteOptions(grace_period_seconds=5)
                        )
                        
                        logger.info(f"✅ Deletion request sent for {pod['name']}")
                        
                    except Exception as e:
                        logger.error(f"❌ Failed to delete pod {pod['name']}: {e}")
                        
            logger.info("📋 Orphan pod cleanup completed")
                        
        except Exception as e:
            logger.error(f"❌ Error during cleanup_orphan_pods: {e}")

    def ensure_minimum_cluster_size(self):
        """Ensure the cluster meets minimum size requirements"""
        pods = self.get_active_cluster_pods()
        if not pods:
            logger.error("❌ No active Redis pods found")
            return False
        
        current_count = len(pods)
        cluster_nodes = self.get_cluster_nodes_info(pods[0]['name'])
        
        if cluster_nodes:
            masters, replicas = self.count_cluster_roles(cluster_nodes)
            logger.info(f"📊 Current cluster: {masters} masters, {replicas} replicas ({current_count} total)")
            
            # Check if we need to scale up to meet minimum requirements
            if masters < self.min_masters or replicas < self.min_replicas or current_count < self.min_nodes:
                logger.warning(f"🚨 Below minimum requirements! Scaling up...")
                logger.info(f"   Required: {self.min_masters} masters, {self.min_replicas} replicas, {self.min_nodes} total")
                logger.info(f"   Current:  {masters} masters, {replicas} replicas, {current_count} total")
                
                # Calculate how many nodes we need to add
                needed_nodes = max(self.min_nodes - current_count, 0)
                needed_masters = max(self.min_masters - masters, 0)
                needed_replicas = max(self.min_replicas - replicas, 0)
                
                # Ensure we add in pairs and meet all requirements
                total_needed = max(needed_nodes, needed_masters + needed_replicas)
                if total_needed % 2 != 0:
                    total_needed += 1  # Make it even for master-replica pairs
                
                logger.info(f"🔧 Will add {total_needed} nodes to meet requirements")
                return self.scale_up(current_count)
            else:
                logger.info("✅ Cluster meets minimum requirements")
                return True
        else:
            logger.error("❌ Could not get cluster information")
            return False

    def test_redis_setup(self):
        """Test Redis connection and authentication at startup"""
        logger.info("🧪 Testing Redis connection and authentication...")
        
        pods = self.get_redis_pods()
        if not pods:
            logger.warning("⚠️ No Redis pods found during startup test")
            return
        
        test_pod = pods[0]['name']
        logger.info(f"🔍 Testing connection to {test_pod}...")
        
        # Test basic ping
        success, output = self.execute_redis_command(test_pod, "ping")
        if success and "PONG" in output:
            logger.info("✅ Redis authentication successful")
        else:
            logger.error(f"❌ Redis authentication failed: {output}")
            logger.error("💡 Check your REDIS_PASSWORD environment variable")
            return
        
        # Test cluster nodes command
        success, output = self.execute_redis_command(test_pod, "cluster nodes")
        if success and output.strip():
            lines = output.strip().split('\n')
            logger.info(f"✅ Cluster nodes command successful - found {len(lines)} nodes")
        else:
            logger.error(f"❌ Cluster nodes command failed: {output}")

    def test_redis_connection(self, pod_name: str) -> bool:
        """Test Redis connection"""
        success, output = self.execute_redis_command(pod_name, "ping")
        return success and "PONG" in output

    def get_cluster_info(self, pod_name: str) -> Optional[Dict]:
        """Get cluster information"""
        success, output = self.execute_redis_command(pod_name, "cluster info")
        if not success:
            return None
        
        info = {}
        for line in output.strip().split('\n'):
            if ':' in line:
                key, value = line.split(':', 1)
                info[key] = value.strip()
        
        return info

    def get_active_cluster_pods(self) -> List[Dict]:
        """
        Return only Redis pods that are actually part of the cluster (master or slave).
        This avoids counting orphan pods that exist in the StatefulSet but are not active.
        """
        all_pods = self.get_redis_pods()
        if not all_pods:
            return []

        # Pick a primary pod to query cluster nodes
        primary_pod_name = all_pods[0]['name']
        cluster_nodes = self.get_cluster_nodes_info(primary_pod_name)
        if not cluster_nodes:
            logger.warning("⚠️ Could not get cluster nodes; returning all pods as fallback")
            return all_pods

        active_pods = []
        ip_to_pod = {pod['ip']: pod for pod in all_pods}

        for ip, node_info in cluster_nodes.items():
            if ip in ip_to_pod:
                active_pods.append(ip_to_pod[ip])
            else:
                logger.debug(f"⚠️ Cluster node {ip} exists but no matching pod found")

        return sorted(active_pods, key=lambda x: x['name'])

    def check_and_repair_cluster(self, pods: List[Dict]) -> bool:
        """Check cluster health and attempt repairs if needed"""
        if not pods:
            return False
            
        primary_pod = pods[0]['name']
        
        # Get cluster info
        cluster_info = self.get_cluster_info(primary_pod)
        if not cluster_info:
            logger.error("❌ Could not get cluster info for health check")
            return False
        
        cluster_state = cluster_info.get('cluster_state', 'unknown')
        cluster_slots_assigned = int(cluster_info.get('cluster_slots_assigned', '0'))
        cluster_known_nodes = int(cluster_info.get('cluster_known_nodes', '0'))
        
        # Get detailed role information
        cluster_nodes = self.get_cluster_nodes_info(primary_pod)
        masters, replicas = self.count_cluster_roles(cluster_nodes) if cluster_nodes else (0, 0)
        
        logger.info(f"🏥 Cluster Health Check:")
        logger.info(f"   State: {cluster_state}")
        logger.info(f"   Masters: {masters}, Replicas: {replicas}")
        logger.info(f"   Slots assigned: {cluster_slots_assigned}/16384")
        logger.info(f"   Known nodes: {cluster_known_nodes}")
        logger.info(f"   Running pods: {len(pods)}")
        
        # Check minimum requirements
        min_requirements_met = (masters >= self.min_masters and 
                               replicas >= self.min_replicas and 
                               len(pods) >= self.min_nodes)
        
        if not min_requirements_met:
            logger.warning(f"🚨 Below minimum requirements: {masters}/{self.min_masters} masters, {replicas}/{self.min_replicas} replicas")
        
        # If cluster is unhealthy, attempt repair
        if (cluster_state == 'fail' or 
            cluster_slots_assigned < 16384 or 
            cluster_known_nodes != len(pods) or 
            not min_requirements_met):
            
            logger.warning("🚨 Cluster is unhealthy, attempting repair...")
            
            # First, try to forget failed nodes
            success, nodes_output = self.execute_redis_command(primary_pod, "cluster nodes")
            if success:
                current_pod_ips = {pod['ip'] for pod in pods}
                
                # Parse cluster nodes and find disconnected ones
                for line in nodes_output.strip().split('\n'):
                    if not line.strip():
                        continue
                    
                    parts = line.split()
                    if len(parts) >= 8:
                        node_id = parts[0]
                        ip_port = parts[1].split('@')[0]
                        flags = parts[2]
                        link_state = parts[7]
                        
                        node_ip = ip_port.split(':')[0]
                        
                        # If this node is disconnected and not in our current pods, forget it
                        if ('fail' in flags or link_state == 'disconnected') and node_ip not in current_pod_ips:
                            logger.info(f"🗑️ Forgetting failed node: {node_id[:8]}... ({ip_port})")
                            forget_cmd = f"cluster forget {node_id}"
                            
                            # Try to forget from multiple nodes
                            for pod in pods[:3]:  # Try first 3 pods
                                success, output = self.execute_redis_command(pod['name'], forget_cmd)
                                if success:
                                    logger.info(f"   ✅ Forgotten from {pod['name']}")
                                else:
                                    logger.debug(f"   ❌ Failed to forget from {pod['name']}: {output}")
            
            # Wait a bit for cluster to update
            time.sleep(10)
            
            # Try cluster fix
            logger.info("🔧 Attempting cluster fix...")
            cluster_endpoint = f"{pods[0]['ip']}:6379"
            fix_cmd = f"--cluster fix {cluster_endpoint}"
            
            success, output = self.execute_redis_command(primary_pod, fix_cmd)
            if success:
                logger.info("✅ Cluster fix completed")
            else:
                logger.warning(f"⚠️ Cluster fix had issues: {output}")
            
            # Final rebalance attempt
            time.sleep(5)
            logger.info("⚖️ Final rebalance attempt...")
            rebalance_cmd = f"--cluster rebalance {cluster_endpoint}"
            
            success, output = self.execute_redis_command(primary_pod, rebalance_cmd)
            if success:
                logger.info("✅ Rebalance completed successfully")
            else:
                logger.warning(f"⚠️ Rebalance had issues: {output}")
        
        # Re-check after repairs
        final_cluster_info = self.get_cluster_info(primary_pod)
        final_state = final_cluster_info.get('cluster_state', 'unknown') if final_cluster_info else 'unknown'
        
        # Update cluster nodes info after repairs
        final_cluster_nodes = self.get_cluster_nodes_info(primary_pod)
        final_masters, final_replicas = self.count_cluster_roles(final_cluster_nodes) if final_cluster_nodes else (0, 0)
        
        cluster_healthy = (final_state == 'ok' and 
                          final_masters >= self.min_masters and 
                          final_replicas >= self.min_replicas)
        
        if cluster_healthy:
            logger.info("✅ Cluster is healthy")
        else:
            logger.warning(f"⚠️ Cluster still unhealthy: state={final_state}, masters={final_masters}, replicas={final_replicas}")
        
        return cluster_healthy

    def monitor_and_scale(self):
        """Main monitoring and scaling loop"""
        logger.info("🚀 Redis Autoscaler Started with Master-Replica Pair Scaling")
        logger.info(f"   Scale-up threshold: {self.upscale_threshold}%")
        logger.info(f"   Scale-down threshold: {self.downscale_threshold}%")
        logger.info(f"   Cooldown period: {self.cooldown_minutes} minutes")
        logger.info(f"   Minimum: {self.min_masters} masters, {self.min_replicas} replicas ({self.min_nodes} total)")
        logger.info(f"   Maximum nodes: {self.max_nodes}")

        # Initial minimum size check
        logger.info("🔍 Initial cluster size check...")
        self.ensure_minimum_cluster_size()

        while True:
            try:
                pods = self.get_active_cluster_pods()
                
                if not pods:
                    logger.warning("❌ No Redis pods found")
                    time.sleep(self.check_interval)
                    continue
                
                current_count = len(pods)
                sts_count = self.get_statefulset_replica_count()
                
                logger.info(f"📊 Monitoring {current_count} Redis pods (StatefulSet: {sts_count})")
                
                # Check cluster health and ensure minimum requirements
                cluster_healthy = self.check_and_repair_cluster(pods)
                
                # Always ensure minimum requirements are met
                if not self.ensure_minimum_cluster_size():
                    logger.warning("⚠️ Failed to ensure minimum cluster size")
                    time.sleep(self.check_interval)
                    continue
                
                # Test connectivity and show cluster status
                if pods:
                    first_pod = pods[0]['name']
                    if self.test_redis_connection(first_pod):
                        cluster_info = self.get_cluster_info(first_pod)
                        cluster_nodes = self.get_cluster_nodes_info(first_pod)
                        
                        if cluster_info and cluster_nodes:
                            state = cluster_info.get('cluster_state', 'unknown')
                            nodes = cluster_info.get('cluster_known_nodes', '0')
                            slots = cluster_info.get('cluster_slots_assigned', '0')
                            masters, replicas = self.count_cluster_roles(cluster_nodes)
                            logger.info(f"🔍 Cluster: {state}, {masters}M/{replicas}R, {slots}/16384 slots")
                
                # Only proceed with scaling if cluster is healthy
                if not cluster_healthy:
                    logger.warning("⚠️ Cluster is unhealthy, skipping scaling decisions")
                    time.sleep(self.check_interval)
                    continue
                
                # Monitor CPU and make scaling decisions
                high_cpu = 0
                low_cpu = 0
                for pod in pods:
                    cpu = self.get_pod_cpu_usage(pod['name'])
                    if cpu is not None:
                        icon = "🔥" if cpu > self.upscale_threshold else "❄️" if cpu < self.downscale_threshold else "✅"
                        logger.info(f"   {icon} {pod['name']}: {cpu:.1f}%")
                        
                        if cpu > self.upscale_threshold:
                            high_cpu += 1
                        elif cpu < self.downscale_threshold:
                            low_cpu += 1
                    else:
                        logger.info(f"   📊 {pod['name']}: No CPU metrics")
                
                # Scaling decisions
                if not self.is_in_cooldown():
                    if self.should_scale_up(pods):
                        logger.info("🚀 SCALING UP: Adding master-replica pair")
                        if self.scale_up(current_count):
                            logger.info("✅ Scale-up completed")
                        else:
                            logger.error("❌ Scale-up failed")
                    
                    else:
                        nodes_to_remove = self.should_scale_down(pods)
                        if nodes_to_remove:
                            logger.info(f"🔽 SCALING DOWN: Removing {len(nodes_to_remove)} nodes: {nodes_to_remove}")
                            if self.scale_down(nodes_to_remove, current_count):
                                logger.info("✅ Scale-down completed")
                            else:
                                logger.error("❌ Scale-down failed")
                        else:
                            logger.info("😌 Cluster load is stable")
                else:
                    remaining = self.cooldown_minutes - ((datetime.now() - self.last_scale_time).seconds // 60)
                    logger.info(f"⏸️ In cooldown ({remaining}min remaining)")
            except Exception as e:
                logger.error(f"❌ Error in monitoring loop: {e}")
                import traceback
                traceback.print_exc()
            
            logger.info("=" * 60)
            time.sleep(self.check_interval)

def main():
    autoscaler = RedisAutoscaler()
    autoscaler.monitor_and_scale()

if __name__ == "__main__":
    main()

# SAVE THIS

    # scale read 
    # write latecny gets bad why? how can we go around it
    # resharding is a very heavy operation (last resort operation)
    # explore everyhting before resharding
        # add repliacas
        # scaling up container 
    # tie into the decision tree
        # track read and write requests (ratio)
        # figure out the distribution of the load (for both read and write)

        # what is the bottleneck (CPU, disk, etc) *dont make
        # what should be the decision that we should be resharding

        # if read heavy + high CPU 
            # add replica
        
        # figure out how the breakdown of the variables determine which one you should do
    

# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# make it deliverable by researching what issues peoiple are looking for in redis that you can adress and focus on the issues taht you can tackle
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

# keep posting on slack

# clickhouse and elasticsearch