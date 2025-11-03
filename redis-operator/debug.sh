#!/bin/sh
set -x # Print commands as they run, for easy debugging

# --- START: DEBUGGING TRAP (FIXED) ---
# This trap now correctly saves and propagates the REAL exit code.
trap 'exit_code=$?; echo "--- SCRIPT EXITED (Status: $exit_code) ---"; echo "Pod will stay alive for 10 minutes."; sleep 600; exit $exit_code' EXIT
# --- END: DEBUGGING TRAP ---

# Turn on exit-on-error for setup
set -e

echo "--- Smart Reshard Job Started ---"
# Hardcoding a 10-minute (600s) timeout for testing
wait_until=$(($(date +%s) + 600))
rebalance_timeout=$((600 - 30))
if [ $rebalance_timeout -lt 60 ]; then
	rebalance_timeout=60
fi

# --- Manually set variables for testing ---
EXPECTED_NODES=10
ANY_POD_HOST="redis-cluster-0.redis-cluster-headless.default.svc.cluster.local"
ANY_POD_PORT="6379"
ANY_POD_ENTRYPOINT="redis-cluster-0.redis-cluster-headless.default.svc.cluster.local:6379"
CLUSTER_NAME="redis-cluster"
SERVICE_NAME="redis-cluster-headless"
NAMESPACE="default"
# ---

# --- NEW: Fix cluster state before doing anything ---
echo "Running 'redis-cli --cluster fix' to clean up any stuck slots..."
# We pipe 'yes' to automatically approve any fixes (like for stuck slots)
# --- THIS IS THE FIX ---
yes | redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT --cluster fix $ANY_POD_ENTRYPOINT
# --- END FIX ---
echo "Cluster fix complete."
# --- END NEW BLOCK ---

# --- Loop 1: Find and Add Orphaned Master/Replica Pairs ---
echo "Checking for orphaned nodes to add to the cluster..."

# Get the list of nodes ALREADY in the cluster by their IP
cluster_nodes_output=$(redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT cluster nodes)
actual_nodes_count=$(echo "$cluster_nodes_output" | grep -v fail | wc -l)
# Get just the hostnames/IPs, remove ports
known_node_ips=$(echo "$cluster_nodes_output" | grep -v fail | awk '{ print $2 }' | cut -d'@' -f1 | cut -d':' -f1)

echo "--- Known Node IPs ---"
echo "$known_node_ips"
echo "----------------------"


if [ "$actual_nodes_count" -ge "$EXPECTED_NODES" ]; then
	echo "All $EXPECTED_NODES nodes are already in the cluster. Proceeding to rebalance check."
else
	echo "Finding orphan nodes to add..."
	orphan_nodes_dns=""

	# Loop from 0 to (EXPECTED_NODES - 1) and build a list of all pod DNS names
	for i in $(seq 0 $(($EXPECTED_NODES - 1))); do
		pod_dns_base="${CLUSTER_NAME}-${i}.${SERVICE_NAME}.${NAMESPACE}.svc.cluster.local"
		
		pod_ip=$(getent hosts $pod_dns_base | awk '{ print $1 }' || true) 

		if [ -z "$pod_ip" ]; then
			echo "WARNING: Could not resolve DNS for $pod_dns_base. Skipping."
			continue
		fi
		
		# Check if this pod's IP is in the list of known IPs
		if ! echo "$known_node_ips" | grep -q "$pod_ip"; then
			echo "Found orphan: $pod_dns_base (IP: $pod_ip)"
			orphan_nodes_dns="$orphan_nodes_dns $pod_dns_base"
		else
			echo "Node $pod_dns_base (IP: $pod_ip) is already known. Skipping."
		fi
	done

	if [ -z "$orphan_nodes_dns" ]; then
		echo "ERROR: Node count mismatch ($actual_nodes_count vs $EXPECTED_NODES) but no orphan nodes found."
		echo "Known node IPs:"
		echo "$known_node_ips"
		exit 1
	fi

	echo "Found orphan(s): $orphan_nodes_dns"

	master_dns_base=""
	for orphan_dns in $orphan_nodes_dns; do
		if [ -z "$master_dns_base" ]; then
			# This is the first node in a pair, save it as master
			master_dns_base=$orphan_dns
		else
			# This is the second node, save it as replica
			replica_dns_base=$orphan_dns
			master_dns_port="${master_dns_base}:6379"
			replica_dns_port="${replica_dns_base}:6379"

			# --- ADD THE PAIR ---
			echo "Adding pair: MASTER=$master_dns_base, REPLICA=$replica_dns_base"
			
			# 1. Add Master
			echo "Adding new master: $master_dns_port"
			if ! redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT --cluster add-node $master_dns_port $ANY_POD_HOST:$ANY_POD_PORT; then
				echo "WARNING: Failed to add master $master_dns_port. Checking if it already exists..."
				master_ip_check=$(getent hosts $master_dns_base | awk '{ print $1 }')
				if [ -z "$master_ip_check" ] || ! redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT cluster nodes | grep -q $master_ip_check; then
					echo "ERROR: Failed add-node and node is not in cluster. Exiting."
					exit 1
				fi
				echo "Node $master_dns_base already exists, proceeding..."
			fi
			
			echo "Waiting up to 60s for new master $master_dns_base to be fully recognized..."
			master_wait_until=$(($(date +%s) + 60))
			master_ip=$(getent hosts $master_dns_base | awk '{ print $1 }')
			new_master_id=""
			
			while [ $(date +%s) -lt $master_wait_until ]; do
				new_master_id=$(redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT cluster nodes | grep $master_ip | grep master | awk '{ print $1 }' || true)
				if [ -n "$new_master_id" ]; then
					echo "New master is recognized with ID: $new_master_id"
					break
				fi
				echo "Waiting for $master_dns_base to be listed as a master..."
				sleep 2
			done
			
			if [ -z "$new_master_id" ]; then
				echo "ERROR: Timeout waiting for new master $master_dns_base to be recognized."
				redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT cluster nodes
				exit 1
			fi

			# 3. Add Replica
			echo "Adding new replica $replica_dns_port for master $new_master_id"
			if ! redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT --cluster add-node $replica_dns_port $ANY_POD_HOST:$ANY_POD_PORT --cluster-slave --cluster-master-id $new_master_id; then
				echo "WARNING: Failed to add replica $replica_dns_port. Checking if it already exists..."
				replica_ip=$(getent hosts $replica_dns_base | awk '{ print $1 }')
				if [ -z "$replica_ip" ] || ! redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT cluster nodes | grep -q $replica_ip; then
					echo "ERROR: Failed add-node and replica is not in cluster. Exiting."
					exit 1
				fi
				echo "Node $replica_dns_base already exists, proceeding..."
			fi
			
			master_dns_base=""
		fi
	done

	if [ -n "$master_dns_base" ]; then
		echo "WARNING: Found odd number of orphans. Adding last node $master_dns_base as a master without a replica."
		master_dns_port="${master_dns_base}:6379"
		if ! redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT --cluster add-node $master_dns_port $ANY_POD_HOST:$ANY_POD_PORT; then
			echo "WARNING: Failed to add master $master_dns_port. Checking if it already exists..."
			master_ip_check=$(getent hosts $master_dns_base | awk '{ print $1 }')
			if [ -z "$master_ip_check" ] || ! redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT cluster nodes | grep -q $master_ip_check; then
				echo "ERROR: Failed add-node and node is not in cluster. Exiting."
				exit 1
			fi
		fi
	fi
fi

# --- Loop 2: Wait for all nodes to join ---
echo "Waiting for all $EXPECTED_NODES nodes to be registered in the cluster..."
while true; do
	cluster_nodes_output_check=$(redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT cluster nodes)
	actual_nodes=$(echo "$cluster_nodes_output_check" | grep -v fail | wc -l)
	
	if [ "$actual_nodes" -eq "$EXPECTED_NODES" ]; then
		echo "All $EXPECTED_NODES nodes have joined."
		break
	fi
	
	if [ $(date +%s) -gt $wait_until ]; then
		echo "Timeout: Not all nodes joined. Expected $EXPECTED_NODES, got $actual_nodes"
		echo "$cluster_nodes_output_check"
		exit 1
	fi
	
	echo "Waiting... $actual_nodes/$EXPECTED_NODES nodes joined."
	sleep 5
done

# --- Loop 3: Wait for an empty master ---
echo "Waiting for an empty master to be ready..."
while true; do
	cluster_nodes_output=$(redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT cluster nodes)
	
	empty_masters=$(echo "$cluster_nodes_output" | grep master | grep -v fail | awk '$9==""' | wc -l)

	if [ "$empty_masters" -gt 0 ]; then
		echo "Found $empty_masters empty master(s). Ready to rebalance."
		break
	fi

	if [ $(date +%s) -gt $wait_until ]; then
		echo "Timeout: All nodes joined, but no empty master found."
		echo "$cluster_nodes_output"
		exit 1
	fi
	
	echo "Waiting... all nodes joined, but 0 empty masters found."
	sleep 5
done

# --- Execute: Run the rebalance ---
echo "Disabling full coverage check to prevent CLUSTERDOWN error during rebalance..."
redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT config set cluster-require-full-coverage no

echo "Starting cluster rebalance with $rebalance_timeout second timeout..."
timeout $rebalance_timeout redis-cli --cluster rebalance $ANY_POD_ENTRYPOINT --cluster-use-empty-masters --cluster-yes

echo "Re-enabling full coverage check..."
redis-cli -h $ANY_POD_HOST -p $ANY_POD_PORT config set cluster-require-full-coverage yes

echo "Rebalance command finished successfully."
echo "--- Smart Reshard Job Finished ---"
