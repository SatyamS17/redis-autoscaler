#!/bin/sh
set -ex

echo "Starting cluster scale-up job..."
echo "Entrypoint is $ENTRYPOINT_HOST"

# 1. Get all pod IPs from DNS
# We nslookup the headless service to get all pod IPs
POD_IPS=$(nslookup $HEADLESS_SVC | grep 'Address:' | awk 'NR>1 {print $2}')
if [ -z "$POD_IPS" ]; then
    echo "FATAL: Could not resolve any IPs for headless service $HEADLESS_SVC"
    exit 1
fi
echo "Found Pod IPs: $POD_IPS"

# 2. Get all node IPs already in the cluster
KNOWN_NODE_IPS=$(redis-cli -h $ENTRYPOINT_HOST cluster nodes | awk '{print $2}' | sed 's/:.*//')
echo "Found Known Cluster IPs: $KNOWN_NODE_IPS"

# 3. Loop and add any new nodes
NEW_NODES_ADDED=false
for IP in $POD_IPS; do
    if ! echo "$KNOWN_NODE_IPS" | grep -q "$IP"; then
        echo "New node $IP not found in cluster. Adding..."
        redis-cli --cluster add-node $IP:6379 $ENTRYPOINT_HOST || echo "add-node failed, assuming node is joining."
        NEW_NODES_ADDED=true
    else
        echo "Node $IP is already in the cluster."
    fi
done

if [ "$NEW_NODES_ADDED" = true ]; then
    echo "New nodes were added, sleeping 5s for them to handshake..."
    sleep 5
fi

# 4. Now that all nodes are added, rebalance to use the new empty masters.
echo "All nodes are in the cluster. Rebalancing..."
redis-cli --cluster rebalance $ENTRYPOINT_HOST --cluster-use-empty-masters --cluster-yes

echo "Scale-up and rebalance job complete."