#!/bin/sh
set -ex

echo "Attempting to drain pod with IP: $POD_TO_DRAIN_IP"
echo "Entrypoint is $ENTRYPOINT_HOST"

# 1. Find the Redis Node ID for the pod we want to drain
NODE_ID=$(redis-cli -h $ENTRYPOINT_HOST cluster nodes | grep "$POD_TO_DRAIN_IP:6379" | awk '{ print $1 }')

if [ -z "${NODE_ID}" ]; then
  echo "Could not find node with IP $POD_TO_DRAIN_IP. Exiting gracefully."
  exit 0
fi
echo "Found node ID ${NODE_ID} for IP $POD_TO_DRAIN_IP"

# 2. Check if the node is a master and has slots
SLOTS=$(redis-cli -h $ENTRYPOINT_HOST cluster nodes | grep "${NODE_ID}" | awk '{ print $9 }')
if [ -z "${SLOTS}" ]; then
  echo "Node ${NODE_ID} is not a master or has no slots. No draining needed."
  exit 0
fi

# 3. Find a recipient node
TO_NODE_ID=$(redis-cli -h $ENTRYPOINT_HOST cluster nodes | grep master | grep -v "${NODE_ID}" | head -n 1 | awk '{ print $1 }')
if [ -z "${TO_NODE_ID}" ]; then
  echo "FATAL: Could not find another master to migrate slots to."
  exit 1
fi

# 4. Migrate slots
echo "Migrating all slots from ${NODE_ID} to ${TO_NODE_ID}"
redis-cli --cluster reshard $ENTRYPOINT_HOST --cluster-from ${NODE_ID} --cluster-to ${TO_NODE_ID} --cluster-slots 16384 --cluster-yes

echo "Drain successful."