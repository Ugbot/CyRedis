#!/bin/sh

# Wait a bit more to ensure all nodes are fully ready
echo "Waiting for all cluster nodes to be ready..."
sleep 5

# Check if cluster is already initialized
CLUSTER_INFO=$(redis-cli -h redis-cluster-node1 -p 7000 cluster info 2>/dev/null | grep cluster_state | cut -d: -f2 | tr -d '\r\n')

if [ "$CLUSTER_INFO" = "ok" ]; then
    echo "Redis cluster is already initialized and running"
    exit 0
fi

echo "Initializing Redis cluster..."

# Create cluster with 3 masters and 3 replicas
# Format: redis-cli --cluster create host1:port1 host2:port2 ... --cluster-replicas <num_replicas>
# The first 3 nodes will be masters, the next 3 will be replicas

redis-cli --cluster create \
    redis-cluster-node1:7000 \
    redis-cluster-node2:7001 \
    redis-cluster-node3:7002 \
    redis-cluster-node4:7003 \
    redis-cluster-node5:7004 \
    redis-cluster-node6:7005 \
    --cluster-replicas 1 \
    --cluster-yes

if [ $? -eq 0 ]; then
    echo "Redis cluster initialized successfully"

    # Display cluster information
    echo ""
    echo "Cluster nodes:"
    redis-cli -h redis-cluster-node1 -p 7000 cluster nodes

    echo ""
    echo "Cluster info:"
    redis-cli -h redis-cluster-node1 -p 7000 cluster info
else
    echo "Failed to initialize Redis cluster"
    exit 1
fi
