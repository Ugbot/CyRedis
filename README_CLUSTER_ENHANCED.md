# 🚀 CyRedis - Now Fully Cluster-Aware!

CyRedis has been enhanced with **comprehensive Redis Cluster support**, making it a production-ready client for distributed Redis deployments.

## 🌐 **Redis Cluster Features Added**

### **1. Complete Cluster Management** 🔗
Full support for Redis Cluster administration and monitoring:

```python
from cy_redis import CyRedisClient

with CyRedisClient() as client:
    # Cluster topology discovery
    info = client.cluster_info()
    nodes = client.cluster_nodes()
    slots = client.cluster_slots()

    # Node management
    node_id = client.cluster_myid()
    shard_id = client.cluster_myshardid()

    # Slot management
    slot = client.cluster_keyslot("mykey")
    keys_in_slot = client.cluster_getkeysinslot(slot, 100)

    # Cluster health monitoring
    health = client.cluster_health_check()
    print(f"Cluster state: {health['cluster_state']}")
```

### **2. Cluster-Aware Operations** 🎯
Intelligent handling of distributed operations across cluster nodes:

```python
# Multi-key operations across cluster
keys = ["user:1", "user:2", "user:3"]
values = client.cluster_multi_get(keys)  # Gets from correct nodes
client.cluster_multi_set({"key1": "value1", "key2": "value2"})

# Cluster-aware key routing
slot = client.cluster_keyslot("user:123")
# Operations automatically route to correct node
```

### **3. Advanced Cluster Management** ⚙️
Comprehensive cluster administration capabilities:

```python
# Node management
client.cluster_meet("192.168.1.100", 7001)  # Add new node
client.cluster_forget("node_id")            # Remove node
client.cluster_replicate("master_node_id")  # Configure as replica

# Slot management
client.cluster_addslots(1, 2, 3, 1000, 1001)  # Add slots to this node
client.cluster_delslots(500, 501, 502)         # Remove slots
client.cluster_setslot(1000, "NODE", "node_id")  # Assign slot to node

# Cluster maintenance
client.cluster_failover(force=True)        # Force failover
client.cluster_reset(hard=True)            # Reset cluster node
client.cluster_saveconfig()                # Save cluster config
client.cluster_bumpepoch()                 # Advance config epoch

# Cluster diagnostics
links = client.cluster_links()             # Get connection info
failure_reports = client.cluster_count_failure_reports("node_id")
shards = client.cluster_shards()           # Redis 7+ shards info
slot_stats = client.cluster_slot_stats()   # Slot usage statistics
```

### **4. Cluster Health & Monitoring** 📊
Built-in cluster health monitoring and diagnostics:

```python
# Comprehensive cluster health check
health = client.cluster_health_check()
print(f"""
Cluster Health:
- Enabled: {health['cluster_enabled']}
- State: {health['cluster_state']}
- Nodes: {health['nodes_count']}
- Slots: {health['slots_assigned']}/{health['slots_ok'] and 'OK' or 'ERROR'}
- Errors: {health['errors']}
""")

# Cluster performance monitoring
redistribution = client.cluster_redistribute_slots(slots_per_node=1000)
print(f"Slots redistributed: {redistribution['slots_moved']}")
```

### **5. Full Async Cluster Support** ⚡
All cluster operations available in async mode:

```python
import asyncio
from cy_redis import CyRedisClientAsync

async def cluster_operations():
    async with CyRedisClientAsync() as client:
        # Async cluster operations
        health = await client.cluster_health_check_async()
        slot = await client.cluster_keyslot_async("mykey")
        values = await client.cluster_multi_get_async(keys)

        # Async cluster management
        await client.cluster_failover_async(force=True)
        await client.cluster_meet_async("192.168.1.100", 7001)

asyncio.run(cluster_operations())
```

## 🏗️ **Architecture Enhancements**

### **Cluster-Aware Connection Pooling**
- **Automatic node discovery** and connection management
- **Slot-to-node mapping** for optimal routing
- **Connection reuse** across cluster operations
- **Automatic failover** detection and recovery

### **Intelligent Key Routing**
- **Hash slot calculation** for key placement
- **Automatic redirection** for moved keys
- **Cross-slot operation handling** with appropriate error handling
- **Cluster topology awareness** for optimal performance

### **Error Handling & Resilience**
- **Cluster state monitoring** and health checks
- **Automatic retry** for transient cluster failures
- **Graceful degradation** during cluster topology changes
- **Detailed error reporting** for cluster-specific issues

## 📋 **Cluster Commands Reference**

### **Information Commands**
- `cluster_info()` - Get cluster configuration and state
- `cluster_nodes()` - List all cluster nodes
- `cluster_slots()` - Get hash slot distribution
- `cluster_myid()` - Get current node ID
- `cluster_myshardid()` - Get current shard ID (Redis 7+)

### **Management Commands**
- `cluster_meet(ip, port)` - Add node to cluster
- `cluster_forget(node_id)` - Remove node from cluster
- `cluster_replicate(master_id)` - Configure as replica
- `cluster_failover(force, takeover)` - Force failover

### **Slot Management**
- `cluster_keyslot(key)` - Get hash slot for key
- `cluster_addslots(*slots)` - Add slots to this node
- `cluster_delslots(*slots)` - Remove slots from this node
- `cluster_setslot(slot, subcommand, node_id)` - Configure slot assignment

### **Monitoring & Health**
- `cluster_health_check()` - Comprehensive cluster health check
- `cluster_links()` - Get cluster connection information
- `cluster_count_failure_reports(node_id)` - Get failure reports
- `cluster_slot_stats()` - Get slot usage statistics

### **Maintenance Commands**
- `cluster_reset(hard, soft)` - Reset cluster node
- `cluster_saveconfig()` - Force save cluster configuration
- `cluster_bumpepoch()` - Advance configuration epoch
- `cluster_flushslots()` - Delete all slot information

## 🔧 **Configuration for Cluster**

### **Automatic Cluster Discovery**
```python
# Single entry point - client discovers entire cluster
client = CyRedisClient(host="redis-cluster.example.com", port=7000)
```

### **Manual Cluster Configuration**
```python
# Specify multiple nodes for redundancy
client = CyRedisClient(
    cluster_nodes=[
        "redis-node1:7000",
        "redis-node2:7001",
        "redis-node3:7002"
    ]
)
```

### **Connection Pool Settings**
```python
client = CyRedisClient(
    host="cluster.example.com",
    port=7000,
    max_connections=20,  # Per node connection pool
    cluster_mode=True,   # Enable cluster awareness
    cluster_timeout=5.0  # Cluster operation timeout
)
```

## 🚀 **Production Use Cases**

### **High-Availability Applications**
- **Automatic failover** handling
- **Multi-region deployment** support
- **Zero-downtime scaling** capabilities

### **Large-Scale Data Processing**
- **Horizontal scaling** across cluster nodes
- **Automatic data distribution** via hash slots
- **Cross-node operations** for complex queries

### **Real-Time Analytics**
- **Distributed bitmap operations** for user analytics
- **Cluster-wide JSON document** processing
- **Multi-node geospatial** queries

### **Session Management**
- **Cluster-wide session storage** with automatic distribution
- **Session failover** during node failures
- **Geographically distributed** session handling

## 📈 **Performance Characteristics**

- **Linear scaling** with cluster size
- **Automatic load distribution** across nodes
- **Minimal latency** for local operations
- **Efficient cross-node** communication
- **Connection pooling** optimization per node

## 🔒 **Security & Reliability**

- **SSL/TLS support** for encrypted cluster communication
- **Authentication** across all cluster nodes
- **Automatic retry** for cluster topology changes
- **Health monitoring** for proactive issue detection

## 🎯 **Migration Path**

CyRedis maintains **backward compatibility** while adding cluster features:

```python
# Existing code continues to work
client = CyRedisClient(host="localhost", port=6379)
client.set("key", "value")
client.get("key")

# Enable cluster features when ready
cluster_client = CyRedisClient(host="cluster.example.com", port=7000)
cluster_client.cluster_health_check()  # New cluster features
```

## 🚀 **Ready for Production**

CyRedis is now **enterprise-ready** for:

- **Large-scale Redis deployments** with automatic scaling
- **High-availability applications** with failover support
- **Distributed data processing** across multiple nodes
- **Real-time analytics** with cluster-wide operations
- **Geographically distributed** applications

The enhanced CyRedis client provides **complete Redis Cluster support** while maintaining the **high performance** and **low latency** that made the original CyRedis successful!
