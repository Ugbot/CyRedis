# CyRedis Docker Test Infrastructure

This directory contains a comprehensive Docker Compose setup for testing the CyRedis library against various Redis configurations.

## Overview

The test infrastructure provides three Redis deployment modes:

1. **Redis Standalone** - Single Redis instance for basic testing
2. **Redis Cluster** - 6-node cluster (3 masters + 3 replicas) for distributed testing
3. **Redis Sentinel** - High availability setup with 1 master, 2 replicas, and 3 sentinels

## Services

### Redis Standalone

- **Container**: `cyredis-standalone`
- **Port**: `6379`
- **Purpose**: Basic Redis functionality testing
- **Connection**: `redis://localhost:6379`
- **Features**:
  - AOF persistence enabled
  - 256MB memory limit
  - LRU eviction policy

### Redis Cluster

A 6-node Redis cluster with automatic sharding:

| Node | Container | Port | Bus Port | Role |
|------|-----------|------|----------|------|
| Node 1 | cyredis-cluster-node1 | 7000 | 17000 | Master |
| Node 2 | cyredis-cluster-node2 | 7001 | 17001 | Master |
| Node 3 | cyredis-cluster-node3 | 7002 | 17002 | Master |
| Node 4 | cyredis-cluster-node4 | 7003 | 17003 | Replica |
| Node 5 | cyredis-cluster-node5 | 7004 | 17004 | Replica |
| Node 6 | cyredis-cluster-node6 | 7005 | 17005 | Replica |

- **Connection**: `redis://localhost:7000` (any node, cluster-aware clients will discover others)
- **Purpose**: Testing cluster mode, sharding, and failover
- **Features**:
  - Automatic slot distribution
  - Built-in replication
  - Automatic failover
  - 256MB memory per node

### Redis Sentinel

High-availability setup with automatic failover:

#### Redis Instances

| Instance | Container | Port | Role |
|----------|-----------|------|------|
| Master | cyredis-sentinel-master | 6380 | Master |
| Replica 1 | cyredis-sentinel-replica1 | 6381 | Replica |
| Replica 2 | cyredis-sentinel-replica2 | 6382 | Replica |

#### Sentinel Instances

| Sentinel | Container | Port |
|----------|-----------|------|
| Sentinel 1 | cyredis-sentinel1 | 26379 |
| Sentinel 2 | cyredis-sentinel2 | 26380 |
| Sentinel 3 | cyredis-sentinel3 | 26381 |

- **Master Name**: `mymaster`
- **Connection (Master)**: `redis://localhost:6380`
- **Sentinel Connection**: `sentinel://localhost:26379,localhost:26380,localhost:26381`
- **Quorum**: 2 sentinels required for failover
- **Purpose**: Testing high-availability and automatic failover scenarios

## Quick Start

### Start All Services

```bash
cd tests/docker
docker compose up -d
```

### Start Specific Services

```bash
# Standalone only
docker compose up -d redis-standalone

# Cluster only
docker compose up -d redis-cluster-node1 redis-cluster-node2 redis-cluster-node3 \
                     redis-cluster-node4 redis-cluster-node5 redis-cluster-node6 \
                     redis-cluster-init

# Sentinel only
docker compose up -d redis-sentinel-master redis-sentinel-replica1 redis-sentinel-replica2 \
                     redis-sentinel1 redis-sentinel2 redis-sentinel3
```

### Check Status

```bash
# View all running containers
docker compose ps

# View logs
docker compose logs -f

# View logs for specific service
docker compose logs -f redis-standalone
docker compose logs -f redis-cluster-init
docker compose logs -f redis-sentinel1
```

### Stop Services

```bash
# Stop all services
docker compose down

# Stop and remove volumes (clean slate)
docker compose down -v
```

## Testing Connection

### Standalone

```bash
# Using redis-cli
docker compose exec redis-standalone redis-cli ping

# Using Python (from your tests)
import redis
r = redis.Redis(host='localhost', port=6379)
r.ping()
```

### Cluster

```bash
# Using redis-cli
docker compose exec redis-cluster-node1 redis-cli -c -p 7000 cluster info

# Using Python (from your tests)
from redis.cluster import RedisCluster
rc = RedisCluster(host='localhost', port=7000)
rc.ping()
```

### Sentinel

```bash
# Check master info via sentinel
docker compose exec redis-sentinel1 redis-cli -p 26379 sentinel master mymaster

# Connect to master via redis-cli
docker compose exec redis-sentinel-master redis-cli -p 6380 ping

# Using Python (from your tests)
from redis.sentinel import Sentinel
sentinel = Sentinel([('localhost', 26379), ('localhost', 26380), ('localhost', 26381)])
master = sentinel.master_for('mymaster', socket_timeout=0.1)
master.ping()
```

## Health Checks

All services include health checks that monitor:

- Redis instance availability (ping)
- Service startup readiness
- Connection health

Wait for all services to be healthy before running tests:

```bash
# Check health status
docker compose ps

# Wait for specific service
timeout 60 bash -c 'until docker compose exec redis-standalone redis-cli ping &> /dev/null; do sleep 1; done'
```

## CI/CD Integration

### GitHub Actions Example

```yaml
services:
  redis-standalone:
    image: redis:7-alpine
    ports:
      - 6379:6379
    options: >-
      --health-cmd "redis-cli ping"
      --health-interval 5s
      --health-timeout 3s
      --health-retries 5
```

### Using Docker Compose in CI

```bash
# Start services
docker compose -f tests/docker/docker-compose.yml up -d

# Wait for health
docker compose -f tests/docker/docker-compose.yml ps

# Run tests
pytest tests/

# Cleanup
docker compose -f tests/docker/docker-compose.yml down -v
```

## Advanced Usage

### Cluster Operations

```bash
# View cluster nodes
docker compose exec redis-cluster-node1 redis-cli -p 7000 cluster nodes

# Check cluster info
docker compose exec redis-cluster-node1 redis-cli -p 7000 cluster info

# Check key slot
docker compose exec redis-cluster-node1 redis-cli -p 7000 cluster keyslot mykey

# Rebalance cluster (if needed)
docker compose exec redis-cluster-init redis-cli --cluster rebalance redis-cluster-node1:7000
```

### Sentinel Operations

```bash
# Get master info
docker compose exec redis-sentinel1 redis-cli -p 26379 sentinel master mymaster

# Get replicas info
docker compose exec redis-sentinel1 redis-cli -p 26379 sentinel replicas mymaster

# Get sentinel info
docker compose exec redis-sentinel1 redis-cli -p 26379 sentinel sentinels mymaster

# Manually trigger failover (for testing)
docker compose exec redis-sentinel1 redis-cli -p 26379 sentinel failover mymaster

# Check sentinel monitoring
docker compose exec redis-sentinel1 redis-cli -p 26379 sentinel ckquorum mymaster
```

### Data Persistence

All services use Docker volumes for data persistence:

```bash
# List volumes
docker volume ls | grep cyredis

# Inspect volume
docker volume inspect tests_redis-standalone-data

# Backup volume
docker run --rm -v tests_redis-standalone-data:/data -v $(pwd):/backup alpine tar czf /backup/redis-backup.tar.gz /data

# Restore volume
docker run --rm -v tests_redis-standalone-data:/data -v $(pwd):/backup alpine tar xzf /backup/redis-backup.tar.gz -C /
```

### Monitoring and Debugging

```bash
# Monitor commands in real-time
docker compose exec redis-standalone redis-cli monitor

# Get server statistics
docker compose exec redis-standalone redis-cli info

# Check memory usage
docker compose exec redis-standalone redis-cli info memory

# View slow queries
docker compose exec redis-standalone redis-cli slowlog get 10

# Check latency
docker compose exec redis-standalone redis-cli --latency

# Memory analysis
docker compose exec redis-standalone redis-cli --bigkeys
```

## Troubleshooting

### Cluster Won't Initialize

```bash
# Check if nodes are running
docker compose ps

# Manually initialize cluster
docker compose exec redis-cluster-node1 redis-cli --cluster create \
  redis-cluster-node1:7000 redis-cluster-node2:7001 redis-cluster-node3:7002 \
  redis-cluster-node4:7003 redis-cluster-node5:7004 redis-cluster-node6:7005 \
  --cluster-replicas 1 --cluster-yes

# Reset cluster state
docker compose down -v
docker compose up -d
```

### Sentinel Not Detecting Master

```bash
# Check sentinel logs
docker compose logs redis-sentinel1

# Verify master configuration
docker compose exec redis-sentinel-master redis-cli -p 6380 info replication

# Reset sentinel state
docker compose restart redis-sentinel1 redis-sentinel2 redis-sentinel3
```

### Connection Issues

```bash
# Check network
docker network inspect tests_cyredis-test

# Verify port bindings
docker compose ps

# Test connectivity
docker compose exec redis-standalone redis-cli -h localhost -p 6379 ping
```

## Configuration Files

- `docker-compose.yml` - Main orchestration file
- `redis-cluster/redis-cluster.conf` - Cluster node configuration
- `redis-cluster/init-cluster.sh` - Cluster initialization script
- `redis-sentinel/sentinel.conf` - Sentinel configuration
- `redis-sentinel/redis-master.conf` - Master node configuration
- `redis-sentinel/redis-replica.conf` - Replica node configuration

## Environment Variables

You can customize the setup using environment variables:

```bash
# Set custom ports
REDIS_PORT=6380 docker compose up -d redis-standalone

# Set custom memory limit
REDIS_MAXMEMORY=512mb docker compose up -d
```

## Performance Tuning

The configurations include production-ready settings:

- AOF persistence with `everysec` fsync
- Memory limits with LRU eviction
- Replication with diskless sync
- Latency monitoring enabled
- Slow query logging
- TCP keepalive enabled
- Optimized buffer limits

Adjust these in the configuration files based on your testing needs.

## Network Architecture

All services run on the `cyredis-test` bridge network, allowing:

- Inter-container communication using service names
- Port mapping to localhost for external access
- Isolated network for testing
- DNS resolution between containers

## Security Notes

This setup is designed for **local development and testing only**:

- Protected mode is disabled
- No authentication configured
- All ports exposed to localhost
- Bind to all interfaces within Docker network

**Do not use these configurations in production!**

For production deployments:
- Enable protected mode
- Set strong passwords (`requirepass`)
- Use TLS/SSL encryption
- Restrict network access
- Enable ACLs (Redis 6+)
- Regular backups
- Monitor security events

## License

Part of the CyRedis project.
