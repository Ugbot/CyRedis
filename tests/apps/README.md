# CyRedis Test Applications

This directory contains complete, runnable applications that demonstrate CyRedis library capabilities. Each app showcases specific features and patterns for building real-world systems with Redis.

## 📋 Table of Contents

- [Quick Start](#quick-start)
- [Applications](#applications)
- [Prerequisites](#prerequisites)
- [Common Patterns](#common-patterns)

## 🚀 Quick Start

All applications can be run directly with Python. Make sure Redis is running (either locally or via Docker):

```bash
# Start Redis via Docker
cd tests/docker
docker compose up -d redis-standalone

# Run any application
python tests/apps/<app_name>.py --help
```

## 📱 Applications

### 1. Simple Key-Value Store (`simple_kv_app.py`)

A basic key-value store with CLI interface.

**Features:**
- SET/GET/DELETE operations
- Key expiration (TTL)
- Batch operations
- Key pattern searching

**Usage:**
```bash
# Set a key
python simple_kv_app.py set mykey "Hello World"

# Get a key
python simple_kv_app.py get mykey

# Set with expiration
python simple_kv_app.py set session:123 "user_data" --ttl 3600

# List keys
python simple_kv_app.py keys "session:*"

# Delete a key
python simple_kv_app.py delete mykey
```

**Use Cases:**
- Configuration storage
- Session management
- Cache layer

---

### 2. Message Queue System (`message_queue_app.py`)

Producer-consumer message queue implementation.

**Features:**
- Multiple producers and consumers
- Message persistence
- FIFO ordering
- Blocking pop operations
- Message acknowledgment

**Usage:**
```bash
# Start consumer
python message_queue_app.py consume --queue tasks

# Produce messages
python message_queue_app.py produce --queue tasks --count 10

# Monitor queue
python message_queue_app.py status --queue tasks
```

**Use Cases:**
- Task queues
- Event processing
- Work distribution

---

### 3. Distributed Cache (`distributed_cache_app.py`)

Write-through/write-behind cache with eviction policies.

**Features:**
- TTL-based expiration
- LRU eviction
- Cache hit/miss tracking
- Batch operations
- Cache warming

**Usage:**
```bash
# Start cache server
python distributed_cache_app.py server

# Set cached values
python distributed_cache_app.py set user:123 '{"name":"John"}' --ttl 300

# Get with stats
python distributed_cache_app.py get user:123

# View statistics
python distributed_cache_app.py stats
```

**Use Cases:**
- Database query cache
- API response cache
- Session storage

---

### 4. Task Scheduler (`task_scheduler_app.py`)

Distributed task scheduling system.

**Features:**
- Scheduled task execution
- Recurring tasks (cron-like)
- Task priorities
- Delayed execution
- Task cancellation

**Usage:**
```bash
# Start scheduler
python task_scheduler_app.py scheduler

# Schedule a task
python task_scheduler_app.py schedule "send_email" --delay 60 --data '{"to":"user@example.com"}'

# Schedule recurring task
python task_scheduler_app.py schedule "backup" --cron "0 2 * * *"

# List scheduled tasks
python task_scheduler_app.py list
```

**Use Cases:**
- Cron job replacement
- Delayed notifications
- Batch processing

---

### 5. Session Manager (`session_manager_app.py`)

Distributed session management for web applications.

**Features:**
- Session creation/destruction
- Session data storage
- TTL-based expiration
- Session refresh
- Multi-device support

**Usage:**
```bash
# Create session
python session_manager_app.py create --user-id 123

# Get session data
python session_manager_app.py get <session-id>

# Update session
python session_manager_app.py update <session-id> --data '{"page":"dashboard"}'

# Refresh TTL
python session_manager_app.py refresh <session-id>

# Delete session
python session_manager_app.py delete <session-id>
```

**Use Cases:**
- Web application sessions
- JWT token storage
- User state management

---

### 6. Rate Limiter (`rate_limiter_app.py`)

Token bucket and sliding window rate limiting.

**Features:**
- Per-user rate limits
- Multiple time windows
- Distributed rate limiting
- Custom limits per endpoint
- Rate limit headers

**Usage:**
```bash
# Start rate limiter service
python rate_limiter_app.py service

# Check rate limit
python rate_limiter_app.py check --user-id user123 --endpoint /api/data

# Test rate limiting
python rate_limiter_app.py test --user-id user123 --requests 100

# View limits
python rate_limiter_app.py limits
```

**Use Cases:**
- API rate limiting
- DDoS prevention
- Resource throttling

---

### 7. Pub/Sub Chat (`pubsub_chat_app.py`)

Real-time chat using Redis Pub/Sub.

**Features:**
- Multiple chat rooms
- Private messages
- User presence
- Message history (optional)
- Broadcasting

**Usage:**
```bash
# Join chat room
python pubsub_chat_app.py chat --room lobby --username Alice

# Send message
# (type messages in the interactive prompt)

# List active rooms
python pubsub_chat_app.py rooms
```

**Use Cases:**
- Chat applications
- Real-time notifications
- Live updates

---

### 8. Leaderboard (`leaderboard_app.py`)

Real-time leaderboard using sorted sets.

**Features:**
- Score updates
- Ranking queries
- Top N players
- Range queries
- Score increment/decrement

**Usage:**
```bash
# Start leaderboard service
python leaderboard_app.py service

# Add/update score
python leaderboard_app.py score --player Alice --score 1000

# Increment score
python leaderboard_app.py increment --player Alice --amount 50

# View top players
python leaderboard_app.py top --count 10

# Get player rank
python leaderboard_app.py rank --player Alice

# View leaderboard
python leaderboard_app.py view
```

**Use Cases:**
- Gaming leaderboards
- Social rankings
- Trending content

---

### 9. Job Queue with Workers (`job_queue_app.py`)

Distributed job queue with worker processes.

**Features:**
- Priority queues (high, normal, low)
- Job retry mechanism
- Dead letter queue
- Multiple workers
- Job status tracking

**Usage:**
```bash
# Start workers
python job_queue_app.py worker --workers 3

# Enqueue jobs
python job_queue_app.py enqueue --count 10

# Monitor queue
python job_queue_app.py monitor

# Process specific priority
python job_queue_app.py worker --priority high
```

**Use Cases:**
- Background job processing
- Task distribution
- Work queue systems

---

### 10. Metrics Collector (`metrics_collector_app.py`)

Real-time metrics collection and aggregation.

**Features:**
- Counter metrics
- Gauge metrics
- Histogram/percentiles
- Time-series data
- Real-time dashboard
- Rolling windows

**Usage:**
```bash
# Start dashboard
python metrics_collector_app.py dashboard

# Simulate metrics
python metrics_collector_app.py simulate --duration 60

# Record custom metrics
python metrics_collector_app.py record counter api.requests 1
python metrics_collector_app.py record gauge cpu.usage 75.5
python metrics_collector_app.py record histogram response.time 150.5
```

**Use Cases:**
- Application monitoring
- Performance tracking
- Analytics systems

---

## 📋 Prerequisites

### Redis Server

All applications require a running Redis server. You can use:

**Option 1: Docker (Recommended)**
```bash
cd tests/docker
docker compose up -d redis-standalone
```

**Option 2: Local Redis**
```bash
redis-server
```

### Python Dependencies

Install CyRedis and dependencies:

```bash
# Using UV (recommended)
uv pip install -e .

# Or using pip
pip install -e .
```

### Build CyRedis

Build the Cython extensions:

```bash
bash build_optimized.sh
# or
make build
```

## 🔧 Common Patterns

### Application Structure

Each application follows this pattern:

```python
# 1. Import CyRedis client
try:
    from optimized_redis import OptimizedRedis as Redis
except ImportError:
    from redis_wrapper import HighPerformanceRedis as Redis

# 2. Create client
redis = Redis(host='localhost', port=6379)

# 3. Implement application logic
# ...

# 4. Provide CLI interface
if __name__ == "__main__":
    main()
```

### Error Handling

All apps include robust error handling:

```python
try:
    result = redis.get(key)
except Exception as e:
    print(f"Error: {e}")
    sys.exit(1)
```

### Signal Handling

Applications that run continuously handle shutdown gracefully:

```python
import signal

def signal_handler(sig, frame):
    print("\nShutting down...")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)
```

### Connection Management

Apps use context managers when available:

```python
with Redis() as client:
    client.set("key", "value")
```

## 🧪 Testing Applications

### Manual Testing

Run applications manually to test functionality:

```bash
# Terminal 1: Start a service
python tests/apps/message_queue_app.py consume

# Terminal 2: Interact with it
python tests/apps/message_queue_app.py produce --count 5
```

### Load Testing

Some apps include built-in load testing:

```bash
# Rate limiter load test
python tests/apps/rate_limiter_app.py test --requests 1000

# Metrics simulation
python tests/apps/metrics_collector_app.py simulate --duration 300
```

### Integration with Tests

Applications can be used in integration tests:

```python
import subprocess

def test_message_queue():
    # Start consumer in background
    consumer = subprocess.Popen(
        ["python", "tests/apps/message_queue_app.py", "consume"]
    )

    # Produce messages
    subprocess.run(
        ["python", "tests/apps/message_queue_app.py", "produce", "--count", "5"]
    )

    # Cleanup
    consumer.terminate()
```

## 🎯 Use Case Matrix

| Application | Caching | Messaging | Persistence | Real-time | Distributed |
|------------|---------|-----------|-------------|-----------|-------------|
| Simple KV  | ✅ | ❌ | ✅ | ❌ | ✅ |
| Message Queue | ❌ | ✅ | ✅ | ✅ | ✅ |
| Distributed Cache | ✅ | ❌ | ✅ | ❌ | ✅ |
| Task Scheduler | ❌ | ✅ | ✅ | ✅ | ✅ |
| Session Manager | ✅ | ❌ | ✅ | ❌ | ✅ |
| Rate Limiter | ❌ | ❌ | ✅ | ✅ | ✅ |
| Pub/Sub Chat | ❌ | ✅ | ❌ | ✅ | ✅ |
| Leaderboard | ❌ | ❌ | ✅ | ✅ | ✅ |
| Job Queue | ❌ | ✅ | ✅ | ✅ | ✅ |
| Metrics Collector | ❌ | ❌ | ✅ | ✅ | ✅ |

## 📚 Further Reading

- [CyRedis Documentation](../../README.md)
- [Integration Tests](../integration/)
- [Testing Guide](../../TESTING.md)
- [Redis Documentation](https://redis.io/docs)

## 🤝 Contributing

To add a new test application:

1. Create `<app_name>_app.py` in this directory
2. Include comprehensive docstring with usage examples
3. Implement CLI using `argparse`
4. Add error handling and signal management
5. Update this README
6. Add to use case matrix

## 📝 Notes

- All applications use CyRedis's Cython-optimized client for maximum performance
- Applications gracefully handle Redis connection failures
- Most apps include interactive modes for easy testing
- Check individual app `--help` for detailed options
- All apps support custom Redis host/port via environment variables

## 🐛 Troubleshooting

**Connection refused:**
```bash
# Ensure Redis is running
docker compose -f tests/docker/docker-compose.yml up -d redis-standalone
# or
redis-server
```

**Import errors:**
```bash
# Build CyRedis extensions
bash build_optimized.sh
```

**Permission errors:**
```bash
# Make apps executable
chmod +x tests/apps/*.py
```

---

**Happy Testing! 🚀**
