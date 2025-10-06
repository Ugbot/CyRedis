# 🚀 CyRedis - High-Performance Redis Client with Enterprise Features

A comprehensive, high-performance Cython Redis client that provides **FastAPI-style** web application support with advanced features for distributed, real-time applications.

![CyRedis Logo](https://img.shields.io/badge/CyRedis-High%20Performance%20Redis-blue)
![Python](https://img.shields.io/badge/Python-3.7+-green)
![Redis](https://img.shields.io/badge/Redis-6.0+-red)
![License](https://img.shields.io/badge/License-MIT-yellow)

## 🌟 Features

### Core Redis Operations
- **High Performance**: Cython-optimized Redis operations with native speed
- **Connection Pooling**: Efficient connection management with automatic failover
- **Protocol Support**: RESP2 and RESP3 protocol negotiation
- **Async Support**: Full async/await support with uvloop optimization

### Advanced Data Structures
- **Streams**: Redis Streams with consumer groups
- **JSON**: RedisJSON operations
- **Geospatial**: Redis geospatial indexing
- **Time Series**: RedisTimeSeries support
- **Bloom Filters**: Probabilistic data structures
- **Bitmaps**: Bit-level operations

### Web Application Support (FastAPI-Style)
- **Worker Coordination**: Graceful scaling and recovery of crashed workers
- **JWT Authentication**: Token management with refresh tokens and blacklisting
- **Session Management**: Multi-session tracking with automatic cleanup
- **2FA Support**: Time-based OTP with backup codes
- **Rate Limiting**: Built-in rate limiting capabilities
- **Streaming Iterators**: Real-time data streaming over SSE/WebSockets

### Distributed Features
- **Distributed Locks**: Redis-based distributed locking
- **Shared State**: Thread-safe shared dictionaries across processes
- **Cluster Operations**: Redis Cluster support with slot management
- **Health Monitoring**: Automatic dead worker detection and recovery

## 🚀 Quick Start

### Installation

```bash
# Install from PyPI
pip install cyredis

# Or install from source
git clone https://github.com/yourusername/cyredis.git
cd cyredis
pip install -e .
```

### Basic Usage

```python
from cy_redis import HighPerformanceRedis

# Synchronous usage
with HighPerformanceRedis() as redis:
    redis.set("key", "value")
    value = redis.get("key")
    print(value)  # "value"

# Async usage
import asyncio

async def main():
    async with HighPerformanceRedis() as redis:
        await redis.set_async("key", "value")
        value = await redis.get_async("key")
        print(value)

asyncio.run(main())
```

## 🌐 FastAPI Integration

CyRedis provides **enterprise-grade** web application support that integrates seamlessly with FastAPI:

### Basic FastAPI Setup

```python
from fastapi import FastAPI
from cy_redis.web_app_support import WebApplicationSupport

app = FastAPI()

# Initialize CyRedis web app support
app_support = WebApplicationSupport()

@app.on_event("startup")
async def startup():
    app_support.initialize()

@app.on_event("shutdown")
async def shutdown():
    app_support.shutdown_graceful()  # Graceful shutdown with workload yielding

@app.post("/login")
async def login(username: str, password: str):
    result = await app_support.authenticate_user(username, password)
    if not result['success']:
        raise HTTPException(status_code=401, detail=result['error'])
    return result

@app.get("/protected")
async def protected_route(token: str):
    payload = app_support.verify_user_access(token)
    if not payload:
        raise HTTPException(status_code=401, detail="Invalid token")
    return {"user_id": payload['user_id']}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
```

### Real-time Streaming with SSE/WebSockets

```python
from fastapi.responses import StreamingResponse
from fastapi import WebSocket, WebSocketDisconnect

@app.get("/sse/chat/{user_id}")
async def sse_chat(user_id: str, token: str):
    """Real-time chat via Server-Sent Events"""
    # Verify JWT token
    payload = app_support.jwt_middleware(token, required_permissions=['stream_access'])
    if not payload or payload['user_id'] != user_id:
        raise HTTPException(status_code=403, detail="Access denied")

    def generate_sse():
        async def sse_generator():
            async with app_support.stream_iterator(
                "chat_messages",
                consumer_group=f"chat_{user_id}",
                consumer_name=f"user_{user_id}"
            ) as stream_iter:
                async for messages in stream_iter:
                    for message in messages:
                        yield f"data: {json.dumps(message['data'])}\n\n"
        return sse_generator()

    return StreamingResponse(generate_sse(), media_type="text/event-stream")

@app.websocket("/ws/chat/{user_id}")
async def websocket_chat(websocket: WebSocket, user_id: str, token: str):
    """Real-time chat via WebSocket"""
    # Verify websocket token
    ws_payload = app_support.jwt_middleware(token, required_permissions=['stream_access', 'realtime_access'])
    if not ws_payload or ws_payload['user_id'] != user_id:
        await websocket.send_json({"error": "Invalid websocket token"})
        await websocket.close()
        return

    await websocket.accept()

    try:
        async with app_support.stream_iterator(
            "chat_messages",
            consumer_group=f"ws_chat_{user_id}",
            consumer_name=f"ws_{uuid.uuid4().hex[:8]}"
        ) as stream_iter:
            async for messages in stream_iter:
                for message in messages:
                    await websocket.send_json(message['data'])
    except WebSocketDisconnect:
        pass
    except Exception as e:
        await websocket.send_json({"error": str(e)})
    finally:
        await websocket.close()
```

### JWT Authentication Middleware

```python
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

security = HTTPBearer()

async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """JWT authentication dependency"""
    token = credentials.credentials
    payload = app_support.jwt_middleware(token)
    if not payload:
        raise HTTPException(status_code=401, detail="Invalid authentication token")
    return payload

@app.get("/api/data")
async def get_data(current_user = Depends(get_current_user)):
    """Protected API endpoint"""
    return {"user_id": current_user['user_id'], "data": "sensitive information"}

@app.post("/api/write")
async def write_data(data: dict, current_user = Depends(get_current_user)):
    """Protected write endpoint"""
    # Check if user has write permissions
    if not app_support.require_scopes(current_user['access_token'], ['write']):
        raise HTTPException(status_code=403, detail="Insufficient permissions")

    # Process the write operation
    return {"status": "success"}
```

## 🎯 Worker Coordination Features

CyRedis provides **enterprise-grade** worker coordination for handling scaling and recovery:

### Automatic Worker Registration

```python
# Workers automatically register themselves
worker_info = app_support.get_worker_info()
print(f"Worker ID: {worker_info['worker_id']}")
print(f"Status: {worker_info['status']}")
```

### Graceful Shutdown with Workload Yielding

```python
# Graceful shutdown yields workload to other workers
app_support.shutdown_graceful()

# Process:
# 1. Detect active sessions and tasks
# 2. Find available healthy workers
# 3. Redistribute sessions and tasks
# 4. Wait for active work to complete
# 5. Clean shutdown
```

### Dead Worker Detection & Recovery

```python
# Monitor worker health
cluster_info = app_support.get_cluster_info()
print(f"Healthy workers: {cluster_info['healthy_workers']}")
print(f"Dead workers: {cluster_info['dead_workers']}")

# Force recovery of specific worker
app_support.force_worker_recovery("worker_123")
```

## 📊 Monitoring & Statistics

### System Statistics

```python
stats = app_support.get_system_stats()
print(json.dumps(stats, indent=2))
# Output includes:
# - User counts and session info
# - Worker cluster health
# - Queue statistics
# - Rate limiting stats
# - Shared dictionary stats
```

### Worker Monitoring

```python
# Monitor individual worker
worker_stats = app_support.get_worker_info()
print(f"Worker uptime: {worker_stats['uptime_seconds']}s")
print(f"Worker health: {worker_stats['status']}")

# Monitor entire cluster
cluster_stats = app_support.get_cluster_info()
print(f"Total workers: {cluster_stats['total_workers']}")
print(f"Healthy workers: {cluster_stats['healthy_workers']}")
```

## 🔒 Security Features

### JWT Token Management

```python
# Create different token types
access_token = app_support.create_access_token(user_id)
websocket_token = app_support.create_websocket_token(user_id)
api_token = app_support.create_api_token(user_id, scopes=['read', 'write'])

# Verify tokens
payload = app_support.verify_user_access(access_token)
ws_payload = app_support.verify_websocket_token(websocket_token)
api_payload = app_support.verify_api_token(api_token, ['read'])

# Token revocation
app_support.revoke_token(access_token)
```

### Authentication Middleware

```python
# Different authentication levels
@app.get("/public")
async def public_endpoint():
    return {"message": "Public data"}

@app.get("/protected")
async def protected_endpoint(token: str):
    payload = app_support.require_auth(token)
    if not payload:
        raise HTTPException(status_code=401)
    return {"user_id": payload['user_id']}

@app.get("/admin")
async def admin_endpoint(token: str):
    if not app_support.require_scopes(token, ['admin']):
        raise HTTPException(status_code=403)
    return {"message": "Admin data"}
```

## 🎪 Real-time Streaming

### Redis Stream Iterators

```python
# Stream chat messages
async with app_support.stream_iterator(
    "chat_messages",
    consumer_group="chat_users",
    consumer_name=f"user_{user_id}"
) as stream_iter:
    async for messages in stream_iter:
        for message in messages:
            # Process each message
            yield f"data: {json.dumps(message['data'])}\n\n"
```

### List-based Notifications

```python
# Stream user notifications
async with app_support.list_iterator(f"notifications:{user_id}") as list_iter:
    async for items in list_iter:
        for item in items:
            # Process each notification
            yield f"data: {json.dumps({'message': item})}\n\n"
```

### Pub/Sub Broadcasting

```python
# Listen to pub/sub channels
async with app_support.pubsub_iterator(["user_notifications", "global_news"]) as pubsub_iter:
    async for message in pubsub_iter:
        # Process broadcast message
        yield f"data: {json.dumps(message)}\n\n"
```

## 🧪 Testing

### Run All Tests

```bash
# Run all tests
pytest

# With coverage
pytest --cov=cy_redis --cov-report=html

# Specific test categories
pytest -m integration    # Redis integration tests
pytest -m unit          # Unit tests
pytest -m worker_coordination  # Worker coordination tests
```

### Test Scripts

```bash
# Run worker coordination tests
python tests/test_worker_coordination.py

# Run with main test runner
./scripts/run_tests.sh --worker-coord --coverage

# Run example applications
python examples/web_app_example.py
python examples/streaming_example.py
```

### Docker Testing

```bash
# Start test environment
./scripts/run_tests.sh --docker-up --all

# Run tests with Docker
./scripts/run_tests.sh --docker-up --integration
```

## 📚 Examples

### Complete Examples

- **`examples/web_app_example.py`**: Full web application with authentication, sessions, and shared state
- **`examples/streaming_example.py`**: Real-time streaming with SSE/WebSocket integration
- **`examples/enhanced_cyredis_demo.py`**: Advanced features demonstration

### Usage Patterns

```python
# 1. Initialize web app support
from cy_redis.web_app_support import WebApplicationSupport

app_support = WebApplicationSupport()

# 2. User authentication
tokens = app_support.authenticate_user(user_id, password)
access_token = tokens['access_token']

# 3. Session management
session_id = app_support.create_user_session(user_id)
session = app_support.get_session(session_id)

# 4. Real-time streaming
async with app_support.stream_iterator("chat_messages") as stream:
    async for messages in stream:
        for message in messages:
            yield message

# 5. Shared state
shared_dict = app_support.get_shared_dict("user_data")
shared_dict[user_id] = {"preferences": {...}}

# 6. Worker coordination
cluster_info = app_support.get_cluster_info()
worker_info = app_support.get_worker_info()
```

## 🔧 Configuration Options

### Redis Connection

```python
app_support = WebApplicationSupport(
    host="redis-cluster.example.com",
    port=6379,
    max_connections=20,
    max_workers=8
)
```

### Token Configuration

```python
# Custom token settings
token_manager = TokenManager(
    redis_client,
    secret_key="your-secret-key",
    access_token_expiry=900,    # 15 minutes
    refresh_token_expiry=604800  # 7 days
)
```

### Session Configuration

```python
session_manager = SessionManager(
    redis_client,
    session_timeout=7200,    # 2 hours
    cleanup_interval=600     # 10 minutes
)
```

## 🚨 Troubleshooting

### Common Issues

1. **High Memory Usage**
   ```python
   # Adjust cache settings
   shared_dict = app_support.get_shared_dict('large_dataset')
   shared_dict.cache_ttl = 10  # Shorter cache for volatile data
   ```

2. **Worker Lock Contention**
   ```python
   # Increase lock timeouts
   lock = DistributedLock(redis, "my_lock", ttl_ms=10000)
   ```

3. **Token Verification Failures**
   ```python
   # Check Redis connectivity and token blacklisting
   payload = app_support.verify_user_access(token)
   ```

### Performance Optimization

```python
# Connection pooling
redis_client = CyRedisClient(max_connections=20)

# Caching strategy
fast_dict = app_support.get_shared_dict('fast_changing_data')
fast_dict.cache_ttl = 10  # Short cache

slow_dict = app_support.get_shared_dict('static_config')
slow_dict.cache_ttl = 300  # Long cache
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new features
4. Run the test suite
5. Submit a pull request

## 📄 License

MIT License - see LICENSE file for details.

## 🙏 Acknowledgments

- Built on top of the excellent [hiredis](https://github.com/redis/hiredis) C library
- Inspired by [redis-py](https://github.com/redis/redis-py) and [FastAPI](https://github.com/tiangolo/fastapi)
- Uses [uvloop](https://github.com/MagicStack/uvloop) for async performance

## 📞 Support

- **Issues**: [GitHub Issues](https://github.com/yourusername/cyredis/issues)
- **Discussions**: [GitHub Discussions](https://github.com/yourusername/cyredis/discussions)
- **Documentation**: [Full Documentation](https://cyredis.readthedocs.io/)

---

**CyRedis** - Bringing **enterprise-grade** Redis functionality to Python with **FastAPI-style** web application support! 🚀
