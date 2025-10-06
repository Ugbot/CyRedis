# CyRedis Web Application Support Features

This document describes the FastAPI-style features implemented in CyRedis, providing enterprise-grade web application capabilities including worker queues, token management, session lifecycle, 2FA, and shared dictionaries.

## 🚀 Overview

CyRedis now includes comprehensive web application support that mirrors many of FastAPI's powerful features:

- **Worker Queue System**: Distributed task processing with retry logic and scheduling
- **Token Management**: JWT tokens with refresh tokens and blacklisting
- **Session Lifecycle**: Multi-session tracking with expiration and cleanup
- **Two-Factor Authentication**: TOTP support with backup codes
- **Short-lived Password Tokens**: Secure password reset functionality
- **Shared State Management**: Concurrent dictionaries for all users
- **Lifecycle Hooks**: Startup/shutdown event management
- **Worker Coordination**: Graceful scaling and recovery of crashed workers
- **Workload Yielding**: Automatic workload redistribution during shutdown

## 📁 Core Components

### 1. WebApplicationSupport Class

The main entry point that integrates all features:

```python
from cy_redis.web_app_support import WebApplicationSupport

# Initialize with context manager
with WebApplicationSupport() as app_support:
    # Use all features
    pass

# Or initialize manually
app_support = WebApplicationSupport()
app_support.initialize()
# ... use features ...
app_support.shutdown()
```

### 2. Worker Queue System

**Features:**
- Distributed task processing across multiple workers
- Automatic retry with exponential backoff
- Scheduled task execution
- Task prioritization and monitoring

**Usage:**
```python
# Enqueue immediate task
task_id = app_support.enqueue_task({
    'type': 'send_email',
    'to': 'user@example.com',
    'subject': 'Welcome!'
})

# Enqueue delayed task (schedule for later)
task_id = app_support.enqueue_task({
    'type': 'cleanup',
    'target': 'expired_sessions'
}, delay=3600)  # Execute in 1 hour

# Get queue statistics
stats = app_support.get_queue_stats()
print(f"Queue length: {stats['queue_length']}")
```

**Custom Worker Implementation:**
```python
from cy_redis.web_app_support import WorkerQueue

class EmailWorker(WorkerQueue):
    def process_task(self, task):
        task_data = task['data']
        if task_data['type'] == 'send_email':
            return self.send_email(task_data)
        return {"status": "processed"}

    def send_email(self, data):
        # Your email sending logic here
        return {"status": "sent", "recipient": data['to']}
```

### 3. Token Management

**Features:**
- JWT access tokens (15-minute expiry by default)
- Refresh tokens (7-day expiry by default)
- Token blacklisting for logout
- Automatic token verification

**Usage:**
```python
# Create authentication tokens
tokens = app_support.authenticate_user(user_id, password)
access_token = tokens['access_token']
refresh_token = tokens['refresh_token']

# Verify access token
payload = app_support.verify_user_access(access_token)
if payload:
    user_id = payload['user_id']

# Refresh expired access token
new_token = app_support.refresh_access_token(refresh_token)

# Revoke token (logout)
app_support.revoke_token(access_token)
```

### 4. Session Lifecycle Management

**Features:**
- Multiple sessions per user
- Automatic session expiration
- Session cleanup and garbage collection
- Cross-device session tracking

**Usage:**
```python
# Create new session
session_id = app_support.create_user_session(
    user_id,
    device_info={'browser': 'Chrome', 'os': 'Linux'}
)

# Get session data
session = app_support.get_session(session_id)
if session:
    print(f"User: {session['user_id']}")

# Destroy session
app_support.destroy_session(session_id)

# Get all user sessions
sessions = app_support.multi_session_tracker.get_user_sessions(user_id)

# Revoke all user sessions
app_support.multi_session_tracker.revoke_all_user_sessions(user_id)
```

### 5. Two-Factor Authentication (2FA)

**Features:**
- TOTP (Time-based One-Time Password) support
- Backup codes for account recovery
- QR code generation for authenticator apps
- Secure 2FA verification

**Usage:**
```python
# Enable 2FA for user
setup_info = app_support.enable_2fa(user_id)
print(f"TOTP Secret: {setup_info['totp_secret']}")
print(f"Backup Codes: {setup_info['backup_codes']}")
print(f"QR Code URL: {setup_info['qr_code_url']}")

# Verify TOTP token
is_valid = app_support.verify_totp(user_id, token_123456)

# Verify backup code
is_valid = app_support.two_factor_auth.verify_backup_code(user_id, backup_code)

# Disable 2FA
success = app_support.two_factor_auth.disable_2fa(user_id)
```

### 6. Short-lived Password Tokens

**Features:**
- Secure password reset tokens (15-minute expiry)
- One-time use tokens
- Automatic cleanup of expired tokens

**Usage:**
```python
# Request password reset
reset_token = app_support.password_reset.create_reset_token(user_id, email)

# Verify and use reset token
token_data = app_support.password_reset.verify_reset_token(reset_token)
if token_data:
    # Reset password
    user_id = token_data['user_id']
    new_password_hash = hash_new_password(new_password)
    # Update user password...
```

### 7. Shared State Management

**Features:**
- Distributed locks for concurrency control
- Shared counters with atomic operations
- Shared data storage with optional expiry
- Event publishing and subscription

**Usage:**
```python
# Distributed locking
lock_id = app_support.shared_state.acquire_lock('user_update')
if lock_id:
    try:
        # Critical section
        update_user_data(user_id)
    finally:
        app_support.shared_state.release_lock('user_update', lock_id)

# Atomic counters
app_support.increment_counter('total_logins', 1)
count = app_support.get_counter('total_logins')

# Shared data storage
app_support.set_shared_data('last_maintenance', {
    'timestamp': time.time(),
    'type': 'daily'
}, expiry=86400)  # Expire in 24 hours

data = app_support.get_shared_data('last_maintenance')
```

### 8. Concurrent Shared Dictionaries

**Features:**
- Thread-safe and process-safe dictionaries
- Redis-backed with local caching
- Atomic operations and bulk updates
- Automatic synchronization across instances

### 9. Worker Coordination & Recovery

**Features:**
- Automatic worker registration and health monitoring
- Graceful shutdown with workload yielding
- Dead worker detection and automatic recovery
- Session and task redistribution
- Cluster-wide statistics and monitoring

**Usage:**
```python
# Get worker information
worker_info = app_support.get_worker_info()
print(f"Worker ID: {worker_info['worker_id']}")
print(f"Status: {worker_info['status']}")
print(f"Uptime: {worker_info['uptime_seconds']} seconds")

# Get cluster information
cluster_info = app_support.get_cluster_info()
print(f"Total workers: {cluster_info['total_workers']}")
print(f"Healthy workers: {cluster_info['healthy_workers']}")
print(f"Dead workers: {cluster_info['dead_workers']}")

# Force recovery of a specific worker
app_support.force_worker_recovery("worker_123")

# Graceful shutdown with workload yielding
app_support.shutdown_graceful()

# Immediate shutdown (for emergency situations)
app_support.shutdown_immediate()
```

**Graceful Shutdown Process:**
1. **Workload Analysis**: Identify active sessions and tasks
2. **Worker Discovery**: Find available healthy workers
3. **Workload Distribution**: Redistribute sessions and tasks to healthy workers
4. **Task Completion Wait**: Wait for active tasks to complete (configurable timeout)
5. **Session Transfer**: Transfer session ownership to other workers
6. **Clean Shutdown**: Run shutdown hooks and mark worker as stopped

**Usage:**
```python
# Get or create shared dictionary
user_profiles = app_support.get_shared_dict('user_profiles')

# Dictionary operations (all atomic and thread-safe)
user_profiles['user:123'] = {
    'id': '123',
    'name': 'John Doe',
    'preferences': {'theme': 'dark'}
}

profile = user_profiles['user:123']
user_profiles.update({'user:123': {'active': True}})

# Atomic numeric operations
user_profiles.increment('user:123:login_count', 1)

# Bulk operations
user_profiles.bulk_update({
    'user:124': {'name': 'Jane Doe'},
    'user:125': {'name': 'Bob Smith'}
})

# Statistics and monitoring
stats = user_profiles.get_stats()
print(f"Dictionary size: {stats['key_count']}")
```

## 🔧 Configuration

### Worker Queue Configuration
```python
# Custom worker queue with specific settings
email_queue = WorkerQueue(
    "email",
    redis_client,
    max_workers=8,
    queue_type="priority"  # Different queue types
)
```

### Token Configuration
```python
# Custom token manager settings
token_manager = TokenManager(
    redis_client,
    secret_key="your-secret-key",
    access_token_expiry=900,    # 15 minutes
    refresh_token_expiry=604800  # 7 days
)
```

### Session Configuration
```python
# Custom session manager settings
session_manager = SessionManager(
    redis_client,
    session_timeout=7200,    # 2 hours
    cleanup_interval=600     # 10 minutes
)
```

## 📊 Monitoring and Statistics

### System-wide Statistics
```python
stats = app_support.get_system_stats()
print(json.dumps(stats, indent=2))
# Output includes:
# - User counts
# - Active sessions
# - Queue statistics
# - Rate limiting stats
# - Shared dictionary stats
```

### Queue Monitoring
```python
queue_stats = app_support.get_queue_stats()
print(f"""
Queue Statistics:
- Length: {queue_stats['queue_length']}
- Processing: {queue_stats['processing_count']}
- Completed: {queue_stats['completed_count']}
- Failed: {queue_stats['failed_count']}
- Scheduled: {queue_stats['scheduled_count']}
""")
```

### Shared Dictionary Monitoring
```python
stats = shared_dict.get_stats()
print(f"""
Dictionary '{shared_dict.dict_name}' Stats:
- Keys: {stats['key_count']}
- Size: {stats['total_size_bytes']} bytes
- Cache Age: {stats['cache_age_seconds']} seconds
- Synced: {shared_dict.is_synced()}
""")
```

## 🔒 Security Features

### Rate Limiting
```python
# Check rate limits before authentication
rate_limit = app_support.rate_limits.get(f"login_attempts:{username}", {})
if rate_limit.get('blocked_until') and time.time() < rate_limit['blocked_until']:
    return {"error": "Account temporarily locked"}
```

### Token Blacklisting
```python
# Automatic token blacklisting on logout
def logout(session_id):
    # Get user's tokens from session
    tokens = get_user_tokens(session_id)

    # Blacklist all tokens
    for token in tokens:
        app_support.revoke_token(token)

    # Destroy session
    app_support.destroy_session(session_id)
```

### Secure Session Management
```python
# Multi-device session tracking
sessions = app_support.multi_session_tracker.get_user_sessions(user_id)
for session in sessions:
    if session.get('device_info', {}).get('ip') == suspicious_ip:
        app_support.multi_session_tracker.revoke_session(session['session_id'])
```

## 🚀 Production Deployment

### Gunicorn Integration
```python
# app_factory.py
from cy_redis.web_app_support import WebApplicationSupport

def create_app():
    app_support = WebApplicationSupport()
    app_support.initialize()

    # Your FastAPI/FastAPI app setup
    from fastapi import FastAPI
    app = FastAPI()

    @app.on_event("startup")
    async def startup():
        app_support.initialize()

    @app.on_event("shutdown")
    async def shutdown():
        # Use graceful shutdown for proper workload yielding
        app_support.shutdown_graceful()

    return app
```

### Enhanced Gunicorn Configuration

```bash
# gunicorn.conf.py
import multiprocessing
import os

# Worker configuration for graceful scaling
workers = multiprocessing.cpu_count() * 2 + 1
worker_class = "uvicorn.workers.UvicornWorker"
worker_connections = 1000

# Timeout settings for graceful shutdown
timeout = 30
graceful_timeout = 30
keepalive = 2

# Preload application for better performance
preload_app = True

# Worker recycling
max_requests = 1000
max_requests_jitter = 50

# Environment variables for worker coordination
os.environ.setdefault("WORKER_ID", f"worker_{os.getpid()}")
```

### Graceful Worker Shutdown

When Gunicorn sends SIGTERM to workers, the enhanced lifecycle manager:

1. **Receives SIGTERM** and initiates graceful shutdown
2. **Yields workload** to other healthy workers
3. **Waits for active tasks** to complete (30 second timeout)
4. **Redistributes sessions** to other workers
5. **Runs cleanup hooks** and marks worker as stopped
6. **Exits cleanly** without losing user sessions or tasks

**Benefits:**
- **Zero-downtime deployments** with proper workload transfer
- **Session persistence** across worker restarts
- **Task continuity** with automatic redistribution
- **Health monitoring** with automatic dead worker detection
- **Automatic recovery** when workers crash unexpectedly

### Docker Deployment
```dockerfile
# Dockerfile
FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
EXPOSE 8000

CMD ["uvicorn", "app_factory:create_app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4"]
```

### Kubernetes Deployment
```yaml
# deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: cyredis-webapp
spec:
  replicas: 3
  selector:
    matchLabels:
      app: cyredis-webapp
  template:
    metadata:
      labels:
        app: cyredis-webapp
    spec:
      containers:
      - name: webapp
        image: cyredis-webapp:latest
        ports:
        - containerPort: 8000
        env:
        - name: REDIS_URL
          value: "redis://redis-service:6379"
```

## 🎯 Best Practices

### 1. Error Handling
```python
try:
    result = app_support.authenticate_user(user_id, password)
except Exception as e:
    logger.error(f"Authentication failed: {e}")
    return {"error": "Internal server error"}
```

### 2. Resource Cleanup
```python
# Always cleanup resources
try:
    # Use web app features
    pass
finally:
    app_support.shutdown()
```

### 3. Monitoring Integration
```python
# Regular health checks
def health_check():
    stats = app_support.get_system_stats()
    if stats['errors']:
        raise HealthCheckError("System unhealthy")
    return {"status": "healthy", "stats": stats}
```

### 4. Configuration Management
```python
# Environment-based configuration
config = {
    'redis_host': os.getenv('REDIS_HOST', 'localhost'),
    'redis_port': int(os.getenv('REDIS_PORT', 6379)),
    'session_timeout': int(os.getenv('SESSION_TIMEOUT', 3600)),
    'rate_limit_per_minute': int(os.getenv('RATE_LIMIT_PER_MINUTE', 100))
}

app_support = WebApplicationSupport(**config)
```

## 🔍 Troubleshooting

### Common Issues

1. **High Memory Usage**
   - Check shared dictionary cache settings
   - Monitor queue backlog
   - Enable automatic cleanup

2. **Lock Contention**
   - Increase lock timeout values
   - Use shorter lock durations for non-critical operations
   - Implement lock retry logic

3. **Token Verification Failures**
   - Check Redis connectivity
   - Verify token blacklisting cleanup
   - Monitor token expiry times

4. **Session Cleanup Issues**
   - Check cleanup interval settings
   - Monitor Redis memory usage
   - Verify Redis persistence settings

### Debug Mode
```python
# Enable debug logging
import logging
logging.basicConfig(level=logging.DEBUG)

# Monitor Redis operations
app_support.redis_client.debug_mode = True
```

## 📈 Performance Optimization

### 1. Connection Pooling
```python
# Use connection pooling for high throughput
redis_client = CyRedisClient(
    host=redis_host,
    port=redis_port,
    max_connections=20,  # Increase for high load
    max_workers=8        # Match your worker threads
)
```

### 2. Caching Strategy
```python
# Adjust cache TTL based on data volatility
fast_changing_dict = app_support.get_shared_dict('user_activity')
slow_changing_dict = app_support.get_shared_dict('system_config')

# Fast-changing data: shorter cache
fast_changing_dict.cache_ttl = 10

# Slow-changing data: longer cache
slow_changing_dict.cache_ttl = 300
```

### 3. Queue Optimization
```python
# Use multiple queue types for different priorities
high_priority_queue = WorkerQueue("critical", redis_client, max_workers=4)
low_priority_queue = WorkerQueue("background", redis_client, max_workers=2)

# Route tasks based on priority
if task['priority'] == 'high':
    high_priority_queue.enqueue(task_data)
else:
    low_priority_queue.enqueue(task_data)
```

## 🔗 Integration Examples

### FastAPI Integration
```python
from fastapi import FastAPI, Depends, HTTPException
from cy_redis.web_app_support import WebApplicationSupport

app = FastAPI()
app_support = WebApplicationSupport()

@app.on_event("startup")
async def startup():
    app_support.initialize()

@app.on_event("shutdown")
async def shutdown():
    app_support.shutdown()

@app.post("/login")
async def login(username: str, password: str):
    result = await app.authenticate_user(username, password)
    if not result['success']:
        raise HTTPException(status_code=401, detail=result['error'])
    return result

@app.post("/protected")
async def protected_route(token: str = Depends(oauth2_scheme)):
    payload = app_support.verify_user_access(token)
    if not payload:
        raise HTTPException(status_code=401, detail="Invalid token")
    return {"user_id": payload['user_id']}
```

### Django Integration
```python
# settings.py
CACHES = {
    'default': {
        'BACKEND': 'django_redis.cache.RedisCache',
        'LOCATION': 'redis://127.0.0.1:6379/1',
        'OPTIONS': {
            'CLIENT_CLASS': 'cy_redis.cy_redis_client.CyRedisClient',
        }
    }
}

# views.py
from cy_redis.web_app_support import WebApplicationSupport

class LoginView(View):
    def post(self, request):
        app_support = WebApplicationSupport()
        result = await app_support.authenticate_user(
            request.POST['username'],
            request.POST['password']
        )

        if result['success']:
            request.session['user_id'] = result['user_id']
            request.session['access_token'] = result['tokens']['access_token']
            return redirect('dashboard')
        else:
            return render(request, 'login.html', {'error': result['error']})
```

## 🎉 Conclusion

CyRedis Web Application Support provides enterprise-grade features that enable you to build scalable, secure web applications with:

- **Scalability**: Distributed task processing and shared state
- **Security**: JWT tokens, 2FA, rate limiting, and secure sessions
- **Reliability**: Automatic retries, cleanup, and monitoring
- **Performance**: Cython optimization and Redis-backed operations
- **Flexibility**: Customizable worker queues and lifecycle management

These features work together to provide FastAPI-like capabilities while leveraging Redis for high-performance, distributed operations across multiple application instances and worker processes.
