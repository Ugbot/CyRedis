# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# cython: nonecheck=False
# distutils: language = c

"""
Web Application Support Module for CyRedis
Provides FastAPI-style features including worker queues, token management,
session lifecycle, 2FA, and multi-session tracking.
"""

import asyncio
import json
import time
import uuid
import hashlib
import hmac
import base64
import secrets
import socket
from typing import Dict, List, Optional, Any, Callable, Union, Tuple
from concurrent.futures import ThreadPoolExecutor
import threading
from datetime import datetime, timedelta
import os
import signal
import sys

# Import core Redis functionality
from cy_redis.cy_redis_client import CyRedisClient
from cy_redis.distributed import CyDistributedLock


# Exception classes
class WebAppSupportError(Exception):
    pass

class TokenError(WebAppSupportError):
    pass

class SessionError(WebAppSupportError):
    pass

class WorkerQueueError(WebAppSupportError):
    pass


# ===== WORKER QUEUE SYSTEM =====

cdef class WorkerQueue:
    """
    Distributed worker queue system for background task processing.
    Supports multiple queue types and worker pools.
    """

    def __cinit__(self, str queue_name, CyRedisClient redis_client,
                  int max_workers=4, str queue_type="default"):
        self.queue_name = queue_name
        self.redis_client = redis_client
        self.max_workers = max_workers
        self.queue_type = queue_type
        self.workers = []
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.lock = threading.Lock()
        self.is_running = False

        # Queue keys
        self.queue_key = f"wq:{queue_type}:{queue_name}"
        self.processing_key = f"wq:processing:{queue_type}:{queue_name}"
        self.completed_key = f"wq:completed:{queue_type}:{queue_name}"
        self.failed_key = f"wq:failed:{queue_type}:{queue_name}"

    def start(self):
        """Start the worker queue"""
        if self.is_running:
            return

        self.is_running = True
        # Start worker threads
        for i in range(self.max_workers):
            worker = threading.Thread(target=self._worker_loop, args=(i,))
            worker.daemon = True
            worker.start()
            self.workers.append(worker)

    def stop(self):
        """Stop the worker queue"""
        self.is_running = False
        # Wait for workers to finish current tasks
        for worker in self.workers:
            worker.join(timeout=5.0)

        self.executor.shutdown(wait=True)

    def enqueue(self, task_data: Dict[str, Any], delay: int = 0) -> str:
        """
        Enqueue a task for processing.
        Returns task ID.
        """
        task_id = str(uuid.uuid4())
        task = {
            'id': task_id,
            'data': task_data,
            'created_at': time.time(),
            'delay': delay,
            'attempts': 0,
            'max_attempts': 3
        }

        if delay > 0:
            # Schedule for future execution
            scheduled_time = time.time() + delay
            self.redis_client.zadd(f"wq:scheduled:{self.queue_type}:{self.queue_name}",
                                 {json.dumps(task): scheduled_time})
        else:
            # Add to immediate queue
            self.redis_client.lpush(self.queue_key, json.dumps(task))

        return task_id

    def _worker_loop(self, worker_id: int):
        """Main worker loop"""
        while self.is_running:
            try:
                # Try to get a task from scheduled queue first
                scheduled_task = self._get_scheduled_task()
                if scheduled_task:
                    self._process_task(scheduled_task, worker_id)
                    continue

                # Get task from main queue
                task_data = self.redis_client.brpoplpush(
                    self.queue_key,
                    self.processing_key,
                    timeout=1
                )

                if task_data:
                    task = json.loads(task_data)
                    self._process_task(task, worker_id)

            except Exception as e:
                print(f"Worker {worker_id} error: {e}")
                time.sleep(1)

    def _get_scheduled_task(self) -> Optional[Dict[str, Any]]:
        """Get a task that is ready to be processed from scheduled queue"""
        current_time = time.time()
        tasks = self.redis_client.zrangebyscore(
            f"wq:scheduled:{self.queue_type}:{self.queue_name}",
            0, current_time, num=1, withscores=True
        )

        if tasks:
            task_data, _ = tasks[0]
            task = json.loads(task_data)

            # Remove from scheduled queue
            self.redis_client.zrem(
                f"wq:scheduled:{self.queue_type}:{self.queue_name}",
                task_data
            )

            # Add to main queue for immediate processing
            self.redis_client.lpush(self.queue_key, task_data)
            return task

        return None

    def _process_task(self, task: Dict[str, Any], worker_id: int):
        """Process a single task"""
        try:
            task['attempts'] += 1
            task['started_at'] = time.time()
            task['worker_id'] = worker_id

            # Update processing status
            self.redis_client.hset(
                self.processing_key,
                task['id'],
                json.dumps(task)
            )

            # Process the task (this would be overridden by subclasses)
            result = self.process_task(task)

            # Mark as completed
            task['completed_at'] = time.time()
            task['result'] = result

            self.redis_client.lpush(self.completed_key, json.dumps(task))
            self.redis_client.hdel(self.processing_key, task['id'])

        except Exception as e:
            task['failed_at'] = time.time()
            task['error'] = str(e)

            if task['attempts'] < task['max_attempts']:
                # Re-queue for retry
                retry_delay = min(60, 2 ** task['attempts'])  # Exponential backoff
                task['retry_at'] = time.time() + retry_delay
                self.redis_client.lpush(self.queue_key, json.dumps(task))
            else:
                # Mark as failed
                self.redis_client.lpush(self.failed_key, json.dumps(task))

            self.redis_client.hdel(self.processing_key, task['id'])

    def process_task(self, task: Dict[str, Any]) -> Any:
        """
        Process a task. Override this method in subclasses.
        Default implementation just returns the task data.
        """
        return task['data']

    def get_queue_stats(self) -> Dict[str, Any]:
        """Get queue statistics"""
        return {
            'queue_length': self.redis_client.llen(self.queue_key),
            'processing_count': self.redis_client.hlen(self.processing_key),
            'completed_count': self.redis_client.llen(self.completed_key),
            'failed_count': self.redis_client.llen(self.failed_key),
            'scheduled_count': self.redis_client.zcard(f"wq:scheduled:{self.queue_type}:{self.queue_name}")
        }


# ===== TOKEN MANAGEMENT SYSTEM =====

cdef class TokenManager:
    """
    JWT token management system with refresh tokens and blacklisting.
    Supports access tokens, refresh tokens, and token revocation.
    """

    def __cinit__(self, CyRedisClient redis_client, str secret_key=None,
                  int access_token_expiry=900, int refresh_token_expiry=604800):
        self.redis_client = redis_client
        self.secret_key = secret_key or secrets.token_urlsafe(32)
        self.access_token_expiry = access_token_expiry  # 15 minutes
        self.refresh_token_expiry = refresh_token_expiry  # 7 days

        # Token storage keys
        self.blacklist_key = "tokens:blacklist"
        self.refresh_tokens_key = "tokens:refresh"

    def create_access_token(self, user_id: str, claims: Dict[str, Any] = None) -> str:
        """Create a new access token"""
        claims = claims or {}
        now = int(time.time())

        payload = {
            'user_id': user_id,
            'type': 'access',
            'iat': now,
            'exp': now + self.access_token_expiry,
            **claims
        }

        return self._create_jwt_token(payload)

    def create_refresh_token(self, user_id: str, claims: Dict[str, Any] = None) -> str:
        """Create a new refresh token"""
        claims = claims or {}
        now = int(time.time())

        payload = {
            'user_id': user_id,
            'type': 'refresh',
            'iat': now,
            'exp': now + self.refresh_token_expiry,
            **claims
        }

        token = self._create_jwt_token(payload)

        # Store refresh token hash for validation
        token_hash = hashlib.sha256(token.encode()).hexdigest()
        self.redis_client.hset(self.refresh_tokens_key, token_hash, user_id)

        return token

    def verify_token(self, token: str) -> Optional[Dict[str, Any]]:
        """Verify and decode a JWT token"""
        try:
            # Check if token is blacklisted
            token_hash = hashlib.sha256(token.encode()).hexdigest()
            if self.redis_client.hexists(self.blacklist_key, token_hash):
                return None

            # Decode token
            payload = self._decode_jwt_token(token)

            # Check expiration
            if payload['exp'] < time.time():
                return None

            return payload

        except Exception:
            return None

    def refresh_access_token(self, refresh_token: str) -> Optional[str]:
        """Create new access token from refresh token"""
        payload = self.verify_token(refresh_token)
        if not payload or payload.get('type') != 'refresh':
            return None

        # Verify refresh token exists in store
        token_hash = hashlib.sha256(refresh_token.encode()).hexdigest()
        stored_user_id = self.redis_client.hget(self.refresh_tokens_key, token_hash)
        if not stored_user_id or stored_user_id != payload['user_id']:
            return None

        # Create new access token
        return self.create_access_token(payload['user_id'], payload)

    def revoke_token(self, token: str):
        """Revoke a token by adding it to blacklist"""
        token_hash = hashlib.sha256(token.encode()).hexdigest()
        self.redis_client.hset(self.blacklist_key, token_hash, str(time.time()))

    def revoke_all_user_tokens(self, user_id: str):
        """Revoke all tokens for a user"""
        # This is a simplified implementation - in production you'd want
        # to track user tokens more efficiently
        pass

    def cleanup_expired_tokens(self):
        """Clean up expired tokens from blacklist"""
        current_time = time.time()
        # Remove expired blacklist entries
        # This would need more sophisticated implementation

    def _create_jwt_token(self, payload: Dict[str, Any]) -> str:
        """Create JWT token (simplified implementation)"""
        import jwt

        return jwt.encode(payload, self.secret_key, algorithm='HS256')

    def _decode_jwt_token(self, token: str) -> Dict[str, Any]:
        """Decode JWT token (simplified implementation)"""
        import jwt

        return jwt.decode(token, self.secret_key, algorithms=['HS256'])

    def create_websocket_token(self, user_id: str, session_id: str = None,
                              permissions: List[str] = None, expiry: int = None) -> str:
        """Create a JWT token specifically for websocket/streaming connections"""
        permissions = permissions or []
        expiry = expiry or (self.access_token_expiry * 4)  # Longer expiry for websockets

        claims = {
            'type': 'websocket',
            'permissions': permissions,
            'stream_access': True,
            'realtime_access': True
        }

        if session_id:
            claims['session_id'] = session_id

        return self.create_access_token(user_id, claims)

    def verify_websocket_token(self, token: str) -> Optional[Dict[str, Any]]:
        """Verify websocket token and check permissions"""
        payload = self.verify_token(token)

        if not payload:
            return None

        # Check if it's a websocket token
        if payload.get('type') != 'websocket':
            return None

        # Check required permissions
        required_permissions = ['stream_access', 'realtime_access']
        for perm in required_permissions:
            if not payload.get(perm, False):
                return None

        return payload

    def create_api_token(self, user_id: str, scopes: List[str] = None,
                        expiry: int = None) -> str:
        """Create an API token for programmatic access"""
        scopes = scopes or ['read']
        expiry = expiry or (self.access_token_expiry * 8)  # Longer for API access

        claims = {
            'type': 'api',
            'scopes': scopes,
            'api_access': True
        }

        return self.create_access_token(user_id, claims)

    def verify_api_token(self, token: str, required_scopes: List[str] = None) -> Optional[Dict[str, Any]]:
        """Verify API token and check scopes"""
        payload = self.verify_token(token)

        if not payload:
            return None

        # Check if it's an API token
        if payload.get('type') != 'api':
            return None

        # Check required scopes
        if required_scopes:
            token_scopes = payload.get('scopes', [])
            for scope in required_scopes:
                if scope not in token_scopes:
                    return None

        return payload

    # ===== JWT AUTHENTICATION MIDDLEWARE =====

    def jwt_middleware(self, token: str, required_permissions: List[str] = None,
                      required_scopes: List[str] = None) -> Optional[Dict[str, Any]]:
        """
        JWT authentication middleware for protecting endpoints.
        Supports both regular tokens and specialized token types.
        """
        if not token:
            return None

        # Try different token types based on requirements
        if required_scopes:
            # API token verification
            return self.verify_api_token(token, required_scopes)
        elif required_permissions:
            # Websocket token verification
            return self.verify_websocket_token(token)
        else:
            # Regular access token verification
            return self.verify_user_access(token)

    def require_auth(self, token: str, user_id: str = None) -> bool:
        """Simple authentication check"""
        payload = self.verify_user_access(token)
        if not payload:
            return False

        if user_id and payload.get('user_id') != user_id:
            return False

        return True

    def require_permissions(self, token: str, permissions: List[str]) -> bool:
        """Check if token has required permissions"""
        payload = self.verify_websocket_token(token)
        if not payload:
            return False

        token_permissions = payload.get('permissions', [])
        return all(perm in token_permissions for perm in permissions)

    def require_scopes(self, token: str, scopes: List[str]) -> bool:
        """Check if token has required scopes"""
        payload = self.verify_api_token(token, scopes)
        return payload is not None


# ===== SESSION LIFECYCLE MANAGEMENT =====

cdef class SessionManager:
    """
    Session lifecycle management with expiration and cleanup.
    Supports multiple sessions per user with automatic cleanup.
    """

    def __cinit__(self, CyRedisClient redis_client, int session_timeout=3600,
                  int cleanup_interval=300):
        self.redis_client = redis_client
        self.session_timeout = session_timeout  # 1 hour default
        self.cleanup_interval = cleanup_interval  # 5 minutes
        self.sessions_key = "sessions:active"
        self.users_sessions_key = "sessions:by_user"
        self.last_cleanup = 0

    def create_session(self, user_id: str, session_data: Dict[str, Any] = None) -> str:
        """Create a new session for user"""
        session_id = str(uuid.uuid4())
        session_data = session_data or {}

        session = {
            'id': session_id,
            'user_id': user_id,
            'created_at': time.time(),
            'last_accessed': time.time(),
            'expires_at': time.time() + self.session_timeout,
            'data': session_data,
            'is_active': True
        }

        # Store session data
        self.redis_client.hset(f"session:{session_id}", mapping=session)

        # Add to active sessions set
        self.redis_client.sadd(self.sessions_key, session_id)

        # Add to user's sessions
        self.redis_client.sadd(f"{self.users_sessions_key}:{user_id}", session_id)

        return session_id

    def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get session data by ID"""
        session_data = self.redis_client.hgetall(f"session:{session_id}")

        if not session_data:
            return None

        # Check if session is expired or inactive
        expires_at = float(session_data.get(b'expires_at', b'0'))
        is_active = session_data.get(b'is_active', b'true').lower() == b'true'

        if not is_active or time.time() > expires_at:
            self.destroy_session(session_id)
            return None

        # Update last accessed time
        self.redis_client.hset(f"session:{session_id}", 'last_accessed', time.time())

        return {k.decode(): v.decode() for k, v in session_data.items()}

    def update_session(self, session_id: str, session_data: Dict[str, Any]) -> bool:
        """Update session data"""
        session = self.get_session(session_id)
        if not session:
            return False

        # Update session data
        update_data = {'data': json.dumps(session_data)}
        update_data['last_accessed'] = time.time()

        self.redis_client.hset(f"session:{session_id}", mapping=update_data)
        return True

    def destroy_session(self, session_id: str):
        """Destroy a session"""
        session_data = self.redis_client.hgetall(f"session:{session_id}")

        if session_data:
            user_id = session_data.get(b'user_id', b'').decode()

            # Remove from active sessions
            self.redis_client.srem(self.sessions_key, session_id)

            # Remove from user's sessions
            if user_id:
                self.redis_client.srem(f"{self.users_sessions_key}:{user_id}", session_id)

            # Delete session data
            self.redis_client.delete(f"session:{session_id}")

    def destroy_user_sessions(self, user_id: str):
        """Destroy all sessions for a user"""
        user_sessions = self.redis_client.smembers(f"{self.users_sessions_key}:{user_id}")

        for session_id_bytes in user_sessions:
            session_id = session_id_bytes.decode()
            self.destroy_session(session_id)

    def get_user_sessions(self, user_id: str) -> List[Dict[str, Any]]:
        """Get all active sessions for a user"""
        session_ids = self.redis_client.smembers(f"{self.users_sessions_key}:{user_id}")

        sessions = []
        for session_id_bytes in session_ids:
            session_id = session_id_bytes.decode()
            session = self.get_session(session_id)
            if session:
                sessions.append(session)

        return sessions

    def extend_session(self, session_id: str, additional_time: int = None) -> bool:
        """Extend session expiration time"""
        additional_time = additional_time or self.session_timeout
        session = self.get_session(session_id)

        if not session:
            return False

        new_expires_at = time.time() + additional_time
        self.redis_client.hset(f"session:{session_id}", 'expires_at', new_expires_at)

        return True

    def cleanup_expired_sessions(self):
        """Clean up expired sessions"""
        current_time = time.time()

        # Only cleanup if enough time has passed
        if current_time - self.last_cleanup < self.cleanup_interval:
            return

        self.last_cleanup = current_time

        # Get all active session IDs
        session_ids = self.redis_client.smembers(self.sessions_key)

        expired_count = 0
        for session_id_bytes in session_ids:
            session_id = session_id_bytes.decode()

            # Check if expired
            expires_at_data = self.redis_client.hget(f"session:{session_id}", 'expires_at')
            if expires_at_data:
                expires_at = float(expires_at_data)
                if current_time > expires_at:
                    self.destroy_session(session_id)
                    expired_count += 1

        return expired_count


# ===== TWO-FACTOR AUTHENTICATION =====

cdef class TwoFactorAuth:
    """
    Two-factor authentication support with TOTP and backup codes.
    """

    def __cinit__(self, CyRedisClient redis_client):
        self.redis_client = redis_client
        self.backup_codes_key = "2fa:backup_codes"
        self.totp_secrets_key = "2fa:totp_secrets"

    def enable_2fa(self, user_id: str) -> Dict[str, Any]:
        """Enable 2FA for user"""
        # Generate TOTP secret
        totp_secret = secrets.token_urlsafe(32)

        # Generate backup codes
        backup_codes = []
        for _ in range(10):
            code = ''.join(secrets.choice('0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ') for _ in range(8))
            backup_codes.append(code)

        # Hash backup codes for storage
        hashed_codes = [hashlib.sha256(code.encode()).hexdigest() for code in backup_codes]

        # Store data
        self.redis_client.hset(f"{self.totp_secrets_key}:{user_id}", 'secret', totp_secret)
        self.redis_client.hset(f"{self.backup_codes_key}:{user_id}", mapping={
            code_hash: '1' for code_hash in hashed_codes
        })

        return {
            'totp_secret': totp_secret,
            'backup_codes': backup_codes,
            'qr_code_url': self._generate_qr_url(user_id, totp_secret)
        }

    def verify_totp(self, user_id: str, token: str) -> bool:
        """Verify TOTP token"""
        secret_data = self.redis_client.hget(f"{self.totp_secrets_key}:{user_id}", 'secret')
        if not secret_data:
            return False

        totp_secret = secret_data.decode()
        return self._verify_totp_token(totp_secret, token)

    def verify_backup_code(self, user_id: str, code: str) -> bool:
        """Verify and consume backup code"""
        code_hash = hashlib.sha256(code.encode()).hexdigest()

        # Check if code exists and is unused
        if not self.redis_client.hexists(f"{self.backup_codes_key}:{user_id}", code_hash):
            return False

        # Mark code as used
        self.redis_client.hdel(f"{self.backup_codes_key}:{user_id}", code_hash)

        return True

    def disable_2fa(self, user_id: str):
        """Disable 2FA for user"""
        self.redis_client.delete(f"{self.totp_secrets_key}:{user_id}")
        self.redis_client.delete(f"{self.backup_codes_key}:{user_id}")

    def is_2fa_enabled(self, user_id: str) -> bool:
        """Check if 2FA is enabled for user"""
        return self.redis_client.exists(f"{self.totp_secrets_key}:{user_id}") > 0

    def get_remaining_backup_codes(self, user_id: str) -> int:
        """Get count of remaining backup codes"""
        return self.redis_client.hlen(f"{self.backup_codes_key}:{user_id}")

    def _verify_totp_token(self, secret: str, token: str) -> bool:
        """Verify TOTP token against secret"""
        # Simplified TOTP verification - in production use a proper library
        import hmac
        import hashlib
        import time
        import base64

        # Decode secret from base32 if needed
        try:
            # This is a simplified implementation
            # In production, use pyotp or similar library
            current_time = int(time.time() / 30)  # 30-second windows

            for time_offset in range(-1, 2):  # Check current and adjacent windows
                time_bytes = current_time + time_offset
                time_bytes = time_bytes.to_bytes(8, 'big')

                hmac_digest = hmac.new(
                    base64.b32decode(secret.upper()),
                    time_bytes,
                    hashlib.sha1
                ).digest()

                # Get the last 4 bytes and convert to integer
                offset = hmac_digest[-1] & 0x0F
                code = hmac_digest[offset:offset + 4]
                code_int = int.from_bytes(code, 'big') & 0x7FFFFFFF
                code_str = str(code_int % 1000000).zfill(6)

                if hmac.compare_digest(code_str, token):
                    return True

        except Exception:
            pass

        return False

    def _generate_qr_url(self, user_id: str, secret: str) -> str:
        """Generate QR code URL for TOTP setup"""
        service_name = "CyRedisApp"
        return f"otpauth://totp/{service_name}:{user_id}?secret={secret}&issuer={service_name}"


# ===== SHORT-LIVED PASSWORD TOKENS =====

cdef class PasswordResetManager:
    """
    Short-lived password tokens for password reset functionality.
    """

    def __cinit__(self, CyRedisClient redis_client, int token_expiry=900):
        self.redis_client = redis_client
        self.token_expiry = token_expiry  # 15 minutes
        self.tokens_key = "password_reset:tokens"

    def create_reset_token(self, user_id: str, email: str) -> str:
        """Create password reset token"""
        token = secrets.token_urlsafe(32)
        token_hash = hashlib.sha256(token.encode()).hexdigest()

        token_data = {
            'user_id': user_id,
            'email': email,
            'created_at': time.time(),
            'expires_at': time.time() + self.token_expiry,
            'used': False
        }

        self.redis_client.hset(f"{self.tokens_key}:{token_hash}", mapping=token_data)

        return token

    def verify_reset_token(self, token: str) -> Optional[Dict[str, Any]]:
        """Verify password reset token"""
        token_hash = hashlib.sha256(token.encode()).hexdigest()
        token_data = self.redis_client.hgetall(f"{self.tokens_key}:{token_hash}")

        if not token_data:
            return None

        # Check expiration
        expires_at = float(token_data.get(b'expires_at', b'0'))
        if time.time() > expires_at:
            self.redis_client.delete(f"{self.tokens_key}:{token_hash}")
            return None

        # Check if already used
        used = token_data.get(b'used', b'false').lower() == b'true'
        if used:
            return None

        # Mark as used
        self.redis_client.hset(f"{self.tokens_key}:{token_hash}", 'used', 'true')

        return {
            'user_id': token_data.get(b'user_id', b'').decode(),
            'email': token_data.get(b'email', b'').decode()
        }

    def cleanup_expired_tokens(self):
        """Clean up expired tokens"""
        # This would need pattern scanning in production
        # For now, tokens are cleaned up when verified
        pass


# ===== MULTI-SESSION TRACKING =====

cdef class MultiSessionTracker:
    """
    Track and manage multiple user sessions across different devices/browsers.
    """

    def __cinit__(self, CyRedisClient redis_client, SessionManager session_manager):
        self.redis_client = redis_client
        self.session_manager = session_manager
        self.sessions_key = "sessions:multi"

    def register_session(self, user_id: str, session_id: str,
                        device_info: Dict[str, Any] = None) -> bool:
        """Register a new session for tracking"""
        device_info = device_info or {}

        session_info = {
            'session_id': session_id,
            'user_id': user_id,
            'device_info': json.dumps(device_info),
            'registered_at': time.time(),
            'last_seen': time.time(),
            'is_active': True
        }

        # Store session info
        self.redis_client.hset(f"{self.sessions_key}:{session_id}", mapping=session_info)

        # Add to user's session list
        self.redis_client.sadd(f"{self.sessions_key}:user:{user_id}", session_id)

        return True

    def update_session_activity(self, session_id: str):
        """Update last seen time for session"""
        self.redis_client.hset(f"{self.sessions_key}:{session_id}", 'last_seen', time.time())

    def get_user_sessions(self, user_id: str) -> List[Dict[str, Any]]:
        """Get all sessions for a user"""
        session_ids = self.redis_client.smembers(f"{self.sessions_key}:user:{user_id}")

        sessions = []
        for session_id_bytes in session_ids:
            session_id = session_id_bytes.decode()
            session_data = self.redis_client.hgetall(f"{self.sessions_key}:{session_id}")

            if session_data:
                session_info = {k.decode(): v.decode() for k, v in session_data.items()}
                sessions.append(session_info)

        return sessions

    def revoke_session(self, session_id: str):
        """Revoke a specific session"""
        # Mark as inactive
        self.redis_client.hset(f"{self.sessions_key}:{session_id}", 'is_active', 'false')

        # Also destroy the actual session
        session_data = self.redis_client.hgetall(f"{self.sessions_key}:{session_id}")
        if session_data:
            actual_session_id = session_data.get(b'session_id', b'').decode()
            if actual_session_id:
                self.session_manager.destroy_session(actual_session_id)

    def revoke_all_user_sessions(self, user_id: str):
        """Revoke all sessions for a user"""
        sessions = self.get_user_sessions(user_id)

        for session in sessions:
            if session.get('is_active') == 'true':
                self.revoke_session(session['session_id'])

    def cleanup_inactive_sessions(self, max_age: int = 86400):
        """Clean up sessions that haven't been seen for too long"""
        current_time = time.time()
        cutoff_time = current_time - max_age

        # This would need pattern scanning in production
        # For now, sessions are checked when accessed
        pass


# ===== SHARED STATE MANAGEMENT =====

cdef class SharedStateManager:
    """
    Shared state management for worker processes.
    Provides distributed locks, counters, and shared data structures.
    """

    def __cinit__(self, CyRedisClient redis_client):
        self.redis_client = redis_client
        self.locks_key = "shared:locks"
        self.counters_key = "shared:counters"
        self.data_key = "shared:data"

    def acquire_lock(self, lock_name: str, timeout: int = 30) -> str:
        """Acquire a distributed lock"""
        lock_id = str(uuid.uuid4())
        lock_key = f"{self.locks_key}:{lock_name}"

        current_time = time.time()
        expires_at = current_time + timeout

        # Try to acquire lock
        if self.redis_client.set(lock_key, lock_id, ex=timeout, nx=True):
            return lock_id

        return None

    def release_lock(self, lock_name: str, lock_id: str) -> bool:
        """Release a distributed lock"""
        lock_key = f"{self.locks_key}:{lock_name}"
        stored_id = self.redis_client.get(lock_key)

        if stored_id == lock_id:
            self.redis_client.delete(lock_key)
            return True

        return False

    def increment_counter(self, counter_name: str, increment: int = 1) -> int:
        """Increment a shared counter"""
        counter_key = f"{self.counters_key}:{counter_name}"
        return self.redis_client.incr(counter_key, increment)

    def get_counter(self, counter_name: str) -> int:
        """Get counter value"""
        counter_key = f"{self.counters_key}:{counter_name}"
        value = self.redis_client.get(counter_key)
        return int(value) if value else 0

    def set_shared_data(self, key: str, data: Any, expiry: int = None):
        """Set shared data with optional expiry"""
        data_key = f"{self.data_key}:{key}"
        if expiry:
            self.redis_client.set(data_key, json.dumps(data), ex=expiry)
        else:
            self.redis_client.set(data_key, json.dumps(data))

    def get_shared_data(self, key: str) -> Any:
        """Get shared data"""
        data_key = f"{self.data_key}:{key}"
        data = self.redis_client.get(data_key)
        return json.loads(data) if data else None

    def publish_event(self, channel: str, event_data: Dict[str, Any]):
        """Publish event to shared channel"""
        self.redis_client.publish(channel, json.dumps(event_data))

    def subscribe_to_events(self, channels: List[str]) -> List[str]:
        """Subscribe to events (returns message format)"""
        # This is a simplified implementation
        # In production, you'd use Redis pub/sub properly
        return []


# ===== LIFECYCLE HOOKS =====

cdef class LifecycleManager:
    """
    Enhanced lifecycle hooks for startup/shutdown events with worker coordination.
    Manages initialization, cleanup, and graceful shutdown with workload yielding.
    """

    def __cinit__(self, CyRedisClient redis_client, str worker_id=None):
        self.redis_client = redis_client
        self.startup_hooks = []
        self.shutdown_hooks = []
        self.is_initialized = False
        self.worker_id = worker_id or self._generate_worker_id()

        # Worker coordination keys
        self.worker_status_key = "workers:status"
        self.workload_transfer_key = "workers:workload_transfer"
        self.heartbeat_key = f"workers:heartbeat:{self.worker_id}"

        # Register this worker
        self._register_worker()

    def _generate_worker_id(self) -> str:
        """Generate unique worker ID"""
        import os
        import socket
        import time

        pid = os.getpid()
        hostname = socket.gethostname()
        timestamp = int(time.time() * 1000)
        return f"worker_{hostname}_{pid}_{timestamp}"

    def _register_worker(self):
        """Register this worker in the cluster"""
        worker_info = {
            'worker_id': self.worker_id,
            'hostname': socket.gethostname(),
            'pid': os.getpid(),
            'status': 'starting',
            'started_at': time.time(),
            'last_heartbeat': time.time()
        }

        self.redis_client.hset(self.worker_status_key, self.worker_id, json.dumps(worker_info))

    def _update_heartbeat(self):
        """Update worker heartbeat"""
        self.redis_client.hset(self.heartbeat_key, 'last_heartbeat', time.time())

    def _mark_worker_dead(self):
        """Mark worker as dead"""
        worker_info = json.loads(self.redis_client.hget(self.worker_status_key, self.worker_id) or '{}')
        if worker_info:
            worker_info['status'] = 'dead'
            worker_info['died_at'] = time.time()
            self.redis_client.hset(self.worker_status_key, self.worker_id, json.dumps(worker_info))

    def add_startup_hook(self, hook_func: Callable, priority: int = 0):
        """Add a startup hook function with priority"""
        self.startup_hooks.append((priority, hook_func))
        # Sort by priority (lower number = higher priority)
        self.startup_hooks.sort(key=lambda x: x[0])

    def add_shutdown_hook(self, hook_func: Callable, priority: int = 0):
        """Add a shutdown hook function with priority"""
        self.shutdown_hooks.append((priority, hook_func))
        # Sort by priority (lower number = higher priority)
        self.shutdown_hooks.sort(key=lambda x: x[0])

    def initialize(self):
        """Run all startup hooks in priority order"""
        if self.is_initialized:
            return

        print(f"Initializing worker {self.worker_id}")

        # Update status
        self._update_worker_status('initializing')

        try:
            # Run startup hooks in priority order
            for priority, hook in self.startup_hooks:
                try:
                    hook()
                except Exception as e:
                    print(f"Startup hook (priority {priority}) error: {e}")

            self.is_initialized = True
            self._update_worker_status('running')
            print(f"Worker {self.worker_id} initialized successfully")

        except Exception as e:
            print(f"Worker {self.worker_id} initialization failed: {e}")
            self._update_worker_status('failed')
            raise

    def shutdown(self, graceful: bool = True):
        """Run all shutdown hooks with graceful workload yielding"""
        if not self.is_initialized:
            return

        print(f"Shutting down worker {self.worker_id} (graceful={graceful})")

        if graceful:
            # Graceful shutdown process
            self._graceful_shutdown()
        else:
            # Immediate shutdown
            self._immediate_shutdown()

    def _graceful_shutdown(self):
        """Perform graceful shutdown with workload yielding"""
        # Update status
        self._update_worker_status('shutting_down')

        try:
            # 1. Yield current workload to other workers
            self._yield_workload()

            # 2. Wait for active tasks to complete (configurable timeout)
            self._wait_for_tasks(timeout=30)

            # 3. Transfer session state if needed
            self._transfer_session_state()

            # 4. Run shutdown hooks in reverse priority order (cleanup first)
            for priority, hook in reversed(self.shutdown_hooks):
                try:
                    hook()
                except Exception as e:
                    print(f"Shutdown hook (priority {priority}) error: {e}")

            # 5. Mark worker as stopped
            self._update_worker_status('stopped')

        except Exception as e:
            print(f"Graceful shutdown error: {e}")
            # Fall back to immediate shutdown
            self._immediate_shutdown()

    def _immediate_shutdown(self):
        """Perform immediate shutdown without waiting"""
        try:
            # Run shutdown hooks in reverse priority order
            for priority, hook in reversed(self.shutdown_hooks):
                try:
                    hook()
                except Exception as e:
                    print(f"Shutdown hook (priority {priority}) error: {e}")

            # Mark worker as dead
            self._mark_worker_dead()

        finally:
            self.is_initialized = False
            print(f"Worker {self.worker_id} shutdown complete")

    def _yield_workload(self):
        """Yield current workload to other available workers"""
        try:
            # Get current worker's active tasks/sessions
            active_workload = self._get_active_workload()

            if active_workload:
                # Find available workers
                available_workers = self._get_available_workers()

                if available_workers:
                    # Distribute workload to other workers
                    self._distribute_workload(active_workload, available_workers)
                    print(f"Yielded {len(active_workload)} workload items to other workers")
                else:
                    print("No available workers to yield workload to")

        except Exception as e:
            print(f"Workload yielding error: {e}")

    def _get_active_workload(self) -> Dict[str, Any]:
        """Get current worker's active workload"""
        workload = {}

        try:
            # Get active sessions for this worker
            active_sessions = self.redis_client.smembers(f"sessions:worker:{self.worker_id}")
            workload['sessions'] = list(active_sessions)

            # Get pending tasks for this worker
            pending_tasks = self.redis_client.llen(f"tasks:worker:{self.worker_id}")
            workload['pending_tasks'] = pending_tasks

            # Get in-progress tasks
            processing_tasks = self.redis_client.hlen(f"tasks:processing:{self.worker_id}")
            workload['processing_tasks'] = processing_tasks

        except Exception as e:
            print(f"Error getting active workload: {e}")

        return workload

    def _get_available_workers(self) -> List[str]:
        """Get list of available workers"""
        try:
            workers = self.redis_client.hgetall(self.worker_status_key)
            available = []

            for worker_id, worker_info_str in workers.items():
                worker_info = json.loads(worker_info_str)
                if (worker_id != self.worker_id and
                    worker_info.get('status') == 'running' and
                    time.time() - worker_info.get('last_heartbeat', 0) < 60):  # 60 second timeout
                    available.append(worker_id)

            return available

        except Exception as e:
            print(f"Error getting available workers: {e}")
            return []

    def _distribute_workload(self, workload: Dict[str, Any], available_workers: List[str]):
        """Distribute workload to available workers"""
        try:
            # Distribute sessions
            if workload.get('sessions'):
                sessions_per_worker = len(workload['sessions']) // len(available_workers)
                for i, worker_id in enumerate(available_workers):
                    start_idx = i * sessions_per_worker
                    end_idx = start_idx + sessions_per_worker if i < len(available_workers) - 1 else len(workload['sessions'])
                    worker_sessions = workload['sessions'][start_idx:end_idx]

                    for session_id in worker_sessions:
                        # Transfer session ownership
                        self.redis_client.hset(f"session:{session_id}", 'worker_id', worker_id)

                    print(f"Transferred {len(worker_sessions)} sessions to worker {worker_id}")

            # Mark tasks for redistribution
            if workload.get('processing_tasks', 0) > 0:
                # Move processing tasks back to main queue for redistribution
                processing_key = f"tasks:processing:{self.worker_id}"
                tasks = self.redis_client.hgetall(processing_key)

                for task_id, task_data in tasks.items():
                    # Add back to main queue
                    self.redis_client.lpush("tasks:main_queue", task_data)
                    # Remove from processing
                    self.redis_client.hdel(processing_key, task_id)

                print(f"Redistributed {len(tasks)} processing tasks")

        except Exception as e:
            print(f"Error distributing workload: {e}")

    def _wait_for_tasks(self, timeout: int = 30):
        """Wait for active tasks to complete"""
        start_time = time.time()

        while time.time() - start_time < timeout:
            active_tasks = self._get_active_workload()

            # Check if we still have active work
            has_work = (len(active_tasks.get('sessions', [])) > 0 or
                       active_tasks.get('processing_tasks', 0) > 0)

            if not has_work:
                print("All active tasks completed")
                break

            print(f"Waiting for {len(active_tasks.get('sessions', []))} sessions and {active_tasks.get('processing_tasks', 0)} tasks to complete...")
            time.sleep(2)

        if time.time() - start_time >= timeout:
            print(f"Shutdown timeout reached after {timeout} seconds")

    def _transfer_session_state(self):
        """Transfer session state to other workers if needed"""
        # This would handle transferring in-memory session state
        # For now, we rely on Redis persistence
        pass

    def _update_worker_status(self, status: str):
        """Update worker status in Redis"""
        try:
            worker_info = json.loads(self.redis_client.hget(self.worker_status_key, self.worker_id) or '{}')
            worker_info['status'] = status
            worker_info['last_heartbeat'] = time.time()
            self.redis_client.hset(self.worker_status_key, self.worker_id, json.dumps(worker_info))
        except Exception as e:
            print(f"Error updating worker status: {e}")

    def get_worker_stats(self) -> Dict[str, Any]:
        """Get statistics for this worker"""
        try:
            worker_info = json.loads(self.redis_client.hget(self.worker_status_key, self.worker_id) or '{}')
            uptime = time.time() - worker_info.get('started_at', time.time())

            return {
                'worker_id': self.worker_id,
                'status': worker_info.get('status', 'unknown'),
                'uptime_seconds': uptime,
                'hostname': worker_info.get('hostname', 'unknown'),
                'pid': worker_info.get('pid', 0)
            }

        except Exception as e:
            print(f"Error getting worker stats: {e}")
            return {'error': str(e)}

    def is_healthy(self) -> bool:
        """Check if worker is healthy"""
        try:
            worker_info = json.loads(self.redis_client.hget(self.worker_status_key, self.worker_id) or '{}')
            last_heartbeat = worker_info.get('last_heartbeat', 0)
            return time.time() - last_heartbeat < 60  # 60 second timeout
        except Exception:
            return False


# ===== WORKER COORDINATION SYSTEM =====

cdef class WorkerCoordinator:
    """
    Coordinates multiple workers for graceful scaling and recovery.
    Handles worker registration, health monitoring, and workload distribution.
    """

    def __cinit__(self, CyRedisClient redis_client, str coordinator_id=None):
        self.redis_client = redis_client
        self.coordinator_id = coordinator_id or f"coordinator_{int(time.time())}"
        self.worker_status_key = "workers:status"
        self.dead_workers_key = "workers:dead"
        self.workload_distribution_key = "workers:workload_distribution"

    def register_worker(self, worker_id: str, worker_info: Dict[str, Any]):
        """Register a new worker"""
        worker_info['registered_at'] = time.time()
        worker_info['coordinator_id'] = self.coordinator_id

        self.redis_client.hset(self.worker_status_key, worker_id, json.dumps(worker_info))

    def unregister_worker(self, worker_id: str):
        """Unregister a worker"""
        self.redis_client.hdel(self.worker_status_key, worker_id)

    def get_all_workers(self) -> Dict[str, Dict[str, Any]]:
        """Get all registered workers"""
        workers = self.redis_client.hgetall(self.worker_status_key)
        return {worker_id: json.loads(worker_info) for worker_id, worker_info in workers.items()}

    def get_healthy_workers(self) -> List[str]:
        """Get list of healthy workers"""
        workers = self.get_all_workers()
        healthy = []

        for worker_id, worker_info in workers.items():
            if (worker_info.get('status') == 'running' and
                time.time() - worker_info.get('last_heartbeat', 0) < 60):
                healthy.append(worker_id)

        return healthy

    def detect_dead_workers(self) -> List[str]:
        """Detect workers that have stopped responding"""
        workers = self.get_all_workers()
        dead = []

        for worker_id, worker_info in workers.items():
            if (worker_info.get('status') != 'stopped' and
                time.time() - worker_info.get('last_heartbeat', 0) > 120):  # 2 minute timeout
                dead.append(worker_id)

        return dead

    def handle_dead_worker(self, worker_id: str):
        """Handle a dead worker by redistributing its workload"""
        print(f"Handling dead worker: {worker_id}")

        try:
            # Mark worker as dead
            worker_info = self.redis_client.hget(self.worker_status_key, worker_id)
            if worker_info:
                worker_info = json.loads(worker_info)
                worker_info['status'] = 'dead'
                worker_info['died_at'] = time.time()
                self.redis_client.hset(self.worker_status_key, worker_id, json.dumps(worker_info))

            # Redistribute workload
            self._redistribute_dead_worker_workload(worker_id)

        except Exception as e:
            print(f"Error handling dead worker {worker_id}: {e}")

    def _redistribute_dead_worker_workload(self, worker_id: str):
        """Redistribute workload from dead worker"""
        try:
            # Find sessions owned by dead worker
            dead_worker_sessions = self.redis_client.smembers(f"sessions:worker:{worker_id}")

            if dead_worker_sessions:
                # Find healthy workers to take over sessions
                healthy_workers = self.get_healthy_workers()
                if healthy_workers:
                    # Distribute sessions among healthy workers
                    sessions_per_worker = len(dead_worker_sessions) // len(healthy_workers)

                    for i, healthy_worker_id in enumerate(healthy_workers):
                        start_idx = i * sessions_per_worker
                        end_idx = start_idx + sessions_per_worker if i < len(healthy_workers) - 1 else len(dead_worker_sessions)
                        worker_sessions = list(dead_worker_sessions)[start_idx:end_idx]

                        for session_id in worker_sessions:
                            # Transfer session ownership
                            self.redis_client.hset(f"session:{session_id}", 'worker_id', healthy_worker_id)

                        print(f"Redistributed {len(worker_sessions)} sessions to worker {healthy_worker_id}")

            # Handle dead worker's processing tasks
            processing_key = f"tasks:processing:{worker_id}"
            dead_tasks = self.redis_client.hgetall(processing_key)

            if dead_tasks:
                for task_id, task_data in dead_tasks.items():
                    # Add back to main queue for redistribution
                    self.redis_client.lpush("tasks:main_queue", task_data)

                print(f"Redistributed {len(dead_tasks)} tasks from dead worker")

        except Exception as e:
            print(f"Error redistributing dead worker workload: {e}")

    def get_cluster_stats(self) -> Dict[str, Any]:
        """Get cluster-wide statistics"""
        workers = self.get_all_workers()

        stats = {
            'total_workers': len(workers),
            'healthy_workers': len(self.get_healthy_workers()),
            'dead_workers': len(self.detect_dead_workers()),
            'workers_by_status': {}
        }

        # Count workers by status
        for worker_info in workers.values():
            status = worker_info.get('status', 'unknown')
            stats['workers_by_status'][status] = stats['workers_by_status'].get(status, 0) + 1

        return stats


# ===== ENHANCED WEB APP SUPPORT WITH WORKER COORDINATION =====

class WebAppSupport:
    """
    Enhanced main class with worker coordination and graceful shutdown.
    """

    def __init__(self, redis_client: CyRedisClient = None,
                 host: str = "localhost", port: int = 6379):
        self.redis_client = redis_client or CyRedisClient(host, port)

        # Initialize all managers
        self.worker_queue = WorkerQueue("default", self.redis_client)
        self.token_manager = TokenManager(self.redis_client)
        self.session_manager = SessionManager(self.redis_client)
        self.two_factor_auth = TwoFactorAuth(self.redis_client)
        self.password_reset = PasswordResetManager(self.redis_client)
        self.multi_session_tracker = MultiSessionTracker(self.redis_client, self.session_manager)
        self.shared_state = SharedStateManager(self.redis_client)

        # Enhanced lifecycle manager with worker coordination
        self.lifecycle_manager = LifecycleManager(self.redis_client)

        # Worker coordinator for scaling and recovery
        self.worker_coordinator = WorkerCoordinator(self.redis_client)

        # Setup enhanced lifecycle hooks
        self._setup_enhanced_lifecycle_hooks()

    def _setup_enhanced_lifecycle_hooks(self):
        """Setup enhanced lifecycle hooks with worker coordination"""
        def enhanced_startup():
            self.worker_queue.start()
            print("WebAppSupport with worker coordination initialized")

            # Start background monitoring
            import threading
            monitor_thread = threading.Thread(target=self._monitor_workers, daemon=True)
            monitor_thread.start()

        def enhanced_shutdown():
            # Graceful shutdown by default
            self.lifecycle_manager.shutdown(graceful=True)
            self.worker_queue.stop()
            print("WebAppSupport with worker coordination shutdown")

        def immediate_shutdown():
            # Immediate shutdown for SIGKILL scenarios
            self.lifecycle_manager.shutdown(graceful=False)

        # Add hooks with priorities (0 = highest priority)
        self.lifecycle_manager.add_startup_hook(enhanced_startup, priority=0)
        self.lifecycle_manager.add_shutdown_hook(enhanced_shutdown, priority=0)

        # Register signal handlers for graceful shutdown
        import signal
        import sys

        def signal_handler(sig, frame):
            print(f"Received signal {sig}, initiating graceful shutdown...")
            if sig == signal.SIGTERM:
                enhanced_shutdown()
            else:
                immediate_shutdown()
            sys.exit(0)

        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

    def _monitor_workers(self):
        """Background worker monitoring for dead worker detection"""
        while True:
            try:
                dead_workers = self.worker_coordinator.detect_dead_workers()
                for worker_id in dead_workers:
                    self.worker_coordinator.handle_dead_worker(worker_id)

                time.sleep(30)  # Check every 30 seconds

            except Exception as e:
                print(f"Worker monitoring error: {e}")
                time.sleep(60)  # Wait longer on error

    def initialize(self):
        """Initialize the enhanced web app support system"""
        self.lifecycle_manager.initialize()

    def shutdown(self, graceful: bool = True):
        """Shutdown the enhanced web app support system"""
        self.lifecycle_manager.shutdown(graceful)

    def get_worker_info(self) -> Dict[str, Any]:
        """Get information about this worker"""
        return self.lifecycle_manager.get_worker_stats()

    def get_cluster_info(self) -> Dict[str, Any]:
        """Get information about the entire worker cluster"""
        return self.worker_coordinator.get_cluster_stats()

    def force_worker_recovery(self, worker_id: str):
        """Force recovery of a specific worker"""
        self.worker_coordinator.handle_dead_worker(worker_id)

    # Convenience methods for common operations
    def create_user_session(self, user_id: str, device_info: Dict[str, Any] = None) -> str:
        """Create a new user session with multi-session tracking"""
        session_id = self.session_manager.create_session(user_id)
        self.multi_session_tracker.register_session(user_id, session_id, device_info)
        return session_id

    def authenticate_user(self, user_id: str, password: str) -> Dict[str, Any]:
        """Authenticate user and return tokens and session info"""
        # This would integrate with your user authentication system
        access_token = self.token_manager.create_access_token(user_id)
        refresh_token = self.token_manager.create_refresh_token(user_id)

        return {
            'access_token': access_token,
            'refresh_token': refresh_token,
            'token_type': 'bearer'
        }

    def verify_user_access(self, token: str, required_claims: Dict[str, Any] = None) -> Optional[Dict[str, Any]]:
        """Verify user access token and check claims"""
        payload = self.token_manager.verify_token(token)
        if not payload:
            return None

        # Check required claims
        if required_claims:
            for claim, value in required_claims.items():
                if payload.get(claim) != value:
                    return None

        return payload


# ===== PYTHON WRAPPER CLASS =====

class WebApplicationSupport:
    """
    Python wrapper for WebAppSupport functionality with enhanced worker coordination.
    """

    def __init__(self, redis_client: CyRedisClient = None,
                 host: str = "localhost", port: int = 6379):
        self.redis_client = redis_client or CyRedisClient(host, port)

        # Initialize all managers
        self.worker_queue = WorkerQueue("default", self.redis_client)
        self.token_manager = TokenManager(self.redis_client)
        self.session_manager = SessionManager(self.redis_client)
        self.two_factor_auth = TwoFactorAuth(self.redis_client)
        self.password_reset = PasswordResetManager(self.redis_client)
        self.multi_session_tracker = MultiSessionTracker(self.redis_client, self.session_manager)
        self.shared_state = SharedStateManager(self.redis_client)

        # Enhanced lifecycle manager with worker coordination
        self.lifecycle_manager = LifecycleManager(self.redis_client)

        # Worker coordinator for scaling and recovery
        self.worker_coordinator = WorkerCoordinator(self.redis_client)

        # Setup enhanced lifecycle hooks
        self._setup_enhanced_lifecycle_hooks()

    def _setup_enhanced_lifecycle_hooks(self):
        """Setup enhanced lifecycle hooks with worker coordination"""
        def enhanced_startup():
            self.worker_queue.start()
            print("WebAppSupport with worker coordination initialized")

            # Start background monitoring
            import threading
            monitor_thread = threading.Thread(target=self._monitor_workers, daemon=True)
            monitor_thread.start()

        def enhanced_shutdown():
            # Graceful shutdown by default
            self.lifecycle_manager.shutdown(graceful=True)
            self.worker_queue.stop()
            print("WebAppSupport with worker coordination shutdown")

        def immediate_shutdown():
            # Immediate shutdown for SIGKILL scenarios
            self.lifecycle_manager.shutdown(graceful=False)

        # Add hooks with priorities (0 = highest priority)
        self.lifecycle_manager.add_startup_hook(enhanced_startup, priority=0)
        self.lifecycle_manager.add_shutdown_hook(enhanced_shutdown, priority=0)

        # Register signal handlers for graceful shutdown
        def signal_handler(sig, frame):
            print(f"Received signal {sig}, initiating graceful shutdown...")
            if sig == signal.SIGTERM:
                enhanced_shutdown()
            else:
                immediate_shutdown()
            sys.exit(0)

        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

    def _monitor_workers(self):
        """Background worker monitoring for dead worker detection"""
        while True:
            try:
                dead_workers = self.worker_coordinator.detect_dead_workers()
                for worker_id in dead_workers:
                    self.worker_coordinator.handle_dead_worker(worker_id)

                time.sleep(30)  # Check every 30 seconds

            except Exception as e:
                print(f"Worker monitoring error: {e}")
                time.sleep(60)  # Wait longer on error

    def initialize(self):
        """Initialize the web app support system"""
        self.lifecycle_manager.initialize()

    def shutdown(self):
        """Shutdown the web app support system"""
        self.lifecycle_manager.shutdown()

    # Convenience methods for common operations
    def create_user_session(self, user_id: str, device_info: Dict[str, Any] = None) -> str:
        """Create a new user session with multi-session tracking"""
        session_id = self.session_manager.create_session(user_id)
        self.multi_session_tracker.register_session(user_id, session_id, device_info)
        return session_id

    def authenticate_user(self, user_id: str, password: str) -> Dict[str, Any]:
        """Authenticate user and return tokens and session info"""
        # This would integrate with your user authentication system
        access_token = self.token_manager.create_access_token(user_id)
        refresh_token = self.token_manager.create_refresh_token(user_id)

        return {
            'access_token': access_token,
            'refresh_token': refresh_token,
            'token_type': 'bearer'
        }

    def verify_user_access(self, token: str, required_claims: Dict[str, Any] = None) -> Optional[Dict[str, Any]]:
        """Verify user access token and check claims"""
        payload = self.token_manager.verify_token(token)
        if not payload:
            return None

        # Check required claims
        if required_claims:
            for claim, value in required_claims.items():
                if payload.get(claim) != value:
                    return None

        return payload

    # ===== REDIS DATA STRUCTURE ITERATORS =====

    def stream_iterator(self, stream_key: str, consumer_group: str = None,
                       consumer_name: str = None, batch_size: int = 10,
                       block_ms: int = 1000) -> RedisStreamIterator:
        """Create an async iterator for Redis streams."""
        return RedisStreamIterator(
            self.redis_client, stream_key, consumer_group,
            consumer_name, batch_size, block_ms
        )

    def list_iterator(self, list_key: str, batch_size: int = 10,
                     block_ms: int = 1000) -> RedisListIterator:
        """Create an async iterator for Redis lists."""
        return RedisListIterator(
            self.redis_client, list_key, batch_size, block_ms
        )

    def pubsub_iterator(self, channels: Union[str, List[str]],
                       timeout_ms: int = 1000) -> RedisPubSubIterator:
        """Create an async iterator for Redis pub/sub."""
        return RedisPubSubIterator(
            self.redis_client, channels, timeout_ms
        )


# ===== REDIS DATA STRUCTURE ITERATORS =====

cdef class RedisStreamIterator:
    """
    Async iterator for Redis streams.
    Allows iteration over stream messages for real-time streaming.
    """

    def __cinit__(self, CyRedisClient redis_client, str stream_key,
                  str consumer_group=None, str consumer_name=None,
                  int batch_size=10, int block_ms=1000):
        self.redis_client = redis_client
        self.stream_key = stream_key
        self.consumer_group = consumer_group
        self.consumer_name = consumer_name
        self.batch_size = batch_size
        self.block_ms = block_ms
        self.last_id = "0"  # Start from beginning
        self.is_closed = False

    async def __aiter__(self):
        """Async iterator protocol."""
        return self

    async def __anext__(self):
        """Get next batch of messages."""
        if self.is_closed:
            raise StopAsyncIteration

        try:
            if self.consumer_group and self.consumer_name:
                # Use consumer group for reliable delivery
                messages = await self._read_from_consumer_group()
            else:
                # Use simple stream reading
                messages = await self._read_from_stream()

            if not messages:
                raise StopAsyncIteration

            # Update last_id for next iteration
            if messages:
                self.last_id = messages[-1]['id']

            return messages

        except Exception as e:
            self.is_closed = True
            raise StopAsyncIteration from e

    async def _read_from_stream(self):
        """Read messages from stream without consumer group."""
        try:
            # Use XREAD with BLOCK
            result = await self._execute_xread()

            if not result:
                return []

            # Parse the result
            messages = []
            for stream_data in result:
                if len(stream_data) >= 2:
                    stream_name = stream_data[0]
                    msg_list = stream_data[1]

                    for msg in msg_list:
                        if len(msg) >= 2:
                            msg_id = msg[0]
                            msg_data = {}

                            # Parse field-value pairs
                            for i in range(1, len(msg), 2):
                                if i + 1 < len(msg):
                                    field = msg[i]
                                    value = msg[i + 1]
                                    msg_data[field] = value

                            messages.append({
                                'id': msg_id,
                                'stream': stream_name,
                                'data': msg_data
                            })

            return messages[:self.batch_size]

        except Exception as e:
            print(f"Error reading from stream: {e}")
            return []

    async def _read_from_consumer_group(self):
        """Read messages from consumer group."""
        try:
            # Use XREADGROUP with BLOCK
            result = await self._execute_xreadgroup()

            if not result:
                return []

            # Parse consumer group result
            messages = []
            for stream_data in result:
                if len(stream_data) >= 2:
                    stream_name = stream_data[0]
                    msg_list = stream_data[1]

                    for msg in msg_list:
                        if len(msg) >= 2:
                            msg_id = msg[0]
                            msg_data = {}

                            # Parse field-value pairs
                            for i in range(1, len(msg), 2):
                                if i + 1 < len(msg):
                                    field = msg[i]
                                    value = msg[i + 1]
                                    msg_data[field] = value

                            messages.append({
                                'id': msg_id,
                                'stream': stream_name,
                                'data': msg_data
                            })

            return messages[:self.batch_size]

        except Exception as e:
            print(f"Error reading from consumer group: {e}")
            return []

    async def _execute_xread(self):
        """Execute XREAD command."""
        loop = asyncio.get_event_loop()

        def _xread_sync():
            return self.redis_client.xread(
                streams={self.stream_key: self.last_id},
                count=self.batch_size,
                block=self.block_ms
            )

        return await loop.run_in_executor(None, _xread_sync)

    async def _execute_xreadgroup(self):
        """Execute XREADGROUP command."""
        loop = asyncio.get_event_loop()

        def _xreadgroup_sync():
            # Create consumer group if it doesn't exist
            try:
                self.redis_client.xgroup_create(
                    self.stream_key,
                    self.consumer_group,
                    mkstream=True
                )
            except Exception:
                # Group might already exist
                pass

            return self.redis_client.xreadgroup(
                groupname=self.consumer_group,
                consumername=self.consumer_name,
                streams={self.stream_key: '>'},
                count=self.batch_size,
                block=self.block_ms
            )

        return await loop.run_in_executor(None, _xreadgroup_sync)

    def close(self):
        """Close the iterator."""
        self.is_closed = True

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        self.close()


cdef class RedisListIterator:
    """
    Async iterator for Redis lists.
    Allows iteration over list items for real-time streaming.
    """

    def __cinit__(self, CyRedisClient redis_client, str list_key,
                  int batch_size=10, int block_ms=1000):
        self.redis_client = redis_client
        self.list_key = list_key
        self.batch_size = batch_size
        self.block_ms = block_ms
        self.is_closed = False
        self.last_index = 0

    async def __aiter__(self):
        """Async iterator protocol."""
        return self

    async def __anext__(self):
        """Get next batch of list items."""
        if self.is_closed:
            raise StopAsyncIteration

        try:
            items = await self._read_from_list()

            if not items:
                raise StopAsyncIteration

            # Update last_index for next iteration
            self.last_index += len(items)

            return items

        except Exception as e:
            self.is_closed = True
            raise StopAsyncIteration from e

    async def _read_from_list(self):
        """Read items from list."""
        loop = asyncio.get_event_loop()

        def _list_read_sync():
            # Get list length
            list_length = self.redis_client.llen(self.list_key)

            if list_length == 0:
                return []

            # Calculate range to read
            start = self.last_index
            end = min(start + self.batch_size - 1, list_length - 1)

            if start >= list_length:
                return []

            # Read items from list
            items = self.redis_client.lrange(self.list_key, start, end)

            return items

        return await loop.run_in_executor(None, _list_read_sync)

    def close(self):
        """Close the iterator."""
        self.is_closed = True

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        self.close()


cdef class RedisPubSubIterator:
    """
    Async iterator for Redis pub/sub.
    Allows iteration over pub/sub messages for real-time streaming.
    """

    def __cinit__(self, CyRedisClient redis_client, list channels,
                  int timeout_ms=1000):
        self.redis_client = redis_client
        self.channels = channels if isinstance(channels, list) else [channels]
        self.timeout_ms = timeout_ms
        self.is_closed = False
        self.pubsub = None

    async def __aiter__(self):
        """Async iterator protocol."""
        return self

    async def __anext__(self):
        """Get next pub/sub message."""
        if self.is_closed:
            raise StopAsyncIteration

        try:
            if not self.pubsub:
                await self._initialize_pubsub()

            message = await self._get_next_message()

            if not message:
                raise StopAsyncIteration

            return message

        except Exception as e:
            self.is_closed = True
            raise StopAsyncIteration from e

    async def _initialize_pubsub(self):
        """Initialize pub/sub connection."""
        loop = asyncio.get_event_loop()

        def _init_pubsub():
            # Import redis here to avoid issues in Cython context
            import redis
            self.pubsub = self.redis_client.pubsub()

            # Subscribe to channels
            for channel in self.channels:
                self.pubsub.subscribe(channel)

            return True

        await loop.run_in_executor(None, _init_pubsub)

    async def _get_next_message(self):
        """Get next message from pub/sub."""
        loop = asyncio.get_event_loop()

        def _get_message():
            if not self.pubsub:
                return None

            # Get message with timeout
            message = self.pubsub.get_message(timeout=self.timeout_ms / 1000)

            if message and message['type'] == 'message':
                return {
                    'type': 'message',
                    'channel': message['channel'].decode() if isinstance(message['channel'], bytes) else message['channel'],
                    'data': message['data'].decode() if isinstance(message['data'], bytes) else message['data']
                }

            return None

        return await loop.run_in_executor(None, _get_message)

    def close(self):
        """Close the iterator."""
        self.is_closed = True
        if self.pubsub:
            try:
                self.pubsub.close()
            except Exception:
                pass
            self.pubsub = None

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        self.close()


# Python wrapper class
class WebApplicationSupport:
    """
    Python wrapper for WebAppSupport functionality.
    """

    def __init__(self, host: str = "localhost", port: int = 6379):
        self.support = WebAppSupport(host=host, port=port)

    def __enter__(self):
        self.support.initialize()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.support.shutdown()

    # Delegate all methods to the Cython implementation
    def create_user_session(self, user_id: str, device_info: Dict[str, Any] = None) -> str:
        return self.support.create_user_session(user_id, device_info)

    def authenticate_user(self, user_id: str, password: str) -> Dict[str, Any]:
        return self.support.authenticate_user(user_id, password)

    def verify_user_access(self, token: str, required_claims: Dict[str, Any] = None) -> Optional[Dict[str, Any]]:
        return self.support.verify_user_access(token, required_claims)

    # JWT authentication methods
    def jwt_middleware(self, token: str, required_permissions: List[str] = None,
                      required_scopes: List[str] = None) -> Optional[Dict[str, Any]]:
        """JWT authentication middleware for protecting endpoints."""
        return self.support.jwt_middleware(token, required_permissions, required_scopes)

    def require_auth(self, token: str, user_id: str = None) -> bool:
        """Simple authentication check."""
        return self.support.require_auth(token, user_id)

    def require_permissions(self, token: str, permissions: List[str]) -> bool:
        """Check if token has required permissions."""
        return self.support.require_permissions(token, permissions)

    def require_scopes(self, token: str, scopes: List[str]) -> bool:
        """Check if token has required scopes."""
        return self.support.require_scopes(token, scopes)

    def create_websocket_token(self, user_id: str, session_id: str = None,
                              permissions: List[str] = None, expiry: int = None) -> str:
        """Create a JWT token for websocket connections."""
        return self.support.create_websocket_token(user_id, session_id, permissions, expiry)

    def create_api_token(self, user_id: str, scopes: List[str] = None,
                        expiry: int = None) -> str:
        """Create an API token for programmatic access."""
        return self.support.create_api_token(user_id, scopes, expiry)

    # Worker queue methods
    def enqueue_task(self, task_data: Dict[str, Any], delay: int = 0) -> str:
        return self.support.worker_queue.enqueue(task_data, delay)

    def get_queue_stats(self) -> Dict[str, Any]:
        return self.support.worker_queue.get_queue_stats()

    # Session management methods
    def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        return self.support.session_manager.get_session(session_id)

    def destroy_session(self, session_id: str):
        return self.support.session_manager.destroy_session(session_id)

    # 2FA methods
    def enable_2fa(self, user_id: str) -> Dict[str, Any]:
        return self.support.two_factor_auth.enable_2fa(user_id)

    def verify_totp(self, user_id: str, token: str) -> bool:
        return self.support.two_factor_auth.verify_totp(user_id, token)

    # Token management methods
    def refresh_access_token(self, refresh_token: str) -> Optional[str]:
        return self.support.token_manager.refresh_access_token(refresh_token)

    def revoke_token(self, token: str):
        return self.support.token_manager.revoke_token(token)

    # Shared state methods
    def increment_counter(self, counter_name: str, increment: int = 1) -> int:
        return self.support.shared_state.increment_counter(counter_name, increment)

    def get_counter(self, counter_name: str) -> int:
        return self.support.shared_state.get_counter(counter_name)

    def set_shared_data(self, key: str, data: Any, expiry: int = None):
        return self.support.shared_state.set_shared_data(key, data, expiry)

    def get_shared_data(self, key: str) -> Any:
        return self.support.shared_state.get_shared_data(key)

    # Redis streaming iterators
    def stream_iterator(self, stream_key: str, consumer_group: str = None,
                       consumer_name: str = None, batch_size: int = 10,
                       block_ms: int = 1000):
        """Create an async iterator for Redis streams."""
        return self.support.stream_iterator(stream_key, consumer_group, consumer_name, batch_size, block_ms)

    def list_iterator(self, list_key: str, batch_size: int = 10,
                     block_ms: int = 1000):
        """Create an async iterator for Redis lists."""
        return self.support.list_iterator(list_key, batch_size, block_ms)

    def pubsub_iterator(self, channels, timeout_ms: int = 1000):
        """Create an async iterator for Redis pub/sub."""
        return self.support.pubsub_iterator(channels, timeout_ms)

    # Worker coordination methods
    def get_worker_info(self) -> Dict[str, Any]:
        """Get information about this worker"""
        return self.support.get_worker_info()

    def get_cluster_info(self) -> Dict[str, Any]:
        """Get information about the entire worker cluster"""
        return self.support.get_cluster_info()

    def force_worker_recovery(self, worker_id: str):
        """Force recovery of a specific worker"""
        self.support.force_worker_recovery(worker_id)

    def shutdown_graceful(self):
        """Perform graceful shutdown with workload yielding"""
        self.support.shutdown(graceful=True)

    def shutdown_immediate(self):
        """Perform immediate shutdown without waiting"""
        self.support.shutdown(graceful=False)

    # Shared dictionary methods
    def get_shared_dict(self, name: str) -> 'ConcurrentSharedDict':
        """Get a concurrent shared dictionary by name"""
        return ConcurrentSharedDict(name, self.support.redis_client)

    def create_shared_dict(self, name: str, initial_data: Dict[str, Any] = None) -> 'ConcurrentSharedDict':
        """Create a new concurrent shared dictionary"""
        shared_dict = ConcurrentSharedDict(name, self.support.redis_client)
        if initial_data:
            shared_dict.update(initial_data)
        return shared_dict


# ===== CONCURRENT SHARED DICTIONARY =====

cdef class ConcurrentSharedDict:
    """
    Concurrent shared dictionary for all users with Redis replication.
    Thread-safe and process-safe dictionary that can be accessed by multiple users.
    """

    def __cinit__(self, str dict_name, CyRedisClient redis_client):
        self.dict_name = dict_name
        self.redis_client = redis_client
        self.dict_key = f"shared_dict:{dict_name}"
        self.lock_key = f"{self.dict_key}:lock"
        self.local_cache = {}
        self.cache_ttl = 30  # 30 seconds cache
        self.last_sync = 0

        # Initialize distributed lock for concurrency control
        self.lock = CyDistributedLock(redis_client, self.lock_key, ttl_ms=5000)

    cdef dict _load_from_redis(self):
        """Load dictionary from Redis with caching"""
        cdef str data = self.redis_client.get(self.dict_key)
        if data:
            try:
                return json.loads(data)
            except json.JSONDecodeError:
                return {}
        return {}

    cdef void _save_to_redis(self, dict data):
        """Save dictionary to Redis"""
        cdef str json_data = json.dumps(data, sort_keys=True)
        self.redis_client.set(self.dict_key, json_data)

    cdef bint _is_cache_valid(self):
        """Check if local cache is still valid"""
        return (time.time() - self.last_sync) < self.cache_ttl

    cdef void _invalidate_cache(self):
        """Invalidate local cache"""
        self.last_sync = 0
        self.local_cache.clear()

    cdef dict _ensure_synced(self):
        """Ensure local cache is synced with Redis"""
        if not self._is_cache_valid():
            self.local_cache = self._load_from_redis()
            self.last_sync = <long>time.time()
        return self.local_cache

    # Dictionary interface - thread-safe and process-safe
    def __getitem__(self, key):
        """Get item with automatic sync"""
        cdef dict data = self._ensure_synced()
        if key not in data:
            raise KeyError(key)
        return data[key]

    def __setitem__(self, key, value):
        """Set item with distributed locking"""
        if not self.lock.try_acquire(blocking=True, timeout=5.0):
            raise RuntimeError(f"Could not acquire lock for shared dict '{self.dict_name}'")

        try:
            cdef dict data = self._load_from_redis()  # Always get latest from Redis
            data[key] = value
            self._save_to_redis(data)
            self._invalidate_cache()  # Force sync on next access
        finally:
            self.lock.release()

    def __delitem__(self, key):
        """Delete item with distributed locking"""
        if not self.lock.try_acquire(blocking=True, timeout=5.0):
            raise RuntimeError(f"Could not acquire lock for shared dict '{self.dict_name}'")

        try:
            cdef dict data = self._load_from_redis()
            if key in data:
                del data[key]
                self._save_to_redis(data)
                self._invalidate_cache()
        finally:
            self.lock.release()

    def __contains__(self, key):
        """Check if key exists"""
        cdef dict data = self._ensure_synced()
        return key in data

    def __len__(self):
        """Get dictionary length"""
        cdef dict data = self._ensure_synced()
        return len(data)

    def __iter__(self):
        """Iterate over keys"""
        cdef dict data = self._ensure_synced()
        return iter(data)

    def keys(self):
        """Get dictionary keys"""
        cdef dict data = self._ensure_synced()
        return data.keys()

    def values(self):
        """Get dictionary values"""
        cdef dict data = self._ensure_synced()
        return data.values()

    def items(self):
        """Get dictionary items"""
        cdef dict data = self._ensure_synced()
        return data.items()

    def get(self, key, default=None):
        """Get with default value"""
        cdef dict data = self._ensure_synced()
        return data.get(key, default)

    # Advanced concurrent operations
    def update(self, other: Dict[str, Any]):
        """Update dictionary with another dict (atomic operation)"""
        if not self.lock.try_acquire(blocking=True, timeout=5.0):
            raise RuntimeError(f"Could not acquire lock for shared dict '{self.dict_name}'")

        try:
            cdef dict data = self._load_from_redis()
            data.update(other)
            self._save_to_redis(data)
            self._invalidate_cache()
        finally:
            self.lock.release()

    def clear(self):
        """Clear the dictionary (atomic operation)"""
        if not self.lock.try_acquire(blocking=True, timeout=5.0):
            raise RuntimeError(f"Could not acquire lock for shared dict '{self.dict_name}'")

        try:
            self._save_to_redis({})
            self._invalidate_cache()
        finally:
            self.lock.release()

    def pop(self, key, default=None):
        """Pop item from dictionary (atomic operation)"""
        if not self.lock.try_acquire(blocking=True, timeout=5.0):
            raise RuntimeError(f"Could not acquire lock for shared dict '{self.dict_name}'")

        try:
            cdef dict data = self._load_from_redis()
            cdef object result = data.pop(key, default)
            self._save_to_redis(data)
            self._invalidate_cache()
            return result
        finally:
            self.lock.release()

    def popitem(self):
        """Pop random item from dictionary (atomic operation)"""
        if not self.lock.try_acquire(blocking=True, timeout=5.0):
            raise RuntimeError(f"Could not acquire lock for shared dict '{self.dict_name}'")

        try:
            cdef dict data = self._load_from_redis()
            cdef tuple result = data.popitem()
            self._save_to_redis(data)
            self._invalidate_cache()
            return result
        finally:
            self.lock.release()

    def setdefault(self, key, default=None):
        """Set default value if key doesn't exist (atomic operation)"""
        if not self.lock.try_acquire(blocking=True, timeout=5.0):
            raise RuntimeError(f"Could not acquire lock for shared dict '{self.dict_name}'")

        try:
            cdef dict data = self._load_from_redis()
            if key not in data:
                data[key] = default
                self._save_to_redis(data)
                self._invalidate_cache()
            return data[key]
        finally:
            self.lock.release()

    # Atomic numeric operations
    def increment(self, key: str, amount: int = 1) -> int:
        """Atomically increment a numeric value"""
        if not self.lock.try_acquire(blocking=True, timeout=5.0):
            raise RuntimeError(f"Could not acquire lock for shared dict '{self.dict_name}'")

        try:
            cdef dict data = self._load_from_redis()
            cdef int current = data.get(key, 0)
            if not isinstance(current, int):
                current = 0
            current += amount
            data[key] = current
            self._save_to_redis(data)
            self._invalidate_cache()
            return current
        finally:
            self.lock.release()

    def increment_float(self, key: str, amount: float = 1.0) -> float:
        """Atomically increment a float value"""
        if not self.lock.try_acquire(blocking=True, timeout=5.0):
            raise RuntimeError(f"Could not acquire lock for shared dict '{self.dict_name}'")

        try:
            cdef dict data = self._load_from_redis()
            cdef float current = data.get(key, 0.0)
            if not isinstance(current, (int, float)):
                current = 0.0
            current += amount
            data[key] = current
            self._save_to_redis(data)
            self._invalidate_cache()
            return current
        finally:
            self.lock.release()

    # Bulk operations for efficiency
    def bulk_update(self, updates: Dict[str, Any]):
        """Bulk update multiple keys at once (atomic operation)"""
        if not self.lock.try_acquire(blocking=True, timeout=5.0):
            raise RuntimeError(f"Could not acquire lock for shared dict '{self.dict_name}'")

        try:
            cdef dict data = self._load_from_redis()
            data.update(updates)
            self._save_to_redis(data)
            self._invalidate_cache()
        finally:
            self.lock.release()

    def bulk_get(self, keys: List[str]) -> List[Any]:
        """Bulk get multiple keys efficiently"""
        cdef dict data = self._ensure_synced()
        return [data.get(key) for key in keys]

    def multi_get(self, *keys: str) -> Dict[str, Any]:
        """Get multiple keys as a dictionary"""
        cdef dict data = self._ensure_synced()
        return {key: data.get(key) for key in keys}

    def multi_set(self, mapping: Dict[str, Any]):
        """Set multiple key-value pairs (atomic operation)"""
        if not self.lock.try_acquire(blocking=True, timeout=5.0):
            raise RuntimeError(f"Could not acquire lock for shared dict '{self.dict_name}'")

        try:
            cdef dict data = self._load_from_redis()
            data.update(mapping)
            self._save_to_redis(data)
            self._invalidate_cache()
        finally:
            self.lock.release()

    # Synchronization and statistics
    def sync(self):
        """Force synchronization with Redis"""
        self._invalidate_cache()
        self._ensure_synced()

    def is_synced(self) -> bool:
        """Check if local cache is synchronized"""
        return self._is_cache_valid()

    def get_stats(self) -> Dict[str, Any]:
        """Get dictionary statistics"""
        cdef dict data = self._ensure_synced()
        cdef long total_size = len(json.dumps(data))

        return {
            'name': self.dict_name,
            'key_count': len(data),
            'total_size_bytes': total_size,
            'cache_age_seconds': <long>time.time() - self.last_sync,
            'cache_ttl_seconds': self.cache_ttl,
            'lock_key': self.lock_key,
            'redis_key': self.dict_key
        }

    def copy(self) -> Dict[str, Any]:
        """Create a copy of the dictionary"""
        cdef dict data = self._ensure_synced()
        return data.copy()

    def __repr__(self):
        """String representation"""
        cdef dict data = self._ensure_synced()
        return f"ConcurrentSharedDict('{self.dict_name}', {len(data)} items)"

    def __str__(self):
        """String representation"""
        return self.__repr__()


# Python wrapper for ConcurrentSharedDict
class ConcurrentSharedDictWrapper:
    """
    Python wrapper for ConcurrentSharedDict providing dict-like interface.
    """

    def __init__(self, dict_name: str, redis_client: CyRedisClient):
        self.dict = ConcurrentSharedDict(dict_name, redis_client)

    def __getitem__(self, key):
        return self.dict[key]

    def __setitem__(self, key, value):
        self.dict[key] = value

    def __delitem__(self, key):
        del self.dict[key]

    def __contains__(self, key):
        return key in self.dict

    def __len__(self):
        return len(self.dict)

    def __iter__(self):
        return iter(self.dict)

    def keys(self):
        return self.dict.keys()

    def values(self):
        return self.dict.values()

    def items(self):
        return self.dict.items()

    def get(self, key, default=None):
        return self.dict.get(key, default)

    def update(self, other):
        self.dict.update(other)

    def clear(self):
        self.dict.clear()

    def pop(self, key, default=None):
        return self.dict.pop(key, default)

    def popitem(self):
        return self.dict.popitem()

    def setdefault(self, key, default=None):
        return self.dict.setdefault(key, default)

    def increment(self, key: str, amount: int = 1) -> int:
        return self.dict.increment(key, amount)

    def increment_float(self, key: str, amount: float = 1.0) -> float:
        return self.dict.increment_float(key, amount)

    def bulk_update(self, updates):
        self.dict.bulk_update(updates)

    def bulk_get(self, keys):
        return self.dict.bulk_get(keys)

    def multi_get(self, *keys):
        return self.dict.multi_get(*keys)

    def multi_set(self, mapping):
        self.dict.multi_set(mapping)

    def sync(self):
        self.dict.sync()

    def is_synced(self) -> bool:
        return self.dict.is_synced()

    def get_stats(self):
        return self.dict.get_stats()

    def copy(self):
        return self.dict.copy()

    def __repr__(self):
        return self.dict.__repr__()

    def __str__(self):
        return self.dict.__str__()
