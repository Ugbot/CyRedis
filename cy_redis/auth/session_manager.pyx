# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# cython: nonecheck=False
# distutils: language = c

"""
Session Lifecycle Management for CyRedis Web Application Support.
Provides session creation, management, expiration, and cleanup.
"""

import json
import time
import uuid
from typing import Dict, List, Optional, Any

# Import core Redis functionality
from cy_redis.core.cy_redis_client cimport CyRedisClient


# Exception classes
class SessionError(Exception):
    pass


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
