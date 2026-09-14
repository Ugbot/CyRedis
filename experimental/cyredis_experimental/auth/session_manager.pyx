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
from typing import Any, Dict, List, Optional

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

    cdef object redis_client
    cdef int session_timeout
    cdef int cleanup_interval
    cdef double last_cleanup
    cdef str sessions_key
    cdef str users_sessions_key

    def __cinit__(self, CyRedisClient redis_client, int session_timeout=3600,
                  int cleanup_interval=300):
        # Preconditions: timeouts must be positive durations in seconds.
        assert redis_client is not None, "redis_client must not be None"
        assert session_timeout > 0, "session_timeout must be positive"
        assert cleanup_interval > 0, "cleanup_interval must be positive"

        self.redis_client = redis_client
        self.session_timeout = session_timeout  # 1 hour default
        self.cleanup_interval = cleanup_interval  # 5 minutes
        self.sessions_key = "sessions:active"
        self.users_sessions_key = "sessions:by_user"
        self.last_cleanup = 0

        # Postcondition: the two index keys must be distinct, or active-set
        # and per-user-set bookkeeping would collide.
        assert self.sessions_key != self.users_sessions_key, "index keys must differ"

    def create_session(self, user_id: str, session_data: Dict[str, Any] = None) -> str:
        """Create a new session for user"""
        assert user_id, "user_id must be a non-empty string"
        assert session_data is None or isinstance(session_data, dict), (
            "session_data must be a dict or None"
        )

        session_id = str(uuid.uuid4())
        session_data = session_data or {}
        now = time.time()
        expires_at = now + self.session_timeout

        session = {
            'id': session_id,
            'user_id': user_id,
            'created_at': now,
            'last_accessed': now,
            'expires_at': expires_at,
            'data': session_data,
            'is_active': True
        }

        # Invariant: a freshly created session must expire in the future.
        assert session['expires_at'] > session['created_at'], (
            "new session must expire after creation"
        )
        # A uuid4 string is canonically 36 chars (32 hex + 4 dashes).
        assert len(session_id) == 36, "uuid4 session id must be 36 chars"

        # Store session data
        self.redis_client.hset(f"session:{session_id}", mapping=session)
        # Bound the session in Redis itself so expired sessions are reclaimed
        # even if no code reads them again.
        self.redis_client.expire(f"session:{session_id}", self.session_timeout)

        # Add to active sessions set
        self.redis_client.sadd(self.sessions_key, session_id)

        # Add to user's sessions
        self.redis_client.sadd(f"{self.users_sessions_key}:{user_id}", session_id)

        return session_id

    def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get session data by ID"""
        assert session_id, "session_id must be a non-empty string"

        session_data = self.redis_client.hgetall(f"session:{session_id}")

        if not session_data:
            return None

        # Check if session is expired or inactive. The native client decodes
        # hash fields to str, so read string keys (not bytes).
        expires_at = float(session_data.get('expires_at', '0'))
        is_active = str(session_data.get('is_active', 'true')).lower() == 'true'

        session_is_expired = time.time() > expires_at
        if not is_active or session_is_expired:
            self.destroy_session(session_id)
            return None

        # Update last accessed time
        self.redis_client.hset(f"session:{session_id}", 'last_accessed', time.time())

        return dict(session_data)

    def update_session(self, session_id: str, session_data: Dict[str, Any]) -> bool:
        """Update session data"""
        assert session_id, "session_id must be a non-empty string"
        assert isinstance(session_data, dict), "session_data must be a dict"

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
        assert session_id, "session_id must be a non-empty string"
        session_data = self.redis_client.hgetall(f"session:{session_id}")

        if session_data:
            user_id = session_data.get('user_id', '')

            # Remove from active sessions
            self.redis_client.srem(self.sessions_key, session_id)

            # Remove from user's sessions
            if user_id:
                self.redis_client.srem(f"{self.users_sessions_key}:{user_id}", session_id)

            # Delete session data
            self.redis_client.delete(f"session:{session_id}")

    def destroy_user_sessions(self, user_id: str):
        """Destroy all sessions for a user"""
        assert user_id, "user_id must be a non-empty string"
        user_sessions = self.redis_client.smembers(f"{self.users_sessions_key}:{user_id}")

        # Fail-safe cap: one user should never accumulate more sessions than
        # this; the bound turns a runaway set into a loud, local failure.
        MAX_SESSIONS_PER_USER = 1_000_000
        assert len(user_sessions) <= MAX_SESSIONS_PER_USER, "user session set too large"

        for session_id in user_sessions:
            self.destroy_session(session_id)

    def get_user_sessions(self, user_id: str) -> List[Dict[str, Any]]:
        """Get all active sessions for a user"""
        assert user_id, "user_id must be a non-empty string"
        session_ids = self.redis_client.smembers(f"{self.users_sessions_key}:{user_id}")

        MAX_SESSIONS_PER_USER = 1_000_000
        assert len(session_ids) <= MAX_SESSIONS_PER_USER, "user session set too large"

        sessions = []
        for session_id in session_ids:
            session = self.get_session(session_id)
            if session:
                sessions.append(session)

        # Postcondition: we can never return more live sessions than ids seen.
        assert len(sessions) <= len(session_ids), "more sessions than ids"
        return sessions

    def extend_session(self, session_id: str, additional_time: int = None) -> bool:
        """Extend session expiration time"""
        assert session_id, "session_id must be a non-empty string"
        assert additional_time is None or additional_time > 0, (
            "additional_time must be positive when supplied"
        )

        additional_time = additional_time or self.session_timeout
        session = self.get_session(session_id)

        if not session:
            return False

        new_expires_at = time.time() + additional_time
        # Invariant: extending a session must push expiry into the future.
        assert new_expires_at > time.time() - 1, "extended expiry must be in the future"
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

        # Fail-safe cap on the active-session set; the loop is bounded by the
        # set size, and this turns an unbounded set into a loud failure.
        MAX_ACTIVE_SESSIONS = 10_000_000
        assert len(session_ids) <= MAX_ACTIVE_SESSIONS, "active session set too large"

        expired_count = 0
        for session_id in session_ids:
            # Check if expired
            expires_at_data = self.redis_client.hget(f"session:{session_id}", 'expires_at')
            if expires_at_data:
                expires_at = float(expires_at_data)
                if current_time > expires_at:
                    self.destroy_session(session_id)
                    expired_count += 1

        # Postcondition: we cannot expire more sessions than we examined.
        assert expired_count <= len(session_ids), "expired more sessions than scanned"
        return expired_count
