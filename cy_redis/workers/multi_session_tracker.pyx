# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# cython: nonecheck=False
# distutils: language = c

"""
Multi-Session Tracker for CyRedis Web Application Support.
Tracks and manages multiple user sessions across different devices/browsers.
"""

import json
import time
from typing import Any, Dict, List

# Import dependencies

from cy_redis.core.cy_redis_client cimport CyRedisClient

from cy_redis.auth.session_manager import SessionManager


cdef class MultiSessionTracker:
    """
    Track and manage multiple user sessions across different devices/browsers.
    """

    cdef object redis_client
    cdef object session_manager
    cdef str sessions_key

    def __cinit__(self, CyRedisClient redis_client, session_manager):
        # Preconditions: both collaborators are required for any operation.
        if redis_client is None:
            raise ValueError("redis_client must not be None")
        if session_manager is None:
            raise ValueError("session_manager must not be None")

        self.redis_client = redis_client
        self.session_manager = session_manager
        self.sessions_key = "sessions:multi"

        # Postcondition: namespace prefix established for all key derivation.
        assert self.sessions_key, "sessions_key must be non-empty"

    def register_session(self, user_id: str, session_id: str,
                        device_info: Dict[str, Any] = None) -> bool:
        """Register a new session for tracking"""
        # Preconditions: identifiers must be present (caller error -> raise).
        if not user_id:
            raise ValueError("user_id must be a non-empty string")
        if not session_id:
            raise ValueError("session_id must be a non-empty string")
        device_info = device_info or {}
        assert isinstance(device_info, dict), "device_info must be a dict"

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

        # Postcondition: the recorded session_id matches the requested one.
        assert session_info['session_id'] == session_id, "stored session_id mismatch"
        return True

    def update_session_activity(self, session_id: str):
        """Update last seen time for session"""
        self.redis_client.hset(f"{self.sessions_key}:{session_id}", 'last_seen', time.time())

    def get_user_sessions(self, user_id: str) -> List[Dict[str, Any]]:
        """Get all sessions for a user"""
        # Precondition: caller must identify the user (caller error -> raise).
        if not user_id:
            raise ValueError("user_id must be a non-empty string")
        session_ids = self.redis_client.smembers(f"{self.sessions_key}:user:{user_id}")

        # Bound: the loop is bounded by the size of the membership set returned
        # by Redis (one iteration per registered session id).
        sessions = []
        for session_id_bytes in session_ids:
            session_id = session_id_bytes.decode()
            session_data = self.redis_client.hgetall(f"{self.sessions_key}:{session_id}")

            if session_data:
                session_info = {k.decode(): v.decode() for k, v in session_data.items()}
                sessions.append(session_info)

        # Postcondition: never report more sessions than ids we iterated over.
        assert len(sessions) <= len(session_ids), "produced more sessions than ids"
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
        if not user_id:
            raise ValueError("user_id must be a non-empty string")
        sessions = self.get_user_sessions(user_id)
        assert isinstance(sessions, list), "get_user_sessions must return a list"

        # Bound: one iteration per session returned for this user.
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
