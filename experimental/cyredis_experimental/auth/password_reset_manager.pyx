# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# cython: nonecheck=False
# distutils: language = c

"""
Password Reset Manager for CyRedis Web Application Support.
Provides short-lived tokens for password reset functionality.
"""

import hashlib
import secrets
import time
from typing import Any, Dict, Optional

# Import core Redis functionality

from cy_redis.core.cy_redis_client cimport CyRedisClient


cdef class PasswordResetManager:
    """
    Short-lived password tokens for password reset functionality.
    """

    cdef object redis_client
    cdef int token_expiry
    cdef str tokens_key

    def __cinit__(self, CyRedisClient redis_client, int token_expiry=900):
        # Preconditions for invariants the rest of the class depends on.
        assert redis_client is not None, "redis_client must not be None"
        assert token_expiry > 0, "token_expiry must be positive"

        self.redis_client = redis_client
        self.token_expiry = token_expiry  # 15 minutes
        self.tokens_key = "password_reset:tokens"

        assert self.tokens_key, "tokens_key prefix must be non-empty"

    def create_reset_token(self, user_id: str, email: str) -> str:
        """Create password reset token"""
        assert user_id, "user_id must be a non-empty string"
        assert email, "email must be a non-empty string"

        token = secrets.token_urlsafe(32)
        assert token, "generated reset token must be non-empty"

        token_hash = hashlib.sha256(token.encode()).hexdigest()
        assert len(token_hash) == 64, "sha256 hexdigest must be 64 chars"

        now = time.time()
        token_data = {
            'user_id': user_id,
            'email': email,
            'created_at': now,
            'expires_at': now + self.token_expiry,
            'used': False
        }

        # Invariant: a freshly minted token must expire strictly in the future.
        assert token_data['expires_at'] > token_data['created_at'], (
            "reset token must expire after creation"
        )

        self.redis_client.hset(f"{self.tokens_key}:{token_hash}", mapping=token_data)
        # Expire the token in Redis at its logical lifetime so stale tokens
        # cannot accumulate and a leaked token cannot be replayed past expiry.
        self.redis_client.expire(f"{self.tokens_key}:{token_hash}", self.token_expiry)

        return token

    def verify_reset_token(self, token: str) -> Optional[Dict[str, Any]]:
        """Verify password reset token"""
        if not token:
            return None
        assert isinstance(token, str), "token must be a str"

        token_hash = hashlib.sha256(token.encode()).hexdigest()
        assert len(token_hash) == 64, "sha256 hexdigest must be 64 chars"

        token_data = self.redis_client.hgetall(f"{self.tokens_key}:{token_hash}")

        if not token_data:
            return None

        # Check expiration. The native client decodes hash fields to str, so
        # read string keys (not bytes).
        expires_at = float(token_data.get('expires_at', '0'))
        if time.time() > expires_at:
            self.redis_client.delete(f"{self.tokens_key}:{token_hash}")
            return None

        # Atomically claim the token: only the first caller flips 'used' from
        # anything-but-'true' to 'true'. This closes the check-then-set race
        # that would otherwise let two concurrent requests both consume one
        # single-use token (reset-token replay).
        claimed = self.redis_client.eval(
            "if redis.call('HGET', KEYS[1], 'used') == 'true' then return 0 end "
            "redis.call('HSET', KEYS[1], 'used', 'true') return 1",
            1, f"{self.tokens_key}:{token_hash}")
        if not claimed:
            return None

        return {
            'user_id': token_data.get('user_id', ''),
            'email': token_data.get('email', '')
        }

    def cleanup_expired_tokens(self):
        """Clean up expired tokens"""
        # This would need pattern scanning in production
        # For now, tokens are cleaned up when verified
        pass
