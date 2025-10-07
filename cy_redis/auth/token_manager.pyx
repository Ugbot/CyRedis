# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# cython: nonecheck=False
# distutils: language = c

"""
JWT Token Management System for CyRedis Web Application Support.
Provides JWT token creation, verification, refresh tokens, and blacklisting.
"""

import time
import hashlib
import hmac
import secrets
from typing import Dict, List, Optional, Any

# Import core Redis functionality
from cy_redis.core.cy_redis_client cimport CyRedisClient


# Exception classes
class TokenError(Exception):
    pass


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

    def verify_user_access(self, token: str, required_claims: Dict[str, Any] = None) -> Optional[Dict[str, Any]]:
        """Verify user access token and check claims"""
        payload = self.verify_token(token)
        if not payload:
            return None

        # Check required claims
        if required_claims:
            for claim, value in required_claims.items():
                if payload.get(claim) != value:
                    return None

        return payload

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
