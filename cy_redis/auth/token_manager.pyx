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

import hashlib
import hmac
import secrets
import time
from typing import Any, Dict, List, Optional

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

    cdef object redis_client
    cdef str secret_key
    cdef int access_token_expiry
    cdef int refresh_token_expiry
    cdef str blacklist_key
    cdef str refresh_tokens_key

    def __cinit__(self, CyRedisClient redis_client, str secret_key=None,
                  int access_token_expiry=900, int refresh_token_expiry=604800):
        # Preconditions: the C signature guarantees the types, but not the
        # semantic invariants the rest of this class relies on.
        assert redis_client is not None, "redis_client must not be None"
        assert access_token_expiry > 0, "access_token_expiry must be positive"
        assert refresh_token_expiry > 0, "refresh_token_expiry must be positive"

        self.redis_client = redis_client
        self.secret_key = secret_key or secrets.token_urlsafe(32)
        self.access_token_expiry = access_token_expiry  # 15 minutes
        self.refresh_token_expiry = refresh_token_expiry  # 7 days

        # Token storage keys
        self.blacklist_key = "tokens:blacklist"
        self.refresh_tokens_key = "tokens:refresh"

        # Postconditions: a non-empty signing key is required for every
        # token operation; refresh tokens must outlive access tokens.
        assert self.secret_key, "secret_key must be a non-empty string"
        assert self.refresh_token_expiry >= self.access_token_expiry, (
            "refresh tokens must not expire before access tokens"
        )

    def create_access_token(self, user_id: str, claims: Dict[str, Any] = None) -> str:
        """Create a new access token"""
        assert user_id, "user_id must be a non-empty string"
        assert claims is None or isinstance(claims, dict), "claims must be a dict or None"

        claims = claims or {}
        now = int(time.time())
        expires_at = now + self.access_token_expiry

        payload = {
            'user_id': user_id,
            'type': 'access',
            'iat': now,
            'exp': expires_at,
            **claims
        }

        # Postcondition: the payload must describe a token that expires
        # strictly in the future relative to its issue time. Note: the 'type'
        # field may be overridden by caller-supplied claims (see
        # refresh_access_token), so it is not asserted here.
        assert payload['exp'] > payload['iat'], "token must expire after issuance"

        token = self._create_jwt_token(payload)
        assert token, "JWT encoding must yield a non-empty token"
        return token

    def create_refresh_token(self, user_id: str, claims: Dict[str, Any] = None) -> str:
        """Create a new refresh token"""
        assert user_id, "user_id must be a non-empty string"
        assert claims is None or isinstance(claims, dict), "claims must be a dict or None"

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
        assert token, "JWT encoding must yield a non-empty token"

        # Store refresh token hash for validation. SHA-256 hexdigest is a
        # fixed 64-character string by construction.
        token_hash = hashlib.sha256(token.encode()).hexdigest()
        assert len(token_hash) == 64, "sha256 hexdigest must be 64 chars"
        self.redis_client.hset(self.refresh_tokens_key, token_hash, user_id)

        return token

    def verify_token(self, token: str) -> Optional[Dict[str, Any]]:
        """Verify and decode a JWT token"""
        # Precondition: a token must be a non-empty string to be verifiable.
        # An empty/None token is caller error in negative space, not a crash.
        if not token:
            return None
        assert isinstance(token, str), "token must be a str"

        token_hash = hashlib.sha256(token.encode()).hexdigest()
        assert len(token_hash) == 64, "sha256 hexdigest must be 64 chars"

        try:
            # Check if token is blacklisted
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

        # Check required claims. Bounded by the caller-supplied dict; a
        # generous fail-safe cap guards against a pathologically large map.
        if required_claims:
            assert isinstance(required_claims, dict), "required_claims must be a dict"
            MAX_REQUIRED_CLAIMS = 4096
            assert len(required_claims) <= MAX_REQUIRED_CLAIMS, "too many required claims"
            for claim, value in required_claims.items():
                if payload.get(claim) != value:
                    return None

        return payload

    def refresh_access_token(self, refresh_token: str) -> Optional[str]:
        """Create new access token from refresh token"""
        if not refresh_token:
            return None
        assert isinstance(refresh_token, str), "refresh_token must be a str"

        payload = self.verify_token(refresh_token)
        if not payload or payload.get('type') != 'refresh':
            return None

        # Invariant: a verified payload always carries the user it was issued for.
        assert 'user_id' in payload, "refresh payload must contain user_id"

        # Verify refresh token exists in store
        token_hash = hashlib.sha256(refresh_token.encode()).hexdigest()
        assert len(token_hash) == 64, "sha256 hexdigest must be 64 chars"
        stored_user_id = self.redis_client.hget(self.refresh_tokens_key, token_hash)
        if not stored_user_id or stored_user_id != payload['user_id']:
            return None

        # Create new access token
        return self.create_access_token(payload['user_id'], payload)

    def revoke_token(self, token: str):
        """Revoke a token by adding it to blacklist"""
        assert token, "token must be a non-empty string"
        token_hash = hashlib.sha256(token.encode()).hexdigest()
        assert len(token_hash) == 64, "sha256 hexdigest must be 64 chars"
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
        assert isinstance(payload, dict), "payload must be a dict"
        assert 'exp' in payload and 'iat' in payload, "payload must carry iat and exp"
        import jwt

        encoded = jwt.encode(payload, self.secret_key, algorithm='HS256')
        # PyJWT returns str on modern versions, bytes on very old ones.
        if isinstance(encoded, bytes):
            encoded = encoded.decode('ascii')
        assert encoded, "jwt.encode must produce a non-empty token"
        return encoded

    def _decode_jwt_token(self, token: str) -> Dict[str, Any]:
        """Decode JWT token (simplified implementation)"""
        assert token, "token must be a non-empty string"
        import jwt

        decoded = jwt.decode(token, self.secret_key, algorithms=['HS256'])
        # Postcondition: a successfully decoded JWT is always a claims mapping.
        assert isinstance(decoded, dict), "decoded JWT payload must be a dict"
        return decoded

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

        # Check required permissions. This list is a fixed, statically known
        # set of two flags, so the loop is bounded by construction.
        required_permissions = ['stream_access', 'realtime_access']
        assert len(required_permissions) == 2, "websocket permission set is fixed at two"
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

        # Check required scopes. Bounded by the caller-supplied list with a
        # fail-safe cap against an unbounded request.
        if required_scopes:
            assert isinstance(required_scopes, (list, tuple)), "required_scopes must be a sequence"
            MAX_REQUIRED_SCOPES = 4096
            assert len(required_scopes) <= MAX_REQUIRED_SCOPES, "too many required scopes"
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
        assert isinstance(permissions, (list, tuple)), "permissions must be a sequence"
        MAX_PERMISSIONS = 4096
        assert len(permissions) <= MAX_PERMISSIONS, "too many permissions requested"

        payload = self.verify_websocket_token(token)
        if not payload:
            return False

        token_permissions = payload.get('permissions', [])
        return all(perm in token_permissions for perm in permissions)

    def require_scopes(self, token: str, scopes: List[str]) -> bool:
        """Check if token has required scopes"""
        payload = self.verify_api_token(token, scopes)
        return payload is not None
