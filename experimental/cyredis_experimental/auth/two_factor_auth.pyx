# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# cython: nonecheck=False
# distutils: language = c

"""
Two-Factor Authentication for CyRedis Web Application Support.
Provides TOTP and backup code authentication.
"""

import base64
import hashlib
import hmac
import secrets
import time
from typing import Any, Dict

# Import core Redis functionality

from cy_redis.core.cy_redis_client cimport CyRedisClient


cdef class TwoFactorAuth:
    """
    Two-factor authentication support with TOTP and backup codes.
    """

    cdef object redis_client
    cdef str backup_codes_key
    cdef str totp_secrets_key

    def __cinit__(self, CyRedisClient redis_client):
        assert redis_client is not None, "redis_client must not be None"
        self.redis_client = redis_client
        self.backup_codes_key = "2fa:backup_codes"
        self.totp_secrets_key = "2fa:totp_secrets"
        # Postcondition: the two namespaces must not collide.
        assert self.backup_codes_key != self.totp_secrets_key, "2fa key namespaces must differ"

    def enable_2fa(self, user_id: str) -> Dict[str, Any]:
        """Enable 2FA for user"""
        assert user_id, "user_id must be a non-empty string"

        # Generate the TOTP secret as RFC 4648 base32. Authenticator apps and
        # the verifier (_verify_totp_token -> base64.b32decode) both require
        # base32; a token_urlsafe() secret is not valid base32, which made
        # verification always fail. 20 random bytes = 160 bits (TOTP standard).
        totp_secret = base64.b32encode(secrets.token_bytes(20)).decode('ascii')
        assert totp_secret, "generated TOTP secret must be non-empty"
        # Postcondition: a valid base32 secret round-trips through b32decode.
        assert base64.b32decode(totp_secret), "TOTP secret must be valid base32"

        # Generate backup codes. The count is a fixed, statically bounded loop.
        BACKUP_CODE_COUNT = 10
        BACKUP_CODE_LENGTH = 8
        backup_codes = []
        for _ in range(BACKUP_CODE_COUNT):
            code = ''.join(
                secrets.choice('0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ')
                for _ in range(BACKUP_CODE_LENGTH)
            )
            backup_codes.append(code)

        assert len(backup_codes) == BACKUP_CODE_COUNT, "must generate the requested code count"

        # Hash backup codes for storage
        hashed_codes = [hashlib.sha256(code.encode()).hexdigest() for code in backup_codes]
        # Invariant: hashing is 1:1, and collisions would silently drop codes.
        assert len(set(hashed_codes)) == BACKUP_CODE_COUNT, "backup code hashes must be unique"

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
        assert user_id, "user_id must be a non-empty string"
        if not token:
            return False

        secret_data = self.redis_client.hget(f"{self.totp_secrets_key}:{user_id}", 'secret')
        if not secret_data:
            return False

        # The client decodes string replies to str already; tolerate bytes too.
        totp_secret = secret_data.decode() if isinstance(secret_data, bytes) else secret_data
        assert totp_secret, "stored TOTP secret must be non-empty"
        return self._verify_totp_token(totp_secret, token)

    def verify_backup_code(self, user_id: str, code: str) -> bool:
        """Verify and consume backup code"""
        assert user_id, "user_id must be a non-empty string"
        if not code:
            return False

        code_hash = hashlib.sha256(code.encode()).hexdigest()
        assert len(code_hash) == 64, "sha256 hexdigest must be 64 chars"

        # Check if code exists and is unused
        if not self.redis_client.hexists(f"{self.backup_codes_key}:{user_id}", code_hash):
            return False

        # Mark code as used
        self.redis_client.hdel(f"{self.backup_codes_key}:{user_id}", code_hash)

        return True

    def disable_2fa(self, user_id: str):
        """Disable 2FA for user"""
        assert user_id, "user_id must be a non-empty string"
        self.redis_client.delete(f"{self.totp_secrets_key}:{user_id}")
        self.redis_client.delete(f"{self.backup_codes_key}:{user_id}")

    def is_2fa_enabled(self, user_id: str) -> bool:
        """Check if 2FA is enabled for user"""
        assert user_id, "user_id must be a non-empty string"
        return self.redis_client.exists(f"{self.totp_secrets_key}:{user_id}") > 0

    def get_remaining_backup_codes(self, user_id: str) -> int:
        """Get count of remaining backup codes"""
        assert user_id, "user_id must be a non-empty string"
        remaining = self.redis_client.hlen(f"{self.backup_codes_key}:{user_id}")
        # Postcondition: a count of stored hashes is never negative.
        assert remaining >= 0, "backup code count must be non-negative"
        return remaining

    def _verify_totp_token(self, secret: str, token: str) -> bool:
        """Verify TOTP token against secret"""
        assert secret, "secret must be a non-empty string"
        if not token:
            return False

        # Simplified TOTP verification - in production use a proper library
        import base64
        import hashlib
        import hmac
        import time

        # Check the current window and the two adjacent ones; this loop is
        # statically bounded to exactly three iterations. The bound is asserted
        # outside the verification try/except so a regression fails loudly
        # rather than being swallowed as a verification miss.
        WINDOW_OFFSET_MIN = -1
        WINDOW_OFFSET_STOP = 2
        assert WINDOW_OFFSET_STOP - WINDOW_OFFSET_MIN == 3, "TOTP checks exactly three windows"

        # Decode secret from base32 if needed
        try:
            # This is a simplified implementation
            # In production, use pyotp or similar library
            current_time = int(time.time() / 30)  # 30-second windows

            for time_offset in range(WINDOW_OFFSET_MIN, WINDOW_OFFSET_STOP):
                counter = current_time + time_offset
                counter_bytes = counter.to_bytes(8, 'big')

                hmac_digest = hmac.new(
                    base64.b32decode(secret.upper()),
                    counter_bytes,
                    hashlib.sha1
                ).digest()
                # An HMAC-SHA1 digest is always 20 bytes; the dynamic-truncation
                # offset below relies on that.
                assert len(hmac_digest) == 20, "HMAC-SHA1 digest must be 20 bytes"

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
        assert user_id, "user_id must be a non-empty string"
        assert secret, "secret must be a non-empty string"
        service_name = "CyRedisApp"
        url = f"otpauth://totp/{service_name}:{user_id}?secret={secret}&issuer={service_name}"
        # Postcondition: the provisioning URI must carry the otpauth scheme.
        assert url.startswith("otpauth://totp/"), "QR url must be an otpauth TOTP URI"
        return url
