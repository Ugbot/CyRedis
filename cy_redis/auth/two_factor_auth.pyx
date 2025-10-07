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

import hashlib
import hmac
import time
import base64
import secrets
from typing import Dict, Any

# Import core Redis functionality
from cy_redis.core.cy_redis_client cimport CyRedisClient


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
