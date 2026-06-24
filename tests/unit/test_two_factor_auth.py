"""Two-factor authentication: TOTP enrolment/verification and backup codes.

Requires the [auth] extra (pyotp is used here to compute the expected TOTP
code from the enrolment secret, cross-checking our verifier).
"""
import uuid

import pytest

pyotp = pytest.importorskip("pyotp")

from cy_redis import CyRedisClient
from cy_redis.auth import TwoFactorAuth


@pytest.fixture
def tfa():
    return TwoFactorAuth(CyRedisClient(host="localhost", port=6379))


def _uid():
    return "test:2fa:" + uuid.uuid4().hex[:12]


@pytest.mark.redis
def test_totp_secret_is_valid_base32_and_verifies(tfa):
    """The enrolment secret is base32, and a code computed from it verifies."""
    import base64

    uid = _uid()
    try:
        result = tfa.enable_2fa(uid)
        secret = result["totp_secret"]
        # Must be decodable base32 (the bug: it used to be token_urlsafe).
        base64.b32decode(secret)
        code = pyotp.TOTP(secret).now()
        assert tfa.verify_totp(uid, code) is True
        assert tfa.verify_totp(uid, "000000") is False
        # Provisioning URI carries exactly the enrolment secret.
        uri_secret = result["qr_code_url"].split("secret=")[1].split("&")[0]
        assert uri_secret == secret
    finally:
        tfa.disable_2fa(uid)


@pytest.mark.redis
def test_backup_codes_are_single_use(tfa):
    uid = _uid()
    try:
        result = tfa.enable_2fa(uid)
        code = result["backup_codes"][0]
        assert tfa.verify_backup_code(uid, code) is True
        # A consumed backup code cannot be reused.
        assert tfa.verify_backup_code(uid, code) is False
    finally:
        tfa.disable_2fa(uid)
