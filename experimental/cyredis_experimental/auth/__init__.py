"""Authentication and security components for CyRedis.

These managers require the ``auth`` extra (``pip install cy-redis[auth]``),
which pulls in PyJWT and pyotp. If those dependencies are absent the symbols
resolve to ``None``.
"""

try:
    from cyredis_experimental.auth.password_reset_manager import PasswordResetManager
    from cyredis_experimental.auth.session_manager import SessionError, SessionManager
    from cyredis_experimental.auth.token_manager import TokenError, TokenManager
    from cyredis_experimental.auth.two_factor_auth import TwoFactorAuth
except ImportError:  # optional 'auth' extra not installed (PyJWT/pyotp missing)
    TokenManager = TokenError = None
    SessionManager = SessionError = None
    TwoFactorAuth = None
    PasswordResetManager = None

__all__ = [
    "TokenManager",
    "TokenError",
    "SessionManager",
    "SessionError",
    "TwoFactorAuth",
    "PasswordResetManager",
]
