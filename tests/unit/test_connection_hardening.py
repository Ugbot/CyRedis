"""
Unit tests for native connection hardening in cy_redis.core.cy_redis_client:
constructor validation, connect retry/backoff, and the TLS support module.

These replace the old test_connection_pool.py, which exercised a pure-Python
shim ("to satisfy test expectations", per its own docstring) rather than the
real client. The behaviors it aimed at — retry with backoff, TLS — are now
implemented natively and tested here against the shipped code.
"""

import random
import socket
import string
import time
import uuid

import pytest

# The client raises its own ConnectionError (a RedisError subclass, distinct
# from the builtin); the tls_support module, being dependency-free, raises the
# builtin one.
from cy_redis.core.cy_redis_client import ConnectionError as CyConnectionError
from cy_redis.core.cy_redis_client import (
    CyRedisClient,
    CyRedisConnection,
    CyRedisConnectionPool,
)


def _random_pem_path() -> str:
    """A path that certainly does not exist, different every run."""
    token = "".join(random.choices(string.ascii_lowercase, k=12))
    return f"/nonexistent-{token}/{uuid.uuid4()}.pem"


def _closed_port() -> int:
    """A port that was just released, so connecting to it is refused."""
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@pytest.mark.unit
class TestConstructorValidation:
    def test_certfile_without_keyfile_rejected(self):
        with pytest.raises(ValueError, match="together"):
            CyRedisConnection(ssl_certfile=_random_pem_path())

    def test_keyfile_without_certfile_rejected(self):
        with pytest.raises(ValueError, match="together"):
            CyRedisConnection(ssl_keyfile=_random_pem_path())

    def test_negative_connect_retries_rejected(self):
        with pytest.raises(ValueError):
            CyRedisConnection(connect_retries=-random.randint(1, 100))

    def test_negative_connect_backoff_rejected(self):
        with pytest.raises(ValueError):
            CyRedisConnection(connect_backoff=-random.random() - 0.001)

    def test_valid_tls_params_accepted(self):
        # Construction must not touch the filesystem or network; bogus paths
        # only fail at connect time.
        conn = CyRedisConnection(
            use_tls=True,
            ssl_ca_certs=_random_pem_path(),
            ssl_certfile=_random_pem_path(),
            ssl_keyfile=_random_pem_path(),
            ssl_server_name=f"host-{uuid.uuid4().hex[:8]}.example",
        )
        assert conn is not None


@pytest.mark.unit
class TestConnectRetryBackoff:
    def test_refused_connection_fails_after_retries(self):
        conn = CyRedisConnection(
            "127.0.0.1",
            _closed_port(),
            timeout=0.5,
            connect_retries=random.randint(0, 2),
            connect_backoff=0.01,
        )
        assert conn.connect() == -1
        assert conn.connected is False

    def test_backoff_delays_between_attempts(self):
        # 2 retries at base 0.2s → sleeps of 0.2 + 0.4 = 0.6s minimum.
        # A refused connect on loopback itself is near-instant, so elapsed
        # time below the backoff sum means the sleeps did not happen.
        conn = CyRedisConnection(
            "127.0.0.1",
            _closed_port(),
            timeout=0.5,
            connect_retries=2,
            connect_backoff=0.2,
        )
        start = time.monotonic()
        assert conn.connect() == -1
        elapsed = time.monotonic() - start
        assert elapsed >= 0.6, f"expected >= 0.6s of backoff, took {elapsed:.3f}s"

    def test_zero_retries_fails_fast(self):
        conn = CyRedisConnection(
            "127.0.0.1",
            _closed_port(),
            timeout=0.5,
            connect_retries=0,
            connect_backoff=0.5,
        )
        start = time.monotonic()
        assert conn.connect() == -1
        elapsed = time.monotonic() - start
        assert elapsed < 0.5, f"no-retry connect should not sleep, took {elapsed:.3f}s"

    def test_pool_reports_connection_error_on_dead_host(self):
        pool = CyRedisConnectionPool("127.0.0.1", _closed_port(), connect_retries=0)
        with pytest.raises(CyConnectionError):
            pool.get_connection()

    def test_client_accepts_hardening_params(self):
        # Construction is lazy — no connection is attempted until a command.
        client = CyRedisClient(
            "127.0.0.1", _closed_port(), connect_retries=1, connect_backoff=0.01
        )
        with pytest.raises(CyConnectionError):
            client.ping()


@pytest.mark.unit
class TestTLSSupportModule:
    """The tls_support extension (present on every OpenSSL-enabled build)."""

    tls = pytest.importorskip(
        "cy_redis.core.tls_support",
        reason="this build has no TLS support (OpenSSL was absent at compile time)",
    )

    def test_default_context_uses_system_store(self):
        capsule = self.tls.create_ssl_context()
        assert capsule is not None

    def test_missing_ca_file_raises_connection_error(self):
        with pytest.raises(ConnectionError, match="TLS context"):
            self.tls.create_ssl_context(ca_certs=_random_pem_path())

    def test_missing_client_cert_raises_connection_error(self):
        with pytest.raises(ConnectionError, match="TLS context"):
            self.tls.create_ssl_context(
                certfile=_random_pem_path(), keyfile=_random_pem_path()
            )

    def test_initiate_tls_rejects_foreign_objects(self):
        for not_a_capsule in (object(), b"bytes", 42, None):
            with pytest.raises(TypeError):
                self.tls.initiate_tls(1, not_a_capsule)
