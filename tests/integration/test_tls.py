"""
Integration tests for native TLS: a real TLS handshake against a real
redis-server, certificates generated fresh for every run.

Requires the `redis-server` and `openssl` binaries; skips cleanly when either
is missing or when the local redis-server was built without TLS support.
"""

import random
import shutil
import socket
import string
import subprocess
import time
import uuid

import pytest

tls_support = pytest.importorskip(
    "cy_redis.core.tls_support",
    reason="this build has no TLS support (OpenSSL was absent at compile time)",
)

# The client's ConnectionError is a RedisError subclass, not the builtin.
from cy_redis.core.cy_redis_client import ConnectionError as CyConnectionError
from cy_redis.core.cy_redis_client import (
    CyRedisClient,
    RedisError,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        shutil.which("redis-server") is None, reason="redis-server binary not available"
    ),
    pytest.mark.skipif(
        shutil.which("openssl") is None, reason="openssl binary not available"
    ),
]


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _random_value(length: int = 32) -> str:
    return "".join(random.choices(string.ascii_letters + string.digits, k=length))


def _openssl(*args, cwd):
    subprocess.run(
        ["openssl", *args], cwd=cwd, check=True, capture_output=True, text=True
    )


@pytest.fixture(scope="module")
def tls_certs(tmp_path_factory):
    """A throwaway CA plus a server cert for localhost, minted per test run."""
    certdir = tmp_path_factory.mktemp("tls-certs")
    _openssl(
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-days",
        "1",
        "-keyout",
        "ca.key",
        "-out",
        "ca.crt",
        "-subj",
        f"/CN=cyredis-test-ca-{uuid.uuid4().hex[:8]}",
        cwd=certdir,
    )
    _openssl(
        "req",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-keyout",
        "server.key",
        "-out",
        "server.csr",
        "-subj",
        "/CN=localhost",
        cwd=certdir,
    )
    # SAN so hostname verification holds for both "localhost" and 127.0.0.1.
    (certdir / "san.cnf").write_text("subjectAltName=DNS:localhost,IP:127.0.0.1\n")
    _openssl(
        "x509",
        "-req",
        "-in",
        "server.csr",
        "-CA",
        "ca.crt",
        "-CAkey",
        "ca.key",
        "-CAcreateserial",
        "-days",
        "1",
        "-extfile",
        "san.cnf",
        "-out",
        "server.crt",
        cwd=certdir,
    )
    return certdir


@pytest.fixture(scope="module")
def tls_redis(tls_certs):
    """A redis-server speaking only TLS on a random port."""
    port = _free_port()
    proc = subprocess.Popen(
        [
            "redis-server",
            "--port",
            "0",
            "--tls-port",
            str(port),
            "--tls-cert-file",
            str(tls_certs / "server.crt"),
            "--tls-key-file",
            str(tls_certs / "server.key"),
            "--tls-ca-cert-file",
            str(tls_certs / "ca.crt"),
            "--tls-auth-clients",
            "no",
            "--save",
            "",
            "--appendonly",
            "no",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    # Wait for the port to accept TCP before handing it to tests.
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            out = proc.stdout.read()
            pytest.skip(f"redis-server refused to start with TLS: {out[-400:]}")
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
            break
        except OSError:
            time.sleep(0.1)
    else:
        proc.terminate()
        pytest.skip("TLS redis-server did not come up within 10s")
    yield port
    proc.terminate()
    proc.wait(timeout=5)


class TestTLSRoundTrip:
    def test_set_get_over_tls(self, tls_redis, tls_certs):
        client = CyRedisClient(
            "localhost", tls_redis, use_tls=True, ssl_ca_certs=str(tls_certs / "ca.crt")
        )
        key = f"tls-test:{uuid.uuid4()}"
        value = _random_value(random.randint(16, 256))
        assert client.ping() is True
        client.set(key, value)
        assert client.get(key) == value
        client.delete(key)

    def test_many_random_round_trips(self, tls_redis, tls_certs):
        client = CyRedisClient(
            "localhost", tls_redis, use_tls=True, ssl_ca_certs=str(tls_certs / "ca.crt")
        )
        pairs = {
            f"tls-bulk:{uuid.uuid4()}": _random_value(random.randint(1, 512))
            for _ in range(20)
        }
        for k, v in pairs.items():
            client.set(k, v)
        for k, v in pairs.items():
            assert client.get(k) == v
        client.delete(*pairs.keys())

    def test_untrusted_ca_is_rejected(self, tls_redis, tls_certs, tmp_path):
        # A different CA that never signed the server's certificate.
        _openssl(
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-keyout",
            "other.key",
            "-out",
            "other.crt",
            "-subj",
            "/CN=untrusted-ca",
            cwd=tmp_path,
        )
        client = CyRedisClient(
            "localhost",
            tls_redis,
            use_tls=True,
            connect_retries=0,
            ssl_ca_certs=str(tmp_path / "other.crt"),
        )
        with pytest.raises(CyConnectionError):
            client.ping()

    def test_plaintext_client_cannot_talk_to_tls_port(self, tls_redis):
        # The TCP connect succeeds, but plaintext RESP over a TLS socket dies
        # on the first command.
        client = CyRedisClient("localhost", tls_redis, connect_retries=0)
        with pytest.raises((CyConnectionError, RedisError)):
            client.ping()

    def test_tls_client_rejects_plaintext_server(
        self, redis_host, redis_port, redis_available, tls_certs
    ):
        if not redis_available:
            pytest.skip("plain Redis not available")
        client = CyRedisClient(
            redis_host,
            redis_port,
            use_tls=True,
            connect_retries=0,
            ssl_ca_certs=str(tls_certs / "ca.crt"),
        )
        with pytest.raises(CyConnectionError):
            client.ping()


class TestClientCertificates:
    def test_client_cert_and_key_load(self, tls_certs, tmp_path):
        _openssl(
            "req",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            "client.key",
            "-out",
            "client.csr",
            "-subj",
            "/CN=cyredis-test-client",
            cwd=tmp_path,
        )
        _openssl(
            "x509",
            "-req",
            "-in",
            "client.csr",
            "-CA",
            str(tls_certs / "ca.crt"),
            "-CAkey",
            str(tls_certs / "ca.key"),
            "-CAcreateserial",
            "-days",
            "1",
            "-out",
            "client.crt",
            cwd=tmp_path,
        )
        capsule = tls_support.create_ssl_context(
            ca_certs=str(tls_certs / "ca.crt"),
            certfile=str(tmp_path / "client.crt"),
            keyfile=str(tmp_path / "client.key"),
        )
        assert capsule is not None

    def test_mismatched_key_is_rejected(self, tls_certs, tmp_path):
        # A key that does not match the certificate must fail at load time.
        _openssl("genrsa", "-out", "wrong.key", "2048", cwd=tmp_path)
        with pytest.raises(ConnectionError, match="TLS context"):
            tls_support.create_ssl_context(
                ca_certs=str(tls_certs / "ca.crt"),
                certfile=str(tls_certs / "server.crt"),
                keyfile=str(tmp_path / "wrong.key"),
            )
