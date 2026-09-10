"""Redis vs Valkey identification.

Valkey answers `INFO server` with a compatibility `redis_version:` line before
its own `server_name:valkey`/`valkey_version:` lines, so detection has to read
the whole section rather than trusting the first version line it sees.
"""

import pytest

from cy_redis.core.cy_redis_client import CyRedisClient
from tests.server_env import REDIS_HOST, REDIS_PORT


@pytest.fixture
def client():
    return CyRedisClient(host=REDIS_HOST, port=REDIS_PORT)


def _info_server(client: CyRedisClient) -> str:
    info = client.execute_command(["INFO", "server"])
    return info.decode("utf-8", "replace") if isinstance(info, bytes) else info


@pytest.mark.redis
def test_detects_the_server_actually_running(client):
    info = _info_server(client)
    expected = "valkey" if "valkey_version:" in info else "redis"
    assert client.detect_server_type() == expected


@pytest.mark.redis
def test_detection_is_cached(client):
    assert client.server_type is None
    detected = client.detect_server_type()
    assert client.server_type == detected
    assert client.detect_server_type() == detected
