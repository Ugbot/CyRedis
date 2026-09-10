"""
Pytest configuration for unit tests

This file contains shared fixtures and configuration for all unit tests.
"""

import os
import sys
from typing import Any, List

import pytest

from tests.server_env import REDIS_HOST, REDIS_PORT


def pytest_configure(config: Any) -> None:
    """Configure pytest"""
    # Add custom markers
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line("markers", "redis: marks tests that require Redis server")
    config.addinivalue_line("markers", "integration: marks tests as integration tests")


@pytest.fixture(scope="session")
def redis_available() -> bool:
    """Check if Redis is available for testing"""
    try:
        # Prefer the compiled core client; fallback to top-level if present
        try:
            from cy_redis.core.cy_redis_client import CyRedisClient
        except ImportError:
            from cy_redis.core.cy_redis_client import CyRedisClient
        if CyRedisClient is None:
            return False
        client = CyRedisClient(host=REDIS_HOST, port=REDIS_PORT)
        client.set("pytest_test", "ok")
        result = client.get("pytest_test")
        client.delete("pytest_test")
        return result == "ok"
    except Exception:
        return False


CY_GAME_SO = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../cyredis_game/module/cy_game.so")
)


def _cy_game_loaded(client: Any) -> bool:
    """Whether the server already serves the cy_game module's commands."""
    modules = client.execute_command(["MODULE", "LIST"]) or []
    for module in modules:
        fields = module if isinstance(module, (list, tuple)) else []
        if any(field in ("cy_game", b"cy_game") for field in fields):
            return True
    return False


CY_GAME_HOST = os.getenv("CY_GAME_REDIS_HOST", REDIS_HOST)
CY_GAME_PORT = int(os.getenv("CY_GAME_REDIS_PORT", "0"))


@pytest.fixture(scope="session")
def module_loaded() -> Any:
    """A client for a server that serves the cy_game module's commands.

    CI and the docker-compose stack run a second server started with
    ``--loadmodule``; ``CY_GAME_REDIS_PORT`` points the module tests at it.
    Without that, the default server is used and the module is loaded from
    the build tree, which only works where MODULE LOAD is permitted.
    """
    from cy_redis.core.cy_redis_client import CyRedisClient

    host = CY_GAME_HOST if CY_GAME_PORT else REDIS_HOST
    port = CY_GAME_PORT or REDIS_PORT
    client = CyRedisClient(host=host, port=port)
    try:
        client.ping()
    except Exception as exc:
        pytest.skip(f"No Redis at {host}:{port} for the cy_game module: {exc}")

    if _cy_game_loaded(client):
        return client
    if not os.path.exists(CY_GAME_SO):
        pytest.skip("cy_game.so not built — run: make module")
    try:
        client.execute_command(["MODULE", "LOAD", CY_GAME_SO])
    except Exception as exc:
        pytest.skip(f"Could not load cy_game.so: {exc}")
    return client


@pytest.fixture(autouse=True)
def check_redis(request: Any, redis_available: bool) -> None:
    """Automatically skip tests that require Redis if it's not available"""
    if request.node.get_closest_marker("redis"):
        if not redis_available:
            pytest.skip("Redis not available")


def pytest_collection_modifyitems(config: Any, items: List[Any]) -> None:
    """Modify test collection"""
    for item in items:
        # Automatically mark tests that use redis_client fixture
        if "redis_client" in item.fixturenames:
            item.add_marker(pytest.mark.redis)


# Environment setup
@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """Setup test environment"""
    # Set test environment variables
    os.environ["CYREDIS_TEST_MODE"] = "1"

    yield

    # Cleanup
    if "CYREDIS_TEST_MODE" in os.environ:
        del os.environ["CYREDIS_TEST_MODE"]
