"""Fixtures for the experimental test tree.

Run from the repository root (so ``tests.server_env`` resolves) with

    uv run pytest experimental/tests

after building both the main package and ``experimental/``. The unsupported
subsystems are exercised against the same Redis the supported suite uses,
plus an optional module-bearing server for cy_game.
"""

import os
from typing import Any, Generator, List

import pytest

from tests.server_env import REDIS_HOST, REDIS_PORT

REDIS_DB = int(os.getenv("REDIS_DB", "15"))

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
CY_GAME_SO = os.path.join(
    REPO_ROOT, "experimental", "cyredis_experimental", "game", "module", "cy_game.so"
)
CY_GAME_HOST = os.getenv("CY_GAME_REDIS_HOST", REDIS_HOST)
CY_GAME_PORT = int(os.getenv("CY_GAME_REDIS_PORT", "0"))


def pytest_configure(config: Any) -> None:
    for marker in (
        "slow: marks tests as slow",
        "redis: marks tests that require a Redis server",
        "integration: marks tests as integration tests",
        "unit: marks tests as unit tests",
        "worker_coordination: worker coordination and recovery tests",
        "game_module: tests requiring the cy_game Redis module",
        "pgcache: tests for the pgcache Redis module",
        "rpc: RPC tests",
        "messaging: messaging tests",
    ):
        config.addinivalue_line("markers", marker)


def _make_client():
    from cy_redis.core.cy_redis_client import CyRedisClient

    return CyRedisClient(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)


@pytest.fixture(scope="session")
def redis_available() -> bool:
    try:
        client = _make_client()
        return client.execute_command(["PING"]) in ("PONG", b"PONG")
    except Exception:
        return False


@pytest.fixture
def redis_client(redis_available: bool) -> Generator[Any, None, None]:
    """A CyRedisClient on the dedicated test DB, flushed on teardown."""
    if not redis_available:
        pytest.skip("Redis not available")
    client = _make_client()
    yield client
    try:
        client.execute_command(["FLUSHDB"])
    except Exception:
        pass


@pytest.fixture
def hp_redis_client(redis_client: Any) -> Any:
    """The worker/lock/queue tests were written against this name."""
    return redis_client


def _cy_game_loaded(client: Any) -> bool:
    modules = client.execute_command(["MODULE", "LIST"]) or []
    for module in modules:
        fields = module if isinstance(module, (list, tuple)) else []
        if any(field in ("cy_game", b"cy_game") for field in fields):
            return True
    return False


@pytest.fixture(scope="session")
def module_loaded() -> Any:
    """A client for a server that serves the cy_game module's commands.

    ``CY_GAME_REDIS_PORT`` points at a server started with ``--loadmodule``;
    otherwise the default server is used and the module is loaded from the
    build tree, which only works where MODULE LOAD is permitted.
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


def pytest_collection_modifyitems(config: Any, items: List[Any]) -> None:
    for item in items:
        if "redis_client" in item.fixturenames or "hp_redis_client" in item.fixturenames:
            item.add_marker(pytest.mark.redis)
