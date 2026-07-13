"""
Integration tests for the Redis-backed RPC layer: service discovery,
request/response round-trips, error propagation, timeouts, and liveness
pruning — all against a real Redis with randomized payloads.
"""
import random
import string
import threading
import time
import uuid

import pytest

from cy_redis.communication.rpc import (
    CyRPCClient,
    CyRPCRequest,
    CyRPCResponse,
    CyRPCServer,
    CyRPCServiceRegistry,
    RPCError,
    RPCServiceUnavailable,
    RPCTimeoutError,
)

pytestmark = [pytest.mark.integration, pytest.mark.rpc]


def _random_word(k: int = 10) -> str:
    return "".join(random.choices(string.ascii_lowercase, k=k))


def _random_payload(depth: int = 2):
    """A random JSON-serializable structure."""
    if depth == 0:
        return random.choice([
            random.randint(-10**6, 10**6),
            random.random(),
            _random_word(random.randint(1, 30)),
            None,
            bool(random.getrandbits(1)),
        ])
    return {
        _random_word(6): _random_payload(depth - 1)
        for _ in range(random.randint(1, 4))
    }


@pytest.fixture
def namespace(redis_client):
    """A unique namespace per test, wiped afterwards."""
    ns = f"rpctest:{uuid.uuid4().hex[:12]}"
    yield ns
    # Remove everything the test left behind.
    cursor = "0"
    while True:
        result = redis_client.execute_command(["SCAN", cursor, "MATCH", f"{ns}:*"])
        cursor = str(result[0])
        for key in result[1] or []:
            redis_client.delete(key)
        if cursor == "0":
            break


@pytest.fixture
def rpc_pair(redis_client, namespace):
    """A started server + client on the same namespace; stopped afterwards."""
    service = f"svc-{_random_word(8)}"
    server = CyRPCServer(redis_client, service, namespace=namespace)

    @server.handler("add")
    def add(a, b):
        return a + b

    @server.handler("echo")
    def echo(payload):
        return payload

    @server.handler("boom")
    def boom(message):
        raise ValueError(message)

    server.start(num_workers=random.randint(1, 3))
    client = CyRPCClient(redis_client, namespace=namespace, default_timeout=10)
    yield service, server, client
    client.close()
    server.stop()


class TestSerialization:
    def test_request_round_trips_through_json(self):
        payload = _random_payload()
        req = CyRPCRequest(_random_word(), _random_word(), [payload],
                           {_random_word(): _random_payload()},
                           timeout=random.randint(1, 300))
        restored = CyRPCRequest.from_json(req.to_json())
        assert restored.service == req.service
        assert restored.method == req.method
        assert restored.args == req.args
        assert restored.kwargs == req.kwargs
        assert restored.request_id == req.request_id
        assert restored.timeout == req.timeout

    def test_response_round_trips_through_json(self):
        resp = CyRPCResponse(str(uuid.uuid4()), success=False,
                             error=_random_word(20), error_type="ValueError")
        restored = CyRPCResponse.from_json(resp.to_json())
        assert restored.request_id == resp.request_id
        assert restored.success == resp.success
        assert restored.error == resp.error
        assert restored.error_type == resp.error_type

    def test_request_rejects_nonpositive_timeout(self):
        with pytest.raises(ValueError):
            CyRPCRequest(_random_word(), _random_word(),
                         timeout=-random.randint(0, 10))


class TestRPCRoundTrip:
    def test_call_returns_handler_result(self, rpc_pair):
        service, _, client = rpc_pair
        a, b = random.randint(-10**9, 10**9), random.randint(-10**9, 10**9)
        assert client.call(service, "add", [a, b]) == a + b

    def test_complex_payload_survives_round_trip(self, rpc_pair):
        service, _, client = rpc_pair
        payload = _random_payload(depth=3)
        assert client.call(service, "echo", [payload]) == payload

    def test_unknown_method_raises_rpc_error(self, rpc_pair):
        service, _, client = rpc_pair
        with pytest.raises(RPCError, match="Unknown method"):
            client.call(service, f"missing_{_random_word()}")

    def test_handler_exception_reaches_caller(self, rpc_pair):
        service, _, client = rpc_pair
        marker = f"broken-{uuid.uuid4().hex[:8]}"
        with pytest.raises(RPCError, match=marker):
            client.call(service, "boom", [marker])

    def test_concurrent_calls_all_answered(self, rpc_pair):
        service, _, client = rpc_pair
        inputs = [(random.randint(-10**6, 10**6), random.randint(-10**6, 10**6))
                  for _ in range(random.randint(5, 10))]
        results = {}

        def worker(i, a, b):
            results[i] = client.call(service, "add", [a, b])

        threads = [threading.Thread(target=worker, args=(i, a, b))
                   for i, (a, b) in enumerate(inputs)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=15)
        assert results == {i: a + b for i, (a, b) in enumerate(inputs)}

    @pytest.mark.asyncio
    async def test_call_async(self, rpc_pair):
        service, _, client = rpc_pair
        a, b = random.randint(-10**6, 10**6), random.randint(-10**6, 10**6)
        assert await client.call_async(service, "add", [a, b]) == a + b


class TestFailureModes:
    def test_no_service_instances(self, redis_client, namespace):
        client = CyRPCClient(redis_client, namespace=namespace)
        try:
            with pytest.raises(RPCServiceUnavailable):
                client.call(f"ghost-{_random_word()}", "anything")
        finally:
            client.close()

    def test_registered_but_unresponsive_service_times_out(self, redis_client,
                                                           namespace):
        # An instance that registered but never consumes its queue.
        registry = CyRPCServiceRegistry(redis_client, namespace=namespace)
        service = f"stuck-{_random_word()}"
        registry.register_service(service, str(uuid.uuid4()))
        client = CyRPCClient(redis_client, namespace=namespace)
        try:
            start = time.monotonic()
            with pytest.raises(RPCTimeoutError):
                client.call(service, "anything", timeout=1)
            assert time.monotonic() - start < 5
        finally:
            client.close()

    def test_server_requires_handlers_before_start(self, redis_client, namespace):
        server = CyRPCServer(redis_client, f"empty-{_random_word()}",
                             namespace=namespace)
        with pytest.raises(RuntimeError, match="no handlers"):
            server.start()

    def test_stopped_server_disappears_from_discovery(self, redis_client,
                                                      namespace):
        service = f"transient-{_random_word()}"
        server = CyRPCServer(redis_client, service, namespace=namespace)
        server.register_handler("noop", lambda: None)
        server.start()
        registry = CyRPCServiceRegistry(redis_client, namespace=namespace)
        assert len(registry.discover_services(service)) == 1
        server.stop()
        assert registry.discover_services(service) == []


class TestDiscoveryLiveness:
    def test_silent_instance_is_pruned(self, redis_client, namespace):
        # heartbeat_interval=1 → anything silent for >2s is considered dead.
        registry = CyRPCServiceRegistry(redis_client, namespace=namespace,
                                        heartbeat_interval=1)
        service = f"mayfly-{_random_word()}"
        instance = str(uuid.uuid4())
        registry.register_service(service, instance)
        assert len(registry.discover_services(service)) == 1
        time.sleep(2.5)
        assert registry.discover_services(service) == []

    def test_heartbeat_keeps_instance_alive(self, redis_client, namespace):
        registry = CyRPCServiceRegistry(redis_client, namespace=namespace,
                                        heartbeat_interval=1)
        service = f"survivor-{_random_word()}"
        instance = str(uuid.uuid4())
        registry.register_service(service, instance)
        for _ in range(3):
            time.sleep(1.0)
            registry.send_heartbeat(service, instance)
        assert len(registry.discover_services(service)) == 1
