"""
Unit tests for protocol.pyx - RESP protocol negotiation and connection state
"""

import pytest

from cy_redis.core.protocol import (
    RESP2,
    RESP3,
    ConnectionState,
    ProtocolNegotiator,
)


class FakeClient:
    """Records commands and answers HELLO the way a configured server would."""

    def __init__(self, hello_reply):
        self.hello_reply = hello_reply
        self.commands = []

    def execute_command(self, args):
        self.commands.append(list(args))
        if isinstance(self.hello_reply, Exception):
            raise self.hello_reply
        return self.hello_reply


@pytest.fixture
def connection_state() -> ConnectionState:
    return ConnectionState()


class TestProtocolNegotiator:
    def test_negotiates_resp3_from_hello_map(self):
        client = FakeClient({"proto": 3, "server": "redis"})
        assert ProtocolNegotiator(client).negotiate_protocol() == RESP3
        assert client.commands == [["HELLO", "3"]]

    def test_falls_back_to_resp2_when_hello_unsupported(self):
        client = FakeClient(RuntimeError("ERR unknown command 'HELLO'"))
        assert ProtocolNegotiator(client).negotiate_protocol() == RESP2

    def test_falls_back_to_resp2_when_reply_is_not_a_map(self):
        client = FakeClient(["proto", 3])
        assert ProtocolNegotiator(client).negotiate_protocol() == RESP2

    def test_set_server_protocol_sends_hello(self):
        client = FakeClient({"proto": 2})
        negotiator = ProtocolNegotiator(client)
        negotiator.set_server_protocol(RESP3)
        negotiator.set_server_protocol(RESP2)
        assert client.commands == [["HELLO", "3"], ["HELLO", "2"]]


class TestConnectionState:
    def test_state_creation(self, connection_state):
        assert connection_state.protocol_version == RESP2
        assert connection_state.supports_resp3 is False

    def test_update_from_hello(self, connection_state):
        connection_state.update_from_hello({"proto": 3, "version": "7.0.0", "id": 1})
        assert connection_state.protocol_version == 3
        assert connection_state.supports_resp3 is True

    def test_update_from_info(self, connection_state):
        info_response = """# Server
redis_version:7.0.0
redis_mode:standalone
os:Linux

# Clients
connected_clients:1
"""
        connection_state.update_from_info(info_response)
        assert connection_state.server_info["Server"]["redis_version"] == "7.0.0"
        assert connection_state.server_info["Clients"]["connected_clients"] == "1"

    def test_supports_feature(self, connection_state):
        assert connection_state.supports_feature("pipelining") is True
        assert connection_state.supports_feature("scripts") is True
        assert connection_state.supports_feature("pubsub") is True
        assert connection_state.supports_feature("resp3") is False

        connection_state.supports_resp3 = True
        assert connection_state.supports_feature("resp3") is True
