"""
Unit tests for protocol.pyx - RESP protocol support
"""
import pytest
from cy_redis.protocol import (
    RESPParser, ProtocolNegotiator, ConnectionState,
    PushMessageHandler, RedisProtocol, RedisProtocolError
)


@pytest.fixture
def resp_parser():
    """Create a RESP parser for testing"""
    return RESPParser(protocol_version=2)


@pytest.fixture
def resp3_parser():
    """Create a RESP3 parser for testing"""
    return RESPParser(protocol_version=3)


@pytest.fixture
def connection_state():
    """Create a connection state tracker"""
    return ConnectionState()


class TestRESPParser:
    """Test RESPParser class"""

    def test_parser_creation(self):
        """Test creating RESP parser"""
        parser = RESPParser(protocol_version=2)
        assert parser is not None
        assert parser.protocol_version == 2

    def test_set_protocol_version(self, resp_parser):
        """Test setting protocol version"""
        resp_parser.set_protocol_version(3)
        assert resp_parser.protocol_version == 3

    def test_invalid_protocol_version(self, resp_parser):
        """Test setting invalid protocol version"""
        with pytest.raises(ValueError):
            resp_parser.set_protocol_version(5)


class TestPushMessageHandler:
    """Test PushMessageHandler class"""

    def test_handler_creation(self):
        """Test creating push message handler"""
        handler = PushMessageHandler()
        assert handler is not None

    def test_set_callback(self):
        """Test setting push message callback"""
        handler = PushMessageHandler()
        callback = lambda msg: None
        handler.set_callback(callback)
        assert handler.push_callback is not None

    def test_handle_push_message(self):
        """Test handling push message"""
        handler = PushMessageHandler()
        messages = []

        def callback(msg):
            messages.append(msg)

        handler.set_callback(callback)
        handler.handle_push_message(['test', 'message'])

        assert len(messages) == 1

    def test_get_pending_messages(self):
        """Test getting pending push messages"""
        handler = PushMessageHandler()
        handler.handle_push_message(['msg1'])
        handler.handle_push_message(['msg2'])

        pending = handler.get_pending_messages()
        assert len(pending) == 2

        # Should be cleared after retrieval
        pending2 = handler.get_pending_messages()
        assert len(pending2) == 0


class TestProtocolNegotiator:
    """Test ProtocolNegotiator class"""

    def test_negotiator_creation(self):
        """Test creating protocol negotiator"""
        # Note: This would need a mock Redis client
        # Simplified test
        pass


class TestConnectionState:
    """Test ConnectionState class"""

    def test_state_creation(self):
        """Test creating connection state"""
        state = ConnectionState()
        assert state is not None
        assert state.protocol_version == 2  # Default RESP2
        assert state.supports_resp3 is False

    def test_update_from_hello(self, connection_state):
        """Test updating state from HELLO response"""
        hello_response = {
            'proto': 3,
            'version': '7.0.0',
            'id': 1
        }
        connection_state.update_from_hello(hello_response)

        assert connection_state.protocol_version == 3
        assert connection_state.supports_resp3 is True

    def test_update_from_info(self, connection_state):
        """Test updating state from INFO response"""
        info_response = """# Server
redis_version:7.0.0
redis_mode:standalone
os:Linux

# Clients
connected_clients:1
"""
        connection_state.update_from_info(info_response)
        assert connection_state.server_info is not None

    def test_supports_feature(self, connection_state):
        """Test checking feature support"""
        # Default features
        assert connection_state.supports_feature("pipelining") is True
        assert connection_state.supports_feature("scripts") is True
        assert connection_state.supports_feature("pubsub") is True

        # RESP3 not supported by default
        assert connection_state.supports_feature("resp3") is False

        # Update to RESP3
        connection_state.supports_resp3 = True
        assert connection_state.supports_feature("resp3") is True


class TestRedisProtocol:
    """Test RedisProtocol wrapper"""

    def test_protocol_creation(self):
        """Test creating Redis protocol wrapper"""
        protocol = RedisProtocol(protocol_version=2)
        assert protocol is not None
        assert protocol.protocol_version == 2

    def test_set_protocol_version(self):
        """Test setting protocol version"""
        protocol = RedisProtocol(protocol_version=2)
        protocol.set_protocol_version(3)
        assert protocol.protocol_version == 3

    def test_set_push_callback(self):
        """Test setting push message callback"""
        protocol = RedisProtocol()
        callback = lambda msg: None
        protocol.set_push_callback(callback)
        # No assertion, just ensure it doesn't error

    def test_get_pending_push_messages(self):
        """Test getting pending push messages"""
        protocol = RedisProtocol()
        messages = protocol.get_pending_push_messages()
        assert isinstance(messages, list)

    def test_supports_resp3_property(self):
        """Test supports_resp3 property"""
        protocol = RedisProtocol(protocol_version=2)
        assert protocol.supports_resp3 is False

        protocol.set_protocol_version(3)
        protocol._connection_state.supports_resp3 = True
        assert protocol.supports_resp3 is True


class TestProtocolConstants:
    """Test protocol constants"""

    def test_resp2_constant(self):
        """Test RESP2 constant"""
        from cy_redis.protocol import RESP2
        assert RESP2 == 2

    def test_resp3_constant(self):
        """Test RESP3 constant"""
        from cy_redis.protocol import RESP3
        assert RESP3 == 3


class TestProtocolEdgeCases:
    """Test edge cases for protocol handling"""

    def test_empty_hello_response(self, connection_state):
        """Test handling empty HELLO response"""
        connection_state.update_from_hello({})
        # Should not crash

    def test_empty_info_response(self, connection_state):
        """Test handling empty INFO response"""
        connection_state.update_from_info("")
        # Should not crash

    def test_malformed_info(self, connection_state):
        """Test handling malformed INFO response"""
        info_response = "invalid\ndata\nformat"
        connection_state.update_from_info(info_response)
        # Should not crash


class TestRESP3Features:
    """Test RESP3-specific features"""

    def test_resp3_parser_creation(self):
        """Test creating RESP3 parser"""
        parser = RESPParser(protocol_version=3)
        assert parser.protocol_version == 3

    def test_push_handler_with_resp3(self):
        """Test push message handler with RESP3"""
        parser = RESPParser(protocol_version=3)
        messages = []

        def callback(msg):
            messages.append(msg)

        parser.push_handler.set_callback(callback)
        # Push messages would come from Redis server


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
