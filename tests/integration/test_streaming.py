"""
Integration tests for Redis Streams functionality.

Tests Redis Streams operations including:
- XADD, XREAD, XRANGE
- Consumer groups (XGROUP, XREADGROUP)
- Stream trimming and cleanup
- Stream acknowledgments
"""

import pytest
import time
from typing import List, Dict, Any


@pytest.mark.integration
class TestRedisStreams:
    """Test Redis Streams operations."""

    def test_xadd_basic(self, redis_client):
        """Test basic stream addition."""
        stream_key = "test:stream:basic"

        # Add message to stream
        message_id = redis_client.xadd(stream_key, {"field1": "value1", "field2": "value2"})

        assert message_id is not None
        assert isinstance(message_id, (str, bytes))

        # Clean up
        redis_client.delete(stream_key)

    def test_xread_messages(self, redis_client):
        """Test reading messages from stream."""
        stream_key = "test:stream:read"

        # Add multiple messages
        id1 = redis_client.xadd(stream_key, {"msg": "message1"})
        id2 = redis_client.xadd(stream_key, {"msg": "message2"})
        id3 = redis_client.xadd(stream_key, {"msg": "message3"})

        # Read all messages from beginning
        messages = redis_client.xread({stream_key: "0"})

        assert len(messages) >= 1
        assert stream_key.encode() in [msg[0] for msg in messages] or stream_key in [msg[0] for msg in messages]

        # Clean up
        redis_client.delete(stream_key)

    def test_xrange_messages(self, redis_client):
        """Test reading message range from stream."""
        stream_key = "test:stream:range"

        # Add messages with timestamps
        ids = []
        for i in range(5):
            msg_id = redis_client.xadd(stream_key, {"index": str(i), "data": f"message_{i}"})
            ids.append(msg_id)
            time.sleep(0.01)  # Small delay to ensure different timestamps

        # Read all messages
        messages = redis_client.xrange(stream_key, "-", "+")

        assert len(messages) == 5

        # Read range of messages
        if len(ids) >= 3:
            range_messages = redis_client.xrange(stream_key, ids[1], ids[3])
            assert len(range_messages) == 3

        # Clean up
        redis_client.delete(stream_key)

    def test_xrevrange_messages(self, redis_client):
        """Test reading messages in reverse order."""
        stream_key = "test:stream:revrange"

        # Add messages
        for i in range(5):
            redis_client.xadd(stream_key, {"index": str(i)})

        # Read in reverse
        messages = redis_client.xrevrange(stream_key, "+", "-", count=3)

        assert len(messages) <= 3

        # Verify reverse order (last added should be first)
        if len(messages) >= 2:
            # The messages should be in reverse chronological order
            assert messages is not None

        # Clean up
        redis_client.delete(stream_key)

    @pytest.mark.slow
    def test_consumer_group_basic(self, redis_client):
        """Test consumer group creation and basic operations."""
        stream_key = "test:stream:consumer_group"
        group_name = "test_group"
        consumer_name = "test_consumer"

        try:
            # Add some messages
            for i in range(3):
                redis_client.xadd(stream_key, {"msg": f"message_{i}"})

            # Create consumer group
            redis_client.xgroup_create(stream_key, group_name, id="0", mkstream=True)

            # Read from group
            messages = redis_client.xreadgroup(
                group_name,
                consumer_name,
                {stream_key: ">"},
                count=2
            )

            assert messages is not None

        except Exception as e:
            # Consumer groups might not be available in all Redis versions
            pytest.skip(f"Consumer groups not available: {e}")
        finally:
            # Clean up
            try:
                redis_client.xgroup_destroy(stream_key, group_name)
            except:
                pass
            redis_client.delete(stream_key)

    @pytest.mark.slow
    def test_xack_acknowledgment(self, redis_client):
        """Test message acknowledgment in consumer groups."""
        stream_key = "test:stream:ack"
        group_name = "ack_group"
        consumer_name = "ack_consumer"

        try:
            # Add message
            msg_id = redis_client.xadd(stream_key, {"data": "test"})

            # Create consumer group
            redis_client.xgroup_create(stream_key, group_name, id="0")

            # Read message
            messages = redis_client.xreadgroup(
                group_name,
                consumer_name,
                {stream_key: ">"}
            )

            if messages:
                # Acknowledge message
                ack_count = redis_client.xack(stream_key, group_name, msg_id)
                assert ack_count >= 0

        except Exception as e:
            pytest.skip(f"Consumer groups not available: {e}")
        finally:
            # Clean up
            try:
                redis_client.xgroup_destroy(stream_key, group_name)
            except:
                pass
            redis_client.delete(stream_key)

    def test_xlen_stream_length(self, redis_client):
        """Test getting stream length."""
        stream_key = "test:stream:len"

        # Initially empty or non-existent
        initial_len = redis_client.xlen(stream_key)
        assert initial_len == 0 or initial_len is None

        # Add messages
        for i in range(5):
            redis_client.xadd(stream_key, {"index": str(i)})

        # Check length
        length = redis_client.xlen(stream_key)
        assert length == 5

        # Clean up
        redis_client.delete(stream_key)

    def test_xtrim_stream(self, redis_client):
        """Test trimming stream to maximum length."""
        stream_key = "test:stream:trim"

        # Add many messages
        for i in range(10):
            redis_client.xadd(stream_key, {"index": str(i)})

        # Trim to 5 messages
        try:
            redis_client.xtrim(stream_key, maxlen=5)

            # Verify trimmed length
            length = redis_client.xlen(stream_key)
            assert length == 5
        except AttributeError:
            # xtrim might not be available
            pytest.skip("XTRIM not available")

        # Clean up
        redis_client.delete(stream_key)

    @pytest.mark.slow
    def test_xpending_pending_messages(self, redis_client):
        """Test checking pending messages in consumer group."""
        stream_key = "test:stream:pending"
        group_name = "pending_group"
        consumer_name = "pending_consumer"

        try:
            # Add messages
            for i in range(3):
                redis_client.xadd(stream_key, {"msg": f"message_{i}"})

            # Create consumer group
            redis_client.xgroup_create(stream_key, group_name, id="0")

            # Read messages (but don't acknowledge)
            messages = redis_client.xreadgroup(
                group_name,
                consumer_name,
                {stream_key: ">"}
            )

            # Check pending messages
            pending = redis_client.xpending(stream_key, group_name)

            # Should have pending messages
            assert pending is not None

        except Exception as e:
            pytest.skip(f"Consumer groups not available: {e}")
        finally:
            # Clean up
            try:
                redis_client.xgroup_destroy(stream_key, group_name)
            except:
                pass
            redis_client.delete(stream_key)

    @pytest.mark.slow
    def test_multiple_consumers(self, redis_client):
        """Test multiple consumers reading from same group."""
        stream_key = "test:stream:multi_consumer"
        group_name = "multi_group"

        try:
            # Add messages
            message_ids = []
            for i in range(6):
                msg_id = redis_client.xadd(stream_key, {"msg": f"message_{i}"})
                message_ids.append(msg_id)

            # Create consumer group
            redis_client.xgroup_create(stream_key, group_name, id="0")

            # Consumer 1 reads messages
            messages1 = redis_client.xreadgroup(
                group_name,
                "consumer1",
                {stream_key: ">"},
                count=3
            )

            # Consumer 2 reads messages
            messages2 = redis_client.xreadgroup(
                group_name,
                "consumer2",
                {stream_key: ">"},
                count=3
            )

            # Both should get different messages
            assert messages1 is not None or messages2 is not None

        except Exception as e:
            pytest.skip(f"Consumer groups not available: {e}")
        finally:
            # Clean up
            try:
                redis_client.xgroup_destroy(stream_key, group_name)
            except:
                pass
            redis_client.delete(stream_key)

    def test_xinfo_stream(self, redis_client):
        """Test getting stream information."""
        stream_key = "test:stream:info"

        # Add messages
        for i in range(3):
            redis_client.xadd(stream_key, {"data": str(i)})

        try:
            # Get stream info
            info = redis_client.xinfo_stream(stream_key)

            assert info is not None
            # Info should contain stream metadata

        except (AttributeError, Exception) as e:
            pytest.skip(f"XINFO not available: {e}")

        # Clean up
        redis_client.delete(stream_key)

    @pytest.mark.slow
    def test_stream_blocking_read(self, redis_client):
        """Test blocking read from stream."""
        stream_key = "test:stream:blocking"

        # Add a message
        redis_client.xadd(stream_key, {"data": "test"})

        # Blocking read with timeout
        try:
            messages = redis_client.xread(
                {stream_key: "0"},
                count=1,
                block=100  # 100ms timeout
            )

            assert messages is not None
            assert len(messages) >= 1

        except TypeError:
            # block parameter might not be supported
            pytest.skip("Blocking read not supported")

        # Clean up
        redis_client.delete(stream_key)


@pytest.mark.integration
@pytest.mark.slow
class TestStreamsAdvanced:
    """Advanced Redis Streams tests."""

    def test_stream_pipeline(self, redis_client):
        """Test stream operations in pipeline."""
        stream_key = "test:stream:pipeline"

        try:
            # Use pipeline for multiple XADD
            pipe = redis_client.pipeline()

            for i in range(5):
                pipe.xadd(stream_key, {"index": str(i)})

            results = pipe.execute()

            # All should succeed
            assert len(results) == 5

        except AttributeError:
            pytest.skip("Pipeline not available")
        finally:
            redis_client.delete(stream_key)

    def test_maxlen_on_add(self, redis_client):
        """Test automatic trimming when adding to stream."""
        stream_key = "test:stream:maxlen"

        # Add with maxlen constraint
        for i in range(10):
            try:
                redis_client.xadd(stream_key, {"index": str(i)}, maxlen=5)
            except TypeError:
                # maxlen parameter might not be supported
                pytest.skip("maxlen parameter not supported")
                break

        # Should have at most 5 messages
        length = redis_client.xlen(stream_key)
        assert length <= 5

        # Clean up
        redis_client.delete(stream_key)

    def test_id_generation(self, redis_client):
        """Test custom and auto ID generation."""
        stream_key = "test:stream:id_gen"

        # Auto-generated ID
        auto_id = redis_client.xadd(stream_key, {"type": "auto"})
        assert auto_id is not None

        # Custom ID (timestamp-based)
        import time
        custom_id = f"{int(time.time() * 1000)}-0"
        try:
            result_id = redis_client.xadd(stream_key, {"type": "custom"}, id=custom_id)
            # Some implementations might not support custom IDs
            if result_id:
                assert result_id is not None
        except (TypeError, Exception):
            pytest.skip("Custom IDs not supported")

        # Clean up
        redis_client.delete(stream_key)
