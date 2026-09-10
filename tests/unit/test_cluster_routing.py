"""Slot routing and address parsing — the parts of the cluster and sentinel
clients that are pure logic and need no running deployment."""

import pytest

from cy_redis.core.cluster import _normalize_nodes, key_slot
from cy_redis.core.sentinel import CySentinel


class TestKeySlot:
    """CRC16/XMODEM over the key, or over its hash tag when it has one."""

    @pytest.mark.parametrize(
        "key,slot",
        [
            # Values from the Redis cluster specification.
            ("", 0),
            ("123456789", 12739),
            ("foo", 12182),
            ("bar", 5061),
        ],
    )
    def test_known_slots(self, key, slot):
        assert key_slot(key) == slot

    def test_slot_is_in_range(self):
        for key in ("a", "b" * 200, "user:1000", "{}", "}{"):
            assert 0 <= key_slot(key) < 16384

    def test_hash_tag_groups_keys(self):
        assert key_slot("{user1000}.following") == key_slot("{user1000}.followers")
        assert key_slot("{user1000}.following") == key_slot("user1000")

    def test_empty_hash_tag_is_ignored(self):
        # "{}" is not a tag, so the whole key hashes.
        assert key_slot("foo{}{bar}") == key_slot("foo{}{bar}".encode())
        assert key_slot("{}foo") != key_slot("foo")

    def test_only_the_first_tag_counts(self):
        assert key_slot("{a}{b}") == key_slot("a")

    def test_bytes_and_str_agree(self):
        assert key_slot(b"foo") == key_slot("foo")


class TestNodeParsing:
    """Startup nodes may be given in any of the shapes callers already use."""

    @pytest.mark.parametrize(
        "nodes",
        [
            ["127.0.0.1:7000", "127.0.0.1:7001"],
            [("127.0.0.1", 7000), ("127.0.0.1", 7001)],
            [{"host": "127.0.0.1", "port": 7000}, {"host": "127.0.0.1", "port": 7001}],
        ],
    )
    def test_startup_node_shapes(self, nodes):
        assert _normalize_nodes(nodes) == [
            ("127.0.0.1", 7000),
            ("127.0.0.1", 7001),
        ]

    def test_sentinel_requires_an_address(self):
        with pytest.raises(AssertionError):
            CySentinel([])
