# cython: language_level=3
# distutils: language=c

"""
Redis Cluster client.

A cluster is a set of masters each owning a contiguous range of the 16384
hash slots. This client keeps the slot -> node map locally so a command goes
straight to its owner, and treats the server's MOVED/ASK replies as the
authority whenever the local map is stale:

    MOVED <slot> <host>:<port>   the slot moved for good — repoint and refresh
    ASK   <slot> <host>:<port>   the slot is migrating — this one key lives on
                                 the target already, so re-send it there,
                                 prefixed with ASKING, without touching the map

Multi-key commands (MGET/MSET/DEL) and pipelines are split by owning node and
reassembled in the caller's order, so cross-slot batches work even though the
server rejects them on a single connection.
"""

import threading

from cy_redis.core.cy_redis_client import ConnectionError, CyRedisClient, RedisError

__all__ = ["CyRedisCluster", "ClusterError", "key_slot", "CyRedisClusterPipeline"]

DEF SLOT_COUNT = 16384

cdef unsigned short _CRC16_TAB[256]


cdef void _init_crc16() noexcept:
    """Build the CRC16/XMODEM table Redis uses for slot assignment."""
    cdef int i, bit
    cdef unsigned short crc
    for i in range(256):
        crc = <unsigned short>(i << 8)
        for bit in range(8):
            if crc & 0x8000:
                crc = <unsigned short>((crc << 1) ^ 0x1021)
            else:
                crc = <unsigned short>(crc << 1)
        _CRC16_TAB[i] = crc


_init_crc16()


class ClusterError(RedisError):
    """Raised when the cluster itself cannot serve a command."""


cdef bytes _as_bytes(object value):
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    return str(value).encode("utf-8")


cpdef int key_slot(object key):
    """The hash slot a key belongs to.

    Only the substring between the first ``{`` and the next ``}`` counts when
    that substring is non-empty (a "hash tag"), which is how callers force
    related keys onto one node.
    """
    cdef bytes raw = _as_bytes(key)
    cdef Py_ssize_t open_brace = raw.find(b"{")
    cdef Py_ssize_t close_brace
    cdef unsigned short crc = 0
    cdef Py_ssize_t i
    cdef unsigned char byte

    if open_brace != -1:
        close_brace = raw.find(b"}", open_brace + 1)
        if close_brace > open_brace + 1:
            raw = raw[open_brace + 1:close_brace]

    for i in range(len(raw)):
        byte = raw[i]
        crc = <unsigned short>((crc << 8) ^ _CRC16_TAB[((crc >> 8) ^ byte) & 0xFF])
    return crc & (SLOT_COUNT - 1)


def _decode(value):
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", errors="replace")
    return value


# Commands that address the whole server rather than a key; they run on an
# arbitrary master.
_NODE_COMMANDS = frozenset({
    "ACL", "BGREWRITEAOF", "BGSAVE", "CLIENT", "CLUSTER", "COMMAND", "CONFIG",
    "DBSIZE", "DEBUG", "FLUSHALL", "FLUSHDB", "INFO", "LASTSAVE", "LATENCY",
    "MEMORY", "MODULE", "MONITOR", "PING", "RANDOMKEY", "REPLICAOF", "SAVE",
    "SCAN", "SCRIPT", "SHUTDOWN", "SLAVEOF", "SLOWLOG", "SWAPDB", "TIME",
})


cdef class CyRedisCluster:
    """A client for a Redis Cluster deployment.

    ``nodes`` are only startup hints: the real topology comes from CLUSTER
    SLOTS on whichever of them answers first, and is refreshed whenever a node
    reports a slot it no longer owns.
    """

    cdef list _startup_nodes
    cdef dict _clients            # (host, port) -> CyRedisClient
    cdef list _slots              # SLOT_COUNT entries of (host, port) or None
    cdef object _lock
    cdef str _password
    cdef int _max_connections
    cdef int _max_redirects

    def __init__(self, nodes=None, host="127.0.0.1", port=7000, password=None,
                 max_connections=10, max_redirects=16):
        assert max_redirects > 0, "max_redirects must be positive"
        assert max_connections > 0, "max_connections must be positive"

        self._startup_nodes = _normalize_nodes(nodes) or [(host, int(port))]
        self._clients = {}
        self._slots = [None] * SLOT_COUNT
        self._lock = threading.RLock()
        self._password = password
        self._max_connections = max_connections
        self._max_redirects = max_redirects
        self.refresh_slots()

    # ── topology ──────────────────────────────────────────────────────────

    def _client(self, node):
        """The pooled client for one node, created on first use."""
        with self._lock:
            client = self._clients.get(node)
            if client is None:
                client = CyRedisClient(
                    host=node[0], port=node[1], password=self._password,
                    max_connections=self._max_connections,
                )
                self._clients[node] = client
            return client

    def refresh_slots(self):
        """Rebuild the slot map from the first startup node that answers."""
        errors = []
        for node in list(self._startup_nodes) + list(self._clients.keys()):
            try:
                reply = self._client(node).execute_command(["CLUSTER", "SLOTS"])
            except (RedisError, ConnectionError, OSError) as exc:
                errors.append(f"{node[0]}:{node[1]}: {exc}")
                continue
            if not reply:
                continue
            slots = [None] * SLOT_COUNT
            for entry in reply:
                start = int(entry[0])
                end = int(entry[1])
                master = (_decode(entry[2][0]) or node[0], int(entry[2][1]))
                for slot in range(start, end + 1):
                    slots[slot] = master
            with self._lock:
                self._slots = slots
            return
        raise ClusterError(
            "Could not read the cluster topology from any node: "
            + "; ".join(errors or ["no CLUSTER SLOTS reply"])
        )

    cdef object _node_for_slot(self, int slot):
        node = self._slots[slot]
        if node is None:
            self.refresh_slots()
            node = self._slots[slot]
            if node is None:
                raise ClusterError(f"No node owns slot {slot}")
        return node

    def node_for_key(self, key):
        """The (host, port) currently owning ``key``."""
        return self._node_for_slot(key_slot(key))

    @property
    def nodes(self):
        """The masters in the current slot map."""
        return sorted({node for node in self._slots if node is not None})

    # ── routing ───────────────────────────────────────────────────────────

    def execute_command(self, *args):
        """Run one command on the node owning its key, following redirects."""
        if len(args) == 1 and isinstance(args[0], (list, tuple)):
            command = list(args[0])
        else:
            command = list(args)
        assert command, "execute_command requires a command"

        name = str(command[0]).upper()
        if name in _NODE_COMMANDS or len(command) < 2:
            node = self.nodes[0]
        else:
            node = self.node_for_key(command[1])
        return self._execute_on(node, command)

    def _execute_on(self, node, list command):
        cdef int attempt
        asking = False
        for attempt in range(self._max_redirects):
            client = self._client(node)
            try:
                if asking:
                    # ASKING only applies to the next command, so it has to
                    # travel on the same connection: a pipeline holds one.
                    pipe = client.pipeline()
                    pipe.execute_command(["ASKING"])
                    pipe.execute_command(command)
                    return pipe.execute()[1]
                return client.execute_command(command)
            except RedisError as exc:
                message = str(exc)
                if message.startswith("MOVED"):
                    node = _redirect_target(message)
                    asking = False
                    self.refresh_slots()
                    continue
                if message.startswith("ASK"):
                    node = _redirect_target(message)
                    asking = True
                    continue
                if message.startswith("CLUSTERDOWN") or message.startswith("TRYAGAIN"):
                    self.refresh_slots()
                    asking = False
                    continue
                raise
            except (ConnectionError, OSError):
                # The owner went away; the map is the first thing to distrust.
                self.refresh_slots()
                node = self.nodes[0] if not self._slots else node
                asking = False
        raise ClusterError(
            f"Too many redirects ({self._max_redirects}) for {command[0]}"
        )

    def _group_by_slot(self, keys):
        """Bucket keys by hash slot, keeping each bucket in caller order.

        Multi-key commands are rejected across slots even when the slots
        share an owner, so grouping by node is not enough.
        """
        grouped = {}
        for key in keys:
            grouped.setdefault(key_slot(key), []).append(key)
        return grouped

    # ── commands ──────────────────────────────────────────────────────────

    def ping(self) -> bool:
        return all(self._client(node).ping() for node in self.nodes)

    def set(self, key, value, ex=-1, px=-1, nx=False, xx=False) -> bool:
        command = ["SET", key, value]
        if ex > 0:
            command.extend(["EX", str(ex)])
        elif px > 0:
            command.extend(["PX", str(px)])
        if nx:
            command.append("NX")
        elif xx:
            command.append("XX")
        return self.execute_command(command) is not None

    def get(self, key):
        return self.execute_command(["GET", key])

    def delete(self, *keys) -> int:
        assert keys, "delete requires at least one key"
        deleted = 0
        for slot, slot_keys in self._group_by_slot(keys).items():
            node = self._node_for_slot(slot)
            deleted += int(self._execute_on(node, ["DEL"] + list(slot_keys)) or 0)
        return deleted

    def exists(self, *keys) -> int:
        assert keys, "exists requires at least one key"
        found = 0
        for slot, slot_keys in self._group_by_slot(keys).items():
            node = self._node_for_slot(slot)
            found += int(self._execute_on(node, ["EXISTS"] + list(slot_keys)) or 0)
        return found

    def mget(self, keys):
        """MGET across slots: one MGET per slot, re-ordered on return."""
        keys = list(keys)
        assert keys, "mget requires at least one key"
        values = {}
        for slot, slot_keys in self._group_by_slot(keys).items():
            node = self._node_for_slot(slot)
            replies = self._execute_on(node, ["MGET"] + slot_keys) or []
            for key, value in zip(slot_keys, replies):
                values[key] = value
        return [values.get(key) for key in keys]

    def mset(self, mapping) -> bool:
        assert mapping, "mset requires at least one pair"
        for slot, slot_keys in self._group_by_slot(list(mapping)).items():
            command = ["MSET"]
            for key in slot_keys:
                command.extend([key, mapping[key]])
            self._execute_on(self._node_for_slot(slot), command)
        return True

    def incr(self, key, amount=1) -> int:
        return int(self.execute_command(["INCRBY", key, str(amount)]))

    def keys(self, pattern="*"):
        """KEYS is per-node, so ask every master and concatenate."""
        found = []
        for node in self.nodes:
            found.extend(self._execute_on(node, ["KEYS", pattern]) or [])
        return found

    # ── cluster introspection ─────────────────────────────────────────────

    def cluster_info(self) -> dict:
        raw = _decode(self._client(self.nodes[0]).execute_command(["CLUSTER", "INFO"]))
        info = {}
        for line in raw.splitlines():
            line = line.strip()
            if not line or ":" not in line:
                continue
            field, _, value = line.partition(":")
            info[field] = value
        return info

    def cluster_nodes(self) -> dict:
        """CLUSTER NODES keyed by node id.

        Each line is: <id> <ip:port@cport> <flags> <master> <ping> <pong>
        <epoch> <link-state> [<slot> ...]
        """
        raw = _decode(self._client(self.nodes[0]).execute_command(["CLUSTER", "NODES"]))
        nodes = {}
        for line in raw.splitlines():
            fields = line.split()
            if len(fields) < 8:
                continue
            address = fields[1].split("@")[0]
            host, _, port = address.rpartition(":")
            flags = fields[2].split(",")
            nodes[fields[0]] = {
                "host": host,
                "port": int(port),
                "flags": flags,
                "master": "master" in flags,
                "master_id": None if fields[3] == "-" else fields[3],
                "link_state": fields[7],
                "slots": fields[8:],
            }
        return nodes

    def cluster_slots(self) -> list:
        """The slot ranges as ``[start, end, master, replicas]`` rows."""
        reply = self._client(self.nodes[0]).execute_command(["CLUSTER", "SLOTS"])
        ranges = []
        for entry in reply or []:
            master = (_decode(entry[2][0]), int(entry[2][1]))
            replicas = [(_decode(item[0]), int(item[1])) for item in entry[3:]]
            ranges.append([int(entry[0]), int(entry[1]), master, replicas])
        return ranges

    def cluster_keyslot(self, key) -> int:
        """The slot for ``key``, computed locally (no round trip)."""
        return key_slot(key)

    def cluster_countkeysinslot(self, int slot) -> int:
        assert 0 <= slot < SLOT_COUNT, "slot out of range"
        node = self._node_for_slot(slot)
        return int(self._execute_on(node, ["CLUSTER", "COUNTKEYSINSLOT", str(slot)]))

    # ── batching ──────────────────────────────────────────────────────────

    def pipeline(self):
        return CyRedisClusterPipeline(self)

    def close(self):
        with self._lock:
            for client in self._clients.values():
                pool = client.pool
                if pool is not None:
                    pool.disconnect()
            self._clients = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


class CyRedisClusterPipeline:
    """Batches commands and flushes one pipeline per owning node.

    The server refuses a cross-slot batch on a single connection, so the
    commands are grouped by node, sent as that node's own pipeline, and the
    replies put back in the order they were queued.
    """

    def __init__(self, cluster):
        self._cluster = cluster
        self._commands = []   # (node, args, transform)

    def _queue(self, key, list_args, transform=0):
        node = (self._cluster.node_for_key(key) if key is not None
                else self._cluster.nodes[0])
        self._commands.append((node, list_args, transform))
        return self

    def set(self, key, value, ex=-1, px=-1, nx=False, xx=False):
        args = ["SET", key, value]
        if ex > 0:
            args.extend(["EX", str(ex)])
        elif px > 0:
            args.extend(["PX", str(px)])
        if nx:
            args.append("NX")
        elif xx:
            args.append("XX")
        return self._queue(key, args, transform=1)

    def get(self, key):
        return self._queue(key, ["GET", key])

    def delete(self, *keys):
        assert keys, "delete requires at least one key"
        for key in keys:
            self._queue(key, ["DEL", key])
        return self

    def execute_command(self, *args):
        if len(args) == 1 and isinstance(args[0], (list, tuple)):
            command = list(args[0])
        else:
            command = list(args)
        assert command, "execute_command requires a command"
        key = command[1] if len(command) > 1 else None
        return self._queue(key, command)

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)

        def _command(*args):
            return self.execute_command(name.upper(), *args)

        return _command

    def execute(self):
        """Flush every node's batch and return results in queue order."""
        by_node = {}
        for index, (node, args, transform) in enumerate(self._commands):
            by_node.setdefault(node, []).append((index, args, transform))

        results = [None] * len(self._commands)
        try:
            for node, batch in by_node.items():
                client = self._cluster._client(node)
                pipe = client.pipeline()
                for _, args, _transform in batch:
                    pipe.execute_command(args)
                replies = pipe.execute()
                for (index, _args, transform), reply in zip(batch, replies):
                    results[index] = _apply_transform(reply, transform)
        finally:
            self._commands = []
        return results

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._commands = []
        return False


def _apply_transform(reply, transform):
    if transform == 1:
        return None if reply is None else True
    return reply


def _redirect_target(message):
    """The (host, port) out of a ``MOVED 1234 host:port`` error."""
    parts = message.split()
    assert len(parts) >= 3, f"malformed redirect: {message!r}"
    host, _, port = parts[2].rpartition(":")
    return (host or "127.0.0.1", int(port))


def _normalize_nodes(nodes):
    """Accept ``[(host, port)]``, ``["host:port"]`` or ``[{"host":..}]``."""
    if not nodes:
        return []
    normalized = []
    for node in nodes:
        if isinstance(node, dict):
            normalized.append((node["host"], int(node["port"])))
        elif isinstance(node, (tuple, list)):
            normalized.append((node[0], int(node[1])))
        else:
            host, _, port = str(node).rpartition(":")
            normalized.append((host or "127.0.0.1", int(port)))
    return normalized
