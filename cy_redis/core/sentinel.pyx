# cython: language_level=3
# distutils: language=c

"""
Redis Sentinel client.

Sentinel is a discovery service, not a proxy: it answers where the master of
a named group currently lives, and that answer changes on failover. So this
module resolves the address through the sentinels and hands back a normal
:class:`CyRedisClient` wrapper that re-resolves — rather than fails — when
the master it holds stops being the master:

    * the connection drops (the old master died), or
    * a write comes back ``-READONLY`` (the old master was demoted to replica).

Both are the signal to ask the sentinels again and retry the command once on
the new address.
"""

import threading
import time

from cy_redis.core.cy_redis_client import ConnectionError, CyRedisClient, RedisError

__all__ = ["CySentinel", "SentinelManagedClient", "SentinelError"]


class SentinelError(RedisError):
    """Raised when no sentinel can answer for a master group."""


def _decode(value):
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", errors="replace")
    return value


cdef class CySentinel:
    """A view over a set of sentinels monitoring one or more master groups."""

    cdef list _sentinels
    cdef object _lock
    cdef double _timeout
    cdef str _password
    cdef str _sentinel_password
    cdef dict _clients

    def __init__(self, sentinels, socket_timeout=2.0, password=None,
                 sentinel_password=None):
        assert sentinels, "at least one sentinel address is required"
        assert socket_timeout > 0, "socket_timeout must be positive"
        self._sentinels = _normalize(sentinels)
        self._lock = threading.RLock()
        self._timeout = socket_timeout
        self._password = password
        self._sentinel_password = sentinel_password
        self._clients = {}

    cdef object _sentinel_client(self, node):
        client = self._clients.get(node)
        if client is None:
            client = CyRedisClient(host=node[0], port=node[1],
                                   password=self._sentinel_password,
                                   max_connections=2)
            self._clients[node] = client
        return client

    def _ask_sentinels(self, list command):
        """Run a SENTINEL command on the first sentinel that answers.

        A sentinel that is itself down is expected — that is the whole point
        of running several — so failures move on to the next one and only an
        exhausted list is an error.
        """
        errors = []
        with self._lock:
            for index, node in enumerate(self._sentinels):
                try:
                    reply = self._sentinel_client(node).execute_command(command)
                except (RedisError, ConnectionError, OSError) as exc:
                    errors.append(f"{node[0]}:{node[1]}: {exc}")
                    continue
                # Keep the responsive sentinel first for the next lookup.
                if index:
                    self._sentinels.insert(0, self._sentinels.pop(index))
                return reply
        raise SentinelError(
            "No sentinel answered " + " ".join(str(part) for part in command)
            + ": " + "; ".join(errors or ["no sentinels configured"])
        )

    def discover_master(self, str service_name):
        """The (host, port) the sentinels currently call the master."""
        reply = self._ask_sentinels(
            ["SENTINEL", "get-master-addr-by-name", service_name]
        )
        if not reply:
            raise SentinelError(f"No master known for {service_name!r}")
        return (_decode(reply[0]), int(_decode(reply[1])))

    def discover_replicas(self, str service_name):
        """The replica addresses the sentinels report as usable."""
        reply = self._ask_sentinels(["SENTINEL", "replicas", service_name]) or []
        replicas = []
        for entry in reply:
            fields = {}
            for i in range(0, len(entry) - 1, 2):
                fields[_decode(entry[i])] = _decode(entry[i + 1])
            flags = fields.get("flags", "")
            if "s_down" in flags or "o_down" in flags or "disconnected" in flags:
                continue
            replicas.append((fields["ip"], int(fields["port"])))
        return replicas

    def master_for(self, str service_name, **client_kwargs):
        """A client for the master of ``service_name``, tracking failover."""
        return SentinelManagedClient(self, service_name, **client_kwargs)

    def close(self):
        with self._lock:
            for client in self._clients.values():
                pool = client.pool
                if pool is not None:
                    pool.disconnect()
            self._clients = {}


class SentinelManagedClient:
    """A :class:`CyRedisClient` that follows the master across failovers.

    Every attribute is delegated to the underlying client; the wrapper only
    intervenes when a call fails in a way that means "this is no longer the
    master", in which case it re-resolves the address and retries once.
    """

    _RESOLVE_ATTEMPTS = 3

    def __init__(self, sentinel, service_name, retry_delay=0.5, **client_kwargs):
        assert retry_delay >= 0, "retry_delay must not be negative"
        self._sentinel = sentinel
        self._service_name = service_name
        self._client_kwargs = client_kwargs
        self._retry_delay = retry_delay
        self._lock = threading.RLock()
        self._address = None
        self._client = None
        self._connect()

    # ── master tracking ───────────────────────────────────────────────────

    def _connect(self, force=False):
        with self._lock:
            address = self._sentinel.discover_master(self._service_name)
            if self._client is not None and address == self._address and not force:
                return self._client
            if self._client is not None:
                pool = self._client.pool
                if pool is not None:
                    pool.disconnect()
            self._address = address
            self._client = CyRedisClient(host=address[0], port=address[1],
                                         **self._client_kwargs)
            return self._client

    @property
    def master_address(self):
        return self._address

    @property
    def connection_pool(self):
        return self._client.pool

    @property
    def pool(self):
        return self._client.pool

    def _call(self, name, args, kwargs):
        last_error = None
        for attempt in range(self._RESOLVE_ATTEMPTS):
            client = self._client
            try:
                return getattr(client, name)(*args, **kwargs)
            except (ConnectionError, OSError) as exc:
                last_error = exc
            except RedisError as exc:
                if not str(exc).startswith("READONLY"):
                    raise
                last_error = exc
            if attempt + 1 < self._RESOLVE_ATTEMPTS:
                time.sleep(self._retry_delay)
                self._connect(force=True)
        raise SentinelError(
            f"{name} failed against the {self._service_name} master "
            f"at {self._address}: {last_error}"
        )

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        attribute = getattr(self._client, name)
        if not callable(attribute):
            return attribute

        def _method(*args, **kwargs):
            return self._call(name, args, kwargs)

        return _method

    def pipeline(self):
        # Pipelines hold a connection, so they are bound to the master that
        # is current when they are created rather than retried transparently.
        return self._client.pipeline()

    def close(self):
        with self._lock:
            if self._client is not None:
                pool = self._client.pool
                if pool is not None:
                    pool.disconnect()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


def _normalize(sentinels):
    """Accept ``[(host, port)]``, ``["host:port"]`` or ``[{"host":..}]``."""
    normalized = []
    for node in sentinels:
        if isinstance(node, dict):
            normalized.append((node["host"], int(node["port"])))
        elif isinstance(node, (tuple, list)):
            normalized.append((node[0], int(node[1])))
        else:
            host, _, port = str(node).rpartition(":")
            normalized.append((host or "127.0.0.1", int(port)))
    return normalized
