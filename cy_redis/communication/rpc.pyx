# cython: language_level=3

"""Redis-backed RPC with service discovery.

Request/response flow:

- A service instance registers itself in the registry (a Redis SET of
  instance ids plus one JSON blob per instance, refreshed by heartbeats).
- Clients discover live instances, pick one at random, and LPUSH a JSON
  request onto that instance's request list.
- The server BRPOPs requests (LPUSH + BRPOP = FIFO), dispatches to a
  registered handler, and LPUSHes the JSON response onto a per-request
  response list with a TTL.
- The client BLPOPs the response list, so waiting is a blocking Redis
  round-trip rather than a poll loop.

Everything rides on the core CyRedisClient; no extra dependencies.
"""

import json
import random
import threading
import time
import traceback
import uuid
from concurrent.futures import ThreadPoolExecutor


# How long a response may sit unclaimed before Redis expires it. Longer than
# any sane call timeout so a slow client still finds its answer; bounded so
# abandoned responses cannot accumulate forever.
DEF RESPONSE_TTL_MARGIN = 2


class RPCError(Exception):
    """Base exception for RPC operations"""
    pass


class RPCServiceUnavailable(RPCError):
    """Raised when no service instances are available"""
    pass


class RPCTimeoutError(RPCError):
    """Raised when an RPC call times out"""
    pass


cdef class CyRPCRequest:
    """A single RPC request with JSON serialization."""

    cdef public str service
    cdef public str method
    cdef public list args
    cdef public dict kwargs
    cdef public str request_id
    cdef public long timestamp
    cdef public int timeout

    def __cinit__(self, str service, str method, list args=None,
                  dict kwargs=None, str request_id=None, int timeout=30):
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        self.service = service
        self.method = method
        self.args = args if args is not None else []
        self.kwargs = kwargs if kwargs is not None else {}
        self.request_id = request_id or str(uuid.uuid4())
        self.timestamp = <long>time.time()
        self.timeout = timeout

    def to_dict(self):
        return {
            'service': self.service,
            'method': self.method,
            'args': self.args,
            'kwargs': self.kwargs,
            'request_id': self.request_id,
            'timestamp': self.timestamp,
            'timeout': self.timeout,
        }

    @staticmethod
    def from_dict(dict data):
        return CyRPCRequest(
            data['service'],
            data['method'],
            data.get('args', []),
            data.get('kwargs', {}),
            data.get('request_id'),
            data.get('timeout', 30),
        )

    def to_json(self):
        return json.dumps(self.to_dict())

    @staticmethod
    def from_json(str json_str):
        return CyRPCRequest.from_dict(json.loads(json_str))


cdef class CyRPCResponse:
    """A single RPC response with JSON serialization."""

    cdef public str request_id
    cdef public bint success
    cdef public object result
    cdef public str error
    cdef public str error_type
    cdef public long timestamp

    def __cinit__(self, str request_id, bint success=True, result=None,
                  str error=None, str error_type=None):
        self.request_id = request_id
        self.success = success
        self.result = result
        self.error = error
        self.error_type = error_type
        self.timestamp = <long>time.time()

    def to_dict(self):
        return {
            'request_id': self.request_id,
            'success': self.success,
            'result': self.result,
            'error': self.error,
            'error_type': self.error_type,
            'timestamp': self.timestamp,
        }

    @staticmethod
    def from_dict(dict data):
        return CyRPCResponse(
            data['request_id'],
            data.get('success', True),
            data.get('result'),
            data.get('error'),
            data.get('error_type'),
        )

    def to_json(self):
        return json.dumps(self.to_dict())

    @staticmethod
    def from_json(str json_str):
        return CyRPCResponse.from_dict(json.loads(json_str))


cdef class CyRPCServiceRegistry:
    """Service registry with heartbeat-based liveness.

    Layout in Redis (all under ``namespace``):

    - ``{ns}:services:{name}``            SET of instance ids
    - ``{ns}:services:{name}:{instance}`` JSON blob, TTL = 3× heartbeat
    """

    cdef public object redis
    cdef public str registry_key
    cdef public int heartbeat_interval

    def __cinit__(self, redis_client, str namespace="rpc",
                  int heartbeat_interval=30):
        if heartbeat_interval <= 0:
            raise ValueError("heartbeat_interval must be positive")
        self.redis = redis_client
        self.registry_key = f"{namespace}:services"
        self.heartbeat_interval = heartbeat_interval

    cpdef void register_service(self, str service_name, str service_id,
                                dict metadata=None, str endpoint=""):
        """Register a service instance and make it discoverable."""
        cdef str service_key = f"{self.registry_key}:{service_name}:{service_id}"
        # Float timestamps: second-granularity truncation would blur the
        # liveness boundary (a 2.5s silence could round to exactly 2s).
        cdef dict service_info = {
            'service_id': service_id,
            'endpoint': endpoint,
            'metadata': metadata or {},
            'registered_at': time.time(),
            'last_heartbeat': time.time(),
        }
        self.redis.set(service_key, json.dumps(service_info))
        self.redis.execute_command(
            ['SADD', f"{self.registry_key}:{service_name}", service_id])
        self.redis.execute_command(
            ['EXPIRE', service_key, str(self.heartbeat_interval * 3)])

    cpdef void unregister_service(self, str service_name, str service_id):
        """Remove a service instance from discovery."""
        self.redis.execute_command(
            ['SREM', f"{self.registry_key}:{service_name}", service_id])
        self.redis.delete(f"{self.registry_key}:{service_name}:{service_id}")

    cpdef list discover_services(self, str service_name):
        """Return the live instances of a service, pruning dead ones."""
        cdef str service_set_key = f"{self.registry_key}:{service_name}"
        cdef list service_ids = self.redis.execute_command(
            ['SMEMBERS', service_set_key]) or []
        cdef list services = []
        cdef str service_id, service_key
        cdef object service_json
        cdef double now, last_heartbeat

        for service_id in service_ids:
            service_key = f"{service_set_key}:{service_id}"
            service_json = self.redis.get(service_key)
            if not service_json:
                # Blob expired (missed heartbeats) — drop the stale set entry.
                self.redis.execute_command(['SREM', service_set_key, service_id])
                continue
            try:
                service_info = json.loads(service_json)
                now = time.time()
                last_heartbeat = float(service_info.get('last_heartbeat', 0.0))
                if now - last_heartbeat <= self.heartbeat_interval * 2:
                    services.append(service_info)
                else:
                    # Blob present but heartbeats stopped — treat as dead.
                    self.redis.execute_command(['SREM', service_set_key, service_id])
                    self.redis.delete(service_key)
            except (json.JSONDecodeError, KeyError, TypeError):
                # Corrupt registration — clean it up.
                self.redis.execute_command(['SREM', service_set_key, service_id])
                self.redis.delete(service_key)
        return services

    cpdef void send_heartbeat(self, str service_name, str service_id):
        """Refresh an instance's liveness stamp and TTL."""
        cdef str service_key = f"{self.registry_key}:{service_name}:{service_id}"
        cdef object service_json = self.redis.get(service_key)
        if not service_json:
            return  # expired — the owner should re-register
        try:
            service_info = json.loads(service_json)
        except json.JSONDecodeError:
            return  # corrupt — discovery will clean it up
        service_info['last_heartbeat'] = time.time()
        self.redis.set(service_key, json.dumps(service_info))
        self.redis.execute_command(
            ['EXPIRE', service_key, str(self.heartbeat_interval * 3)])

    cpdef list get_all_services(self):
        """Return every registered instance blob across all services."""
        cdef list services = []
        cdef str cursor = "0"
        cdef str pattern = f"{self.registry_key}:*:*"
        cdef object scan_result, service_json
        cdef list keys
        cdef str key

        while True:
            scan_result = self.redis.execute_command(
                ['SCAN', cursor, 'MATCH', pattern])
            if not scan_result or len(scan_result) < 2:
                break
            cursor = str(scan_result[0])
            keys = scan_result[1] or []
            for key in keys:
                service_json = self.redis.get(key)
                if service_json:
                    try:
                        services.append(json.loads(service_json))
                    except json.JSONDecodeError:
                        pass
            if cursor == "0":
                break
        return services


cdef class CyRPCClient:
    """RPC client: discover an instance, send the request, await the reply."""

    cdef public object redis
    cdef public CyRPCServiceRegistry registry
    cdef public str request_queue_prefix
    cdef public str response_prefix
    cdef public int default_timeout
    cdef object executor

    def __cinit__(self, redis_client, str namespace="rpc", int default_timeout=30):
        if default_timeout <= 0:
            raise ValueError("default_timeout must be positive")
        self.redis = redis_client
        self.registry = CyRPCServiceRegistry(redis_client, namespace)
        self.request_queue_prefix = f"{namespace}:requests"
        self.response_prefix = f"{namespace}:responses"
        self.default_timeout = default_timeout
        self.executor = ThreadPoolExecutor(max_workers=4)

    def close(self):
        """Release the async-call executor."""
        self.executor.shutdown(wait=False)

    cpdef object call(self, str service_name, str method, list args=None,
                      dict kwargs=None, int timeout=-1):
        """Synchronous RPC call. Returns the handler's result.

        Raises RPCServiceUnavailable when no live instance exists,
        RPCTimeoutError when no response arrives within ``timeout`` seconds,
        and RPCError (with the remote traceback message) when the handler
        raised.
        """
        if timeout == -1:
            timeout = self.default_timeout
        if timeout <= 0:
            raise ValueError("timeout must be positive")

        cdef CyRPCRequest request = CyRPCRequest(
            service_name, method, args, kwargs, timeout=timeout)

        cdef list services = self.registry.discover_services(service_name)
        if not services:
            raise RPCServiceUnavailable(
                f"No instances available for service: {service_name}")

        # Random choice spreads load across instances without shared state.
        cdef dict service = random.choice(services)
        cdef str service_id = service['service_id']

        self.redis.execute_command(
            ['LPUSH', f"{self.request_queue_prefix}:{service_id}",
             request.to_json()])

        # The response arrives on a dedicated list; BLPOP blocks server-side
        # instead of polling.
        cdef str response_key = f"{self.response_prefix}:{request.request_id}"
        cdef object popped = self.redis.execute_command(
            ['BLPOP', response_key, str(timeout)])
        if not popped:
            raise RPCTimeoutError(
                f"RPC call to {service_name}.{method} timed out after {timeout}s")

        # BLPOP returns [key, value]
        cdef CyRPCResponse response = CyRPCResponse.from_json(popped[1])
        if response.success:
            return response.result
        if response.error_type == "RPCServiceUnavailable":
            raise RPCServiceUnavailable(response.error)
        if response.error_type == "RPCTimeoutError":
            raise RPCTimeoutError(response.error)
        raise RPCError(response.error or "RPC call failed")

    async def call_async(self, str service_name, str method, list args=None,
                         dict kwargs=None, int timeout=-1):
        """Async RPC call (the blocking call runs on the executor)."""
        import asyncio
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self.executor, self.call, service_name, method, args, kwargs, timeout)


cdef class CyRPCServer:
    """RPC server: register handlers, consume requests, publish responses.

    Usage::

        server = CyRPCServer(redis_client, "billing")
        server.register_handler("charge", charge_fn)
        server.start(num_workers=2)
        ...
        server.stop()
    """

    cdef public object redis
    cdef public CyRPCServiceRegistry registry
    cdef public str service_name
    cdef public str service_id
    cdef public dict handlers
    cdef str request_key
    cdef str response_prefix
    cdef object _threads
    cdef object _stop_event
    cdef object _metadata

    def __cinit__(self, redis_client, str service_name, str service_id=None,
                  str namespace="rpc", int heartbeat_interval=30,
                  dict metadata=None):
        self.redis = redis_client
        self.registry = CyRPCServiceRegistry(redis_client, namespace,
                                             heartbeat_interval)
        self.service_name = service_name
        self.service_id = service_id or str(uuid.uuid4())
        self.handlers = {}
        self.request_key = f"{namespace}:requests:{self.service_id}"
        self.response_prefix = f"{namespace}:responses"
        self._threads = []
        self._stop_event = threading.Event()
        self._metadata = metadata

    def register_handler(self, str method, handler):
        """Expose ``handler`` (any callable) as ``method`` on this service."""
        if not callable(handler):
            raise TypeError(f"handler for {method!r} is not callable")
        self.handlers[method] = handler

    def handler(self, str method):
        """Decorator form of register_handler."""
        def decorator(fn):
            self.register_handler(method, fn)
            return fn
        return decorator

    @property
    def running(self):
        return bool(self._threads) and not self._stop_event.is_set()

    def start(self, int num_workers=1):
        """Register the service and start worker + heartbeat threads."""
        if num_workers <= 0:
            raise ValueError("num_workers must be positive")
        if self._threads:
            raise RuntimeError("server already started")
        if not self.handlers:
            raise RuntimeError("no handlers registered — nothing to serve")

        self._stop_event.clear()
        self.registry.register_service(self.service_name, self.service_id,
                                       metadata=self._metadata)

        cdef int i
        for i in range(num_workers):
            t = threading.Thread(target=self._worker_loop,
                                 name=f"cyredis-rpc-{self.service_name}-{i}",
                                 daemon=True)
            t.start()
            self._threads.append(t)
        hb = threading.Thread(target=self._heartbeat_loop,
                              name=f"cyredis-rpc-hb-{self.service_name}",
                              daemon=True)
        hb.start()
        self._threads.append(hb)

    def stop(self, double join_timeout=5.0):
        """Unregister and stop all threads."""
        self._stop_event.set()
        for t in self._threads:
            t.join(timeout=join_timeout)
        self._threads = []
        self.registry.unregister_service(self.service_name, self.service_id)

    def _worker_loop(self):
        """BRPOP requests and dispatch them until stopped.

        The 1-second BRPOP timeout is the stop-latency/idle-load trade-off:
        stop() takes at most ~1s to be noticed, and an idle worker costs one
        Redis round-trip per second.
        """
        while not self._stop_event.is_set():
            try:
                popped = self.redis.execute_command(
                    ['BRPOP', self.request_key, '1'])
            except ConnectionError:
                # Redis briefly unreachable — back off and retry; the client
                # reconnects on the next command.
                self._stop_event.wait(0.5)
                continue
            if not popped:
                continue
            self._dispatch(popped[1])

    def _dispatch(self, str request_json):
        """Run one request through its handler and publish the response."""
        cdef CyRPCRequest request
        cdef CyRPCResponse response
        try:
            request = CyRPCRequest.from_json(request_json)
        except (json.JSONDecodeError, KeyError, TypeError):
            return  # not a valid request — nothing to respond to

        handler = self.handlers.get(request.method)
        if handler is None:
            response = CyRPCResponse(
                request.request_id, success=False,
                error=f"Unknown method: {request.service}.{request.method}",
                error_type="RPCError")
        else:
            try:
                result = handler(*request.args, **request.kwargs)
                response = CyRPCResponse(request.request_id, success=True,
                                         result=result)
            except Exception as exc:
                # The remote caller gets the message; the full traceback
                # stays server-side where the logs are.
                traceback.print_exc()
                response = CyRPCResponse(
                    request.request_id, success=False, error=str(exc),
                    error_type=type(exc).__name__)

        cdef str response_key = f"{self.response_prefix}:{request.request_id}"
        self.redis.execute_command(['LPUSH', response_key, response.to_json()])
        self.redis.execute_command(
            ['EXPIRE', response_key,
             str(request.timeout * RESPONSE_TTL_MARGIN)])

    def _heartbeat_loop(self):
        """Refresh registry liveness at half the heartbeat interval."""
        cdef double interval = self.registry.heartbeat_interval / 2.0
        while not self._stop_event.wait(interval):
            try:
                self.registry.send_heartbeat(self.service_name, self.service_id)
            except ConnectionError:
                continue  # retry on the next tick
