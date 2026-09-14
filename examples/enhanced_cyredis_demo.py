#!/usr/bin/env python3
"""
CyRedis feature tour.

Everything here runs against a plain Redis 7 / Valkey 8 on localhost:6379
through the supported ``cy-redis`` package. The JSON section is skipped
unless the server has the RedisJSON / valkey-json module loaded.

    uv run python examples/enhanced_cyredis_demo.py
"""

import asyncio
import os
import time

from cy_redis import CyRedisClient
from cy_redis.core import AsyncRedisClient
from cy_redis.features import CyRedisJSON, module_names

HOST = os.environ.get("REDIS_HOST", "localhost")
PORT = int(os.environ.get("REDIS_PORT", "6379"))


def banner(title: str) -> None:
    print(f"\n{title}\n{'=' * len(title)}")


def loaded_modules(client: CyRedisClient) -> set:
    conn = client.pool.get_connection()
    try:
        return module_names(conn)
    finally:
        client.pool.return_connection(conn)


def demo_server_info(client: CyRedisClient) -> None:
    banner("Server")
    print(f"server type : {client.detect_server_type()}")
    print(f"version     : {client.info('server').get('redis_version')}")
    print(f"modules     : {sorted(loaded_modules(client)) or 'none'}")


def demo_strings_and_hashes(client: CyRedisClient) -> None:
    banner("Strings and hashes")
    client.set("demo:counter", "0")
    for _ in range(3):
        client.incr("demo:counter")
    print(f"counter after 3 INCR: {client.get('demo:counter')}")

    client.hset("demo:user:1", mapping={"name": "ada", "lang": "cython", "score": 42})
    print(f"hash        : {client.hgetall('demo:user:1')}")
    print(f"hincrby     : {client.hincrby('demo:user:1', 'score', 8)}")


def demo_bitmaps_and_hll(client: CyRedisClient) -> None:
    banner("Bitmaps and HyperLogLog")
    client.delete("demo:active", "demo:premium", "demo:active_premium", "demo:visitors")
    for user in (1, 3, 5, 7):
        client.setbit("demo:active", user, 1)
    for user in (1, 7):
        client.setbit("demo:premium", user, 1)
    client.bitop("AND", "demo:active_premium", "demo:active", "demo:premium")
    print(f"active users         : {client.bitcount('demo:active')}")
    print(f"first active user    : {client.bitpos('demo:active', 1)}")
    print(f"active AND premium   : {client.bitcount('demo:active_premium')}")

    client.pfadd("demo:visitors", *[f"visitor-{i}" for i in range(1000)])
    print(f"HLL cardinality      : ~{client.pfcount('demo:visitors')}")


def demo_sorted_sets(client: CyRedisClient) -> None:
    banner("Sorted sets")
    client.delete("demo:leaderboard")
    client.zadd(
        "demo:leaderboard", {"ada": 120, "grace": 95, "linus": 150, "guido": 110}
    )
    top = client.zrevrange("demo:leaderboard", 0, 2, withscores=True)
    print(f"top 3       : {top}")
    print(f"rank(guido) : {client.zrevrank('demo:leaderboard', 'guido')}")


def demo_pipeline(client: CyRedisClient) -> None:
    banner("Pipeline")
    started = time.perf_counter()
    with client.pipeline() as pipe:
        for i in range(500):
            pipe.set(f"demo:pipe:{i}", str(i))
        for i in range(0, 500, 100):
            pipe.get(f"demo:pipe:{i}")
        results = pipe.execute()
    elapsed = (time.perf_counter() - started) * 1000
    print(
        f"505 commands in one round trip: {elapsed:.1f} ms; sampled gets = {results[-5:]}"
    )


def demo_streams(client: CyRedisClient) -> None:
    banner("Streams and consumer groups")
    stream, group = "demo:events", "demo-workers"
    client.delete(stream)
    client.xgroup_create(stream, group, id="0", mkstream=True)
    for i in range(5):
        client.xadd(
            stream, {"event": "click", "n": str(i)}, maxlen=1000, approximate=True
        )
    print(f"stream length: {client.xlen(stream)}")

    # Entries come back flattened as (stream, id, {field: value}).
    entries = client.xreadgroup(group, "worker-1", {stream: ">"}, count=10)
    ids = [entry_id for _, entry_id, _ in entries]
    print(f"worker-1 read {len(ids)} messages, first: {entries[0][2]}")
    print(f"acknowledged : {client.xack(stream, group, *ids)}")
    print(f"pending now  : {client.xpending(stream, group)}")


def demo_pubsub(client: CyRedisClient) -> None:
    banner("Pub/Sub")
    with client.pubsub() as subscriber:
        subscriber.subscribe("demo:channel")
        subscriber.get_message(timeout=1.0)  # the subscribe confirmation
        receivers = client.publish("demo:channel", "hello from cy-redis")
        message = subscriber.get_message(timeout=1.0, ignore_subscribe_messages=True)
        print(f"published to {receivers} subscriber(s); received: {message}")


def demo_json(client: CyRedisClient) -> None:
    banner("JSON (module)")
    if not loaded_modules(client) & {"rejson", "json"}:
        print("no JSON module loaded on this server - skipping")
        return
    js = CyRedisJSON(HOST, PORT)
    js.json_set("demo:doc", "$", {"name": "cy-redis", "tags": ["fast"], "stars": 1})
    js.json_arrappend("demo:doc", "$.tags", "cython", "hiredis")
    print(f"stars after NUMINCRBY: {js.json_numincrby('demo:doc', '$.stars', 41)}")
    print(f"document             : {js.json_get('demo:doc', '$')}")


async def demo_async() -> None:
    banner("Async client")
    async with AsyncRedisClient(HOST, PORT) as client:
        await client.set("demo:async", "ok")
        values = await asyncio.gather(*(client.get("demo:async") for _ in range(5)))
        print(f"5 concurrent GETs: {values}")


def cleanup(client: CyRedisClient) -> None:
    keys = list(client.scan_iter(match="demo:*"))
    if keys:
        client.delete(*keys)
    print(f"\nremoved {len(keys)} demo keys")


def main() -> None:
    with CyRedisClient(HOST, PORT) as client:
        demo_server_info(client)
        demo_strings_and_hashes(client)
        demo_bitmaps_and_hll(client)
        demo_sorted_sets(client)
        demo_pipeline(client)
        demo_streams(client)
        demo_pubsub(client)
        demo_json(client)
        asyncio.run(demo_async())
        cleanup(client)


if __name__ == "__main__":
    main()
