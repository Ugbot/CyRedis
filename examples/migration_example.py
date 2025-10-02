#!/usr/bin/env python3
"""
Migration example showing how to adapt existing aioredis code
to use the new high-performance Cython Redis wrapper.
"""

import asyncio
import time
from typing import Dict, List

# Old aioredis-based code (from existing files)
import aioredis

# New high-performance wrapper
from cy_redis import HighPerformanceRedis, ThreadedStreamConsumer


class OldStreamManager:
    """Original aioredis-based stream manager for comparison"""

    def __init__(self, url: str = "redis://localhost"):
        self.redis = None
        self.url = url
        self.user_subs: Dict[str, str] = {}

    async def connect(self):
        self.redis = await aioredis.from_url(self.url)

    async def add_subscription(self, user: str, stream: str):
        self.user_subs[user] = stream

    async def read_stream(self, user: str):
        if user not in self.user_subs:
            return []

        stream = self.user_subs[user]
        try:
            response = await self.redis.xread(
                streams={stream: "0"},
                count=10,
                block=1000
            )
            return response
        except Exception as e:
            print(f"Error reading stream: {e}")
            return []

    async def close(self):
        if self.redis:
            await self.redis.close()


class NewStreamManager:
    """New high-performance stream manager using Cython wrapper"""

    def __init__(self, host: str = "localhost", port: int = 6379):
        self.redis = HighPerformanceRedis(host=host, port=port, max_workers=4)
        self.user_subs: Dict[str, str] = {}

    def add_subscription(self, user: str, stream: str):
        """Synchronous subscription (can be called from any thread)"""
        self.user_subs[user] = stream

    async def read_stream_async(self, user: str) -> List[tuple]:
        """Async stream reading using the new client"""
        if user not in self.user_subs:
            return []

        stream = self.user_subs[user]
        try:
            messages = await self.redis.xread_async({stream: "0"}, count=10, block=1000)
            return messages
        except Exception as e:
            print(f"Error reading stream: {e}")
            return []

    def read_stream_threaded(self, user: str):
        """Threaded stream reading for high performance"""
        if user not in self.user_subs:
            return []

        stream = self.user_subs[user]
        try:
            messages = self.redis.xread({stream: "0"}, count=10, block=1000)
            return messages
        except Exception as e:
            print(f"Error reading stream: {e}")
            return []

    def close(self):
        """Close the Redis connection"""
        self.redis.close()


def benchmark_old_vs_new():
    """Benchmark comparison between old and new implementations"""
    print("=== Performance Benchmark: Old vs New Redis Client ===")

    async def benchmark_old():
        manager = OldStreamManager()
        await manager.connect()

        # Setup test data
        await manager.redis.xadd("bench:stream", {"data": "test"})

        # Benchmark operations
        start_time = time.time()
        for i in range(100):
            await manager.redis.set(f"bench:key:{i}", f"value:{i}")
            await manager.redis.get(f"bench:key:{i}")
        old_time = time.time() - start_time

        await manager.close()
        return old_time

    def benchmark_new():
        manager = NewStreamManager()

        # Setup test data
        manager.redis.xadd("bench:stream", {"data": "test"})

        # Benchmark operations
        start_time = time.time()
        for i in range(100):
            manager.redis.set(f"bench:key:{i}", f"value:{i}")
            manager.redis.get(f"bench:key:{i}")
        new_time = time.time() - start_time

        manager.close()
        return new_time

    # Run benchmarks
    import asyncio

    async def run_benchmarks():
        old_time = await benchmark_old()
        new_time = benchmark_new()

        print(".4f")
        print(".4f")
        print(".2f")

        return old_time, new_time

    return asyncio.run(run_benchmarks())


def migration_stream_create():
    """Migrate the stream-create.py functionality to use the new client"""
    print("\n=== Migrating stream-create.py ===")

    # Original code (from stream-create.py)
    print("Original aioredis version:")
    print("""
import asyncio
import random
import aioredis

async def publish_to_streams():
    redis = aioredis.from_url('redis://localhost')
    stream_names = [f'streamw:{i}' for i in range(1, 6)]
    current_number = 0

    try:
        while True:
            for stream in stream_names:
                data = {'number': current_number}
                current_number += 1
                await redis.xadd(stream, data)
                print(f'Published to {stream}: {data}')
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        pass
""")

    # New high-performance version
    print("New CyRedis version:")
    print("""
from redis_wrapper import HighPerformanceRedis
import time

def publish_to_streams_high_perf():
    redis = HighPerformanceRedis(max_workers=4)
    stream_names = [f'streamw:{i}' for i in range(1, 6)]
    current_number = 0

    try:
        while True:
            # Use threaded operations for higher throughput
            futures = []
            for stream in stream_names:
                data = {'number': current_number}
                current_number += 1
                future = redis.xadd_threaded(stream, data)
                futures.append((stream, data, future))

            # Wait for all operations to complete
            for stream, data, future in futures:
                result = future.result()
                print(f'Published to {stream}: {data} -> {result}')

            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        redis.close()
""")


def migration_stream_read():
    """Migrate the stream-read.py functionality"""
    print("\n=== Migrating stream-read.py ===")

    print("Original aioredis version:")
    print("""
import asyncio
import aioredis

async def subscribe_to_streams():
    redis = aioredis.from_url('redis://localhost')
    subscribed_streams = {f'streamw:{i}': '0' for i in range(1, 6)}

    try:
        while True:
            response = await redis.xread(
                streams=subscribed_streams,
                count=None,
                block=100000
            )
            for stream, messages in response:
                for message_id, data in messages:
                    print(f'Received from {stream}: {data}')
                    subscribed_streams[stream] = message_id
    except asyncio.CancelledError:
        pass
""")

    print("New CyRedis version with threading:")
    print("""
from redis_wrapper import HighPerformanceRedis, ThreadedStreamConsumer
import time

def stream_consumer_callback(stream, message_id, data):
    print(f'Received from {stream}: {data} (ID: {message_id})')

def subscribe_to_streams_high_perf():
    redis = HighPerformanceRedis(max_workers=4)
    consumer = ThreadedStreamConsumer(redis, max_workers=4)

    # Subscribe to all streams
    for i in range(1, 6):
        stream_name = f'streamw:{i}'
        consumer.subscribe(stream_name, '0')
        consumer.set_callback(stream_name, stream_consumer_callback)

    try:
        consumer.start_consuming()
        print("Consuming messages... Press Ctrl+C to stop")

        while consumer.is_running():
            time.sleep(0.1)
    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.stop_consuming()
        redis.close()
""")


def migration_stream_manager():
    """Migrate the AsyncStreamManager functionality"""
    print("\n=== Migrating AsyncStreamManager ===")

    print("Original AsyncStreamManager:")
    print("""
class AsyncStreamManager:
    def __init__(self, URL: str) -> None:
        self.redis = aioredis.Redis.from_url(URL)

    async def add_subscription(self, user: str, stream: str) -> None:
        await self.redis.sadd(f'subscriptions:{user}', stream)

    async def read_stream(self):
        response = await self.redis.xread(streams=self.user_subs, count=None, block=10)
        # ... process response
""")

    print("New HighPerformanceStreamManager:")
    print("""
from redis_wrapper import HighPerformanceRedis
from typing import Dict, Set

class HighPerformanceStreamManager:
    def __init__(self, host: str = "localhost", port: int = 6379):
        self.redis = HighPerformanceRedis(host, port, max_workers=4)
        self.user_subs: Dict[str, Set[str]] = {}

    def add_subscription(self, user: str, stream: str) -> None:
        # Synchronous operation - much faster
        self.redis.set(f'subscriptions:{user}:{stream}', '1')
        if user not in self.user_subs:
            self.user_subs[user] = set()
        self.user_subs[user].add(stream)

    def get_user_streams(self, user: str) -> Set[str]:
        return self.user_subs.get(user, set())

    def read_user_streams(self, user: str):
        # Read all streams for a user concurrently
        user_streams = self.get_user_streams(user)
        if not user_streams:
            return []

        # Create stream dict with current positions (simplified)
        stream_positions = {stream: '0' for stream in user_streams}

        # High-performance read
        return self.redis.xread(stream_positions, count=10, block=100)

    def close(self):
        self.redis.close()
""")


if __name__ == "__main__":
    print("Redis Migration Examples")
    print("=" * 50)

    # Show migration examples
    migration_stream_create()
    migration_stream_read()
    migration_stream_manager()

    # Run performance benchmark
    print("\nRunning performance benchmark...")
    try:
        old_time, new_time = benchmark_old_vs_new()
        print("Migration examples completed!")
    except Exception as e:
        print(f"Benchmark failed (likely missing aioredis): {e}")
        print("Make sure both aioredis and CyRedis are available for comparison.")
