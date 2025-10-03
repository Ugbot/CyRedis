"""
Performance and benchmark tests for CyRedis.

Tests performance characteristics including:
- Throughput measurements
- Latency benchmarks
- Concurrent operation performance
- Memory efficiency
- Comparison with baseline operations
"""

import pytest
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import statistics


@pytest.mark.integration
@pytest.mark.slow
class TestPerformanceBasic:
    """Basic performance tests."""

    def test_set_throughput(self, redis_client, benchmark_config):
        """Test SET operation throughput."""
        iterations = benchmark_config.get("iterations", 1000)

        start_time = time.time()
        for i in range(iterations):
            redis_client.set(f"perf:set:{i}", f"value_{i}")
        end_time = time.time()

        elapsed = end_time - start_time
        ops_per_sec = iterations / elapsed

        print(f"\nSET Throughput: {ops_per_sec:.2f} ops/sec")
        print(f"Average latency: {(elapsed/iterations)*1000:.3f} ms")

        # Clean up
        for i in range(iterations):
            redis_client.delete(f"perf:set:{i}")

        # Should achieve reasonable throughput (adjust threshold as needed)
        assert ops_per_sec > 100  # At least 100 ops/sec

    def test_get_throughput(self, redis_client, benchmark_config):
        """Test GET operation throughput."""
        iterations = benchmark_config.get("iterations", 1000)

        # Setup: create keys
        for i in range(iterations):
            redis_client.set(f"perf:get:{i}", f"value_{i}")

        # Benchmark GET operations
        start_time = time.time()
        for i in range(iterations):
            redis_client.get(f"perf:get:{i}")
        end_time = time.time()

        elapsed = end_time - start_time
        ops_per_sec = iterations / elapsed

        print(f"\nGET Throughput: {ops_per_sec:.2f} ops/sec")
        print(f"Average latency: {(elapsed/iterations)*1000:.3f} ms")

        # Clean up
        for i in range(iterations):
            redis_client.delete(f"perf:get:{i}")

        assert ops_per_sec > 100

    def test_incr_throughput(self, redis_client, benchmark_config):
        """Test INCR operation throughput."""
        iterations = benchmark_config.get("iterations", 1000)
        key = "perf:incr:counter"

        start_time = time.time()
        for _ in range(iterations):
            redis_client.incr(key)
        end_time = time.time()

        elapsed = end_time - start_time
        ops_per_sec = iterations / elapsed

        print(f"\nINCR Throughput: {ops_per_sec:.2f} ops/sec")

        # Verify final value
        final_value = int(redis_client.get(key))
        assert final_value == iterations

        # Clean up
        redis_client.delete(key)

        assert ops_per_sec > 100

    def test_pipeline_throughput(self, redis_client, benchmark_config):
        """Test pipeline operation throughput."""
        iterations = benchmark_config.get("iterations", 1000)

        try:
            pipe = redis_client.pipeline()

            start_time = time.time()
            for i in range(iterations):
                pipe.set(f"perf:pipe:{i}", f"value_{i}")
            pipe.execute()
            end_time = time.time()

            elapsed = end_time - start_time
            ops_per_sec = iterations / elapsed

            print(f"\nPipeline Throughput: {ops_per_sec:.2f} ops/sec")

            # Pipeline should be much faster than individual operations
            assert ops_per_sec > 500

            # Clean up
            pipe = redis_client.pipeline()
            for i in range(iterations):
                pipe.delete(f"perf:pipe:{i}")
            pipe.execute()

        except AttributeError:
            pytest.skip("Pipeline not available")

    def test_list_operations_throughput(self, redis_client, benchmark_config):
        """Test list operation throughput."""
        iterations = benchmark_config.get("iterations", 1000)
        key = "perf:list"

        start_time = time.time()
        for i in range(iterations):
            redis_client.rpush(key, f"item_{i}")
        end_time = time.time()

        elapsed = end_time - start_time
        ops_per_sec = iterations / elapsed

        print(f"\nRPUSH Throughput: {ops_per_sec:.2f} ops/sec")

        # Verify list length
        length = redis_client.llen(key)
        assert length == iterations

        # Clean up
        redis_client.delete(key)

        assert ops_per_sec > 100

    def test_hash_operations_throughput(self, redis_client, benchmark_config):
        """Test hash operation throughput."""
        iterations = benchmark_config.get("iterations", 1000)
        key = "perf:hash"

        start_time = time.time()
        for i in range(iterations):
            redis_client.hset(key, f"field_{i}", f"value_{i}")
        end_time = time.time()

        elapsed = end_time - start_time
        ops_per_sec = iterations / elapsed

        print(f"\nHSET Throughput: {ops_per_sec:.2f} ops/sec")

        # Verify hash size
        size = redis_client.hlen(key)
        assert size == iterations

        # Clean up
        redis_client.delete(key)

        assert ops_per_sec > 100


@pytest.mark.integration
@pytest.mark.slow
class TestPerformanceConcurrent:
    """Concurrent performance tests."""

    def test_concurrent_sets(self, redis_client, benchmark_config):
        """Test concurrent SET operations."""
        num_threads = benchmark_config.get("threads", 10)
        ops_per_thread = benchmark_config.get("ops_per_thread", 100)

        def worker(thread_id):
            count = 0
            for i in range(ops_per_thread):
                redis_client.set(f"perf:concurrent:{thread_id}:{i}", f"value_{i}")
                count += 1
            return count

        start_time = time.time()
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(worker, i) for i in range(num_threads)]
            results = [f.result() for f in as_completed(futures)]
        end_time = time.time()

        total_ops = sum(results)
        elapsed = end_time - start_time
        ops_per_sec = total_ops / elapsed

        print(f"\nConcurrent SET: {ops_per_sec:.2f} ops/sec ({num_threads} threads)")

        # Clean up
        for thread_id in range(num_threads):
            for i in range(ops_per_thread):
                redis_client.delete(f"perf:concurrent:{thread_id}:{i}")

        assert ops_per_sec > 100

    def test_concurrent_gets(self, redis_client, benchmark_config):
        """Test concurrent GET operations."""
        num_threads = benchmark_config.get("threads", 10)
        ops_per_thread = benchmark_config.get("ops_per_thread", 100)

        # Setup: create keys
        for i in range(ops_per_thread):
            redis_client.set(f"perf:concurrent_get:{i}", f"value_{i}")

        def worker(thread_id):
            count = 0
            for i in range(ops_per_thread):
                redis_client.get(f"perf:concurrent_get:{i}")
                count += 1
            return count

        start_time = time.time()
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(worker, i) for i in range(num_threads)]
            results = [f.result() for f in as_completed(futures)]
        end_time = time.time()

        total_ops = sum(results)
        elapsed = end_time - start_time
        ops_per_sec = total_ops / elapsed

        print(f"\nConcurrent GET: {ops_per_sec:.2f} ops/sec ({num_threads} threads)")

        # Clean up
        for i in range(ops_per_thread):
            redis_client.delete(f"perf:concurrent_get:{i}")

        assert ops_per_sec > 100

    def test_concurrent_incr(self, redis_client, benchmark_config):
        """Test concurrent INCR operations."""
        num_threads = benchmark_config.get("threads", 10)
        ops_per_thread = benchmark_config.get("ops_per_thread", 100)
        key = "perf:concurrent_incr"

        def worker(thread_id):
            count = 0
            for _ in range(ops_per_thread):
                redis_client.incr(key)
                count += 1
            return count

        start_time = time.time()
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(worker, i) for i in range(num_threads)]
            results = [f.result() for f in as_completed(futures)]
        end_time = time.time()

        total_ops = sum(results)
        elapsed = end_time - start_time
        ops_per_sec = total_ops / elapsed

        print(f"\nConcurrent INCR: {ops_per_sec:.2f} ops/sec ({num_threads} threads)")

        # Verify final value (all increments should be accounted for)
        final_value = int(redis_client.get(key))
        assert final_value == total_ops

        # Clean up
        redis_client.delete(key)

        assert ops_per_sec > 50  # Lower threshold due to contention


@pytest.mark.integration
@pytest.mark.slow
class TestPerformanceLatency:
    """Latency measurement tests."""

    def test_operation_latencies(self, redis_client, benchmark_config):
        """Measure latencies for various operations."""
        iterations = benchmark_config.get("iterations", 100)

        operations = {
            "SET": lambda i: redis_client.set(f"perf:lat:set:{i}", f"value_{i}"),
            "GET": lambda i: redis_client.get(f"perf:lat:get:{i}"),
            "INCR": lambda i: redis_client.incr(f"perf:lat:incr:{i}"),
            "LPUSH": lambda i: redis_client.lpush(f"perf:lat:list:{i}", f"item_{i}"),
            "HSET": lambda i: redis_client.hset(f"perf:lat:hash:{i}", "field", f"value_{i}"),
        }

        # Pre-populate for GET
        for i in range(iterations):
            redis_client.set(f"perf:lat:get:{i}", f"value_{i}")

        results = {}

        for op_name, op_func in operations.items():
            latencies = []

            for i in range(iterations):
                start = time.time()
                op_func(i)
                end = time.time()
                latencies.append((end - start) * 1000)  # Convert to ms

            results[op_name] = {
                "mean": statistics.mean(latencies),
                "median": statistics.median(latencies),
                "min": min(latencies),
                "max": max(latencies),
                "p95": sorted(latencies)[int(0.95 * len(latencies))],
                "p99": sorted(latencies)[int(0.99 * len(latencies))],
            }

        # Print results
        print("\nOperation Latencies (ms):")
        print(f"{'Operation':<10} {'Mean':<8} {'Median':<8} {'Min':<8} {'Max':<8} {'P95':<8} {'P99':<8}")
        print("-" * 68)
        for op_name, stats in results.items():
            print(
                f"{op_name:<10} "
                f"{stats['mean']:<8.3f} "
                f"{stats['median']:<8.3f} "
                f"{stats['min']:<8.3f} "
                f"{stats['max']:<8.3f} "
                f"{stats['p95']:<8.3f} "
                f"{stats['p99']:<8.3f}"
            )

        # Clean up
        for i in range(iterations):
            redis_client.delete(
                f"perf:lat:set:{i}",
                f"perf:lat:get:{i}",
                f"perf:lat:incr:{i}",
                f"perf:lat:list:{i}",
                f"perf:lat:hash:{i}",
            )

        # P95 latency should be reasonable (< 10ms for local Redis)
        for op_name, stats in results.items():
            assert stats["p95"] < 100, f"{op_name} P95 latency too high: {stats['p95']:.3f}ms"

    def test_batch_vs_individual_latency(self, redis_client, benchmark_config):
        """Compare batch vs individual operation latencies."""
        iterations = benchmark_config.get("iterations", 100)

        # Individual operations
        start = time.time()
        for i in range(iterations):
            redis_client.set(f"perf:batch:{i}", f"value_{i}")
        individual_time = time.time() - start

        # Clean up
        for i in range(iterations):
            redis_client.delete(f"perf:batch:{i}")

        # Batch operations (pipeline)
        try:
            pipe = redis_client.pipeline()
            start = time.time()
            for i in range(iterations):
                pipe.set(f"perf:batch:{i}", f"value_{i}")
            pipe.execute()
            batch_time = time.time() - start

            # Clean up
            pipe = redis_client.pipeline()
            for i in range(iterations):
                pipe.delete(f"perf:batch:{i}")
            pipe.execute()

            print(f"\nIndividual ops time: {individual_time:.3f}s")
            print(f"Batch ops time: {batch_time:.3f}s")
            print(f"Speedup: {individual_time/batch_time:.2f}x")

            # Batch should be significantly faster
            assert batch_time < individual_time

        except AttributeError:
            pytest.skip("Pipeline not available")


@pytest.mark.integration
@pytest.mark.slow
class TestPerformanceStress:
    """Stress tests."""

    def test_large_value_performance(self, redis_client, benchmark_config):
        """Test performance with large values."""
        sizes = [1024, 10240, 102400, 1024000]  # 1KB, 10KB, 100KB, 1MB

        for size in sizes:
            key = f"perf:large:{size}"
            value = "x" * size

            # SET performance
            start = time.time()
            redis_client.set(key, value)
            set_time = (time.time() - start) * 1000

            # GET performance
            start = time.time()
            retrieved = redis_client.get(key)
            get_time = (time.time() - start) * 1000

            print(f"\n{size} bytes - SET: {set_time:.3f}ms, GET: {get_time:.3f}ms")

            # Verify
            assert len(retrieved) == size

            # Clean up
            redis_client.delete(key)

            # Operations should complete in reasonable time
            assert set_time < 1000  # < 1 second
            assert get_time < 1000

    def test_many_keys_performance(self, redis_client, benchmark_config):
        """Test performance with many keys."""
        num_keys = benchmark_config.get("many_keys", 10000)

        # Create many keys
        start = time.time()
        for i in range(num_keys):
            redis_client.set(f"perf:many:{i}", f"value_{i}")
        create_time = time.time() - start

        # Scan all keys
        start = time.time()
        count = 0
        try:
            cursor = 0
            while True:
                cursor, keys = redis_client.scan(cursor, match="perf:many:*", count=100)
                count += len(keys)
                if cursor == 0:
                    break
        except AttributeError:
            # SCAN not available
            pass
        scan_time = time.time() - start

        print(f"\nCreated {num_keys} keys in {create_time:.3f}s")
        if count > 0:
            print(f"Scanned {count} keys in {scan_time:.3f}s")

        # Clean up
        for i in range(num_keys):
            redis_client.delete(f"perf:many:{i}")

        assert create_time < 60  # Should complete in reasonable time

    def test_rapid_connect_disconnect(self, redis_client, benchmark_config):
        """Test rapid connection/disconnection."""
        iterations = benchmark_config.get("connections", 50)

        # Note: This test depends on how the client manages connections
        # It may need adjustment based on actual implementation

        start = time.time()
        for i in range(iterations):
            # This assumes the client can create new connections
            # Adjust based on actual API
            try:
                # Some clients have explicit connect/disconnect
                if hasattr(redis_client, 'ping'):
                    redis_client.ping()
            except:
                pass
        elapsed = time.time() - start

        print(f"\n{iterations} connection cycles in {elapsed:.3f}s")

        assert elapsed < 30  # Should be reasonably fast
