#!/usr/bin/env python3
"""
Metrics Collector and Aggregation System

Demonstrates:
- Time-series metrics collection
- Real-time aggregation
- Rolling windows (1min, 5min, 1hour)
- Multiple metric types (counter, gauge, histogram)
- Dashboard display
- Metric persistence with Redis

Usage:
    # Start collector/dashboard
    python metrics_collector_app.py dashboard

    # Simulate metrics
    python metrics_collector_app.py simulate --duration 60

    # Record custom metrics
    python metrics_collector_app.py record counter api.requests 1
    python metrics_collector_app.py record gauge memory.usage 75.5
"""

import argparse
import json
import time
import random
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from collections import defaultdict
import threading

try:
    from optimized_redis import OptimizedRedis as Redis
except ImportError:
    try:
        from redis_wrapper import HighPerformanceRedis as Redis
    except ImportError:
        print("Error: Could not import CyRedis client")
        sys.exit(1)


class MetricsCollector:
    """Real-time metrics collection and aggregation."""

    def __init__(self, host='localhost', port=6379):
        self.redis = Redis(host=host, port=port)
        self.metrics_key_prefix = "metrics:"
        self.aggregation_key_prefix = "metrics:agg:"
        self.metadata_key = "metrics:metadata"

    def record_counter(self, name: str, value: float = 1.0, tags: Optional[Dict] = None):
        """Record a counter metric (cumulative)."""
        timestamp = int(time.time())
        metric_key = f"{self.metrics_key_prefix}counter:{name}"

        # Increment counter
        self.redis.incrbyfloat(metric_key, value)

        # Store metadata
        self._store_metadata(name, "counter", tags)

        # Add to time series (for graphing)
        ts_key = f"{metric_key}:ts"
        self.redis.zadd(ts_key, {f"{timestamp}:{value}": timestamp})

        # Cleanup old time series data (keep last hour)
        cutoff = timestamp - 3600
        self.redis.zremrangebyscore(ts_key, 0, cutoff)

    def record_gauge(self, name: str, value: float, tags: Optional[Dict] = None):
        """Record a gauge metric (point-in-time value)."""
        timestamp = int(time.time())
        metric_key = f"{self.metrics_key_prefix}gauge:{name}"

        # Set current value
        self.redis.set(metric_key, value)

        # Store metadata
        self._store_metadata(name, "gauge", tags)

        # Add to time series
        ts_key = f"{metric_key}:ts"
        self.redis.zadd(ts_key, {f"{timestamp}:{value}": timestamp})

        # Cleanup old data
        cutoff = timestamp - 3600
        self.redis.zremrangebyscore(ts_key, 0, cutoff)

    def record_histogram(self, name: str, value: float, tags: Optional[Dict] = None):
        """Record a histogram metric (for percentiles)."""
        timestamp = int(time.time())
        metric_key = f"{self.metrics_key_prefix}histogram:{name}"

        # Add to sorted set (values as scores for percentile calculation)
        self.redis.zadd(metric_key, {f"{timestamp}:{value}": value})

        # Store metadata
        self._store_metadata(name, "histogram", tags)

        # Cleanup old data
        cutoff_time = timestamp - 3600
        # Remove by timestamp (this is approximate cleanup)
        members = self.redis.zrangebyscore(metric_key, 0, cutoff_time)
        if members:
            self.redis.zrem(metric_key, *members)

    def _store_metadata(self, name: str, metric_type: str, tags: Optional[Dict]):
        """Store metric metadata."""
        metadata = {
            "name": name,
            "type": metric_type,
            "tags": tags or {},
            "last_updated": datetime.now().isoformat(),
        }
        self.redis.hset(self.metadata_key, name, json.dumps(metadata))

    def get_counter_value(self, name: str) -> float:
        """Get current counter value."""
        metric_key = f"{self.metrics_key_prefix}counter:{name}"
        value = self.redis.get(metric_key)
        return float(value) if value else 0.0

    def get_gauge_value(self, name: str) -> float:
        """Get current gauge value."""
        metric_key = f"{self.metrics_key_prefix}gauge:{name}"
        value = self.redis.get(metric_key)
        return float(value) if value else 0.0

    def get_histogram_stats(self, name: str) -> Dict[str, float]:
        """Get histogram statistics."""
        metric_key = f"{self.metrics_key_prefix}histogram:{name}"

        # Get all values
        members = self.redis.zrange(metric_key, 0, -1, withscores=True)

        if not members:
            return {"count": 0}

        values = [score for _, score in members]
        values.sort()

        count = len(values)
        total = sum(values)

        return {
            "count": count,
            "sum": total,
            "mean": total / count,
            "min": values[0],
            "max": values[-1],
            "p50": values[int(count * 0.50)],
            "p95": values[int(count * 0.95)] if count > 20 else values[-1],
            "p99": values[int(count * 0.99)] if count > 100 else values[-1],
        }

    def get_time_series(self, name: str, metric_type: str, duration: int = 300) -> List[tuple]:
        """Get time series data for the last N seconds."""
        metric_key = f"{self.metrics_key_prefix}{metric_type}:{name}:ts"
        cutoff = int(time.time()) - duration

        # Get data points from sorted set
        members = self.redis.zrangebyscore(metric_key, cutoff, "+inf", withscores=True)

        # Parse timestamp:value format
        data_points = []
        for member, score in members:
            if isinstance(member, bytes):
                member = member.decode('utf-8')

            parts = member.split(':')
            if len(parts) == 2:
                timestamp = int(parts[0])
                value = float(parts[1])
                data_points.append((timestamp, value))

        return sorted(data_points)

    def get_all_metrics(self) -> Dict[str, Any]:
        """Get all metrics metadata."""
        metadata = self.redis.hgetall(self.metadata_key)
        metrics = {}

        for name, data in metadata.items():
            if isinstance(name, bytes):
                name = name.decode('utf-8')
            if isinstance(data, bytes):
                data = data.decode('utf-8')

            metrics[name] = json.loads(data)

        return metrics

    def aggregate_window(self, name: str, metric_type: str, window_seconds: int) -> Dict[str, float]:
        """Aggregate metrics over a time window."""
        data_points = self.get_time_series(name, metric_type, window_seconds)

        if not data_points:
            return {}

        values = [v for _, v in data_points]

        if metric_type == "counter":
            # For counters, sum the increments
            return {"sum": sum(values), "count": len(values)}

        elif metric_type == "gauge":
            # For gauges, get latest, min, max, avg
            return {
                "latest": values[-1],
                "min": min(values),
                "max": max(values),
                "avg": sum(values) / len(values),
            }

        return {}


class MetricsDashboard:
    """Real-time metrics dashboard."""

    def __init__(self):
        self.collector = MetricsCollector()
        self.running = True

    def display(self):
        """Display metrics dashboard."""
        import os

        while self.running:
            try:
                # Clear screen
                os.system('clear' if os.name != 'nt' else 'cls')

                print("=" * 80)
                print(" 📊 METRICS DASHBOARD ".center(80))
                print("=" * 80)
                print(f"Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                print("=" * 80)

                # Get all metrics
                metrics = self.collector.get_all_metrics()

                if not metrics:
                    print("\nNo metrics available yet...")
                else:
                    # Group by type
                    counters = []
                    gauges = []
                    histograms = []

                    for name, meta in metrics.items():
                        if meta["type"] == "counter":
                            counters.append(name)
                        elif meta["type"] == "gauge":
                            gauges.append(name)
                        elif meta["type"] == "histogram":
                            histograms.append(name)

                    # Display counters
                    if counters:
                        print("\n📈 COUNTERS")
                        print("-" * 80)
                        for name in counters:
                            value = self.collector.get_counter_value(name)
                            agg = self.collector.aggregate_window(name, "counter", 60)
                            rate = agg.get("sum", 0) / 60 if agg else 0
                            print(f"  {name:<40} Total: {value:>12.2f}  Rate: {rate:>8.2f}/s")

                    # Display gauges
                    if gauges:
                        print("\n📊 GAUGES")
                        print("-" * 80)
                        for name in gauges:
                            value = self.collector.get_gauge_value(name)
                            agg = self.collector.aggregate_window(name, "gauge", 60)
                            min_val = agg.get("min", 0)
                            max_val = agg.get("max", 0)
                            avg_val = agg.get("avg", 0)
                            print(
                                f"  {name:<40} "
                                f"Current: {value:>8.2f}  "
                                f"Min: {min_val:>8.2f}  "
                                f"Max: {max_val:>8.2f}  "
                                f"Avg: {avg_val:>8.2f}"
                            )

                    # Display histograms
                    if histograms:
                        print("\n📉 HISTOGRAMS")
                        print("-" * 80)
                        for name in histograms:
                            stats = self.collector.get_histogram_stats(name)
                            if stats.get("count", 0) > 0:
                                print(
                                    f"  {name:<40} "
                                    f"Count: {stats['count']:>6}  "
                                    f"Mean: {stats['mean']:>8.2f}  "
                                    f"P95: {stats['p95']:>8.2f}  "
                                    f"P99: {stats['p99']:>8.2f}"
                                )

                print("\n" + "=" * 80)
                print("Press Ctrl+C to exit")

                time.sleep(1)

            except KeyboardInterrupt:
                self.running = False
                print("\n\nDashboard stopped")
                break
            except Exception as e:
                print(f"\nError: {e}")
                time.sleep(1)


def simulate_metrics(duration: int = 60):
    """Simulate metrics generation."""
    collector = MetricsCollector()

    print(f"Simulating metrics for {duration} seconds...")
    start_time = time.time()

    while time.time() - start_time < duration:
        try:
            # Simulate API requests
            collector.record_counter("api.requests", random.randint(1, 10))

            # Simulate response times
            response_time = random.uniform(10, 500)
            collector.record_histogram("api.response_time", response_time)

            # Simulate system metrics
            cpu_usage = random.uniform(10, 90)
            collector.record_gauge("system.cpu_usage", cpu_usage)

            memory_usage = random.uniform(30, 80)
            collector.record_gauge("system.memory_usage", memory_usage)

            # Simulate error rate
            if random.random() < 0.1:  # 10% error rate
                collector.record_counter("api.errors", 1)

            # Simulate active connections
            active_connections = random.randint(10, 100)
            collector.record_gauge("connections.active", active_connections)

            time.sleep(0.1)  # 10 samples per second

        except KeyboardInterrupt:
            print("\nStopped simulation")
            break
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(1)

    print("Simulation complete")


def main():
    parser = argparse.ArgumentParser(description="Metrics Collector System")
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # Dashboard command
    subparsers.add_parser("dashboard", help="Display metrics dashboard")

    # Simulate command
    simulate_parser = subparsers.add_parser("simulate", help="Simulate metrics")
    simulate_parser.add_argument("--duration", type=int, default=60, help="Duration in seconds")

    # Record command
    record_parser = subparsers.add_parser("record", help="Record a metric")
    record_parser.add_argument("type", choices=["counter", "gauge", "histogram"], help="Metric type")
    record_parser.add_argument("name", help="Metric name")
    record_parser.add_argument("value", type=float, help="Metric value")

    args = parser.parse_args()

    if args.command == "dashboard":
        dashboard = MetricsDashboard()
        dashboard.display()

    elif args.command == "simulate":
        simulate_metrics(args.duration)

    elif args.command == "record":
        collector = MetricsCollector()

        if args.type == "counter":
            collector.record_counter(args.name, args.value)
            print(f"✓ Recorded counter {args.name} = {args.value}")

        elif args.type == "gauge":
            collector.record_gauge(args.name, args.value)
            print(f"✓ Recorded gauge {args.name} = {args.value}")

        elif args.type == "histogram":
            collector.record_histogram(args.name, args.value)
            print(f"✓ Recorded histogram {args.name} = {args.value}")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
