#!/usr/bin/env python3
"""
Rate Limiting Service

A distributed rate limiter demonstrating CyRedis for API rate limiting:
- Token bucket algorithm
- Sliding window rate limiting
- Per-user and per-IP limits
- Multiple limit tiers
- Rate limit analytics

Features:
- Multiple rate limiting algorithms
- Configurable time windows
- Burst handling
- Rate limit headers
- Usage analytics
- Auto-expiring counters

Usage:
    # Check rate limit
    python rate_limiter_app.py check --key user:123

    # Simulate requests
    python rate_limiter_app.py simulate --key api:test --count 100

    # Show statistics
    python rate_limiter_app.py stats --key user:123

    # Demo mode
    python rate_limiter_app.py demo
"""

import sys
import argparse
import time
import json
from typing import Dict, Any, Optional, Tuple
from datetime import datetime
from enum import Enum

try:
    from optimized_redis import OptimizedRedis
except ImportError:
    print("Error: CyRedis not properly installed. Run build_optimized.sh first.")
    sys.exit(1)


class RateLimitAlgorithm(Enum):
    """Rate limiting algorithms"""
    FIXED_WINDOW = "fixed_window"
    SLIDING_WINDOW = "sliding_window"
    TOKEN_BUCKET = "token_bucket"


class RateLimiter:
    """Distributed rate limiter using Redis"""

    def __init__(self, redis: OptimizedRedis, namespace: str = "ratelimit"):
        """Initialize rate limiter

        Args:
            redis: Redis client instance
            namespace: Namespace for rate limit keys
        """
        self.redis = redis
        self.namespace = namespace

    def _key(self, identifier: str) -> str:
        """Generate rate limit key"""
        return f"{self.namespace}:{identifier}"

    def check_rate_limit_fixed_window(self, identifier: str, limit: int,
                                       window: int) -> Tuple[bool, Dict[str, Any]]:
        """Check rate limit using fixed window algorithm

        Args:
            identifier: Unique identifier (user ID, IP, API key, etc.)
            limit: Maximum requests per window
            window: Time window in seconds

        Returns:
            Tuple of (allowed, info_dict)
        """
        key = self._key(identifier)
        current_time = int(time.time())
        window_start = current_time - (current_time % window)

        # Use window start as part of key to auto-expire old windows
        window_key = f"{key}:{window_start}"

        # Increment counter
        count = self.redis.incr(window_key)

        # Set expiration on first request of window
        if count == 1:
            self.redis.expire(window_key, window * 2)

        allowed = count <= limit
        remaining = max(0, limit - count)
        reset_time = window_start + window

        return allowed, {
            "limit": limit,
            "remaining": remaining,
            "reset": reset_time,
            "reset_dt": datetime.fromtimestamp(reset_time),
            "current_count": count
        }

    def check_rate_limit_sliding_window(self, identifier: str, limit: int,
                                         window: int) -> Tuple[bool, Dict[str, Any]]:
        """Check rate limit using sliding window algorithm

        Args:
            identifier: Unique identifier
            limit: Maximum requests per window
            window: Time window in seconds

        Returns:
            Tuple of (allowed, info_dict)
        """
        key = self._key(f"sliding:{identifier}")
        current_time = time.time()
        window_start = current_time - window

        # Remove old entries
        self.redis.zremrangebyscore(key, "-inf", window_start)

        # Count requests in current window
        count = self.redis.zcard(key)

        allowed = count < limit

        if allowed:
            # Add current request
            self.redis.zadd(key, {str(current_time): current_time})
            count += 1

            # Set expiration
            self.redis.expire(key, window)

        remaining = max(0, limit - count)

        # Calculate reset time (when oldest request expires)
        oldest = self.redis.zrange(key, 0, 0, withscores=True)
        if oldest:
            reset_time = oldest[0][1] + window
        else:
            reset_time = current_time + window

        return allowed, {
            "limit": limit,
            "remaining": remaining,
            "reset": int(reset_time),
            "reset_dt": datetime.fromtimestamp(reset_time),
            "current_count": count
        }

    def check_rate_limit_token_bucket(self, identifier: str, capacity: int,
                                       refill_rate: int, refill_period: int = 1) -> Tuple[bool, Dict[str, Any]]:
        """Check rate limit using token bucket algorithm

        Args:
            identifier: Unique identifier
            capacity: Bucket capacity
            refill_rate: Tokens added per refill period
            refill_period: Refill period in seconds

        Returns:
            Tuple of (allowed, info_dict)
        """
        key = self._key(f"bucket:{identifier}")
        current_time = time.time()

        # Get bucket state
        bucket_data = self.redis.get(key)

        if bucket_data:
            bucket = json.loads(bucket_data)
            tokens = bucket["tokens"]
            last_refill = bucket["last_refill"]
        else:
            tokens = capacity
            last_refill = current_time

        # Refill tokens based on elapsed time
        elapsed = current_time - last_refill
        refills = int(elapsed / refill_period)

        if refills > 0:
            tokens = min(capacity, tokens + (refills * refill_rate))
            last_refill = current_time

        # Try to consume a token
        allowed = tokens >= 1

        if allowed:
            tokens -= 1

        # Save bucket state
        bucket = {
            "tokens": tokens,
            "last_refill": last_refill
        }
        self.redis.setex(key, 3600, json.dumps(bucket))  # Expire after 1 hour

        return allowed, {
            "capacity": capacity,
            "remaining": int(tokens),
            "refill_rate": f"{refill_rate} tokens per {refill_period}s",
            "current_tokens": tokens
        }

    def get_usage_stats(self, identifier: str, algorithm: str = "fixed_window") -> Dict[str, Any]:
        """Get usage statistics for an identifier

        Args:
            identifier: Unique identifier
            algorithm: Algorithm type

        Returns:
            Usage statistics
        """
        if algorithm == "sliding_window":
            key = self._key(f"sliding:{identifier}")
            total_requests = self.redis.zcard(key)

            # Get request timestamps
            requests = self.redis.zrange(key, 0, -1, withscores=True)
            timestamps = [score for _, score in requests]

            return {
                "total_requests": total_requests,
                "timestamps": timestamps,
                "algorithm": algorithm
            }
        elif algorithm == "token_bucket":
            key = self._key(f"bucket:{identifier}")
            bucket_data = self.redis.get(key)

            if bucket_data:
                bucket = json.loads(bucket_data)
                return {
                    "tokens_remaining": bucket["tokens"],
                    "last_refill": datetime.fromtimestamp(bucket["last_refill"]),
                    "algorithm": algorithm
                }

        return {"algorithm": algorithm, "no_data": True}


class RateLimiterApp:
    """Rate limiter application"""

    def __init__(self, host: str = "localhost", port: int = 6379):
        """Initialize rate limiter app

        Args:
            host: Redis host
            port: Redis port
        """
        self.redis = OptimizedRedis(host=host, port=port)
        self.limiter = RateLimiter(self.redis)
        print(f"✓ Connected to Redis at {host}:{port}")

    def check_limit(self, key: str, algorithm: str = "fixed_window"):
        """Check rate limit for a key"""
        if algorithm == "fixed_window":
            allowed, info = self.limiter.check_rate_limit_fixed_window(key, limit=10, window=60)
        elif algorithm == "sliding_window":
            allowed, info = self.limiter.check_rate_limit_sliding_window(key, limit=10, window=60)
        elif algorithm == "token_bucket":
            allowed, info = self.limiter.check_rate_limit_token_bucket(key, capacity=10, refill_rate=1, refill_period=6)
        else:
            print(f"Unknown algorithm: {algorithm}")
            return

        status = "✓ ALLOWED" if allowed else "✗ RATE LIMITED"
        print(f"\n{status}")
        print(f"Identifier: {key}")
        for k, v in info.items():
            print(f"  {k}: {v}")

    def simulate_requests(self, key: str, count: int = 100, delay: float = 0.1,
                          algorithm: str = "fixed_window"):
        """Simulate multiple requests"""
        print(f"\n🔄 Simulating {count} requests for '{key}' ({algorithm})")
        print("="*60)

        allowed_count = 0
        denied_count = 0

        for i in range(count):
            if algorithm == "fixed_window":
                allowed, info = self.limiter.check_rate_limit_fixed_window(key, limit=10, window=10)
            elif algorithm == "sliding_window":
                allowed, info = self.limiter.check_rate_limit_sliding_window(key, limit=10, window=10)
            else:  # token_bucket
                allowed, info = self.limiter.check_rate_limit_token_bucket(key, capacity=10, refill_rate=2, refill_period=1)

            if allowed:
                allowed_count += 1
                status = "✓"
            else:
                denied_count += 1
                status = "✗"

            print(f"Request {i+1:3d}: {status} (remaining: {info.get('remaining', info.get('current_tokens', 0))})")

            time.sleep(delay)

        print(f"\n📊 Results:")
        print(f"  Allowed: {allowed_count}")
        print(f"  Denied:  {denied_count}")
        print(f"  Rate:    {(allowed_count/count)*100:.1f}%")

    def demo(self):
        """Run demonstration of all algorithms"""
        print("\n" + "="*60)
        print("Rate Limiter Demo")
        print("="*60)

        # Demo 1: Fixed Window
        print("\n1. Fixed Window Rate Limiting (10 req/10s)")
        print("-"*60)
        for i in range(15):
            allowed, info = self.limiter.check_rate_limit_fixed_window(
                "demo:fixed", limit=10, window=10
            )
            status = "✓" if allowed else "✗"
            print(f"Request {i+1:2d}: {status} (remaining: {info['remaining']})")
            time.sleep(0.2)

        time.sleep(2)

        # Demo 2: Sliding Window
        print("\n2. Sliding Window Rate Limiting (5 req/5s)")
        print("-"*60)
        for i in range(10):
            allowed, info = self.limiter.check_rate_limit_sliding_window(
                "demo:sliding", limit=5, window=5
            )
            status = "✓" if allowed else "✗"
            print(f"Request {i+1:2d}: {status} (remaining: {info['remaining']})")
            time.sleep(0.5)

        time.sleep(2)

        # Demo 3: Token Bucket
        print("\n3. Token Bucket Rate Limiting (cap:5, refill:1/sec)")
        print("-"*60)
        for i in range(15):
            allowed, info = self.limiter.check_rate_limit_token_bucket(
                "demo:bucket", capacity=5, refill_rate=1, refill_period=1
            )
            status = "✓" if allowed else "✗"
            tokens = info.get('current_tokens', 0)
            print(f"Request {i+1:2d}: {status} (tokens: {tokens:.1f})")
            time.sleep(0.5)

        print("\n✓ Demo complete")

    def show_stats(self, key: str, algorithm: str = "sliding_window"):
        """Show usage statistics"""
        print(f"\n📊 Usage Statistics for '{key}'")
        print("="*60)

        stats = self.limiter.get_usage_stats(key, algorithm)
        print(json.dumps(stats, indent=2, default=str))

    def close(self):
        """Close Redis connection"""
        self.redis.close()


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Rate Limiting Service")
    subparsers = parser.add_subparsers(dest="command")

    # Check command
    check_parser = subparsers.add_parser("check", help="Check rate limit")
    check_parser.add_argument("--key", required=True, help="Identifier")
    check_parser.add_argument("--algo", default="fixed_window",
                              choices=["fixed_window", "sliding_window", "token_bucket"])

    # Simulate command
    sim_parser = subparsers.add_parser("simulate", help="Simulate requests")
    sim_parser.add_argument("--key", required=True, help="Identifier")
    sim_parser.add_argument("--count", type=int, default=50, help="Number of requests")
    sim_parser.add_argument("--delay", type=float, default=0.1, help="Delay between requests")
    sim_parser.add_argument("--algo", default="fixed_window",
                            choices=["fixed_window", "sliding_window", "token_bucket"])

    # Stats command
    stats_parser = subparsers.add_parser("stats", help="Show statistics")
    stats_parser.add_argument("--key", required=True, help="Identifier")
    stats_parser.add_argument("--algo", default="sliding_window")

    # Demo command
    subparsers.add_parser("demo", help="Run demo")

    parser.add_argument("--host", default="localhost", help="Redis host")
    parser.add_argument("--port", type=int, default=6379, help="Redis port")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    try:
        app = RateLimiterApp(host=args.host, port=args.port)

        if args.command == "check":
            app.check_limit(args.key, args.algo)

        elif args.command == "simulate":
            app.simulate_requests(args.key, args.count, args.delay, args.algo)

        elif args.command == "stats":
            app.show_stats(args.key, args.algo)

        elif args.command == "demo":
            app.demo()

        app.close()

    except KeyboardInterrupt:
        print("\nExiting...")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
