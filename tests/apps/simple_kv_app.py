#!/usr/bin/env python3
"""
Simple Key-Value Store Application

A command-line key-value store demonstrating CyRedis basic operations.
Shows SET, GET, DELETE, EXISTS, and EXPIRE operations with interactive CLI.

Features:
- Basic CRUD operations
- TTL (time-to-live) support
- Key listing and pattern matching
- Interactive command-line interface
- JSON value support

Usage:
    python simple_kv_app.py

    Commands:
        set <key> <value> [ttl]  - Set a key with optional TTL (seconds)
        get <key>                - Get value of a key
        delete <key>             - Delete a key
        exists <key>             - Check if key exists
        ttl <key>                - Get remaining TTL of a key
        keys <pattern>           - List keys matching pattern
        list                     - List all keys
        clear                    - Clear all keys
        quit/exit                - Exit the application
"""

import sys
import argparse
import json
from typing import Optional

try:
    from optimized_redis import OptimizedRedis
except ImportError:
    print("Error: CyRedis not properly installed. Run build_optimized.sh first.")
    sys.exit(1)


class SimpleKVStore:
    """Simple key-value store using CyRedis"""

    def __init__(self, host: str = "localhost", port: int = 6379, prefix: str = "kvapp"):
        """Initialize the KV store

        Args:
            host: Redis host
            port: Redis port
            prefix: Key prefix for namespace isolation
        """
        self.redis = OptimizedRedis(host=host, port=port)
        self.prefix = prefix
        print(f"✓ Connected to Redis at {host}:{port}")
        print(f"✓ Using key prefix: '{prefix}:'")

    def _key(self, key: str) -> str:
        """Add prefix to key"""
        return f"{self.prefix}:{key}"

    def _unkey(self, key: str) -> str:
        """Remove prefix from key"""
        if key.startswith(f"{self.prefix}:"):
            return key[len(self.prefix)+1:]
        return key

    def set(self, key: str, value: str, ttl: Optional[int] = None) -> bool:
        """Set a key-value pair

        Args:
            key: Key name
            value: Value to store
            ttl: Optional time-to-live in seconds

        Returns:
            True if successful
        """
        try:
            redis_key = self._key(key)
            if ttl:
                self.redis.setex(redis_key, ttl, value)
                print(f"✓ Set '{key}' = '{value}' (expires in {ttl}s)")
            else:
                self.redis.set(redis_key, value)
                print(f"✓ Set '{key}' = '{value}'")
            return True
        except Exception as e:
            print(f"✗ Error setting key: {e}")
            return False

    def get(self, key: str) -> Optional[str]:
        """Get value of a key

        Args:
            key: Key name

        Returns:
            Value if exists, None otherwise
        """
        try:
            value = self.redis.get(self._key(key))
            if value is None:
                print(f"✗ Key '{key}' not found")
                return None
            else:
                print(f"✓ '{key}' = '{value}'")
                return value
        except Exception as e:
            print(f"✗ Error getting key: {e}")
            return None

    def delete(self, key: str) -> bool:
        """Delete a key

        Args:
            key: Key name

        Returns:
            True if key was deleted
        """
        try:
            result = self.redis.delete(self._key(key))
            if result > 0:
                print(f"✓ Deleted key '{key}'")
                return True
            else:
                print(f"✗ Key '{key}' not found")
                return False
        except Exception as e:
            print(f"✗ Error deleting key: {e}")
            return False

    def exists(self, key: str) -> bool:
        """Check if key exists

        Args:
            key: Key name

        Returns:
            True if key exists
        """
        try:
            result = self.redis.exists(self._key(key))
            if result:
                print(f"✓ Key '{key}' exists")
            else:
                print(f"✗ Key '{key}' does not exist")
            return bool(result)
        except Exception as e:
            print(f"✗ Error checking key: {e}")
            return False

    def ttl(self, key: str) -> Optional[int]:
        """Get remaining TTL of a key

        Args:
            key: Key name

        Returns:
            TTL in seconds, -1 if no expiry, -2 if key doesn't exist
        """
        try:
            ttl = self.redis.ttl(self._key(key))
            if ttl == -2:
                print(f"✗ Key '{key}' does not exist")
            elif ttl == -1:
                print(f"✓ Key '{key}' has no expiration")
            else:
                print(f"✓ Key '{key}' expires in {ttl} seconds")
            return ttl
        except Exception as e:
            print(f"✗ Error getting TTL: {e}")
            return None

    def keys(self, pattern: str = "*") -> list:
        """List keys matching pattern

        Args:
            pattern: Redis pattern (default: *)

        Returns:
            List of matching keys
        """
        try:
            redis_pattern = self._key(pattern)
            keys = self.redis.keys(redis_pattern)
            keys = [self._unkey(k.decode() if isinstance(k, bytes) else k) for k in keys]

            if keys:
                print(f"✓ Found {len(keys)} key(s):")
                for k in sorted(keys):
                    print(f"  - {k}")
            else:
                print(f"✗ No keys found matching '{pattern}'")

            return keys
        except Exception as e:
            print(f"✗ Error listing keys: {e}")
            return []

    def clear(self) -> bool:
        """Clear all keys with prefix

        Returns:
            True if successful
        """
        try:
            keys = self.redis.keys(self._key("*"))
            if keys:
                count = self.redis.delete(*keys)
                print(f"✓ Deleted {count} key(s)")
                return True
            else:
                print("✓ No keys to delete")
                return True
        except Exception as e:
            print(f"✗ Error clearing keys: {e}")
            return False

    def close(self):
        """Close Redis connection"""
        self.redis.close()
        print("✓ Connection closed")


def interactive_mode(store: SimpleKVStore):
    """Run interactive command-line interface"""
    print("\n" + "="*60)
    print("Simple Key-Value Store - Interactive Mode")
    print("="*60)
    print("Type 'help' for available commands or 'quit' to exit")
    print()

    while True:
        try:
            command = input("kvstore> ").strip()

            if not command:
                continue

            parts = command.split(None, 3)
            cmd = parts[0].lower()

            if cmd in ('quit', 'exit'):
                break

            elif cmd == 'help':
                print("\nAvailable commands:")
                print("  set <key> <value> [ttl]  - Set a key with optional TTL")
                print("  get <key>                - Get value of a key")
                print("  delete <key>             - Delete a key")
                print("  exists <key>             - Check if key exists")
                print("  ttl <key>                - Get remaining TTL")
                print("  keys [pattern]           - List keys (default: *)")
                print("  list                     - List all keys")
                print("  clear                    - Clear all keys")
                print("  help                     - Show this help")
                print("  quit/exit                - Exit")
                print()

            elif cmd == 'set':
                if len(parts) < 3:
                    print("Usage: set <key> <value> [ttl]")
                else:
                    key = parts[1]
                    value = parts[2]
                    ttl = int(parts[3]) if len(parts) > 3 else None
                    store.set(key, value, ttl)

            elif cmd == 'get':
                if len(parts) < 2:
                    print("Usage: get <key>")
                else:
                    store.get(parts[1])

            elif cmd == 'delete':
                if len(parts) < 2:
                    print("Usage: delete <key>")
                else:
                    store.delete(parts[1])

            elif cmd == 'exists':
                if len(parts) < 2:
                    print("Usage: exists <key>")
                else:
                    store.exists(parts[1])

            elif cmd == 'ttl':
                if len(parts) < 2:
                    print("Usage: ttl <key>")
                else:
                    store.ttl(parts[1])

            elif cmd == 'keys':
                pattern = parts[1] if len(parts) > 1 else "*"
                store.keys(pattern)

            elif cmd == 'list':
                store.keys("*")

            elif cmd == 'clear':
                confirm = input("Are you sure you want to clear all keys? (yes/no): ")
                if confirm.lower() == 'yes':
                    store.clear()

            else:
                print(f"Unknown command: {cmd}. Type 'help' for available commands.")

        except KeyboardInterrupt:
            print("\nUse 'quit' or 'exit' to exit")
        except Exception as e:
            print(f"Error: {e}")


def demo_mode(store: SimpleKVStore):
    """Run demonstration of features"""
    print("\n" + "="*60)
    print("Simple Key-Value Store - Demo Mode")
    print("="*60)
    print()

    print("1. Setting some key-value pairs...")
    store.set("user:1:name", "Alice")
    store.set("user:1:email", "alice@example.com")
    store.set("user:2:name", "Bob")
    store.set("session:abc123", "active", ttl=30)
    print()

    print("2. Getting values...")
    store.get("user:1:name")
    store.get("user:2:name")
    store.get("session:abc123")
    print()

    print("3. Checking key existence...")
    store.exists("user:1:name")
    store.exists("user:99:name")
    print()

    print("4. Checking TTL...")
    store.ttl("session:abc123")
    store.ttl("user:1:name")
    print()

    print("5. Listing keys...")
    store.keys("user:*")
    store.keys("session:*")
    print()

    print("6. Deleting a key...")
    store.delete("user:2:name")
    store.keys("user:*")
    print()

    print("Demo complete!")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Simple Key-Value Store using CyRedis",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--host", default="localhost", help="Redis host (default: localhost)")
    parser.add_argument("--port", type=int, default=6379, help="Redis port (default: 6379)")
    parser.add_argument("--prefix", default="kvapp", help="Key prefix (default: kvapp)")
    parser.add_argument("--demo", action="store_true", help="Run demo mode instead of interactive")

    args = parser.parse_args()

    try:
        store = SimpleKVStore(host=args.host, port=args.port, prefix=args.prefix)

        if args.demo:
            demo_mode(store)
        else:
            interactive_mode(store)

        store.close()

    except KeyboardInterrupt:
        print("\nExiting...")
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
