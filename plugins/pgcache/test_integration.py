#!/usr/bin/env python3
"""
Integration tests for pgcache module with PostgreSQL.

These tests require:
1. Running PostgreSQL instance
2. Redis instance with pgcache module loaded
3. Proper database setup
"""

import redis
import psycopg2
import json
import time
import sys
import os
from typing import Dict, List, Any

class PGCcacheIntegrationTester:
    """Integration test suite for pgcache + PostgreSQL"""

    def __init__(self):
        self.redis_client = None
        self.pg_connection = None
        self.test_db = "pgcache_test"
        self.test_table = "test_users"

    def setup_connections(self):
        """Set up Redis and PostgreSQL connections"""
        # Redis setup
        try:
            self.redis_client = redis.Redis(
                host='localhost',
                port=6379,
                decode_responses=True
            )
            self.redis_client.ping()
            print("✅ Redis connection established")
        except Exception as e:
            print(f"❌ Redis connection failed: {e}")
            return False

        # PostgreSQL setup
        try:
            self.pg_connection = psycopg2.connect(
                host='localhost',
                port=5432,
                database='postgres',  # Connect to postgres to create test DB
                user='postgres',
                password=''
            )
            self.pg_connection.autocommit = True
            print("✅ PostgreSQL connection established")
            self.setup_test_database()
            return True
        except Exception as e:
            print(f"❌ PostgreSQL connection failed: {e}")
            return False

    def setup_test_database(self):
        """Create test database and tables"""
        try:
            cursor = self.pg_connection.cursor()

            # Create test database
            cursor.execute(f"DROP DATABASE IF EXISTS {self.test_db}")
            cursor.execute(f"CREATE DATABASE {self.test_db}")
            print(f"✅ Created test database: {self.test_db}")

            # Connect to test database
            self.pg_connection.close()
            self.pg_connection = psycopg2.connect(
                host='localhost',
                port=5432,
                database=self.test_db,
                user='postgres',
                password=''
            )
            self.pg_connection.autocommit = True

            # Create test table
            cursor = self.pg_connection.cursor()
            cursor.execute(f"""
                CREATE TABLE {self.test_table} (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    email VARCHAR(100) UNIQUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Insert test data
            cursor.execute(f"""
                INSERT INTO {self.test_table} (name, email) VALUES
                ('Alice', 'alice@example.com'),
                ('Bob', 'bob@example.com'),
                ('Charlie', 'charlie@example.com')
            """)

            print(f"✅ Created test table: {self.test_table}")
            return True

        except Exception as e:
            print(f"❌ Database setup failed: {e}")
            return False

    def configure_pgcache(self):
        """Configure pgcache for testing"""
        config_commands = [
            "PGCACHE.SETUP.CONNECTIONPOOL", "3",
            "PGCACHE.SETUP.OUTBOX", "1",
            "PGCACHE.SETUP.NOTIFICATIONS", "1",
            "PGCACHE.SETUP.WATCHFORWARDING", "1"
        ]

        for cmd in config_commands:
            result = self.redis_client.execute_command(cmd)
            if result != "OK":
                print(f"❌ Failed to configure: {cmd}")
                return False

        print("✅ pgcache configured for testing")
        return True

    def test_cache_operations(self):
        """Test basic cache read/write operations"""
        print("\n🧪 Testing cache operations...")

        # Test cache write
        test_data = json.dumps({"name": "Test User", "email": "test@example.com"})
        result = self.redis_client.execute_command(
            "PGCACHE.WRITE", self.test_table, '{"id": 1}', test_data, "300"
        )
        if result != "OK":
            print(f"❌ Cache write failed: {result}")
            return False

        # Test cache read
        cached_data = self.redis_client.execute_command(
            "PGCACHE.READ", self.test_table, '{"id": 1}'
        )
        if not cached_data:
            print("❌ Cache read failed: no data returned")
            return False

        # Verify data matches
        if cached_data != test_data:
            print(f"❌ Cache data mismatch: expected {test_data}, got {cached_data}")
            return False

        print("✅ Cache operations working correctly")
        return True

    def test_cache_invalidation(self):
        """Test cache invalidation via notifications"""
        print("\n🧪 Testing cache invalidation...")

        # Set up a watch for the test table
        self.redis_client.execute_command(
            "PGCACHE.WATCH.ADD", self.test_table, "id:*", "test_notifications", "test_stream"
        )

        # Update data in PostgreSQL
        cursor = self.pg_connection.cursor()
        cursor.execute(f"""
            UPDATE {self.test_table}
            SET name = 'Updated Name', email = 'updated@example.com'
            WHERE id = 1
        """)

        # Wait a bit for notification processing
        time.sleep(1)

        # Check if cache was invalidated
        cache_key = f"pg_cache:{self.test_table}:{{\"id\": 1}}"
        cached_value = self.redis_client.get(cache_key)

        if cached_value:
            print("❌ Cache invalidation failed: data still cached")
            return False

        print("✅ Cache invalidation working correctly")
        return True

    def test_watch_forwarding(self):
        """Test watch forwarding to streams"""
        print("\n🧪 Testing watch forwarding...")

        # Check initial stream length
        streams_info = json.loads(self.redis_client.execute_command("PGCACHE.STREAMS.INFO"))
        initial_watch_length = streams_info.get('watch_length', 0)

        # Trigger a watch event
        self.redis_client.execute_command("PGCACHE.WATCH", "test_watch_key")

        # Check if stream length increased
        streams_info = json.loads(self.redis_client.execute_command("PGCACHE.STREAMS.INFO"))
        new_watch_length = streams_info.get('watch_length', 0)

        if new_watch_length <= initial_watch_length:
            print("❌ Watch forwarding failed: no new entries in stream")
            return False

        print("✅ Watch forwarding working correctly")
        return True

    def test_performance_metrics(self):
        """Test performance monitoring"""
        print("\n🧪 Testing performance metrics...")

        # Get initial metrics
        initial_metrics = json.loads(self.redis_client.execute_command("PGCACHE.METRICS"))

        # Perform some operations
        for i in range(5):
            self.redis_client.execute_command("PGCACHE.READ", self.test_table, f'{{"id": {i+1}}}')

        # Check updated metrics
        updated_metrics = json.loads(self.redis_client.execute_command("PGCACHE.METRICS"))

        # Verify queries executed increased
        if updated_metrics['queries_executed'] <= initial_metrics['queries_executed']:
            print("❌ Performance metrics not updating correctly")
            return False

        print("✅ Performance metrics working correctly")
        return True

    def test_transaction_support(self):
        """Test transaction functionality"""
        print("\n🧪 Testing transaction support...")

        # Test transaction status
        tx_status = json.loads(self.redis_client.execute_command("PGCACHE.TRANSACTION.STATUS"))
        if tx_status.get('in_transaction', False):
            print("❌ Unexpected transaction state")
            return False

        print("✅ Transaction support available")
        return True

    def cleanup(self):
        """Clean up test environment"""
        try:
            if self.pg_connection:
                cursor = self.pg_connection.cursor()
                cursor.execute(f"DROP DATABASE IF EXISTS {self.test_db}")
                self.pg_connection.close()

            if self.redis_client:
                self.redis_client.execute_command("FLUSHALL")

            print("✅ Test environment cleaned up")
        except Exception as e:
            print(f"⚠️ Cleanup warning: {e}")

    def run_integration_tests(self):
        """Run all integration tests"""
        print("🚀 Starting pgcache integration tests...")

        if not self.setup_connections():
            print("❌ Connection setup failed")
            return False

        if not self.configure_pgcache():
            print("❌ pgcache configuration failed")
            return False

        tests = [
            self.test_cache_operations,
            self.test_cache_invalidation,
            self.test_watch_forwarding,
            self.test_performance_metrics,
            self.test_transaction_support
        ]

        passed = 0
        total = len(tests)

        for test in tests:
            try:
                if test():
                    passed += 1
                else:
                    print(f"❌ Test failed: {test.__name__}")
            except Exception as e:
                print(f"❌ Test error in {test.__name__}: {e}")

        print("
📊 Integration Test Results:"        print(f"Passed: {passed}/{total}")
        print(f"Failed: {total - passed}")

        if passed == total:
            print("🎉 All integration tests passed!")
            return True
        else:
            print("❌ Some integration tests failed")
            return False

def main():
    """Main integration test runner"""
    tester = PGCcacheIntegrationTester()

    try:
        success = tester.run_integration_tests()
    finally:
        tester.cleanup()

    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
