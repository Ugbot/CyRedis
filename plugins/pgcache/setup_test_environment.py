#!/usr/bin/env python3
"""
Setup script for pgcache test environment.

This script helps set up the required environment for testing:
1. PostgreSQL database and tables
2. Redis with pgcache module
3. Test configuration
"""

import subprocess
import sys
import os
import time
import psycopg2
from pathlib import Path

def check_redis_module():
    """Check if pgcache module is loaded in Redis"""
    try:
        result = subprocess.run(
            ['redis-cli', 'MODULE', 'LIST'],
            capture_output=True, text=True, check=True
        )

        if 'pgcache' in result.stdout.lower():
            print("✅ pgcache module is loaded in Redis")
            return True
        else:
            print("❌ pgcache module not found in Redis")
            return False

    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to check Redis modules: {e}")
        return False

def load_redis_module():
    """Load pgcache module into Redis"""
    try:
        # Assuming the module is built and available
        module_path = Path(__file__).parent / "pgcache.so"

        if not module_path.exists():
            print(f"❌ Module file not found: {module_path}")
            return False

        result = subprocess.run(
            ['redis-cli', 'MODULE', 'LOAD', str(module_path)],
            capture_output=True, text=True, check=True
        )

        print("✅ pgcache module loaded successfully")
        return True

    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to load module: {e}")
        return False

def setup_postgresql():
    """Set up PostgreSQL test database"""
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host='localhost',
            port=5432,
            database='postgres',
            user='postgres',
            password=''
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # Create test database
        test_db = "pgcache_test"
        cursor.execute(f"DROP DATABASE IF EXISTS {test_db}")
        cursor.execute(f"CREATE DATABASE {test_db}")

        print(f"✅ Created test database: {test_db}")

        # Connect to test database and create tables
        conn.close()
        conn = psycopg2.connect(
            host='localhost',
            port=5432,
            database=test_db,
            user='postgres',
            password=''
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # Create test tables
        cursor.execute("""
            CREATE TABLE test_users (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                email VARCHAR(100) UNIQUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cursor.execute("""
            CREATE TABLE test_products (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                price DECIMAL(10,2),
                category VARCHAR(50)
            )
        """)

        # Insert test data
        cursor.execute("""
            INSERT INTO test_users (name, email) VALUES
            ('Alice Johnson', 'alice@example.com'),
            ('Bob Smith', 'bob@example.com'),
            ('Charlie Brown', 'charlie@example.com')
        """)

        cursor.execute("""
            INSERT INTO test_products (name, price, category) VALUES
            ('Laptop', 999.99, 'Electronics'),
            ('Book', 19.99, 'Education'),
            ('Coffee Mug', 9.99, 'Kitchen')
        """)

        # Create a function to send notifications for testing
        cursor.execute("""
            CREATE OR REPLACE FUNCTION notify_cache_invalidation()
            RETURNS TRIGGER AS $$
            BEGIN
                PERFORM pg_notify('cache_invalidation',
                    json_build_object(
                        'table', TG_TABLE_NAME,
                        'operation', TG_OP,
                        'old_data', CASE WHEN TG_OP != 'INSERT' THEN row_to_json(OLD) ELSE NULL END,
                        'new_data', CASE WHEN TG_OP != 'DELETE' THEN row_to_json(NEW) ELSE NULL END
                    )::text
                );
                RETURN COALESCE(NEW, OLD);
            END;
            $$ LANGUAGE plpgsql;
        """)

        # Create triggers
        cursor.execute("""
            CREATE TRIGGER users_invalidation_trigger
                AFTER INSERT OR UPDATE OR DELETE ON test_users
                FOR EACH ROW EXECUTE FUNCTION notify_cache_invalidation();
        """)

        cursor.execute("""
            CREATE TRIGGER products_invalidation_trigger
                AFTER INSERT OR UPDATE OR DELETE ON test_products
                FOR EACH ROW EXECUTE FUNCTION notify_cache_invalidation();
        """)

        print("✅ Created test tables with notification triggers")
        conn.close()
        return True

    except Exception as e:
        print(f"❌ PostgreSQL setup failed: {e}")
        return False

def configure_pgcache():
    """Configure pgcache module for testing"""
    try:
        config_commands = [
            "PGCACHE.SETUP.CONNECTIONPOOL 5",
            "PGCACHE.SETUP.OUTBOX 1",
            "PGCACHE.SETUP.NOTIFICATIONS 1",
            "PGCACHE.SETUP.WATCHFORWARDING 1"
        ]

        for cmd in config_commands:
            result = subprocess.run(
                ['redis-cli'] + cmd.split(),
                capture_output=True, text=True, check=True
            )
            if result.returncode != 0:
                print(f"❌ Failed to configure pgcache: {cmd}")
                return False

        print("✅ pgcache configured for testing")
        return True

    except subprocess.CalledProcessError as e:
        print(f"❌ pgcache configuration failed: {e}")
        return False

def create_test_config():
    """Create test configuration file"""
    config = {
        "redis": {
            "host": "localhost",
            "port": 6379
        },
        "postgresql": {
            "host": "localhost",
            "port": 5432,
            "database": "pgcache_test",
            "user": "postgres",
            "password": ""
        },
        "pgcache": {
            "cache_prefix": "test_cache:",
            "default_ttl": 300,
            "pool_size": 5,
            "enable_notifications": True,
            "enable_wal": False,
            "enable_watch_forwarding": True,
            "enable_outbox": True
        }
    }

    import json
    with open('test_config.json', 'w') as f:
        json.dump(config, f, indent=2)

    print("✅ Created test configuration file")
    return True

def main():
    """Main setup function"""
    print("🔧 Setting up pgcache test environment...")

    # Check if pgcache module is loaded
    if not check_redis_module():
        print("📦 Loading pgcache module...")
        if not load_redis_module():
            print("❌ Cannot proceed without pgcache module")
            sys.exit(1)

    # Setup PostgreSQL
    print("🗄️ Setting up PostgreSQL...")
    if not setup_postgresql():
        print("❌ Cannot proceed without PostgreSQL setup")
        sys.exit(1)

    # Configure pgcache
    print("⚙️ Configuring pgcache...")
    if not configure_pgcache():
        print("❌ pgcache configuration failed")
        sys.exit(1)

    # Create test config file
    create_test_config()

    print("\n🎉 Test environment setup complete!")
    print("You can now run the test suites:")
    print("  python test_pgcache.py        # Basic functionality tests")
    print("  python test_integration.py    # Full integration tests")

if __name__ == "__main__":
    main()
