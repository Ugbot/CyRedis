#!/usr/bin/env python3
"""
Enhanced CyRedis Demo - Showcasing all new features

This demo script demonstrates the comprehensive Redis features now available
in CyRedis, including bitmap operations, Bloom filters, JSON operations,
full-text search, geospatial operations, time series, and advanced operations.
"""

import asyncio
import json
import time
from typing import Dict, List, Any

# Import the enhanced CyRedis client
from cy_redis import CyRedisClient, CyRedisClientAsync

def demo_bitmap_operations():
    """Demonstrate bitmap operations"""
    print("🗺️  Bitmap Operations Demo")
    print("=" * 50)

    with CyRedisClient() as client:
        # Create a bitmap representing user activity (1 = active, 0 = inactive)
        user_bitmap = "user_activity"

        # Set bits for active users (users 1, 3, 5, 7 are active)
        client.setbit(user_bitmap, 1, 1)  # User 1 active
        client.setbit(user_bitmap, 3, 1)  # User 3 active
        client.setbit(user_bitmap, 5, 1)  # User 5 active
        client.setbit(user_bitmap, 7, 1)  # User 7 active

        # Count active users
        active_count = client.bitcount(user_bitmap)
        print(f"Active users: {active_count}")

        # Check if specific users are active
        user_1_active = client.getbit(user_bitmap, 1)
        user_2_active = client.getbit(user_bitmap, 2)
        print(f"User 1 active: {user_1_active}")
        print(f"User 2 active: {user_2_active}")

        # Find first active user
        first_active = client.bitpos(user_bitmap, 1)
        print(f"First active user: {first_active}")

        # Create another bitmap for premium users
        premium_bitmap = "premium_users"
        client.setbit(premium_bitmap, 1, 1)  # User 1 is premium
        client.setbit(premium_bitmap, 7, 1)  # User 7 is premium

        # Find users who are both active and premium (bitwise AND)
        active_premium = "active_premium"
        client.bitop("AND", active_premium, user_bitmap, premium_bitmap)

        premium_active_count = client.bitcount(active_premium)
        print(f"Active premium users: {premium_active_count}")

def demo_bloom_filters():
    """Demonstrate Bloom filter operations"""
    print("\n🌸 Bloom Filter Operations Demo")
    print("=" * 50)

    with CyRedisClient() as client:
        # Create a Bloom filter for email addresses
        email_filter = "email_bloom"

        # Reserve Bloom filter with 0.01% error rate and capacity for 1000 emails
        client.bf_reserve(email_filter, 0.01, 1000)

        # Add some email addresses
        emails = [
            "user1@example.com",
            "user2@example.com",
            "user3@example.com",
            "admin@company.com"
        ]

        for email in emails:
            client.bf_add(email_filter, email)

        # Check if emails exist in the filter
        test_emails = [
            "user1@example.com",  # Should exist
            "user4@example.com",  # Should not exist
            "admin@company.com"   # Should exist
        ]

        for email in test_emails:
            exists = client.bf_exists(email_filter, email)
            status = "EXISTS" if exists else "NOT FOUND"
            print(f"Email '{email}': {status}")

        # Get filter information
        info = client.bf_info(email_filter)
        print(f"Bloom filter info: {info}")

def demo_json_operations():
    """Demonstrate JSON operations"""
    print("\n📄 JSON Operations Demo")
    print("=" * 50)

    with CyRedisClient() as client:
        # Create a complex JSON document
        user_profile = {
            "id": 12345,
            "name": "Alice Johnson",
            "email": "alice@example.com",
            "preferences": {
                "theme": "dark",
                "notifications": True,
                "language": "en"
            },
            "tags": ["premium", "verified", "active"],
            "last_login": time.time()
        }

        # Set the entire JSON document
        client.json_set("user:12345", ".", user_profile)

        # Get specific fields
        user_name = client.json_get("user:12345", ".name")
        user_theme = client.json_get("user:12345", ".preferences.theme")
        print(f"User name: {user_name}")
        print(f"User theme: {user_theme}")

        # Modify specific fields
        client.json_set("user:12345", ".preferences.theme", "light")
        client.json_set("user:12345", ".last_login", time.time())

        # Add to array
        client.json_arrappend("user:12345", ".tags", "vip")

        # Get updated tags
        updated_tags = client.json_get("user:12345", ".tags")
        print(f"Updated tags: {updated_tags}")

        # Get multiple users' data
        client.json_set("user:67890", ".", {
            "id": 67890,
            "name": "Bob Smith",
            "email": "bob@example.com"
        })

        users_data = client.json_mget(["user:12345", "user:67890"], ".name")
        print(f"User names: {users_data}")

def demo_geospatial_operations():
    """Demonstrate geospatial operations"""
    print("\n🗺️  Geospatial Operations Demo")
    print("=" * 50)

    with CyRedisClient() as client:
        # Add some locations (longitude, latitude, member)
        locations = [
            (-122.4194, 37.7749, "San Francisco"),  # San Francisco, CA
            (-118.2437, 34.0522, "Los Angeles"),    # Los Angeles, CA
            (-74.0060, 40.7128, "New York"),       # New York, NY
            (-87.6298, 41.8781, "Chicago"),        # Chicago, IL
            (-71.0589, 42.3601, "Boston")          # Boston, MA
        ]

        for lon, lat, city in locations:
            client.geoadd("cities", lon, lat, city)

        # Find distance between cities
        sf_nyc_distance = client.geodist("cities", "San Francisco", "New York", "km")
        print(f"Distance SF to NYC: {sf_nyc_distance:.2f} km")

        la_sf_distance = client.geodist("cities", "Los Angeles", "San Francisco", "mi")
        print(f"Distance LA to SF: {la_sf_distance:.2f} miles")

        # Find cities within 1000km of Chicago
        nearby_cities = client.georadius("cities", -87.6298, 41.8781, 1000, "km", count=5)
        print(f"Cities within 1000km of Chicago: {nearby_cities}")

        # Get geohash for cities
        geohashes = client.geohash("cities", "San Francisco", "New York")
        print(f"Geohashes: {geohashes}")

def demo_time_series_operations():
    """Demonstrate time series operations"""
    print("\n⏰ Time Series Operations Demo")
    print("=" * 50)

    with CyRedisClient() as client:
        # Create a time series for temperature readings
        temp_series = "sensor:temperature"

        # Create the time series with retention and labels
        client.ts_create(temp_series, retention=86400, labels={
            "sensor_id": "temp_001",
            "location": "office",
            "unit": "celsius"
        })

        # Add some temperature readings
        current_time = int(time.time() * 1000)  # Milliseconds
        temperatures = [
            (current_time - 300000, 22.5),  # 5 minutes ago
            (current_time - 240000, 23.1),  # 4 minutes ago
            (current_time - 180000, 22.8),  # 3 minutes ago
            (current_time - 120000, 23.5),  # 2 minutes ago
            (current_time - 60000, 24.2),   # 1 minute ago
            (current_time, 23.9)            # Now
        ]

        for timestamp, temp in temperatures:
            client.ts_add(temp_series, timestamp, temp)

        # Get the latest reading
        latest = client.ts_get(temp_series, latest=True)
        print(f"Latest temperature: {latest}")

        # Get temperature range for the last 10 minutes
        ten_min_ago = current_time - 600000
        temp_range = client.ts_range(temp_series, ten_min_ago, current_time)
        print(f"Temperature readings (last 10 min): {len(temp_range)} samples")

        # Get time series info
        info = client.ts_info(temp_series)
        print(f"Time series info: {info}")

def demo_full_text_search():
    """Demonstrate full-text search operations"""
    print("\n🔍 Full-Text Search Demo")
    print("=" * 50)

    with CyRedisClient() as client:
        # Create a search index for articles
        index_name = "articles_idx"

        # Define schema for articles
        schema = [
            {"field": "title", "type": "TEXT"},
            {"field": "content", "type": "TEXT"},
            {"field": "author", "type": "TEXT"},
            {"field": "category", "type": "TAG"},
            {"field": "published_date", "type": "NUMERIC"}
        ]

        # Create the search index
        client.ft_create(index_name, schema, {
            "prefix": ["article:"],
            "default_score": 1.0
        })

        # Add some articles
        articles = [
            {
                "title": "Redis Performance Tuning",
                "content": "Learn how to optimize Redis for maximum performance in production environments.",
                "author": "John Doe",
                "category": "database",
                "published_date": 20231201
            },
            {
                "title": "Python Async Best Practices",
                "content": "Essential patterns for writing efficient asynchronous Python applications.",
                "author": "Jane Smith",
                "category": "programming",
                "published_date": 20231202
            },
            {
                "title": "Database Indexing Strategies",
                "content": "Comprehensive guide to creating effective database indexes for optimal query performance.",
                "author": "Bob Johnson",
                "category": "database",
                "published_date": 20231203
            }
        ]

        for i, article in enumerate(articles):
            key = f"article:{i+1}"
            client.json_set(key, ".", article)

        # Search for articles about databases
        db_results = client.ft_search(index_name, "@category:database")
        print(f"Database articles found: {db_results['total']}")

        # Search for articles containing "performance"
        perf_results = client.ft_search(index_name, "performance")
        print(f"Performance articles found: {perf_results['total']}")

        # Get index information
        index_info = client.ft_info(index_name)
        print(f"Index info: {index_info}")

async def demo_async_operations():
    """Demonstrate async operations"""
    print("\n⚡ Async Operations Demo")
    print("=" * 50)

    async with CyRedisClientAsync() as client:
        # Demonstrate async JSON operations
        user_data = {
            "id": 999,
            "name": "Async User",
            "preferences": {"theme": "async"}
        }

        await client.json_set_async("async_user", ".", user_data)

        user = await client.json_get_async("async_user", ".")
        print(f"Async user: {user}")

        # Demonstrate async geospatial operations
        await client.geoadd_async("async_cities", -122.4194, 37.7749, "Async SF")

        distance = await client.geodist_async("async_cities", "Async SF", "San Francisco")
        print(f"Async distance: {distance}")

def demo_advanced_hash_operations():
    """Demonstrate advanced hash operations"""
    print("\n🎯 Advanced Hash Operations Demo")
    print("=" * 50)

    with CyRedisClient() as client:
        # Set hash fields with expiration
        client.hsetex("session:123", "user_id", "456", 3600)  # Expires in 1 hour
        client.hsetex("session:123", "token", "abc123", 3600)

        # Set field-level expiration
        client.hexpire("session:123", 7200, ["user_id"])  # user_id expires in 2 hours

        # Get all fields as dictionary
        session_data = client.hgetall_dict("session:123")
        print(f"Session data: {session_data}")

        # Get string length of field
        user_id_length = client.hstrlen("session:123", "user_id")
        print(f"User ID length: {user_id_length}")

        # Increment float field
        client.hincrbyfloat("user:456", "balance", 10.50)
        balance = client.hincrbyfloat("user:456", "balance", -5.25)
        print(f"Updated balance: {balance}")

        # Get random fields
        random_fields = client.hrandfield("user:456", 2, withvalues=True)
        print(f"Random fields: {random_fields}")

def main():
    """Run all demonstrations"""
    print("🚀 Enhanced CyRedis Feature Demo")
    print("=" * 60)

    try:
        # Run synchronous demos
        demo_bitmap_operations()
        demo_bloom_filters()
        demo_json_operations()
        demo_geospatial_operations()
        demo_time_series_operations()
        demo_full_text_search()
        demo_advanced_hash_operations()

        # Run async demo
        asyncio.run(demo_async_operations())

        print("\n🎉 All demos completed successfully!")
        print("\nCyRedis now supports:")
        print("  • Bitmap operations (SETBIT, GETBIT, BITCOUNT, etc.)")
        print("  • Bloom filters (BF.RESERVE, BF.ADD, BF.EXISTS, etc.)")
        print("  • JSON operations (JSON.SET, JSON.GET, JSON.ARRAPPEND, etc.)")
        print("  • Full-text search (FT.CREATE, FT.SEARCH, etc.)")
        print("  • Geospatial operations (GEOADD, GEODIST, GEORADIUS, etc.)")
        print("  • Time series (TS.CREATE, TS.ADD, TS.RANGE, etc.)")
        print("  • Advanced hash operations (HSETEX, HEXPIRE, HINCRBYFLOAT, etc.)")
        print("  • Async versions of all operations")

    except Exception as e:
        print(f"\n❌ Demo failed: {e}")
        print("Make sure Redis is running and the required modules are loaded.")
        return 1

    return 0

if __name__ == "__main__":
    exit(main())
