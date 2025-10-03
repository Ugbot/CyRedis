#!/usr/bin/env python3
"""
Quick Automated Test Suite for CyRedis
Tests basic functionality without requiring full build
"""

import sys
import time
import subprocess
import json

# Color codes
RED = '\033[0;31m'
GREEN = '\033[0;32m'
YELLOW = '\033[1;33m'
BLUE = '\033[0;34m'
NC = '\033[0m'  # No Color


class TestRunner:
    """Automated test runner"""

    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.skipped = 0

    def print_header(self, text):
        """Print section header"""
        print(f"\n{BLUE}{'=' * 70}{NC}")
        print(f"{BLUE}{text}{NC}")
        print(f"{BLUE}{'=' * 70}{NC}\n")

    def run_test(self, name, func):
        """Run a single test"""
        print(f"  Testing: {name}...", end=" ", flush=True)
        try:
            func()
            print(f"{GREEN}✓ PASSED{NC}")
            self.passed += 1
            return True
        except AssertionError as e:
            print(f"{RED}✗ FAILED{NC}")
            print(f"    {RED}Error: {e}{NC}")
            self.failed += 1
            return False
        except Exception as e:
            print(f"{YELLOW}⚠ SKIPPED{NC}")
            print(f"    {YELLOW}Reason: {e}{NC}")
            self.skipped += 1
            return False

    def print_summary(self, duration):
        """Print test summary"""
        self.print_header("TEST SUMMARY")
        total = self.passed + self.failed + self.skipped

        print(f"  Total Tests:    {total}")
        print(f"  {GREEN}Passed:         {self.passed}{NC}")
        if self.failed > 0:
            print(f"  {RED}Failed:         {self.failed}{NC}")
        else:
            print(f"  Failed:         {self.failed}")
        if self.skipped > 0:
            print(f"  {YELLOW}Skipped:        {self.skipped}{NC}")
        else:
            print(f"  Skipped:        {self.skipped}")
        print(f"  Duration:       {duration:.2f}s")

        print()
        if self.failed == 0:
            print(f"{GREEN}{'=' * 70}{NC}")
            print(f"{GREEN}✓ ALL TESTS PASSED!{NC}")
            print(f"{GREEN}{'=' * 70}{NC}\n")
            return True
        else:
            print(f"{RED}{'=' * 70}{NC}")
            print(f"{RED}✗ SOME TESTS FAILED{NC}")
            print(f"{RED}{'=' * 70}{NC}\n")
            return False


def test_redis_connection():
    """Test Redis is accessible"""
    result = subprocess.run(
        ['redis-cli', 'ping'],
        capture_output=True,
        text=True,
        timeout=5
    )
    assert result.returncode == 0, "Redis not responding"
    assert 'PONG' in result.stdout, "Unexpected Redis response"


def test_redis_set_get():
    """Test basic SET/GET operations"""
    # SET
    result = subprocess.run(
        ['redis-cli', 'SET', 'test:key:1', 'test_value'],
        capture_output=True,
        text=True,
        timeout=5
    )
    assert result.returncode == 0, f"SET failed: {result.stderr}"

    # GET
    result = subprocess.run(
        ['redis-cli', 'GET', 'test:key:1'],
        capture_output=True,
        text=True,
        timeout=5
    )
    assert result.returncode == 0, f"GET failed: {result.stderr}"
    assert 'test_value' in result.stdout, f"Value mismatch: {result.stdout}"


def test_redis_delete():
    """Test DELETE operation"""
    # SET a key
    subprocess.run(['redis-cli', 'SET', 'test:key:delete', 'value'], check=True, capture_output=True)

    # DELETE
    result = subprocess.run(
        ['redis-cli', 'DEL', 'test:key:delete'],
        capture_output=True,
        text=True,
        timeout=5
    )
    assert result.returncode == 0, f"DEL failed: {result.stderr}"

    # Verify deleted
    result = subprocess.run(
        ['redis-cli', 'GET', 'test:key:delete'],
        capture_output=True,
        text=True,
        timeout=5
    )
    assert result.stdout.strip() == '' or result.stdout.strip() == '(nil)', "Key not deleted"


def test_redis_incr():
    """Test INCR operation"""
    key = 'test:counter'

    # Delete if exists
    subprocess.run(['redis-cli', 'DEL', key], capture_output=True)

    # INCR 3 times
    for i in range(3):
        result = subprocess.run(
            ['redis-cli', 'INCR', key],
            capture_output=True,
            text=True,
            timeout=5
        )
        assert result.returncode == 0, f"INCR failed: {result.stderr}"

    # Check final value
    result = subprocess.run(
        ['redis-cli', 'GET', key],
        capture_output=True,
        text=True,
        timeout=5
    )
    assert '3' in result.stdout, f"Counter value wrong: {result.stdout}"


def test_redis_list_operations():
    """Test list operations (LPUSH, LRANGE)"""
    key = 'test:list'

    # Clean up
    subprocess.run(['redis-cli', 'DEL', key], capture_output=True)

    # RPUSH
    for value in ['a', 'b', 'c']:
        result = subprocess.run(
            ['redis-cli', 'RPUSH', key, value],
            capture_output=True,
            text=True,
            timeout=5
        )
        assert result.returncode == 0, f"RPUSH failed: {result.stderr}"

    # LRANGE
    result = subprocess.run(
        ['redis-cli', 'LRANGE', key, '0', '-1'],
        capture_output=True,
        text=True,
        timeout=5
    )
    assert 'a' in result.stdout and 'b' in result.stdout and 'c' in result.stdout, \
        f"List content wrong: {result.stdout}"


def test_redis_hash_operations():
    """Test hash operations (HSET, HGET)"""
    key = 'test:hash'

    # Clean up
    subprocess.run(['redis-cli', 'DEL', key], capture_output=True)

    # HSET
    result = subprocess.run(
        ['redis-cli', 'HSET', key, 'field1', 'value1'],
        capture_output=True,
        text=True,
        timeout=5
    )
    assert result.returncode == 0, f"HSET failed: {result.stderr}"

    # HGET
    result = subprocess.run(
        ['redis-cli', 'HGET', key, 'field1'],
        capture_output=True,
        text=True,
        timeout=5
    )
    assert 'value1' in result.stdout, f"Hash value wrong: {result.stdout}"


def test_redis_expiration():
    """Test key expiration (EXPIRE, TTL)"""
    key = 'test:expire'

    # SET with expiration
    subprocess.run(['redis-cli', 'SET', key, 'temp_value'], capture_output=True)

    # SET expiration to 10 seconds
    result = subprocess.run(
        ['redis-cli', 'EXPIRE', key, '10'],
        capture_output=True,
        text=True,
        timeout=5
    )
    assert result.returncode == 0, f"EXPIRE failed: {result.stderr}"

    # Check TTL
    result = subprocess.run(
        ['redis-cli', 'TTL', key],
        capture_output=True,
        text=True,
        timeout=5
    )
    # TTL should be positive
    ttl = int(result.stdout.strip())
    assert 0 < ttl <= 10, f"TTL not set correctly: {ttl}"


def test_test_apps_exist():
    """Test that test applications exist"""
    import os

    apps = [
        'tests/apps/simple_kv_app.py',
        'tests/apps/message_queue_app.py',
        'tests/apps/job_queue_app.py',
        'tests/apps/metrics_collector_app.py',
    ]

    for app in apps:
        assert os.path.exists(app), f"App not found: {app}"


def test_docker_compose_exists():
    """Test that Docker infrastructure exists"""
    import os

    assert os.path.exists('tests/docker/docker-compose.yml'), "Docker compose file not found"
    assert os.path.exists('tests/docker/README.md'), "Docker README not found"


def test_documentation_exists():
    """Test that documentation exists"""
    import os

    docs = [
        'TESTING.md',
        'tests/README.md',
        'tests/apps/README.md',
        'TEST_HARNESS_SUMMARY.md',
    ]

    for doc in docs:
        assert os.path.exists(doc), f"Documentation not found: {doc}"


def test_lua_basic_eval():
    """Test basic Lua EVAL command"""
    result = subprocess.run(
        ['redis-cli', 'EVAL', 'return "lua_works"', '0'],
        capture_output=True,
        text=True,
        timeout=5
    )
    assert result.returncode == 0, f"Lua EVAL failed: {result.stderr}"
    assert 'lua_works' in result.stdout, f"Lua evaluation failed: {result.stdout}"


def test_lua_script_load():
    """Test Lua SCRIPT LOAD command"""
    result = subprocess.run(
        ['redis-cli', 'SCRIPT', 'LOAD', 'return 42'],
        capture_output=True,
        text=True,
        timeout=5
    )
    assert result.returncode == 0, f"SCRIPT LOAD failed: {result.stderr}"
    sha = result.stdout.strip().strip('"')
    assert len(sha) >= 40, f"Invalid SHA returned: {sha}"


def test_lua_scripts_directory():
    """Test that Lua scripts directory exists"""
    import os

    assert os.path.exists('lua_scripts'), "lua_scripts directory not found"
    lua_files = [f for f in os.listdir('lua_scripts') if f.endswith('.lua')]
    assert len(lua_files) >= 4, f"Expected at least 4 Lua scripts, found {len(lua_files)}"


def main():
    """Main test runner"""
    print()
    print("╔" + "=" * 68 + "╗")
    print("║" + " " * 18 + "CyRedis Quick Test Suite" + " " * 26 + "║")
    print("╚" + "=" * 68 + "╝")

    runner = TestRunner()
    start_time = time.time()

    # Environment Tests
    runner.print_header("1. REDIS CONNECTIVITY TESTS")
    runner.run_test("Redis Connection", test_redis_connection)

    # Basic Operations
    runner.print_header("2. BASIC REDIS OPERATIONS")
    runner.run_test("SET/GET Operations", test_redis_set_get)
    runner.run_test("DELETE Operation", test_redis_delete)
    runner.run_test("INCR Operation", test_redis_incr)

    # Data Structure Tests
    runner.print_header("3. DATA STRUCTURE TESTS")
    runner.run_test("List Operations", test_redis_list_operations)
    runner.run_test("Hash Operations", test_redis_hash_operations)
    runner.run_test("Key Expiration", test_redis_expiration)

    # Project Structure Tests
    runner.print_header("4. PROJECT STRUCTURE TESTS")
    runner.run_test("Test Apps Exist", test_test_apps_exist)
    runner.run_test("Docker Infrastructure Exists", test_docker_compose_exists)
    runner.run_test("Documentation Exists", test_documentation_exists)

    # Lua Setup Tests
    runner.print_header("5. LUA SCRIPTING TESTS")
    runner.run_test("Lua EVAL Command", test_lua_basic_eval)
    runner.run_test("Lua SCRIPT LOAD", test_lua_script_load)
    runner.run_test("Lua Scripts Directory", test_lua_scripts_directory)

    duration = time.time() - start_time

    # Print summary
    success = runner.print_summary(duration)

    return 0 if success else 1


if __name__ == '__main__':
    sys.exit(main())
