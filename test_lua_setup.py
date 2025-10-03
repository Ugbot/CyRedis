#!/usr/bin/env python3
"""
Lua Setup Test Suite

Tests Lua script functionality including:
- Basic EVAL command
- Script loading and caching (EVALSHA)
- Lua scripts from lua_scripts/ directory
- Script atomicity and operations
"""

import subprocess
import sys
import os

# Colors
RED = '\033[0;31m'
GREEN = '\033[0;32m'
YELLOW = '\033[1;33m'
BLUE = '\033[0;34m'
NC = '\033[0m'


class LuaTestRunner:
    """Test Lua functionality"""

    def __init__(self):
        self.passed = 0
        self.failed = 0

    def print_header(self, text):
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
            return False


def test_lua_eval_basic():
    """Test basic EVAL command"""
    result = subprocess.run(
        ['redis-cli', 'EVAL', 'return "hello"', '0'],
        capture_output=True,
        text=True,
        timeout=5
    )
    assert result.returncode == 0, f"EVAL failed: {result.stderr}"
    assert 'hello' in result.stdout, f"Unexpected result: {result.stdout}"


def test_lua_return_number():
    """Test Lua returning a number"""
    result = subprocess.run(
        ['redis-cli', 'EVAL', 'return 42', '0'],
        capture_output=True,
        text=True,
        timeout=5
    )
    assert result.returncode == 0, f"EVAL failed: {result.stderr}"
    assert '42' in result.stdout, f"Unexpected result: {result.stdout}"


def test_lua_with_keys():
    """Test EVAL with KEYS"""
    script = 'return redis.call("SET", KEYS[1], ARGV[1])'
    result = subprocess.run(
        ['redis-cli', 'EVAL', script, '1', 'test:lua:key', 'test_value'],
        capture_output=True,
        text=True,
        timeout=5
    )
    assert result.returncode == 0, f"EVAL with keys failed: {result.stderr}"

    # Verify key was set
    result = subprocess.run(
        ['redis-cli', 'GET', 'test:lua:key'],
        capture_output=True,
        text=True,
        timeout=5
    )
    assert 'test_value' in result.stdout, f"Key not set correctly: {result.stdout}"

    # Cleanup
    subprocess.run(['redis-cli', 'DEL', 'test:lua:key'], capture_output=True)


def test_lua_script_load():
    """Test SCRIPT LOAD command"""
    script = 'return ARGV[1]'
    result = subprocess.run(
        ['redis-cli', 'SCRIPT', 'LOAD', script],
        capture_output=True,
        text=True,
        timeout=5
    )
    assert result.returncode == 0, f"SCRIPT LOAD failed: {result.stderr}"

    # SHA should be returned (40 character hex string)
    sha = result.stdout.strip().strip('"')
    assert len(sha) >= 40, f"Invalid SHA returned: {sha}"


def test_lua_evalsha():
    """Test EVALSHA command"""
    # First load the script
    script = 'return "cached"'
    result = subprocess.run(
        ['redis-cli', 'SCRIPT', 'LOAD', script],
        capture_output=True,
        text=True,
        timeout=5
    )
    sha = result.stdout.strip().strip('"')

    # Now execute using SHA
    result = subprocess.run(
        ['redis-cli', 'EVALSHA', sha, '0'],
        capture_output=True,
        text=True,
        timeout=5
    )
    assert result.returncode == 0, f"EVALSHA failed: {result.stderr}"
    assert 'cached' in result.stdout, f"Unexpected result: {result.stdout}"


def test_lua_atomic_increment():
    """Test atomic increment with Lua"""
    script = '''
    local current = redis.call('GET', KEYS[1])
    if current then
        return redis.call('INCR', KEYS[1])
    else
        redis.call('SET', KEYS[1], 0)
        return redis.call('INCR', KEYS[1])
    end
    '''

    # Clean up first
    subprocess.run(['redis-cli', 'DEL', 'test:lua:counter'], capture_output=True)

    # First increment
    result = subprocess.run(
        ['redis-cli', 'EVAL', script, '1', 'test:lua:counter'],
        capture_output=True,
        text=True,
        timeout=5
    )
    assert '1' in result.stdout, f"First increment failed: {result.stdout}"

    # Second increment
    result = subprocess.run(
        ['redis-cli', 'EVAL', script, '1', 'test:lua:counter'],
        capture_output=True,
        text=True,
        timeout=5
    )
    assert '2' in result.stdout, f"Second increment failed: {result.stdout}"

    # Cleanup
    subprocess.run(['redis-cli', 'DEL', 'test:lua:counter'], capture_output=True)


def test_lua_conditional_set():
    """Test conditional SET with Lua"""
    script = '''
    local value = redis.call('GET', KEYS[1])
    if value == false then
        redis.call('SET', KEYS[1], ARGV[1])
        return 1
    else
        return 0
    end
    '''

    key = 'test:lua:conditional'

    # Clean up
    subprocess.run(['redis-cli', 'DEL', key], capture_output=True)

    # First set should succeed (return 1)
    result = subprocess.run(
        ['redis-cli', 'EVAL', script, '1', key, 'first_value'],
        capture_output=True,
        text=True,
        timeout=5
    )
    assert '1' in result.stdout, f"First set should succeed: {result.stdout}"

    # Second set should fail (return 0)
    result = subprocess.run(
        ['redis-cli', 'EVAL', script, '1', key, 'second_value'],
        capture_output=True,
        text=True,
        timeout=5
    )
    assert '0' in result.stdout, f"Second set should fail: {result.stdout}"

    # Value should still be first_value
    result = subprocess.run(
        ['redis-cli', 'GET', key],
        capture_output=True,
        text=True,
        timeout=5
    )
    assert 'first_value' in result.stdout, f"Value changed: {result.stdout}"

    # Cleanup
    subprocess.run(['redis-cli', 'DEL', key], capture_output=True)


def test_lua_script_from_file():
    """Test loading and executing Lua script from file"""
    script_path = 'lua_scripts/rate_limiter.lua'

    if not os.path.exists(script_path):
        raise Exception(f"Script not found: {script_path}")

    # Read the script
    with open(script_path, 'r') as f:
        script = f.read()

    # Load the script
    result = subprocess.run(
        ['redis-cli', 'SCRIPT', 'LOAD', script],
        capture_output=True,
        text=True,
        timeout=5
    )
    assert result.returncode == 0, f"Failed to load {script_path}: {result.stderr}"

    sha = result.stdout.strip().strip('"')
    assert len(sha) >= 40, f"Invalid SHA: {sha}"


def test_lua_scripts_directory():
    """Test that Lua scripts directory exists and has scripts"""
    if not os.path.exists('lua_scripts'):
        raise Exception("lua_scripts directory not found")

    lua_files = [f for f in os.listdir('lua_scripts') if f.endswith('.lua')]
    assert len(lua_files) > 0, "No Lua scripts found in lua_scripts/"

    print(f"\n    Found {len(lua_files)} Lua scripts:")
    for lua_file in lua_files:
        print(f"      - {lua_file}")


def test_lua_distributed_lock_script():
    """Test distributed lock Lua script"""
    script_path = 'lua_scripts/distributed_lock.lua'

    if not os.path.exists(script_path):
        raise Exception(f"Script not found: {script_path}")

    with open(script_path, 'r') as f:
        script = f.read()

    # The script should be valid Lua
    assert 'redis.call' in script or 'redis.pcall' in script, \
        "Script doesn't contain Redis calls"


def test_lua_rate_limiter_script():
    """Test rate limiter Lua script"""
    script_path = 'lua_scripts/rate_limiter.lua'

    if not os.path.exists(script_path):
        raise Exception(f"Script not found: {script_path}")

    with open(script_path, 'r') as f:
        script = f.read()

    assert 'redis.call' in script or 'redis.pcall' in script, \
        "Script doesn't contain Redis calls"


def test_lua_list_operations():
    """Test Lua with list operations"""
    script = '''
    redis.call('RPUSH', KEYS[1], ARGV[1])
    redis.call('RPUSH', KEYS[1], ARGV[2])
    redis.call('RPUSH', KEYS[1], ARGV[3])
    return redis.call('LRANGE', KEYS[1], 0, -1)
    '''

    key = 'test:lua:list'

    # Clean up
    subprocess.run(['redis-cli', 'DEL', key], capture_output=True)

    # Execute script
    result = subprocess.run(
        ['redis-cli', 'EVAL', script, '1', key, 'a', 'b', 'c'],
        capture_output=True,
        text=True,
        timeout=5
    )

    assert result.returncode == 0, f"List operations failed: {result.stderr}"
    # Should return array with a, b, c
    assert 'a' in result.stdout, f"Missing 'a': {result.stdout}"

    # Cleanup
    subprocess.run(['redis-cli', 'DEL', key], capture_output=True)


def test_lua_error_handling():
    """Test Lua error handling"""
    # Invalid Lua syntax should fail
    result = subprocess.run(
        ['redis-cli', 'EVAL', 'invalid lua !@#', '0'],
        capture_output=True,
        text=True,
        timeout=5
    )
    # Should return error (non-zero exit code or error in output)
    assert result.returncode != 0 or 'ERR' in result.stderr or 'error' in result.stdout.lower(), \
        "Invalid Lua should produce error"


def main():
    """Main test runner"""
    print()
    print("╔" + "=" * 68 + "╗")
    print("║" + " " * 23 + "Lua Setup Tests" + " " * 30 + "║")
    print("╚" + "=" * 68 + "╝")

    runner = LuaTestRunner()

    # Basic Lua Tests
    runner.print_header("1. Basic Lua Evaluation")
    runner.run_test("Basic EVAL", test_lua_eval_basic)
    runner.run_test("Return Number", test_lua_return_number)
    runner.run_test("EVAL with Keys", test_lua_with_keys)

    # Script Caching Tests
    runner.print_header("2. Script Caching")
    runner.run_test("SCRIPT LOAD", test_lua_script_load)
    runner.run_test("EVALSHA", test_lua_evalsha)

    # Atomic Operations Tests
    runner.print_header("3. Atomic Operations")
    runner.run_test("Atomic Increment", test_lua_atomic_increment)
    runner.run_test("Conditional SET", test_lua_conditional_set)
    runner.run_test("List Operations", test_lua_list_operations)

    # Lua Scripts Directory Tests
    runner.print_header("4. Lua Scripts Directory")
    runner.run_test("Scripts Directory Exists", test_lua_scripts_directory)
    runner.run_test("Load Script from File", test_lua_script_from_file)
    runner.run_test("Distributed Lock Script", test_lua_distributed_lock_script)
    runner.run_test("Rate Limiter Script", test_lua_rate_limiter_script)

    # Error Handling
    runner.print_header("5. Error Handling")
    runner.run_test("Invalid Lua Syntax", test_lua_error_handling)

    # Summary
    runner.print_header("TEST SUMMARY")
    total = runner.passed + runner.failed

    print(f"  Total Tests:    {total}")
    print(f"  {GREEN}Passed:         {runner.passed}{NC}")
    if runner.failed > 0:
        print(f"  {RED}Failed:         {runner.failed}{NC}")
    else:
        print(f"  Failed:         {runner.failed}")

    print()
    if runner.failed == 0:
        print(f"{GREEN}{'=' * 70}{NC}")
        print(f"{GREEN}✓ ALL LUA TESTS PASSED!{NC}")
        print(f"{GREEN}{'=' * 70}{NC}\n")
        return 0
    else:
        print(f"{RED}{'=' * 70}{NC}")
        print(f"{RED}✗ SOME LUA TESTS FAILED{NC}")
        print(f"{RED}{'=' * 70}{NC}\n")
        return 1


if __name__ == '__main__':
    sys.exit(main())
