#!/bin/bash

###############################################################################
# CyRedis Automated Test Suite Runner
#
# This script runs a comprehensive test suite with progress reporting
###############################################################################

set -e  # Exit on error

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Test results tracking
TESTS_PASSED=0
TESTS_FAILED=0
START_TIME=$(date +%s)

echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║       CyRedis Automated Test Suite                            ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

# Function to print section header
print_section() {
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
}

# Function to run a test and track results
run_test() {
    local test_name="$1"
    local test_command="$2"

    echo -e "${YELLOW}Running: ${test_name}${NC}"

    if eval "$test_command" > /tmp/test_output_$$.log 2>&1; then
        echo -e "${GREEN}✓ PASSED: ${test_name}${NC}"
        TESTS_PASSED=$((TESTS_PASSED + 1))
        return 0
    else
        echo -e "${RED}✗ FAILED: ${test_name}${NC}"
        echo -e "${RED}Output:${NC}"
        tail -20 /tmp/test_output_$$.log | sed 's/^/  /'
        TESTS_FAILED=$((TESTS_FAILED + 1))
        return 1
    fi
}

# 1. Environment Check
print_section "1. Environment Check"

echo -n "  Checking Redis connectivity... "
if redis-cli ping > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC}"
else
    echo -e "${RED}✗ Redis is not running${NC}"
    echo "  Please start Redis: redis-server"
    exit 1
fi

echo -n "  Checking Python installation... "
if python3 --version > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} $(python3 --version)"
else
    echo -e "${RED}✗${NC}"
    exit 1
fi

echo -n "  Checking pytest installation... "
if python3 -c "import pytest" 2>/dev/null; then
    echo -e "${GREEN}✓${NC}"
else
    echo -e "${YELLOW}⚠ Installing pytest...${NC}"
    pip install pytest pytest-asyncio pytest-cov > /dev/null 2>&1 || true
fi

echo -n "  Checking CyRedis modules... "
if ls cy_redis/*.so > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} ($(ls cy_redis/*.so | wc -l | tr -d ' ') modules)"
else
    echo -e "${RED}✗ Please build: bash build_optimized.sh${NC}"
    exit 1
fi

# 2. Clean Redis data
print_section "2. Preparing Test Environment"

echo "  Flushing Redis test data..."
redis-cli FLUSHDB > /dev/null 2>&1 || true
echo -e "  ${GREEN}✓ Redis cleaned${NC}"

# 3. Run Unit Tests
print_section "3. Unit Tests"

if [ -d "tests/unit" ]; then
    run_test "Redis Core Tests" "pytest tests/unit/test_redis_core.py -v --tb=short -x"
    run_test "Connection Pool Tests" "pytest tests/unit/test_connection_pool.py -v --tb=short -x"
    run_test "Messaging Tests" "pytest tests/unit/test_messaging.py -v --tb=short -x"
else
    echo -e "${YELLOW}⚠ No unit tests directory found${NC}"
fi

# 4. Run Integration Tests (subset)
print_section "4. Integration Tests (Quick)"

if [ -d "tests/integration" ]; then
    run_test "Basic Operations" "pytest tests/integration/test_basic_operations.py -v --tb=short -x" || true
    run_test "Concurrent Access" "pytest tests/integration/test_concurrent_access.py::TestConcurrentOperations::test_concurrent_sets -v --tb=short" || true
    run_test "Streaming Operations" "pytest tests/integration/test_streaming.py::TestRedisStreams::test_xadd_basic -v --tb=short" || true
    run_test "Lua Scripts" "pytest tests/integration/test_lua_scripts.py::TestLuaScripts::test_eval_basic -v --tb=short" || true
else
    echo -e "${YELLOW}⚠ No integration tests directory found${NC}"
fi

# 5. Test Applications (quick smoke tests)
print_section "5. Application Tests (Smoke)"

if [ -d "tests/apps" ]; then
    echo "  Testing simple_kv_app..."
    if python3 tests/apps/simple_kv_app.py set test_key "test_value" > /dev/null 2>&1 && \
       python3 tests/apps/simple_kv_app.py get test_key > /dev/null 2>&1; then
        echo -e "  ${GREEN}✓ simple_kv_app works${NC}"
        TESTS_PASSED=$((TESTS_PASSED + 1))
    else
        echo -e "  ${RED}✗ simple_kv_app failed${NC}"
        TESTS_FAILED=$((TESTS_FAILED + 1))
    fi
else
    echo -e "${YELLOW}⚠ No test apps directory found${NC}"
fi

# 6. Performance Quick Test
print_section "6. Performance Benchmark (Quick)"

if [ -f "tests/integration/test_performance.py" ]; then
    run_test "Basic Performance" "pytest tests/integration/test_performance.py::TestPerformanceBasic::test_set_throughput -v --tb=short" || true
else
    echo -e "${YELLOW}⚠ Performance tests not found${NC}"
fi

# 7. Summary
print_section "7. Test Summary"

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

echo ""
echo "  Total Tests Passed: ${GREEN}${TESTS_PASSED}${NC}"
echo "  Total Tests Failed: ${RED}${TESTS_FAILED}${NC}"
echo "  Duration: ${DURATION}s"
echo ""

# Clean up
rm -f /tmp/test_output_$$.log

if [ $TESTS_FAILED -eq 0 ]; then
    echo -e "${GREEN}╔════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║  ✓ ALL TESTS PASSED!                                          ║${NC}"
    echo -e "${GREEN}╚════════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    exit 0
else
    echo -e "${RED}╔════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${RED}║  ✗ SOME TESTS FAILED                                          ║${NC}"
    echo -e "${RED}╚════════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    echo "To debug failures, run specific tests:"
    echo "  pytest tests/unit/test_redis_core.py -v"
    echo "  pytest tests/integration/test_basic_operations.py -v"
    echo ""
    exit 1
fi
