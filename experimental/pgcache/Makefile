# Makefile for pgcache plugin tests

.PHONY: all test test-unit test-integration test-perf setup clean help

# Default target
all: help

# Help target
help:
	@echo "Available targets:"
	@echo "  setup         - Set up test environment (PostgreSQL + Redis)"
	@echo "  test          - Run all tests (unit + integration + performance)"
	@echo "  test-unit     - Run unit tests only"
	@echo "  test-integration - Run integration tests only (requires PostgreSQL)"
	@echo "  test-perf     - Run performance tests only"
	@echo "  clean         - Clean up test files and results"
	@echo "  build         - Build the pgcache module"
	@echo "  install       - Install required Python packages"

# Build the pgcache module
build:
	@echo "Building pgcache module..."
	@cd .. && make
	@echo "✅ Module built successfully"

# Install Python dependencies
install:
	@echo "Installing Python dependencies..."
	@pip install redis psycopg2-binary
	@echo "✅ Dependencies installed"

# Set up test environment
setup: install build
	@echo "Setting up test environment..."
	@python3 setup_test_environment.py

# Run all tests
test: setup
	@echo "Running complete test suite..."
	@python3 run_tests.py

# Run unit tests only
test-unit: install
	@echo "Running unit tests..."
	@python3 run_tests.py --unit-only

# Run integration tests only
test-integration: install build
	@echo "Running integration tests..."
	@python3 run_tests.py --integration-only

# Run performance tests only
test-perf: install
	@echo "Running performance tests..."
	@python3 test_pgcache.py

# Clean up test files
clean:
	@echo "Cleaning up test files..."
	@rm -f test_results*.json
	@rm -f test_config.json
	@echo "✅ Test files cleaned"

# Show test status
status:
	@echo "Checking test environment status..."
	@python3 -c "
import redis
import psycopg2
try:
    r = redis.Redis(decode_responses=True)
    r.ping()
    print('✅ Redis: Connected')
except:
    print('❌ Redis: Not available')

try:
    pg = psycopg2.connect(host='localhost', database='postgres', user='postgres')
    pg.close()
    print('✅ PostgreSQL: Available')
except:
    print('❌ PostgreSQL: Not available')

# Check if module is loaded
try:
    result = r.execute_command('MODULE', 'LIST')
    if 'pgcache' in str(result).lower():
        print('✅ pgcache module: Loaded')
    else:
        print('❌ pgcache module: Not loaded')
except:
    print('❌ pgcache module: Cannot check')
"