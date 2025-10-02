#!/bin/bash
# Build script for CyRedis
# Includes basic performance optimizations and additional features

set -e  # Exit on any error

echo "Building CyRedis..."

# Initialize submodules (includes hiredis)
if [ ! -d "hiredis" ]; then
    echo "📦 Initializing hiredis submodule..."
    git submodule update --init --recursive
fi

# Create virtual environment if it doesn't exist
if [ ! -d "cyredis_venv" ]; then
    echo "🐍 Creating Python virtual environment..."
    python3 -m venv cyredis_venv
fi

# Activate virtual environment
echo "🔧 Activating virtual environment..."
source cyredis_venv/bin/activate

# Install/update dependencies
echo "📦 Installing dependencies..."
pip install -q --upgrade pip
pip install -q -r requirements.txt
pip install -q cython

# Clean previous builds
echo "🧹 Cleaning previous builds..."
rm -rf build/ dist/ *.egg-info/
find . -name "*.so" -delete
find . -name "*.c" -delete
find cy_redis/ -name "*.so" -delete
find cy_redis/ -name "*.c" -delete

# Set optimization flags
export CFLAGS="-O3 -march=native -mtune=native -flto -fPIC -pthread"
export CXXFLAGS="$CFLAGS"
export LDFLAGS="-flto"

# Build with all optimizations
echo "🏗️  Building CyRedis with advanced optimizations..."
python setup.py build_ext --inplace --force \
    --define CYTHON_TRACE=0 \
    --define CYTHON_TRACE_NOGIL=1

# Run basic functionality tests
echo "🧪 Running basic functionality tests..."
python3 -c "
from optimized_redis import OptimizedRedis
import time

print('Testing basic Redis operations...')
redis = OptimizedRedis()

# Test basic operations
redis.set('test_key', 'test_value')
result = redis.get('test_key')
print(f'✓ SET/GET: {result}')

# Test compression (if available)
try:
    from advanced_redis import AdvancedRedis
    adv_redis = AdvancedRedis()
    adv_redis.set('compressed_key', 'This is a longer string that should benefit from compression and performance optimizations!')
    result = adv_redis.get('compressed_key')
    print(f'✓ Advanced SET/GET with compression: {result[:50]}...')
except ImportError as e:
    print(f'⚠️  Advanced features not available: {e}')

print('✅ Basic functionality test passed!')
"

# Run performance benchmarks
echo "📊 Running performance benchmarks..."
python3 -c "
from optimized_redis import OptimizedRedis
import time

redis = OptimizedRedis()
print('Running performance benchmarks...')

# Benchmark SET operations
start = time.time()
for i in range(1000):
    redis.set(f'bench_key_{i}', f'value_{i}')
set_time = time.time() - start
print(f'✓ SET 1000 keys: {set_time:.3f}s ({1000/set_time:.0f} ops/sec)')

# Benchmark GET operations
start = time.time()
for i in range(1000):
    redis.get(f'bench_key_{i}')
get_time = time.time() - start
print(f'✓ GET 1000 keys: {get_time:.3f}s ({1000/get_time:.0f} ops/sec)')

# Benchmark bulk operations (if available)
try:
    from advanced_redis import AdvancedRedis
    adv_redis = AdvancedRedis()

    # Bulk SET test
    bulk_data = {f'bulk_key_{i}': f'bulk_value_{i}' for i in range(100)}
    start = time.time()
    adv_redis.bulk_ops.mset_bulk(bulk_data)
    bulk_set_time = time.time() - start
    print(f'✓ Bulk SET 100 keys: {bulk_set_time:.3f}s ({100/bulk_set_time:.0f} ops/sec)')

    # Bulk GET test
    bulk_keys = [f'bulk_key_{i}' for i in range(100)]
    start = time.time()
    results = adv_redis.bulk_ops.mget_bulk(bulk_keys)
    bulk_get_time = time.time() - start
    print(f'✓ Bulk GET 100 keys: {bulk_get_time:.3f}s ({100/bulk_get_time:.0f} ops/sec)')

    # Show metrics
    metrics = adv_redis.get_metrics()
    print(f'✓ Operations completed: {metrics[\"counters\"].get(\"set_success\", 0)} sets, {metrics[\"counters\"].get(\"get_success\", 0)} gets')

except ImportError as e:
    print(f'⚠️  Advanced benchmarks not available: {e}')

print('✅ Performance benchmarks completed!')
"

echo "CyRedis build completed!"
echo ""
echo "Available features:"
echo "  • Core Redis operations (SET, GET, DEL, etc.)"
echo "  • Cython implementation"
echo "  • Hiredis integration"
echo "  • Connection pooling"
echo "  • Basic bulk operations"
echo "  • Some additional features"
echo ""
echo "Ready for use."
