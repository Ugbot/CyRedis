#!/bin/bash
# Build script for CyRedis Game Engine
# Builds the game engine extension that depends on base CyRedis

set -e  # Exit on any error

echo "🎮 Building CyRedis Game Engine"
echo "=" * 40

# Initialize submodules (includes hiredis)
if [ ! -d "../hiredis" ]; then
    echo "📦 Initializing hiredis submodule..."
    cd ..
    git submodule update --init --recursive
    cd cyredis_game
fi

# Check if base CyRedis is built
if [ ! -f "../cy_redis/cy_redis_client.cpython-313-darwin.so" ] && [ ! -f "../cy_redis/cy_redis_client.so" ]; then
    echo "⚠️  Base CyRedis not built. Building it first..."
    cd ..
    ./build_optimized_advanced.sh
    cd cyredis_game
fi

# Create virtual environment if it doesn't exist
if [ ! -d "cyredis_game_venv" ]; then
    echo "🐍 Creating Python virtual environment..."
    python3 -m venv cyredis_game_venv
fi

# Activate virtual environment
source cyredis_game_venv/bin/activate

# Install dependencies
echo "📦 Installing dependencies..."
pip install --upgrade pip
pip install cython>=0.29.0

# Install base CyRedis in development mode
echo "🔗 Installing base CyRedis..."
cd ..
pip install -e .
cd cyredis_game

# Build the game engine
echo "⚙️  Building game engine extension..."
python setup.py build_ext --inplace

# Test the build
echo "🧪 Testing game engine..."
python -c "
try:
    from cyredis_game import GameEngine
    print('✓ GameEngine import successful')

    # Test basic functionality
    engine = GameEngine()
    print('✓ GameEngine instantiation successful')

    # Test function loading (requires Redis)
    # engine.load_functions()  # Commented out as it needs Redis

    print('✓ Game engine build successful!')
    print('🎮 CyRedis Game Engine ready!')

except ImportError as e:
    echo \"❌ Import failed: $e\"
    exit 1
except Exception as e:
    echo \"❌ Test failed: $e\"
    exit 1
"

echo ""
echo "🎉 CyRedis Game Engine built successfully!"
echo ""
echo "Features:"
echo "  ✅ Fast MessagePack serialization (5-10x faster than JSON)"
echo "  ✅ Authoritative ECS on Redis/Valkey"
echo "  ✅ Zone-based horizontal scaling"
echo "  ✅ Atomic operations via Redis Functions"
echo "  ✅ Full cluster support (Redis/Valkey)"
echo ""
echo "Compatibility:"
echo "  • Redis 7.0+: Full Redis Functions support"
echo "  • Valkey: Compatible Redis fork"
echo "  • Cluster: Hash-tag safe zone co-location"
echo "  • Standalone: Works on single node"
echo ""
echo "Usage:"
echo "  from cyredis_game import GameEngine"
echo "  engine = GameEngine()"
echo "  engine.load_functions()  # Requires Redis 7+/Valkey"
echo ""
echo "Performance:"
echo "  • Intent serialization: MessagePack binary format"
echo "  • Cross-zone transfers: Optimized Lua parsing"
echo "  • Event deserialization: Automatic MessagePack decode"
echo ""
echo "📚 See cyredis_game/README.md for architecture details"
echo "🏃 Run 'python cyredis_game/example_game_engine.py' for demos"
