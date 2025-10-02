#!/bin/bash
# Build script for CyRedis Cython extension

set -e

echo "=== CyRedis Build Script ==="

# Function to detect OS
detect_os() {
    case "$(uname -s)" in
        Darwin)
            echo "macos"
            ;;
        Linux)
            if [[ -f /etc/debian_version ]]; then
                echo "debian"
            elif [[ -f /etc/redhat-release ]]; then
                echo "redhat"
            elif [[ -f /etc/arch-release ]]; then
                echo "arch"
            else
                echo "linux"
            fi
            ;;
        CYGWIN*|MINGW32*|MSYS*|MINGW*)
            echo "windows"
            ;;
        *)
            echo "unknown"
            ;;
    esac
}

OS=$(detect_os)
echo "Detected OS: $OS"

# Install system dependencies
install_system_deps() {
    echo "Installing system dependencies..."

    case "$OS" in
        macos)
            if command -v brew >/dev/null 2>&1; then
                echo "Installing hiredis via Homebrew..."
                brew install hiredis
            else
                echo "Homebrew not found. Please install Homebrew and run again:"
                echo "  /bin/bash -c \"\$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)\""
                exit 1
            fi
            ;;
        debian)
            echo "Installing hiredis via apt..."
            sudo apt-get update
            sudo apt-get install -y libhiredis-dev build-essential
            ;;
        redhat)
            echo "Installing hiredis via yum/dnf..."
            if command -v dnf >/dev/null 2>&1; then
                sudo dnf install -y hiredis-devel gcc gcc-c++
            else
                sudo yum install -y hiredis-devel gcc gcc-c++
            fi
            ;;
        arch)
            echo "Installing hiredis via pacman..."
            sudo pacman -S hiredis
            ;;
        windows)
            echo "Windows detected. Please install hiredis manually:"
            echo "  - Download from: https://github.com/redis/hiredis/releases"
            echo "  - Or use vcpkg: vcpkg install hiredis"
            exit 1
            ;;
        *)
            echo "Unknown OS. Please install hiredis manually and run:"
            echo "  pip install -e ."
            exit 1
            ;;
    esac
}

# Check if hiredis is available
check_hiredis() {
    echo "Checking for hiredis library..."

    # Try pkg-config first
    if pkg-config --exists hiredis 2>/dev/null; then
        echo "✓ hiredis found via pkg-config"
        return 0
    fi

    # Try to compile a test program
    if command -v cc >/dev/null 2>&1; then
        echo "Testing hiredis compilation..."
        cat > /tmp/test_hiredis.c << 'EOF'
#include <hiredis/hiredis.h>
#include <stdio.h>
int main() {
    redisContext *c = redisConnect("127.0.0.1", 6379);
    if (c && !c->err) {
        redisFree(c);
        return 0;
    }
    return 1;
}
EOF
        if cc -lhiredis /tmp/test_hiredis.c -o /tmp/test_hiredis 2>/dev/null && /tmp/test_hiredis 2>/dev/null; then
            echo "✓ hiredis compilation test passed"
            rm -f /tmp/test_hiredis.c /tmp/test_hiredis
            return 0
        fi
        rm -f /tmp/test_hiredis.c /tmp/test_hiredis
    fi

    echo "✗ hiredis not found"
    return 1
}

# Install hiredis if not found
if ! check_hiredis; then
    install_system_deps

    if ! check_hiredis; then
        echo "Failed to install hiredis. Please install manually and run:"
        echo "  pip install -e ."
        exit 1
    fi
fi

# Install Python dependencies and build
echo "Installing CyRedis..."

# Use pip install in editable mode
if pip install -e .; then
    echo "✓ CyRedis installed successfully!"
else
    echo "✗ Installation failed. Trying fallback build..."

    # Fallback: manual build
    pip install -r requirements.txt
    python setup.py build_ext --inplace

    if [ $? -eq 0 ]; then
        echo "✓ Fallback build successful!"
    else
        echo "✗ Build failed. Please check your environment."
        exit 1
    fi
fi

# Verify the installation
echo "Verifying installation..."
python -c "
try:
    from cy_redis import CyRedisClient, __version__
    print(f'✓ CyRedis {__version__} imported successfully!')
except ImportError as e:
    print(f'✗ Import failed: {e}')
    exit(1)
"

# Run basic test
echo "Running basic functionality test..."
python -c "
from cy_redis import HighPerformanceRedis
import time

try:
    # Only test if Redis is running
    import socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(1)
    result = sock.connect_ex(('localhost', 6379))
    sock.close()

    if result == 0:
        print('Redis server detected, running full test...')
        with HighPerformanceRedis() as redis:
            redis.set('test:key', 'test_value')
            value = redis.get('test:key')
            if value == 'test_value':
                print('✓ Basic functionality test passed!')
                redis.delete('test:key')
            else:
                print('✗ Basic functionality test failed!')
    else:
        print('✓ Redis server not running, but import test passed!')
except Exception as e:
    print(f'✗ Test failed: {e}')
"

echo ""
echo "=== Installation Complete ==="
echo "You can now use CyRedis in your applications!"
echo ""
echo "Example usage:"
echo "  from cy_redis import HighPerformanceRedis"
echo "  redis = HighPerformanceRedis()"
echo "  redis.set('key', 'value')"
echo ""
echo "See example_usage.py and README_CYREDIS.md for more details."