#!/usr/bin/env bash

##############################################################################
# CyRedis Quick Test Script
#
# A fast test runner for rapid development feedback.
# Runs only fast tests by default and provides minimal output.
#
# Usage:
#   ./scripts/test_quick.sh [TEST_PATH] [PYTEST_ARGS...]
#
# Examples:
#   ./scripts/test_quick.sh                           # Run all fast tests
#   ./scripts/test_quick.sh tests/test_basic.py       # Run specific test file
#   ./scripts/test_quick.sh -k test_connection        # Run tests matching pattern
#   ./scripts/test_quick.sh -x                        # Stop on first failure
#
##############################################################################

set -e

# Color definitions
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m'

# Script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Change to project directory
cd "$PROJECT_DIR"

# Print header
echo -e "${BOLD}${BLUE}Quick Test Runner${NC}"
echo ""

# Check if uv is installed
if ! command -v uv >/dev/null 2>&1; then
    echo -e "${RED}Error: uv not found${NC}"
    echo "Install with: curl -LsSf https://astral.sh/uv/install.sh | sh"
    exit 1
fi

# Build pytest command
PYTEST_CMD="uv run pytest"

# Add default options for quick testing
PYTEST_ARGS=(
    "-v"                              # Verbose
    "-x"                              # Stop on first failure
    "--tb=short"                      # Short traceback format
    "--no-header"                     # No header
    "-m" "not slow and not cluster"   # Skip slow and cluster tests
)

# Parse arguments
TEST_PATH="tests/"
EXTRA_ARGS=()

if [[ $# -gt 0 ]]; then
    # Check if first argument is a file/directory
    if [[ -e "$1" ]]; then
        TEST_PATH="$1"
        shift
    fi

    # Collect remaining arguments
    EXTRA_ARGS=("$@")
fi

# Build full command
FULL_CMD="$PYTEST_CMD ${PYTEST_ARGS[*]} ${EXTRA_ARGS[*]} $TEST_PATH"

# Print command
echo -e "${BLUE}Running:${NC} $FULL_CMD"
echo ""

# Run tests
START_TIME=$(date +%s)

if eval "$FULL_CMD"; then
    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))

    echo ""
    echo -e "${GREEN}✓ Tests passed in ${DURATION}s${NC}"
    exit 0
else
    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))

    echo ""
    echo -e "${RED}✗ Tests failed in ${DURATION}s${NC}"
    exit 1
fi
