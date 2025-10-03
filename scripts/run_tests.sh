#!/usr/bin/env bash

##############################################################################
# CyRedis Comprehensive Test Runner
#
# A comprehensive test runner script with Docker management, coverage
# reporting, and colored output.
#
# Usage:
#   ./scripts/run_tests.sh [OPTIONS]
#
# Options:
#   --all                Run all tests (default)
#   --unit              Run unit tests only
#   --integration       Run integration tests only
#   --fast              Run fast tests only (no slow/cluster)
#   --coverage          Generate coverage report
#   --apps              Run test applications/examples
#   --docker-up         Start Docker services before tests
#   --docker-down       Stop Docker services after tests
#   --docker-clean      Clean Docker volumes before starting
#   --no-color          Disable colored output
#   --verbose           Enable verbose output
#   --watch             Run tests in watch mode
#   --help              Show this help message
#
# Examples:
#   ./scripts/run_tests.sh --fast --docker-up
#   ./scripts/run_tests.sh --integration --coverage
#   ./scripts/run_tests.sh --all --docker-up --docker-down
#
##############################################################################

set -e

# Color definitions
if [[ -t 1 ]]; then
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    YELLOW='\033[0;33m'
    BLUE='\033[0;34m'
    MAGENTA='\033[0;35m'
    CYAN='\033[0;36m'
    BOLD='\033[1m'
    NC='\033[0m' # No Color
else
    RED=''
    GREEN=''
    YELLOW=''
    BLUE=''
    MAGENTA=''
    CYAN=''
    BOLD=''
    NC=''
fi

# Script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Default options
TEST_TYPE="all"
COVERAGE=false
DOCKER_UP=false
DOCKER_DOWN=false
DOCKER_CLEAN=false
RUN_APPS=false
VERBOSE=false
WATCH=false
USE_COLOR=true

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --all)
            TEST_TYPE="all"
            shift
            ;;
        --unit)
            TEST_TYPE="unit"
            shift
            ;;
        --integration)
            TEST_TYPE="integration"
            shift
            ;;
        --fast)
            TEST_TYPE="fast"
            shift
            ;;
        --coverage)
            COVERAGE=true
            shift
            ;;
        --apps)
            RUN_APPS=true
            shift
            ;;
        --docker-up)
            DOCKER_UP=true
            shift
            ;;
        --docker-down)
            DOCKER_DOWN=true
            shift
            ;;
        --docker-clean)
            DOCKER_CLEAN=true
            shift
            ;;
        --no-color)
            USE_COLOR=false
            RED=''
            GREEN=''
            YELLOW=''
            BLUE=''
            MAGENTA=''
            CYAN=''
            BOLD=''
            NC=''
            shift
            ;;
        --verbose)
            VERBOSE=true
            shift
            ;;
        --watch)
            WATCH=true
            shift
            ;;
        --help)
            sed -n '3,34p' "$0" | sed 's/^# //'
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Disable colors if requested
if [[ "$USE_COLOR" == false ]]; then
    RED=''
    GREEN=''
    YELLOW=''
    BLUE=''
    MAGENTA=''
    CYAN=''
    BOLD=''
    NC=''
fi

# Helper functions
print_header() {
    echo -e "${BOLD}${BLUE}$1${NC}"
    echo -e "${BLUE}$(printf '=%.0s' {1..60})${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_info() {
    echo -e "${CYAN}ℹ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

# Check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check dependencies
check_dependencies() {
    print_header "Checking Dependencies"

    local missing_deps=()

    if ! command_exists python3; then
        missing_deps+=("python3")
    else
        print_success "Python3 found: $(python3 --version)"
    fi

    if ! command_exists uv; then
        missing_deps+=("uv")
    else
        print_success "UV found: $(uv --version)"
    fi

    if [[ "$DOCKER_UP" == true ]] || [[ "$DOCKER_DOWN" == true ]]; then
        if ! command_exists docker; then
            missing_deps+=("docker")
        else
            print_success "Docker found: $(docker --version)"
        fi
    fi

    if [[ ${#missing_deps[@]} -gt 0 ]]; then
        print_error "Missing dependencies: ${missing_deps[*]}"
        exit 1
    fi

    echo ""
}

# Docker management
manage_docker() {
    if [[ "$DOCKER_CLEAN" == true ]]; then
        print_header "Cleaning Docker Volumes"
        if [[ -f "$PROJECT_DIR/docker-compose.yml" ]]; then
            cd "$PROJECT_DIR"
            docker compose down -v
            print_success "Docker volumes cleaned"
        else
            print_warning "No docker-compose.yml found, skipping clean"
        fi
        echo ""
    fi

    if [[ "$DOCKER_UP" == true ]]; then
        print_header "Starting Docker Services"
        if [[ -f "$PROJECT_DIR/docker-compose.yml" ]]; then
            cd "$PROJECT_DIR"
            docker compose up -d
            print_success "Docker services started"

            # Wait for services to be ready
            print_info "Waiting for services to be ready..."
            sleep 5

            # Show service status
            docker compose ps
        else
            print_warning "No docker-compose.yml found, skipping Docker startup"
        fi
        echo ""
    fi
}

# Run tests
run_tests() {
    print_header "Running Tests: $TEST_TYPE"

    cd "$PROJECT_DIR"

    # Base pytest command
    local pytest_cmd="uv run pytest"

    # Add verbosity
    if [[ "$VERBOSE" == true ]]; then
        pytest_cmd="$pytest_cmd -vv"
    else
        pytest_cmd="$pytest_cmd -v"
    fi

    # Add coverage
    if [[ "$COVERAGE" == true ]]; then
        pytest_cmd="$pytest_cmd --cov=cy_redis --cov-report=html --cov-report=term-missing --cov-report=xml"
    fi

    # Add test markers based on type
    case "$TEST_TYPE" in
        unit)
            pytest_cmd="$pytest_cmd -m 'not integration and not slow'"
            ;;
        integration)
            pytest_cmd="$pytest_cmd -m 'integration'"
            ;;
        fast)
            pytest_cmd="$pytest_cmd -m 'not slow and not cluster'"
            ;;
        all)
            # No marker filters for all tests
            ;;
    esac

    # Add tests directory
    pytest_cmd="$pytest_cmd tests/"

    print_info "Command: $pytest_cmd"
    echo ""

    # Run tests
    if eval "$pytest_cmd"; then
        print_success "Tests passed!"

        # Show coverage report location if generated
        if [[ "$COVERAGE" == true ]]; then
            echo ""
            print_success "Coverage reports generated:"
            print_info "HTML: $PROJECT_DIR/htmlcov/index.html"
            print_info "XML:  $PROJECT_DIR/coverage.xml"
        fi

        return 0
    else
        print_error "Tests failed!"
        return 1
    fi
}

# Run test applications
run_test_apps() {
    print_header "Running Test Applications"

    local examples_dir="$PROJECT_DIR/examples"

    if [[ ! -d "$examples_dir" ]]; then
        print_warning "Examples directory not found"
        return 0
    fi

    local failed_apps=()

    for example in "$examples_dir"/example_*.py; do
        if [[ -f "$example" ]]; then
            local app_name=$(basename "$example")
            print_info "Running $app_name..."

            if uv run python "$example"; then
                print_success "$app_name completed"
            else
                print_warning "$app_name failed (non-critical)"
                failed_apps+=("$app_name")
            fi
            echo ""
        fi
    done

    if [[ ${#failed_apps[@]} -gt 0 ]]; then
        print_warning "Some test apps failed: ${failed_apps[*]}"
    else
        print_success "All test apps completed successfully"
    fi
}

# Watch mode
run_watch_mode() {
    print_header "Running Tests in Watch Mode"
    print_info "Press Ctrl+C to stop"
    echo ""

    cd "$PROJECT_DIR"

    if ! command_exists ptw; then
        print_error "pytest-watch (ptw) not installed"
        print_info "Install with: uv pip install pytest-watch"
        exit 1
    fi

    local pytest_args="-v"

    case "$TEST_TYPE" in
        unit)
            pytest_args="$pytest_args -m 'not integration and not slow'"
            ;;
        integration)
            pytest_args="$pytest_args -m 'integration'"
            ;;
        fast)
            pytest_args="$pytest_args -m 'not slow and not cluster'"
            ;;
    esac

    uv run ptw -- $pytest_args tests/
}

# Cleanup
cleanup() {
    if [[ "$DOCKER_DOWN" == true ]]; then
        echo ""
        print_header "Stopping Docker Services"
        if [[ -f "$PROJECT_DIR/docker-compose.yml" ]]; then
            cd "$PROJECT_DIR"
            docker compose down
            print_success "Docker services stopped"
        else
            print_warning "No docker-compose.yml found"
        fi
    fi
}

# Trap cleanup on exit
trap cleanup EXIT

# Main execution
main() {
    print_header "CyRedis Test Runner"
    echo -e "${CYAN}Project: $PROJECT_DIR${NC}"
    echo -e "${CYAN}Test Type: $TEST_TYPE${NC}"
    echo ""

    # Check dependencies
    check_dependencies

    # Manage Docker
    manage_docker

    # Run in watch mode if requested
    if [[ "$WATCH" == true ]]; then
        run_watch_mode
        exit $?
    fi

    # Run tests
    local test_result=0
    run_tests || test_result=$?

    echo ""

    # Run test apps if requested
    if [[ "$RUN_APPS" == true ]]; then
        run_test_apps
    fi

    # Final summary
    echo ""
    print_header "Test Summary"

    if [[ $test_result -eq 0 ]]; then
        print_success "All tests passed!"
        exit 0
    else
        print_error "Some tests failed"
        exit 1
    fi
}

# Run main function
main
