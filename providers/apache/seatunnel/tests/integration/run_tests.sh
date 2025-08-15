#!/bin/bash
# Integration test runner for SeaTunnel provider

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default values
SKIP_DOCKER_SETUP=${SKIP_DOCKER_SETUP:-false}
SEATUNNEL_VERSION=${SEATUNNEL_VERSION:-2.3.11}
TEST_PATTERN=${TEST_PATTERN:-"test_*.py"}
VERBOSE=${VERBOSE:-false}

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if Docker is running
check_docker() {
    if ! docker info > /dev/null 2>&1; then
        print_error "Docker is not running. Please start Docker and try again."
        exit 1
    fi
    print_status "Docker is running"
}

# Function to check if docker-compose is available
check_docker_compose() {
    if ! command -v docker-compose &> /dev/null; then
        print_error "docker-compose is not installed. Please install docker-compose and try again."
        exit 1
    fi
    print_status "docker-compose is available"
}

# Function to setup test environment
setup_environment() {
    print_status "Setting up test environment..."
    
    # Install test dependencies
    if [ -f "requirements.txt" ]; then
        print_status "Installing test dependencies..."
        pip install -r requirements.txt
    fi
    
    # Install the provider package in development mode
    print_status "Installing SeaTunnel provider in development mode..."
    pip install -e ../../
}

# Function to start SeaTunnel services
start_services() {
    if [ "$SKIP_DOCKER_SETUP" = "true" ]; then
        print_warning "Skipping Docker setup (SKIP_DOCKER_SETUP=true)"
        return 0
    fi
    
    print_status "Starting SeaTunnel services with version $SEATUNNEL_VERSION..."
    
    # Export version for docker-compose
    export SEATUNNEL_VERSION
    
    # Start services
    docker-compose up -d
    
    # Wait for services to be ready
    print_status "Waiting for SeaTunnel to be ready..."
    max_attempts=30
    attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        if curl -f http://localhost:8083/health > /dev/null 2>&1; then
            print_status "SeaTunnel is ready!"
            break
        fi
        
        if [ $attempt -eq $max_attempts ]; then
            print_error "SeaTunnel failed to start within timeout"
            docker-compose logs
            exit 1
        fi
        
        print_status "Attempt $attempt/$max_attempts - waiting for SeaTunnel..."
        sleep 5
        ((attempt++))
    done
}

# Function to run tests
run_tests() {
    print_status "Running integration tests..."
    
    # Set test options
    test_args=()
    
    if [ "$VERBOSE" = "true" ]; then
        test_args+=("-v")
    fi
    
    # Add coverage if available
    if command -v pytest-cov &> /dev/null; then
        test_args+=("--cov=airflow_seatunnel_provider")
        test_args+=("--cov-report=html")
        test_args+=("--cov-report=term")
    fi
    
    # Run tests
    pytest "${test_args[@]}" -m "integration" "$TEST_PATTERN"
}

# Function to cleanup
cleanup() {
    if [ "$SKIP_DOCKER_SETUP" != "true" ]; then
        print_status "Cleaning up services..."
        docker-compose down -v
    fi
}

# Function to show usage
show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -h, --help                Show this help message"
    echo "  -v, --verbose             Run tests in verbose mode"
    echo "  -s, --skip-docker         Skip Docker setup (use existing services)"
    echo "  -p, --pattern PATTERN     Test pattern to run (default: test_*.py)"
    echo "  --seatunnel-version VER   SeaTunnel version to use (default: 2.3.11)"
    echo ""
    echo "Environment variables:"
    echo "  SKIP_DOCKER_SETUP         Skip Docker setup if set to 'true'"
    echo "  SEATUNNEL_VERSION         SeaTunnel version to use"
    echo "  TEST_PATTERN              Test pattern to run"
    echo "  VERBOSE                   Run in verbose mode if set to 'true'"
    echo ""
    echo "Examples:"
    echo "  $0                        Run all integration tests"
    echo "  $0 -v                     Run tests in verbose mode"
    echo "  $0 -s                     Run tests without starting Docker services"
    echo "  $0 -p 'test_hook*'        Run only hook tests"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_usage
            exit 0
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        -s|--skip-docker)
            SKIP_DOCKER_SETUP=true
            shift
            ;;
        -p|--pattern)
            TEST_PATTERN="$2"
            shift 2
            ;;
        --seatunnel-version)
            SEATUNNEL_VERSION="$2"
            shift 2
            ;;
        *)
            print_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Main execution
main() {
    print_status "Starting SeaTunnel provider integration tests"
    print_status "SeaTunnel version: $SEATUNNEL_VERSION"
    print_status "Test pattern: $TEST_PATTERN"
    
    # Setup trap for cleanup
    trap cleanup EXIT
    
    # Check prerequisites
    if [ "$SKIP_DOCKER_SETUP" != "true" ]; then
        check_docker
        check_docker_compose
    fi
    
    # Setup environment
    setup_environment
    
    # Start services
    start_services
    
    # Run tests
    if run_tests; then
        print_status "All tests passed!"
        exit 0
    else
        print_error "Some tests failed!"
        exit 1
    fi
}

# Change to script directory
cd "$(dirname "$0")"

# Run main function
main "$@"