#!/bin/bash

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"

print_status "Starting Kafka integration tests..."
print_status "Script directory: $SCRIPT_DIR"
print_status "Project root: $PROJECT_ROOT"

# Check if we're in the right directory
if [ ! -f "$SCRIPT_DIR/docker-compose.yml" ]; then
    print_error "docker-compose.yml not found in $SCRIPT_DIR"
    exit 1
fi

# Step 1: Check if library is built
print_status "Checking if library is built..."
if [ ! -d "$PROJECT_ROOT/dist" ]; then
    print_warning "Library not built. Building now..."
    cd "$PROJECT_ROOT"
    if command -v pnpm &> /dev/null; then
        pnpm build
    elif command -v npm &> /dev/null; then
        npm run build
    else
        print_error "Neither pnpm nor npm found. Please install one."
        exit 1
    fi
    
    if [ $? -ne 0 ]; then
        print_error "Build failed"
        exit 1
    fi
    print_success "Library built successfully"
else
    print_success "Library already built"
fi

# Step 2: Start Kafka infrastructure
print_status "Starting Kafka infrastructure..."
cd "$SCRIPT_DIR"

# Try podman-compose first, then docker-compose
if command -v podman-compose &> /dev/null; then
    COMPOSE_CMD="podman-compose"
    print_status "Using podman-compose"
elif command -v docker-compose &> /dev/null; then
    COMPOSE_CMD="docker-compose"
    print_status "Using docker-compose"
else
    print_error "Neither podman-compose nor docker-compose found"
    exit 1
fi

# Check if services are already running
if $COMPOSE_CMD ps | grep -q "Up.*healthy"; then
    print_success "Kafka infrastructure already running and healthy"
else
    # Start services
    print_status "Starting Kafka services..."
    $COMPOSE_CMD up -d
    
    if [ $? -ne 0 ]; then
        print_warning "Compose up failed, services might already be running. Checking status..."
        # Check if they're actually running despite the error
        if $COMPOSE_CMD ps | grep -q "Up"; then
            print_success "Services are running despite compose error"
        else
            print_error "Failed to start Kafka infrastructure"
            exit 1
        fi
    else
        print_success "Kafka infrastructure started"
    fi
fi

# Step 3: Wait for services to be ready
print_status "Waiting for Kafka to be ready..."
sleep 10

# Check if Kafka is responding
print_status "Checking Kafka health..."
$COMPOSE_CMD ps

# Step 4: Install test dependencies if needed
if [ ! -d "$SCRIPT_DIR/node_modules" ]; then
    print_status "Installing test dependencies..."
    if command -v pnpm &> /dev/null; then
        pnpm install
    elif command -v npm &> /dev/null; then
        npm install
    fi
fi

# Step 5: Run tests
print_status "Running integration tests..."

TEST_FILES=(
    "producer.test.mjs"
    "consumer.test.mjs" 
    "consumer-stream.test.mjs"
    "consumer-manual-commit.test.mjs"
    "consumer-stream-batch.test.mjs"
    "batch-size-limits.test.mjs"
    "stream-cleanup.test.mjs"
    "otel.test.mjs"
)

PASSED_TESTS=0
FAILED_TESTS=0
TOTAL_TESTS=${#TEST_FILES[@]}

echo ""
echo "========================================="
echo "  KAFKA INTEGRATION TEST SUITE"
echo "========================================="
echo ""

for test_file in "${TEST_FILES[@]}"; do
    if [ -f "$test_file" ]; then
        echo ""
        echo "🧪 Running $test_file"
        echo "----------------------------------------"
        
        # Run the test with timeout
        timeout 180s node "$test_file"
        test_result=$?
        
        if [ $test_result -eq 0 ]; then
            print_success "✅ $test_file PASSED"
            ((PASSED_TESTS++))
        elif [ $test_result -eq 124 ]; then
            print_error "⏰ $test_file TIMED OUT (180s)"
            ((FAILED_TESTS++))
        else
            print_error "❌ $test_file FAILED (exit code: $test_result)"
            ((FAILED_TESTS++))
        fi
        
        echo "----------------------------------------"
    else
        print_warning "Test file $test_file not found, skipping..."
    fi
done

# Step 6: Show summary
echo ""
echo "========================================="
echo "  TEST SUMMARY"
echo "========================================="
echo "Total Tests: $TOTAL_TESTS"
echo "Passed: $PASSED_TESTS"
echo "Failed: $FAILED_TESTS"

if [ $FAILED_TESTS -eq 0 ]; then
    print_success "🎉 All tests passed!"
    exit_code=0
else
    print_error "❌ $FAILED_TESTS test(s) failed"
    exit_code=1
fi

# Step 7: Optional cleanup
echo ""
read -p "Do you want to stop Kafka infrastructure? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    print_status "Stopping Kafka infrastructure..."
    $COMPOSE_CMD down -v
    print_success "Kafka infrastructure stopped"
else
    print_status "Kafka infrastructure left running"
    print_status "To stop manually: cd $SCRIPT_DIR && $COMPOSE_CMD down -v"
fi

echo ""
print_status "Test run completed"
exit $exit_code
