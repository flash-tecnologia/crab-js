# Kafka Integration Tests

This directory contains comprehensive integration tests for the kafka-crab-js library, testing all major components: Producer, Consumer, Consumer Stream, and Consumer Stream Batch Mode.

**Location**: `tests/integration/` - Following standard test organization patterns.

## Test Structure

- **`docker-compose.yml`** - Kafka infrastructure for testing
- **`utils.mjs`** - Shared utilities and helpers
- **`producer.test.mjs`** - Producer functionality tests
- **`consumer.test.mjs`** - Consumer polling API tests
- **`consumer-stream.test.mjs`** - Consumer stream functionality tests
- **`consumer-stream-batch.test.mjs`** - Consumer stream batch mode tests
- **`batch-size-limits.test.mjs`** - Batch size validation and performance tests

## Prerequisites

1. **Container Runtime** - Either:
   - Docker and Docker Compose, OR
   - Podman and podman-compose
2. **Built Library** - Run `vp run build` from the root directory first

## Running Tests

### 1. Automated Test Runner (Recommended)

**From this directory:**

```bash
vp run test
```

You can still use the shell wrapper directly if you prefer:

```bash
./run-tests.sh
```

The automated script will:

- Check if the library is built (builds if needed)
- Start Kafka infrastructure automatically
- Wait for services to be ready
- Run all test suites in sequence
- Show a summary of results
- Optionally stop Kafka when done

### 2. Manual Setup and Testing

If you prefer manual control:

**Start Kafka Infrastructure:**

```bash
vp run kafka:up
```

**Wait for services to be healthy:**

```bash
docker-compose ps
# OR
podman-compose ps
```

**Run Individual Test Suites:**

```bash
vp run test:producer
vp run test:consumer
vp run test:stream
vp run test:batch
```

**Run All Tests Manually:**

```bash
vp run test:manual
```

### 4. Optional: Access Kafka UI

Start with UI profile to access the web interface:

```bash
vp run kafka:ui
```

Visit http://localhost:8080 to inspect topics and messages.

## Test Configuration

Tests use environment variables for configuration:

- `KAFKA_BROKERS` - Kafka broker address (default: `localhost:9092`)
- `KAFKA_LOG_LEVEL` - Log level (default: `info`)

Example:

```bash
KAFKA_BROKERS=localhost:9092 KAFKA_LOG_LEVEL=debug vp run test:producer
```

## Test Coverage

### Producer Tests (`producer.test.mjs`)

- ✅ Single message sending
- ✅ Batch message sending
- ✅ Complex headers handling
- ✅ Multiple topics
- ✅ Large message batches
- ✅ Messages with no key
- ✅ Empty payload handling

### Consumer Tests (`consumer.test.mjs`)

- ✅ Basic message receiving
- ✅ Batch message receiving
- ✅ Multiple topic subscription
- ✅ Manual commit functionality
- ✅ Seek functionality
- ✅ Event handling
- ✅ Message headers processing

### Consumer Stream Tests (`consumer-stream.test.mjs`)

- ✅ Stream data event handling
- ✅ Error handling
- ✅ Multiple topics processing
- ✅ Pause/resume functionality
- ✅ Raw consumer access
- ✅ Stream methods (seek, commit)
- ✅ Unsubscribe functionality
- ✅ Large message batch handling

### Consumer Stream Batch Tests (`consumer-stream-batch.test.mjs`)

- ✅ Basic batch mode functionality
- ✅ Default batch configuration
- ✅ Timeout validation
- ✅ Batch message receiving
- ✅ Mode switching (single ↔ batch)
- ✅ Performance comparison
- ✅ Large batch processing
- ✅ Batch timeout behavior

### Batch Size Limits Tests (`batch-size-limits.test.mjs`)

- ✅ Maximum batch size validation (limit: 10 messages)
- ✅ Boundary condition testing (0, 1, 10, 25, 1000)
- ✅ Performance comparison across batch sizes
- ✅ Timeout boundary validation (1-30000ms)
- ✅ Warning message verification
- ✅ Edge case handling (negative, zero, extreme values)
- ✅ Stream vs polling API consistency

## Test Utilities (`utils.mjs`)

The utilities provide:

- Test environment setup
- Message creation helpers
- Consumer/Producer configuration builders
- Message filtering by test ID
- Cleanup functions
- Message waiting utilities

## Cleanup

Stop and remove containers:

```bash
vp run kafka:down
```

## Troubleshooting

### Tests Timeout

- Ensure Kafka is fully started (check `docker-compose ps` or `podman-compose ps`)
- Increase timeout values in test configuration
- Check container logs: `docker-compose logs kafka` or `podman-compose logs kafka`

### Connection Errors

- Verify KAFKA_BROKERS environment variable
- Ensure no firewall blocking port 9092
- Check if port is already in use

### Test Isolation Issues

- Each test uses unique topic names with nanoid
- Tests filter messages by testId to avoid cross-test interference
- If issues persist, restart Kafka: `docker-compose restart kafka` or `podman-compose restart kafka`

## Performance Notes

- Batch mode typically shows 2-5x throughput improvement
- Stream consumers handle backpressure automatically
- Large message batches (100+ messages) test memory efficiency
- Timeout tests verify proper batch completion under light load
