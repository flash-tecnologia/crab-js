# AGENTS.md

This file provides guidance to Codex (Codex.ai/code) when working with code in this repository.

## Project Overview

kafka-crab-js is a Node.js Kafka client built with Rust and NAPI-RS, providing high-performance Kafka operations through native bindings. The project combines Rust's performance with JavaScript's accessibility, using librdkafka as the underlying Kafka implementation.

## Architecture

### Hybrid Rust/TypeScript Structure

- **Rust Core** (`src/`): Native Kafka implementation using librdkafka
  - `src/kafka/consumer/`: Consumer implementation with context management and helper utilities
  - `src/kafka/producer/`: Producer implementation with message handling
  - `src/kafka_admin.rs`: Kafka administration operations
  - `src/kafka_client_config.rs`: Configuration management
- **TypeScript Layer** (`js-src/`): JavaScript/TypeScript API wrapper
  - `js-src/kafka-client.ts`: Main client interface
  - `js-src/kafka-stream-readable.ts`: Stream processing interface
  - `js-src/index.ts`: Public API exports

### Build System

- Uses NAPI-RS for Rust-to-Node.js bindings
- Custom build script (`build.mjs`) handles both Rust compilation and TypeScript bundling
- Generates both ESM and CommonJS outputs
- Creates separate binding files for different module systems

## Development Commands

### Building

- `pnpm build` - Full production build (Rust + TypeScript)
- `pnpm build:debug` - Debug build for development
- `pnpm prebuild` - Clean dist directory

### Testing

- `pnpm test` - Run unit tests using Node.js test runner
- `pnpm test:integration` - Run integration tests (requires Kafka broker)
- Set `RUN_KAFKA_INTEGRATION=true` to enable integration tests

### Code Quality

- `pnpm lint` - Run oxlint for JavaScript/TypeScript linting
- `pnpm fmt` - Format code using dprint
- No separate typecheck command - handled during build

### Package Management

- Uses pnpm as package manager
- `pnpm artifacts` - NAPI artifacts management
- `pnpm prepublishOnly` - Pre-publish preparations

## Key Configuration

### Testing Environment

- Integration tests require Kafka broker at `localhost:9092` (default)
- Use `KAFKA_BROKERS` env var to override broker address
- Use `KAFKA_LOG_LEVEL` to control logging verbosity
- Tests use unique topic names with nanoid for isolation

### Build Configuration

- TypeScript config focuses on declaration generation only
- dprint handles code formatting with 120 character line width
- Rust build includes static linking and symbol stripping for release

## Important Patterns

### Client Architecture

- `KafkaClient` is the main entry point that creates consumers and producers
- Stream consumers extend Node.js Readable streams for reactive processing
- All operations are async/await based

### Performance Optimizations

- Message ID generation uses atomic counters instead of cryptographic nanoid for 4.8x performance improvement
- Producer delivery results use lock-free DashMap instead of Mutex<HashMap> for zero contention
- Consumer batch processing API (`recvBatch`) provides 2-5x throughput improvement for bulk operations
- Stream consumers use adaptive batching with configurable batch size and timeout parameters
- Input validation is bounds-checked to prevent resource exhaustion
- Lock-free concurrent operations minimize blocking in high-throughput scenarios

### Error Handling

- Rust errors are propagated through NAPI error handling
- JavaScript layer provides typed error interfaces
- Integration tests include retry logic and reconnection patterns

### Configuration Management

- Configuration supports both simple options and advanced librdkafka settings
- Security protocols and connection settings are strongly typed
- Client configuration is immutable after creation
- User-provided configurations are delegated to rdkafka for validation (minimal sanitization for credentials only)
- Configuration security relies on rdkafka's built-in validation rather than custom allowlists

### Batch Processing API

**Two approaches for high-performance message processing:**

#### 1. Direct Batch API

- `consumer.recvBatch(maxMessages, timeoutMs)` - Direct batch message retrieval
- Full control over each batch operation
- Ideal for custom processing logic

#### 2. Stream Batch Methods

- `stream.enableBatchMode(batchSize, batchTimeoutMs)` - Enable batch processing
- `stream.disableBatchMode()` - Return to single message mode
- `stream.isBatchModeEnabled()` - Check current mode
- `stream.getBatchConfig()` - Get current batch settings
- Runtime switching between single and batch modes

**Configuration Guidelines:**

- Optimal batch sizes: 10-100 messages depending on message size and processing time
- Timeout recommendations: 100-1000ms for balanced latency/throughput
- Default behavior unchanged - single message processing
- Fallback to single-message processing ensures compatibility

## Testing Notes

- Unit tests focus on client creation and configuration validation
- Integration tests require running Kafka infrastructure
- Use Docker Compose file in `__test__/integration/compose.yaml` for local testing
- Test isolation is achieved through unique topic names and client IDs

## Development Workflow

### Pre-Commit Best Practices

- Before commit, running fmt and clippy
- Run `cargo fmt` and `cargo clippy` to ensure code formatting and catch potential issues before committing
