<div align="center">

# 🦀 Kafka Crab JS 🦀

A lightweight, flexible, and reliable Kafka client for JavaScript/TypeScript. It is built using Rust and librdkafka, providing a high-performance and feature-rich Kafka client.

[![npm version](https://img.shields.io/npm/v/kafka-crab-js.svg)](https://www.npmjs.com/package/kafka-crab-js)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
</div>

---

## What's New in Version 2.1.0

### New Features

1. **Simplified Message Commits with `commitMessage()`**:
   - New convenience method that accepts a message and commit mode directly
   - Automatically handles `offset + 1` increment internally - no more manual offset arithmetic
   - Available on both `KafkaConsumer` and stream consumers
   - **Before** (v2.0.0):
     ```javascript
     const message = await consumer.recv();
     await consumer.commit(message.topic, message.partition, message.offset + 1, 'Sync');
     ```
   - **After** (v2.1.0):
     ```javascript
     const message = await consumer.recv();
     await consumer.commitMessage(message, 'Sync');
     ```

2. **Enhanced OpenTelemetry Support**:
   - Improved OTEL context propagation for better distributed tracing
   - Safe handling when OTEL SDK is not installed (no-op behavior)
   - Better span context management across producer and consumer operations
   - Seamless integration with standard OTEL SDK setup

3. **CI/CD Improvements**:
   - Updated to Node.js 24 support
   - GitHub Actions updated to v6
   - Improved caching with actions/cache v4

### Migration from 2.0.x

- **No breaking changes** - all existing code continues to work
- The new `commitMessage()` method is additive - existing `commit()` calls remain valid
- OTEL improvements are transparent and require no code changes

---

## What's New in Version 2.0.0

### BREAKING CHANGES ⚠️

This major version includes important breaking changes that improve API consistency and memory management:

1. **Consumer Configuration API Changes**:
   - **REMOVED**: `createTopic` field from `ConsumerConfiguration`
   - **Migration**: Use `createTopic` field in `TopicPartitionConfig` instead when subscribing to topics
   - **Before**: `new KafkaClient().createConsumer({ groupId: 'test', createTopic: true })`
   - **After**: `consumer.subscribe([{ topic: 'my-topic', createTopic: true }])`

2. **Stream Lifecycle Management**:
   - Stream consumers now properly implement Node.js stream lifecycle methods (`_destroy()`)
   - **Memory leak prevention**: Streams now automatically disconnect Kafka consumers during destruction
   - Better resource cleanup ensures no hanging connections or memory leaks
   - Error handling during stream destruction is improved with proper error propagation

3. **Stream Error Handling**:
   - Stream errors now use `destroy(error)` instead of `emit('error')` for proper stream termination
   - This ensures streams are properly closed when errors occur, preventing resource leaks
   - Error propagation follows Node.js stream standards more closely

4. **Rust Configuration Simplification**:
   - Removed `MAX_BATCH_TIMEOUT` constant - now uses only `DEFAULT_BATCH_TIMEOUT` (1000ms)
   - Simplified timeout validation logic in Rust layer
   - Invalid timeout values (< 1ms) now log warnings and use default timeout

5. **Code Quality Improvements**:
   - Fixed linting violations: short variable names and `any` type usage
   - Better TypeScript typing throughout the codebase
   - Removed redundant validation functions for cleaner code

### Major Updates:

1. **Performance Benchmark Suite**:
   - Added comprehensive benchmark suite comparing performance against `kafkajs`, `node-rdkafka`, and `@platformatic/rdkafka`
   - Benchmark scripts with automated setup and data generation
   - Performance testing for both serial and batch processing modes
   - Supports configurable iterations and message sizes for realistic testing scenarios

2. **Enhanced Stream Consumer Configuration**:
   - Stream consumer constructor now accepts optional `ReadableOptions` parameter
   - Default `objectMode: true` for better stream processing ergonomics
   - Improved batch processing configuration with proper defaults

3. **Flexible Configuration System**:
   - **BREAKING**: Configuration properties now accept `Record<string, any>` instead of `Record<string, string>`
   - Enhanced type flexibility for complex configuration values (numbers, booleans, objects)
   - Better support for advanced librdkafka configuration options

4. **Enhanced Topic Management**:
   - Added `createTopic`, `numPartitions`, and `replicas` options to `TopicPartitionConfig`
   - Improved topic creation control during consumer subscription
   - Better handling of partition assignment and topic setup

5. **Build System Improvements**:
   - Updated TypeScript configuration with modular `tsconfig.base.json`
   - Added `typecheck` script for standalone type checking
   - Enhanced build pipeline with better declaration file generation
   - Updated to pnpm@10.15.1 for improved dependency management

6. **Development Tools Enhancement**:
   - Enhanced oxlint configuration with benchmark-specific rules
   - Added support for benchmark directory linting and formatting
   - Improved development workflow with better error handling rules

7. **Consumer Topic Creation Control** (v1.9.0):
   - Added fine-grained control over consumer topic creation behavior
   - Enhanced consistency in topic management and cleanup processes
   - Improved handling of topic creation inconsistencies during consumer operations

8. **Enhanced Manual Commit Testing** (v1.9.0):
   - Consolidated manual commit tests for better reliability and coverage
   - Added comprehensive integration tests with PostRebalance event handling
   - Tests now verify offset persistence across consumer restarts and batch processing

9. **Async Consumer Commit** (v1.8.0 - BREAKING):
   - **BREAKING**: The `consumer.commit()` method is now async and must be awaited
   - **Before**: `consumer.commit(topic, partition, offset, 'Sync')`
   - **After**: `await consumer.commit(topic, partition, offset, 'Sync')`
   - This change improves performance by using `spawn_blocking` for non-blocking operations

### Migration Guide

- **No API changes required** - all public interfaces remain the same
- **Stream memory management** - streams now automatically clean up resources
- **Error handling** - streams will properly terminate on errors (existing error handling code still works)
- **Performance** - simplified timeout logic may provide marginal performance improvements

---

## Features

- 🦀 Simple and intuitive API
- 🚀 High-performance message processing
- 🔄 Automatic reconnection handling
- 🎯 Type-safe interfaces (TypeScript support)
- ⚡ Async/await support
- 🛠️ Configurable consumer and producer options
- 📊 Stream processing support with configurable stream options
- 📦 Message batching capabilities
- 🔍 Comprehensive error handling
- 📈 Performance benchmarking suite
- 🔧 Flexible configuration system supporting complex data types

## Table of Contents

1. [Installation](#installation)
2. [Quick Start](#quick-start)
3. [Consumer Examples](#basic-consumer-setup)
4. [Producer Examples](#producer-examples)
5. [Stream Processing](#stream-processing)
6. [Configuration](#configuration)
7. [Performance Benchmarks](#performance-benchmarks)
8. [Best Practices](#best-practices)
9. [Contributing](#contributing)
10. [License](#license)
11. [OpenTelemetry Instrumentation](#opentelemetry-instrumentation)

## Installation

```bash
npm install kafka-crab-js
# or
yarn add kafka-crab-js
# or
pnpm add kafka-crab-js
```

## Quick Start

### Basic Consumer Setup

```javascript
import { KafkaClient } from 'kafka-crab-js';
async function run() {
  const kafkaClient = new KafkaClient({
    brokers: 'localhost:29092',
    clientId: 'foo-client',
    logLevel: 'debug',
    brokerAddressFamily: 'v4',
  });

  // Create consumer with topic creation control
  const consumer = kafkaClient.createConsumer({
    groupId: 'foo-group',
    configuration: {
      'auto.offset.reset': 'earliest',
      'enable.auto.commit': false, // Use manual commit for better control
    }
  });

  // Subscribe with topic creation options
  await consumer.subscribe([{
    topic: 'foo',
    createTopic: true,
    numPartitions: 3
  }]);

  const message = await consumer.recv();
  const { payload, partition, offset, topic } = message;
  console.log({
    topic,
    partition,
    offset,
    value: payload.toString()
  });

  // Manual commit - two options:
  // Option 1 (v2.1.0+): Simplified with commitMessage
  await consumer.commitMessage(message, 'Sync');
  
  // Option 2: Traditional commit with manual offset increment
  // await consumer.commit(topic, partition, offset + 1, 'Sync');

  consumer.unsubscribe();
}

await run();
```

### Basic Producer Setup

```javascript
import { KafkaClient } from 'kafka-crab-js';

const kafkaClient = new KafkaClient({
  brokers: 'localhost:29092',
  clientId: 'my-client-id',
  logLevel: 'info',
  brokerAddressFamily: 'v4',
});

// Producer configuration is now optional with sensible defaults
const producer = kafkaClient.createProducer({
  configuration: {
    'message.timeout.ms': 5000,  // Now supports number values
    'batch.size': 16384,
    'compression.type': 'snappy'
  }
});

const message = {
  id: 1,
  name: "Sample Message",
  timestamp: new Date().toISOString()
};

const result = await producer.send({
  topic: 'my-topic',
  messages: [{
    payload: Buffer.from(JSON.stringify(message))
  }]
});

const errors = result.map(r => r.error).filter(Boolean);
if (errors.length > 0) {
  console.error('Error sending message:', errors);
} else {
  console.log('Message sent. Offset:', result);
}
```

## Stream Processing

### Enhanced Stream Consumer Example

```javascript
import { KafkaClient } from 'kafka-crab-js';

const kafkaClient = new KafkaClient({
  brokers: 'localhost:29092',
  clientId: 'my-client-id',
  logLevel: 'info',
  brokerAddressFamily: 'v4',
});

// Stream consumer with custom ReadableOptions (v2.0.0+)
const kafkaStream = kafkaClient.createStreamConsumer({
  groupId: `my-group-id`,
  enableAutoCommit: true,
}, {
  objectMode: true,  // Default in v2.0.0+
  highWaterMark: 1024,
  encoding: null
});

await kafkaStream.subscribe([
  { topic: 'foo', createTopic: true },
  { topic: 'bar', createTopic: true }
]);

kafkaStream.on('data', (message) => {
  console.log('>>> Message received:', {
    payload: message.payload.toString(),
    offset: message.offset,
    partition: message.partition,
    topic: message.topic
  });

  if (message.offset > 10) {
    kafkaStream.destroy();
  }
});

kafkaStream.on('close', () => {
  kafkaStream.unsubscribe();
  console.log('Stream ended');
});
```

## Producer Examples

### Batch Message Production

```javascript
const kafkaClient = new KafkaClient({
  brokers: 'localhost:29092',
  clientId: 'my-client-id',
  brokerAddressFamily: 'v4',
});

// Enhanced producer with flexible configuration
const producer = kafkaClient.createProducer({
  configuration: {
    'batch.size': 50000,      // Number value supported
    'linger.ms': 10,          // Number value supported
    'compression.type': 'lz4',
    'enable.idempotence': true  // Boolean value supported
  }
});

const messages = Array.from({ length: 100 }, (_, i) => ({
  payload: Buffer.from(JSON.stringify({
    _id: i,
    name: `Batch Message ${i}`,
    timestamp: new Date().toISOString()
  }))
}));

try {
  const result = await producer.send({
    topic: 'my-topic',
    messages
  });
  console.log('Batch sent. Offset:', result);
  console.assert(result.length === 100);
} catch (error) {
  console.error('Batch error:', error);
}
```

### Producer with Keys and Headers

```javascript
async function produceWithMetadata() {
  const producer = kafkaClient.createProducer({
    configuration: {
      'acks': 'all',
      'retries': 5,
      'max.in.flight.requests.per.connection': 1
    }
  });

  try {
    await producer.send({
      topic: 'user-events',
      messages: [{
        key: 'user-123',
        payload: Buffer.from(JSON.stringify({
          userId: 123,
          action: 'update'
        })),
        headers: {
          'correlation-id': 'txn-123',
          'source': 'user-service'
        }
      }]
    });
  } catch (error) {
    console.error('Error:', error);
  }
}
```

### Reconnecting Kafka Consumer

```javascript
import { KafkaClient } from 'kafka-crab-js'

const kafkaClient = new KafkaClient({
  brokers: 'localhost:29092',
  clientId: 'reconnect-test',
  logLevel: 'debug',
  brokerAddressFamily: 'v4',
  configuration: {
    'auto.offset.reset': 'earliest',
    'session.timeout.ms': 30000,
    'heartbeat.interval.ms': 10000
  },
})

/**
 * Creates and configures a new Kafka stream consumer
 */
async function createConsumer() {
  const kafkaStream = kafkaClient.createStreamConsumer({
    groupId: 'reconnect-test',
    enableAutoCommit: true,
  }, {
    highWaterMark: 100,
    objectMode: true
  })

  await kafkaStream.subscribe([
    { topic: 'foo', createTopic: true },
    { topic: 'bar', createTopic: true },
  ])
  return kafkaStream
}

/**
 * Starts a Kafka consumer with auto-restart capability
 */
async function startConsumer() {
  let counter = 0
  let retryCount = 0
  const MAX_RETRIES = 5
  const RETRY_DELAY = 5000 // 5 seconds

  async function handleRetry() {
    if (retryCount < MAX_RETRIES) {
      retryCount++
      console.log(
        `Attempting to restart consumer (attempt ${retryCount}/${MAX_RETRIES}) in ${RETRY_DELAY / 1000} seconds...`,
      )
      setTimeout(setupConsumerWithRetry, RETRY_DELAY)
    } else {
      console.error(`Maximum retry attempts (${MAX_RETRIES}) reached. Stopping consumer.`)
      process.exit(1)
    }
  }

  async function setupConsumerWithRetry() {
    try {
      const kafkaStream = await createConsumer()
      retryCount = 0 // Reset retry count on successful connection

      console.log('Starting consumer')

      kafkaStream.on('data', (message) => {
        counter++
        console.log('>>> Message received:', {
          counter,
          payload: message.payload.toString(),
          offset: message.offset,
          partition: message.partition,
          topic: message.topic,
        })
      })

      kafkaStream.on('error', async (error) => {
        console.error('Stream error:', error)
        handleRetry()
      })

      kafkaStream.on('close', () => {
        console.log('Stream ended')
        try {
          kafkaStream.unsubscribe()
        } catch (unsubError) {
          console.error('Error unsubscribing:', unsubError)
        }
      })
    } catch (error) {
      console.error('Error setting up consumer:', error)
      handleRetry()
    }
  }

  await setupConsumerWithRetry()
}

await startConsumer()
```

### Examples

You can find some examples on the [example](https://github.com/flash-tecnologia/kafka-crab-js/tree/main/example) folder of this project.

## Configuration

### KafkaConfiguration

| Property | Type | Default | Description |
| --- | --- | --- | --- |
| `brokers` | `string` || List of brokers to connect to |
| `clientId` | `string` | `"rdkafka"` | Client id to use for the connection |
| `securityProtocol` | `SecurityProtocol` || Security protocol to use (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL) |
| `logLevel` | `string` | `info`  | Log level for the client |
| `brokerAddressFamily` | `string` | `"v4"` | Address family to use for the connection (v4, v6) |
| `configuration` | `Record<string, any>` | `{}` | Additional configuration options for the client. **v2.0.0+**: Now supports any value type (string, number, boolean, object) |
| `otel` | `KafkaOtelInstrumentationConfig \| false` | `undefined` | Enable and configure OpenTelemetry instrumentation (see [OpenTelemetry Instrumentation](#opentelemetry-instrumentation)) |

### ConsumerConfiguration

| Property | Type | Default | Description |
| --- | --- | --- | --- |
| `groupId` | `string` || Consumer group ID |
| `enableAutoCommit` | `boolean` | `true` | Enable automatic offset commits |
| `configuration` | `Record<string, any>` | `{}` | Additional consumer configuration options |
| `fetchMetadataTimeout` | `number` | `60000` | Timeout for fetching metadata (ms) |
| `maxBatchMessages` | `number` | `1000` | Maximum messages in a batch operation |

### Consumer Commit Methods

kafka-crab-js provides two methods for committing offsets:

| Method | Signature | Description |
| --- | --- | --- |
| `commit` | `commit(topic, partition, offset, mode)` | Traditional commit - you must calculate `offset + 1` |
| `commitMessage` | `commitMessage(message, mode)` | **v2.1.0+**: Simplified commit - automatically handles offset increment |

```javascript
// Using commitMessage (recommended for v2.1.0+)
const message = await consumer.recv();
await consumer.commitMessage(message, 'Sync');

// Using commit (traditional)
const message = await consumer.recv();
await consumer.commit(message.topic, message.partition, message.offset + 1, 'Sync');
```

Both methods support `'Sync'` and `'Async'` commit modes.

### ProducerConfiguration

| Property | Type | Default | Description |
| --- | --- | --- | --- |
| `queueTimeout` | `number` | `5000` | Queue timeout in milliseconds |
| `autoFlush` | `boolean` | `true` | Enable automatic message flushing |
| `configuration` | `Record<string, any>` | `{}` | Additional producer configuration options |

### TopicPartitionConfig

| Property | Type | Default | Description |
| --- | --- | --- | --- |
| `topic` | `string` || Topic name |
| `allOffsets` | `OffsetModel` || Offset configuration for all partitions |
| `partitionOffset` | `Array<PartitionOffset>` || Per-partition offset configuration |
| `createTopic` | `boolean` | `false` | **v2.0.0+**: Create topic if it doesn't exist |
| `numPartitions` | `number` | `1` | **v2.0.0+**: Number of partitions when creating topic |
| `replicas` | `number` | `1` | **v2.0.0+**: Number of replicas when creating topic |

You can see the available options here: [librdkafka](https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.html).

### OpenTelemetry Configuration

OpenTelemetry instrumentation provides automatic tracing and optional metrics for Kafka operations. Tracing is enabled by default, while metrics are opt-in.

#### Minimal Configuration (Tracing Only)

```typescript
const client = new KafkaClient({
  brokers: 'localhost:9092',
  clientId: 'my-service',
  // Tracing enabled by default with sensible defaults
  otel: {
    serviceName: 'my-kafka-service', // Optional: defaults to 'kafka-client'
  },
})
```

#### Complete Configuration (All Options)

```typescript
const client = new KafkaClient({
  brokers: 'localhost:9092',
  clientId: 'my-service',
  otel: {
    // === Basic Configuration ===
    enabled: true,                    // Enable/disable all OTEL features (default: true)
    serviceName: 'my-kafka-service',  // Service name (default: process.env.OTEL_SERVICE_NAME || 'kafka-client')
    
    // === Tracing Configuration ===
    captureMessagePayload: false,     // Capture message payload in spans (default: false, security sensitive)
    maxPayloadSize: 1024,             // Max payload size to capture in bytes (default: 1024)
    captureMessageHeaders: true,      // Capture message header keys in spans (default: true; values are not recorded)
    enableBatchInstrumentation: true, // Enable batch operation spans (default: true)
    
    // === Broker Attribution (Optional) ===
    serverAddress: 'localhost',       // Broker address for span attributes
    serverPort: 9092,                 // Broker port for span attributes
    
    // === Topic Filtering ===
    ignoreTopics: ['__consumer_offsets', 'internal.*'], // Array of topics to exclude
    // OR use a function:
    // ignoreTopics: (topic) => topic.startsWith('internal.'),
    
    // === Custom Hooks ===
    producerHook: (span, record, metadata) => {
      // Add custom attributes to producer spans
      span.setAttribute('custom.message_count', record.messages.length)
      if (metadata) {
        span.setAttribute('custom.partition', metadata.partition)
      }
    },
    
    messageHook: (span, message) => {
      // Add custom attributes to consumer spans
      span.setAttribute('custom.message_size', message.payload.length)
      
      // Extract business data
      try {
        const data = JSON.parse(message.payload.toString())
        if (data.userId) {
          span.setAttribute('app.user_id', data.userId)
        }
      } catch { /* ignore */ }
    },
    
    // === Metrics Configuration (Opt-in) ===
    metrics: {
      enabled: false,                 // Enable metrics collection (default: false)
      includePartitionId: true,       // Include partition in metrics (default: true)
      serverAddress: 'localhost',     // Broker address for metrics
      serverPort: 9092,               // Broker port for metrics
      
      // Custom histogram buckets (optional, in seconds)
      // Default: [0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10]
      histogramBuckets: [0.001, 0.01, 0.1, 1, 10],
      
      // Custom meter provider (optional)
      // meterProvider: myMeterProvider,
    },
  },
})
```

#### Disable OpenTelemetry Entirely

```typescript
const client = new KafkaClient({
  brokers: 'localhost:9092',
  clientId: 'my-service',
  otel: false, // Completely disable OTEL instrumentation
})
```

#### Configuration Defaults

| Property | Default Value | Description |
|----------|--------------|-------------|
| `enabled` | `true` | Whether OTEL is enabled |
| `serviceName` | `process.env.OTEL_SERVICE_NAME \|\| 'kafka-client'` | Service name for telemetry |
| `captureMessagePayload` | `false` | Capture payloads (security sensitive) |
| `maxPayloadSize` | `1024` | Max payload size in bytes |
| `captureMessageHeaders` | `true` | Capture message header keys (values are not recorded) |
| `enableBatchInstrumentation` | `true` | Enable batch operation spans |
| `metrics.enabled` | `false` | Metrics are opt-in |
| `metrics.includePartitionId` | `true` | Include partition in metrics |

#### How It Works

When instrumentation is active:

- **Automatic Tracing**: Producer `send` calls automatically propagate the active span context via Kafka headers while preserving custom headers and their Buffer/string types.
- **Consumer Spans**: Consumer `recv`, `recvBatch`, and stream consumers generate spans that capture consumer group, topic, partition, offset, and batch size.
- **Context Propagation**: Trace context is automatically injected into producer messages and extracted from consumer messages, enabling distributed tracing across services.
- **Custom Hooks**: Hooks registered through `messageHook` and `producerHook` execute within the active span context so that additional attributes/events can be attached safely.
- **Metrics (Opt-in)**: When enabled, collects OpenTelemetry semantic convention compliant metrics for Kafka operations.

#### Examples

See comprehensive examples in the `example/` directory:
- `example/otel-tracing-example.mjs` - Complete tracing setup with Jaeger
- `example/otel-metrics-example.mjs` - Metrics collection with Prometheus
- `example/README.md` - Full documentation for all examples

## Performance Benchmarks

### Running Benchmarks

kafka-crab-js v2.0.0+ includes a comprehensive benchmark suite to compare performance against other popular Kafka clients:

```bash
# Set up benchmark environment (requires Kafka running locally)
pnpm add -D tsx  # For running TypeScript files directly
npx tsx benchmark/utils/setup.ts

# Run consumer performance benchmarks
node benchmark/consumer.ts
```

### Benchmark Results

*Benchmarks run on macOS with Apple M1 chip processing 50,000 messages*

```
╔════════════════════════╤═════════╤══════════════════╤═══════════╤══════════════════════════╗
║ Slower tests           │ Samples │           Result │ Tolerance │ Difference with previous ║
╟────────────────────────┼─────────┼──────────────────┼───────────┼──────────────────────────╢
║ kafkajs                │   50000 │   2316.67 op/sec │ ±  1.12 % │                          ║
║ kafka-crab-js (serial) │   50000 │  38414.57 op/sec │ ±  2.97 % │ + 1558.18 %              ║
║ node-rdkafka (stream)  │   50000 │  43681.44 op/sec │ ± 13.98 % │ + 13.71 %                ║
║ node-rdkafka (evented) │   53841 │  74137.97 op/sec │ ± 69.30 % │ + 69.72 %                ║
╟────────────────────────┼─────────┼──────────────────┼───────────┼──────────────────────────╢
║ Fastest test           │ Samples │           Result │ Tolerance │ Difference with previous ║
╟────────────────────────┼─────────┼──────────────────┼───────────┼──────────────────────────╢
║ kafka-crab-js (batch)  │   50000 │ 153533.53 op/sec │ ± 15.98 % │ + 107.09 %               ║
╚════════════════════════╧═════════╧══════════════════╧═══════════╧══════════════════════════╝
```

The benchmark suite compares:
- **kafka-crab-js (serial)**: Single message processing - **38,415 ops/sec**
- **kafka-crab-js (batch)**: Batch message processing - **153,534 ops/sec** (fastest)
- **node-rdkafka (evented)**: Event-based processing - **74,138 ops/sec**
- **node-rdkafka (stream)**: Stream-based processing - **43,681 ops/sec**
- **kafkajs**: Official KafkaJS client - **2,317 ops/sec**

Performance characteristics:
- **17x faster than kafkajs** in serial mode, **66x faster in batch mode**
- **High throughput**: Batch processing provides 4x performance improvement over serial mode
- **Low latency**: Optimized for both single and batch message processing
- **Memory efficient**: Lock-free data structures minimize memory overhead
- **Concurrent processing**: Zero-contention concurrent operations

### Benchmark Configuration

You can customize benchmark parameters in `benchmark/utils/definitions.ts`:

```typescript
export const topic = 'benchmarks'
export const brokers = ['localhost:9092', 'localhost:9093', 'localhost:9094']

// Benchmark parameters can be adjusted in consumer.ts:
const iterations = 10_000  // Number of messages to process
const maxBytes = 200       // Maximum message size
```

## Best Practices

### Error Handling
- Always wrap async operations in try-catch blocks
- Implement proper error logging and monitoring
- Handle both operational and programming errors separately

### Performance
- Use batch operations for high-throughput scenarios
- Configure appropriate batch sizes and compression settings
- Monitor and tune consumer group performance
- Leverage the benchmark suite to optimize your specific use case

### Configuration (v2.0.0+)
- Use the flexible configuration system with proper data types:
  ```javascript
  const config = {
    'batch.size': 16384,           // number
    'compression.type': 'snappy',  // string
    'enable.idempotence': true,    // boolean
    'retries': 5                   // number
  }
  ```

### Message Processing
- Validate message formats before processing
- Implement proper serialization/deserialization
- Handle message ordering when required
- Use topic creation options for better topic management

### Stream Processing (v2.0.0+)
- Configure appropriate `ReadableOptions` for your use case
- Use `objectMode: true` for structured message processing
- Set appropriate `highWaterMark` based on memory constraints

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Run the benchmark suite to ensure performance isn't degraded
4. Commit your changes (`git commit -m 'Add some amazing feature'`)
5. Push to the branch (`git push origin feature/amazing-feature`)
6. Open a Pull Request

### Development Commands

```bash
# Build the project
pnpm build

# Run type checking
pnpm typecheck

# Run linting
pnpm lint

# Format code
pnpm fmt

# Run benchmarks
npx tsx benchmark/utils/setup.ts
npx tsx benchmark/consumer.ts
```

## OpenTelemetry Instrumentation

Kafka Crab JS offers turnkey tracing for Kafka workloads:

- **Seamless propagation** – Producer instrumentation injects `traceparent`/`tracestate` into Kafka headers while retaining any existing headers (including `Buffer` values) so downstream systems continue to see custom metadata.
- **Consumer & stream coverage** – Standard consumers, batch consumers, and `createStreamConsumer` streams emit spans that include consumer group, topic, partition, offset, and batch size semantics.
- **Hook-friendly spans** – Both `messageHook` and `producerHook` callbacks run inside the active span context, simplifying attribute decoration or error handling.
- **Header normalization helpers** – The runtime automatically normalizes mixed header carriers to Buffers when talking to the native binding, removing the need for manual conversions.

### Consumer span lifecycle (important)

Kafka Crab JS creates consumer `process <topic>` spans, but only your application knows when processing is complete.

- For single-message consumers, call `message.endSpan()` when you're done processing the message.
- For batch consumers, call `batch.endSpan()` (the returned array is augmented) when you're done processing the batch.
- To avoid optional chaining in user code, use the helper `endSpan(message)` (or `EndSpan(message)`).
- For automatic end + error capture, wrap your handler with `await client.otel.processMessage(message, async (msg) => { ... })` (or `processBatch`).

This closes the span(s) and (when metrics are enabled) records `messaging.process.duration`.

If you prefer not to mutate the native `Message` objects, set `otel.decorateMessages=false`. In that mode,
`recv()`/`recvBatch()` return shallow clones augmented with the same OTEL helper fields.

### Global instrumentation (singleton)

`kafka-crab-js` uses a process-wide OpenTelemetry instrumentation singleton.

- Creating multiple `KafkaClient` instances shares the same instrumentation and configuration.
- Passing different `otel` configs to different clients updates the same singleton (the latest config wins).
- For tests, use `resetKafkaInstrumentation()` to clear the singleton between runs.

### Minimal Setup Example

When using stream consumers, the easiest pattern is calling `endSpan()` in a `finally` block:

```ts
import { KafkaClient, endSpan } from 'kafka-crab-js'
import { NodeTracerProvider } from '@opentelemetry/sdk-trace-node'
import { SimpleSpanProcessor } from '@opentelemetry/sdk-trace-base'
import { AsyncHooksContextManager } from '@opentelemetry/context-async-hooks'
import { context } from '@opentelemetry/api'

const provider = new NodeTracerProvider()
provider.addSpanProcessor(new SimpleSpanProcessor(exporter))
provider.register()

context.setGlobalContextManager(new AsyncHooksContextManager().enable())

const client = new KafkaClient({
  brokers: 'localhost:29092',
  clientId: 'orders-api',
  otel: {
    serviceName: 'orders-service',
    producerHook: span => span.setAttribute('messaging.client.kind', 'producer'),
  },
})

const producer = client.createProducer()
await producer.send({
  topic: 'orders',
  messages: [{
    payload: Buffer.from(JSON.stringify({ orderId: '123' })),
    headers: { 'custom-header': 'foo' },
  }],
})

const consumer = client.createStreamConsumer({
  groupId: 'orders-consumer',
  enableAutoCommit: false,
})

consumer.on('data', message => {
  try {
    console.log(message.headers?.['custom-header']?.toString())
  } finally {
    // IMPORTANT: close the "process <topic>" span when your app finishes handling the message
    endSpan(message)
  }
})
```

Set `otel: false` in the client configuration to opt-out.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

<div align="center">

[![Built with Rust](https://img.shields.io/badge/Built%20with-Rust-orange)](https://www.rust-lang.org/)

</div>
