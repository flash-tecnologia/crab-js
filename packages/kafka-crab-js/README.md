<div align="center">

# 🦀 Kafka Crab JS 🦀

A lightweight, flexible, and reliable Kafka client for JavaScript/TypeScript. It is built using Rust and librdkafka, providing a high-performance and feature-rich Kafka client.

[![npm version](https://img.shields.io/npm/v/kafka-crab-js.svg)](https://www.npmjs.com/package/kafka-crab-js)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

</div>

---

## What's New in Version 3.0.0

### BREAKING CHANGES ⚠️

**OpenTelemetry instrumentation has been moved to a separate package: `kafka-crab-js-otel`**

This change reduces the core package size and makes OTEL an opt-in dependency.

#### Migration from v2.x

**Before (v2.x):**

```javascript
import { KafkaClient } from 'kafka-crab-js'

const client = new KafkaClient({
  brokers: 'localhost:9092',
  clientId: 'my-app',
  otel: {
    serviceName: 'my-service',
    metrics: { enabled: true },
  },
})
```

**After (v3.x):**

```javascript
import { KafkaClient } from 'kafka-crab-js'
import { enableOtelInstrumentation, endSpan } from 'kafka-crab-js-otel'

// 1. Enable OTEL instrumentation BEFORE creating client
// Note: serviceName is set via OTEL SDK Resource, not here
enableOtelInstrumentation({
  metrics: { enabled: true },
})

// 2. Create client with diagnostics enabled
const client = new KafkaClient({
  brokers: 'localhost:9092',
  clientId: 'my-app',
  diagnostics: true, // Required for OTEL to receive events
})

// 3. For consumers, call endSpan() when processing is complete
const message = await consumer.recv()
// ... process message ...
endSpan(message)
```

#### Key Changes

| v2.x                         | v3.x                                                    |
| ---------------------------- | ------------------------------------------------------- |
| `otel` config in KafkaClient | `enableOtelInstrumentation()` from `kafka-crab-js-otel` |
| Automatic span ending        | Call `endSpan(message)` manually for consumers          |
| N/A                          | `diagnostics: true` required in KafkaClient config      |
| OTEL bundled                 | Install `kafka-crab-js-otel` separately                 |

#### New Package Structure

| Package              | Description                                      |
| -------------------- | ------------------------------------------------ |
| `kafka-crab-js`      | Core Kafka client (producer, consumer, streams)  |
| `kafka-crab-js-otel` | OpenTelemetry instrumentation (separate install) |

### Internal Improvements

- Uses Node.js `diagnostics_channel` for observability (zero overhead when not subscribed)
- Cleaner separation of concerns between core and observability
- Smaller bundle size for users who don't need OTEL

---

## Previous Versions

<details>
<summary>What's New in Version 2.1.0</summary>

### New Features

1. **Simplified Message Commits with `commitMessage()`**:
   - New convenience method that accepts a message and commit mode directly
   - Automatically handles `offset + 1` increment internally - no more manual offset arithmetic
   - Available on both `KafkaConsumer` and stream consumers
   - **Before** (v2.0.0):
     ```javascript
     const message = await consumer.recv()
     await consumer.commit(message.topic, message.partition, message.offset + 1, 'Sync')
     ```
   - **After** (v2.1.0):
     ```javascript
     const message = await consumer.recv()
     await consumer.commitMessage(message, 'Sync')
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

</details>

<details>
<summary>What's New in Version 2.0.0</summary>

### BREAKING CHANGES ⚠️

1. **Consumer Configuration API Changes**:
   - **REMOVED**: `createTopic` field from `ConsumerConfiguration`
   - **Migration**: Use `createTopic` field in `TopicPartitionConfig` instead when subscribing to topics

2. **Stream Lifecycle Management**:
   - Stream consumers now properly implement Node.js stream lifecycle methods (`_destroy()`)
   - **Memory leak prevention**: Streams now automatically disconnect Kafka consumers during destruction

3. **Async Consumer Commit**:
   - **BREAKING**: The `consumer.commit()` method is now async and must be awaited

</details>

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
import { KafkaClient } from 'kafka-crab-js'
async function run() {
  const kafkaClient = new KafkaClient({
    brokers: 'localhost:29092',
    clientId: 'foo-client',
    logLevel: 'debug',
    brokerAddressFamily: 'v4',
  })

  // Create consumer with topic creation control
  const consumer = kafkaClient.createConsumer({
    groupId: 'foo-group',
    configuration: {
      'auto.offset.reset': 'earliest',
      'enable.auto.commit': false, // Use manual commit for better control
    },
  })

  // Subscribe with topic creation options
  await consumer.subscribe([
    {
      topic: 'foo',
      createTopic: true,
      numPartitions: 3,
    },
  ])

  const message = await consumer.recv()
  const { payload, partition, offset, topic } = message
  console.log({
    topic,
    partition,
    offset,
    value: payload.toString(),
  })

  // Manual commit - two options:
  // Option 1 (v2.1.0+): Simplified with commitMessage
  await consumer.commitMessage(message, 'Sync')

  // Option 2: Traditional commit with manual offset increment
  // await consumer.commit(topic, partition, offset + 1, 'Sync');

  consumer.unsubscribe()
}

await run()
```

### Basic Producer Setup

```javascript
import { KafkaClient } from 'kafka-crab-js'

const kafkaClient = new KafkaClient({
  brokers: 'localhost:29092',
  clientId: 'my-client-id',
  logLevel: 'info',
  brokerAddressFamily: 'v4',
})

// Producer configuration is now optional with sensible defaults
const producer = kafkaClient.createProducer({
  configuration: {
    'message.timeout.ms': 5000, // Now supports number values
    'batch.size': 16384,
    'compression.type': 'snappy',
  },
})

const message = {
  id: 1,
  name: 'Sample Message',
  timestamp: new Date().toISOString(),
}

const result = await producer.send({
  topic: 'my-topic',
  messages: [
    {
      payload: Buffer.from(JSON.stringify(message)),
    },
  ],
})

const errors = result.map((r) => r.error).filter(Boolean)
if (errors.length > 0) {
  console.error('Error sending message:', errors)
} else {
  console.log('Message sent. Offset:', result)
}
```

## Stream Processing

### Enhanced Stream Consumer Example

```javascript
import { KafkaClient } from 'kafka-crab-js'

const kafkaClient = new KafkaClient({
  brokers: 'localhost:29092',
  clientId: 'my-client-id',
  logLevel: 'info',
  brokerAddressFamily: 'v4',
})

// Stream consumer with custom ReadableOptions (v2.0.0+)
const kafkaStream = kafkaClient.createStreamConsumer(
  {
    groupId: `my-group-id`,
    enableAutoCommit: true,
  },
  {
    objectMode: true, // Default in v2.0.0+
    highWaterMark: 1024,
    encoding: null,
  },
)

await kafkaStream.subscribe([
  { topic: 'foo', createTopic: true },
  { topic: 'bar', createTopic: true },
])

kafkaStream.on('data', (message) => {
  console.log('>>> Message received:', {
    payload: message.payload.toString(),
    offset: message.offset,
    partition: message.partition,
    topic: message.topic,
  })

  if (message.offset > 10) {
    kafkaStream.destroy()
  }
})

kafkaStream.on('close', () => {
  kafkaStream.unsubscribe()
  console.log('Stream ended')
})
```

## Producer Examples

### Batch Message Production

```javascript
const kafkaClient = new KafkaClient({
  brokers: 'localhost:29092',
  clientId: 'my-client-id',
  brokerAddressFamily: 'v4',
})

// Enhanced producer with flexible configuration
const producer = kafkaClient.createProducer({
  configuration: {
    'batch.size': 50000, // Number value supported
    'linger.ms': 10, // Number value supported
    'compression.type': 'lz4',
    'enable.idempotence': true, // Boolean value supported
  },
})

const messages = Array.from({ length: 100 }, (_, i) => ({
  payload: Buffer.from(
    JSON.stringify({
      _id: i,
      name: `Batch Message ${i}`,
      timestamp: new Date().toISOString(),
    }),
  ),
}))

try {
  const result = await producer.send({
    topic: 'my-topic',
    messages,
  })
  console.log('Batch sent. Offset:', result)
  console.assert(result.length === 100)
} catch (error) {
  console.error('Batch error:', error)
}
```

### Producer with Keys and Headers

```javascript
async function produceWithMetadata() {
  const producer = kafkaClient.createProducer({
    configuration: {
      acks: 'all',
      retries: 5,
      'max.in.flight.requests.per.connection': 1,
    },
  })

  try {
    await producer.send({
      topic: 'user-events',
      messages: [
        {
          key: 'user-123',
          payload: Buffer.from(
            JSON.stringify({
              userId: 123,
              action: 'update',
            }),
          ),
          headers: {
            'correlation-id': 'txn-123',
            source: 'user-service',
          },
        },
      ],
    })
  } catch (error) {
    console.error('Error:', error)
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
    'heartbeat.interval.ms': 10000,
  },
})

/**
 * Creates and configures a new Kafka stream consumer
 */
async function createConsumer() {
  const kafkaStream = kafkaClient.createStreamConsumer(
    {
      groupId: 'reconnect-test',
      enableAutoCommit: true,
    },
    {
      highWaterMark: 100,
      objectMode: true,
    },
  )

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

| Property              | Type                  | Default     | Description                                                         |
| --------------------- | --------------------- | ----------- | ------------------------------------------------------------------- |
| `brokers`             | `string`              |             | List of brokers to connect to                                       |
| `clientId`            | `string`              | `"rdkafka"` | Client id to use for the connection                                 |
| `securityProtocol`    | `SecurityProtocol`    |             | Security protocol to use (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL) |
| `logLevel`            | `string`              | `info`      | Log level for the client                                            |
| `brokerAddressFamily` | `string`              | `"v4"`      | Address family to use for the connection (v4, v6)                   |
| `configuration`       | `Record<string, any>` | `{}`        | Additional configuration options for the client                     |
| `diagnostics`         | `boolean`             | `false`     | **v3.0.0+**: Enable diagnostics channel for OTEL instrumentation    |

### ConsumerConfiguration

| Property               | Type                  | Default | Description                               |
| ---------------------- | --------------------- | ------- | ----------------------------------------- |
| `groupId`              | `string`              |         | Consumer group ID                         |
| `enableAutoCommit`     | `boolean`             | `true`  | Enable automatic offset commits           |
| `configuration`        | `Record<string, any>` | `{}`    | Additional consumer configuration options |
| `fetchMetadataTimeout` | `number`              | `60000` | Timeout for fetching metadata (ms)        |
| `maxBatchMessages`     | `number`              | `1000`  | Maximum messages in a batch operation     |

### Consumer Commit Methods

kafka-crab-js provides two methods for committing offsets:

| Method          | Signature                                | Description                                                             |
| --------------- | ---------------------------------------- | ----------------------------------------------------------------------- |
| `commit`        | `commit(topic, partition, offset, mode)` | Traditional commit - you must calculate `offset + 1`                    |
| `commitMessage` | `commitMessage(message, mode)`           | **v2.1.0+**: Simplified commit - automatically handles offset increment |

```javascript
// Using commitMessage (recommended for v2.1.0+)
const message = await consumer.recv()
await consumer.commitMessage(message, 'Sync')

// Using commit (traditional)
const message = await consumer.recv()
await consumer.commit(message.topic, message.partition, message.offset + 1, 'Sync')
```

Both methods support `'Sync'` and `'Async'` commit modes.

### ProducerConfiguration

| Property        | Type                  | Default | Description                               |
| --------------- | --------------------- | ------- | ----------------------------------------- |
| `queueTimeout`  | `number`              | `5000`  | Queue timeout in milliseconds             |
| `autoFlush`     | `boolean`             | `true`  | Enable automatic message flushing         |
| `configuration` | `Record<string, any>` | `{}`    | Additional producer configuration options |

### TopicPartitionConfig

| Property          | Type                     | Default | Description                                           |
| ----------------- | ------------------------ | ------- | ----------------------------------------------------- |
| `topic`           | `string`                 |         | Topic name                                            |
| `allOffsets`      | `OffsetModel`            |         | Offset configuration for all partitions               |
| `partitionOffset` | `Array<PartitionOffset>` |         | Per-partition offset configuration                    |
| `createTopic`     | `boolean`                | `false` | **v2.0.0+**: Create topic if it doesn't exist         |
| `numPartitions`   | `number`                 | `1`     | **v2.0.0+**: Number of partitions when creating topic |
| `replicas`        | `number`                 | `1`     | **v2.0.0+**: Number of replicas when creating topic   |

You can see the available options here: [librdkafka](https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.html).

### OpenTelemetry Instrumentation

> **Note:** Starting with v3.0.0, OpenTelemetry instrumentation has been moved to a separate package: [`kafka-crab-js-otel`](https://www.npmjs.com/package/kafka-crab-js-otel)

#### Installation

```bash
npm install kafka-crab-js-otel @opentelemetry/api
```

#### Usage

```javascript
import { KafkaClient } from 'kafka-crab-js'
import { enableOtelInstrumentation, endSpan } from 'kafka-crab-js-otel'

// Enable instrumentation before creating client
// Note: serviceName is configured via OTEL SDK Resource
enableOtelInstrumentation({
  metrics: { enabled: true },
})

// Create client with diagnostics enabled
const client = new KafkaClient({
  brokers: 'localhost:9092',
  clientId: 'my-app',
  diagnostics: true,
})

// For consumers, call endSpan() when done processing
const message = await consumer.recv()
// ... process message ...
endSpan(message)
```

For complete configuration options and examples, see the [kafka-crab-js-otel README](../kafka-crab-js-otel/README.md).

#### Examples

See comprehensive examples in the `example/` directory:

- `example/otel-tracing-example.mjs` - Complete tracing setup with Jaeger
- `example/otel-metrics-example.mjs` - Metrics collection with Prometheus
- `example/README.md` - Full documentation for all examples

## Performance Benchmarks

### Running Benchmarks

kafka-crab-js v2.0.0+ includes a comprehensive benchmark suite to compare performance against other popular Kafka clients:

```bash
cd ../benchmark

# Set up benchmark environment (requires Kafka running locally)
vp install
vp run setup:consumer

# Run consumer performance benchmarks
vp run benchmark
```

### Benchmark Results

_Benchmarks run on macOS with Apple M1 chip processing 50,000 messages (December 2024)_

```
╔════════════════════════╤═════════╤══════════════════╤═══════════╤══════════════════════════╗
║ Slower tests           │ Samples │           Result │ Tolerance │ Difference with previous ║
╟────────────────────────┼─────────┼──────────────────┼───────────┼──────────────────────────╢
║ kafkajs                │   50000 │    834.12 op/sec │ ±  0.22 % │                          ║
║ node-rdkafka (evented) │   84115 │  24922.67 op/sec │ ± 74.82 % │ + 2887.91 %              ║
║ kafka-crab-js (serial) │   50000 │  43213.86 op/sec │ ±  3.46 % │ + 73.39 %                ║
║ node-rdkafka (stream)  │   50000 │  49805.32 op/sec │ ± 27.10 % │ + 15.25 %                ║
╟────────────────────────┼─────────┼──────────────────┼───────────┼──────────────────────────╢
║ Fastest test           │ Samples │           Result │ Tolerance │ Difference with previous ║
╟────────────────────────┼─────────┼──────────────────┼───────────┼──────────────────────────╢
║ kafka-crab-js (batch)  │   50000 │ 205985.31 op/sec │ ± 16.53 % │ + 313.58 %               ║
╚════════════════════════╧═════════╧══════════════════╧═══════════╧══════════════════════════╝
```

The benchmark suite compares:

- **kafka-crab-js (serial)**: Single message processing - **43,214 ops/sec**
- **kafka-crab-js (batch)**: Batch message processing - **205,985 ops/sec** (fastest)
- **node-rdkafka (evented)**: Event-based processing - **24,923 ops/sec**
- **node-rdkafka (stream)**: Stream-based processing - **49,805 ops/sec**
- **kafkajs**: Official KafkaJS client - **834 ops/sec**

Performance characteristics:

- **52x faster than kafkajs** in serial mode, **247x faster in batch mode**
- **High throughput**: Batch processing provides 4.8x performance improvement over serial mode
- **Low latency**: Optimized for both single and batch message processing
- **Memory efficient**: Lock-free data structures minimize memory overhead
- **Concurrent processing**: Zero-contention concurrent operations

### Benchmark Configuration

You can customize benchmark parameters in `benchmark/utils/definitions.ts`:

```typescript
export const topic = 'benchmarks'
export const brokers = ['localhost:9092', 'localhost:9093', 'localhost:9094']

// Benchmark parameters can be adjusted in consumer.ts:
const iterations = 10_000 // Number of messages to process
const maxBytes = 200 // Maximum message size
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
    'batch.size': 16384, // number
    'compression.type': 'snappy', // string
    'enable.idempotence': true, // boolean
    retries: 5, // number
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
vp run build

# Run type checking
vp run typecheck

# Run linting and formatting
vp check

# Run benchmarks
cd ../benchmark
vp run benchmark
```

## OpenTelemetry Instrumentation

> **Note:** Starting with v3.0.0, OpenTelemetry instrumentation has been moved to a separate package: [`kafka-crab-js-otel`](https://www.npmjs.com/package/kafka-crab-js-otel)

Kafka Crab JS offers turnkey tracing for Kafka workloads:

- **Seamless propagation** – Producer instrumentation injects `traceparent`/`tracestate` into Kafka headers while retaining any existing headers (including `Buffer` values) so downstream systems continue to see custom metadata.
- **Consumer & stream coverage** – Standard consumers, batch consumers, and `createStreamConsumer` streams emit spans that include consumer group, topic, partition, offset, and batch size semantics.
- **Hook-friendly spans** – Both `messageHook` and `producerHook` callbacks run inside the active span context, simplifying attribute decoration or error handling.
- **Zero overhead when disabled** – Uses Node.js `diagnostics_channel` for near-zero cost when OTEL is not active.

### Consumer span lifecycle (important)

Kafka Crab JS creates consumer `process <topic>` spans, but only your application knows when processing is complete.

- For single-message consumers, call `endSpan(message)` when you're done processing the message.
- For batch consumers, call `endSpan(batch)` when you're done processing the batch.
- The `endSpan()` helper is exported from `kafka-crab-js-otel`.

This closes the span(s) and (when metrics are enabled) records `messaging.process.duration`.

### Global instrumentation (singleton)

`kafka-crab-js-otel` uses a process-wide OpenTelemetry instrumentation singleton.

- Call `enableOtelInstrumentation()` **before** creating any `KafkaClient` instances.
- Creating multiple `KafkaClient` instances shares the same instrumentation.
- For tests, use `resetKafkaInstrumentation()` to clear the singleton between runs.

### Minimal Setup Example

When using stream consumers, the easiest pattern is calling `endSpan()` in a `finally` block:

```ts
import { KafkaClient } from 'kafka-crab-js'
import { enableOtelInstrumentation, endSpan } from 'kafka-crab-js-otel'
import { NodeTracerProvider } from '@opentelemetry/sdk-trace-node'
import { SimpleSpanProcessor } from '@opentelemetry/sdk-trace-base'
import { AsyncHooksContextManager } from '@opentelemetry/context-async-hooks'
import { context } from '@opentelemetry/api'

// 1. Set up OpenTelemetry SDK
const provider = new NodeTracerProvider()
provider.addSpanProcessor(new SimpleSpanProcessor(exporter))
provider.register()
context.setGlobalContextManager(new AsyncHooksContextManager().enable())

// 2. Enable kafka-crab-js instrumentation BEFORE creating client
enableOtelInstrumentation({
  captureMessagePayload: true,
  captureMessageHeaders: true,
  producerHook: (span) => span.setAttribute('messaging.client.kind', 'producer'),
})

// 3. Create client with diagnostics enabled
const client = new KafkaClient({
  brokers: 'localhost:29092',
  clientId: 'orders-api',
  diagnostics: true, // Required for OTEL to receive events
})

// 4. Use producer - spans are created automatically
const producer = client.createProducer()
await producer.send({
  topic: 'orders',
  messages: [
    {
      payload: Buffer.from(JSON.stringify({ orderId: '123' })),
      headers: { 'custom-header': 'foo' },
    },
  ],
})

// 5. Use stream consumer with proper cleanup
const consumer = client.createStreamConsumer({
  groupId: 'orders-consumer',
  enableAutoCommit: false,
})

await consumer.subscribe('orders')

consumer.on('data', (message) => {
  try {
    console.log(message.headers?.['custom-header']?.toString())
  } finally {
    // IMPORTANT: close the "process <topic>" span when your app finishes handling the message
    endSpan(message)
  }
})

// Proper cleanup for stream consumers - use destroy() not disconnect()
consumer.on('close', () => console.log('Consumer closed'))
// consumer.destroy()
```

To disable OTEL, simply don't call `enableOtelInstrumentation()` or set `diagnostics: false` in the client config.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

<div align="center">

[![Built with Rust](https://img.shields.io/badge/Built%20with-Rust-orange)](https://www.rust-lang.org/)

</div>
