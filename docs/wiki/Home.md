# Crab JS Documentation

Documentation for the Crab JS native Node.js packages. This wiki currently focuses on `kafka-crab-js`, the Kafka
binding built with Rust for high performance and type safety.

## Installation

```bash
pnpm install kafka-crab-js
```

## What's New in Version 3.0.0

### Breaking Changes

**OpenTelemetry instrumentation has been moved to a separate package: `kafka-crab-js-otel`**

This change reduces the core package size and makes OTEL an opt-in dependency.

**Before (v2.x):**

```typescript
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

```typescript
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

### Package Structure

| Package              | Description                                      |
| -------------------- | ------------------------------------------------ |
| `kafka-crab-js`      | Core Kafka client (producer, consumer, streams)  |
| `pdf-crab-js`        | Native PDF generation and rendering helpers      |
| `kafka-crab-js-otel` | OpenTelemetry instrumentation (separate install) |

## Basic Usage

### Creating a Kafka Client

```typescript
import { KafkaClient } from 'kafka-crab-js'

const kafkaClient = new KafkaClient({
  brokers: 'localhost:9092',
  clientId: 'my-app',
  logLevel: 'info',
  brokerAddressFamily: 'v4',
  // Optional additional configuration
  configuration: {
    'auto.offset.reset': 'earliest',
  },
  // v3.0.0+: Enable for OTEL instrumentation
  diagnostics: true,
})
```

### Producer Example

```typescript
async function produceMessages() {
  const producer = kafkaClient.createProducer({
    configuration: {
      'message.timeout.ms': '5000',
    },
  })

  try {
    const result = await producer.send({
      topic: 'my-topic',
      messages: [
        {
          key: Buffer.from('message-key'),
          headers: { 'correlation-id': Buffer.from('correlation-123') },
          payload: Buffer.from(JSON.stringify({ id: 1, name: 'Test Message' })),
        },
      ],
    })

    console.log('Message sent. Offset:', result)
  } catch (error) {
    console.error('Error sending message', error)
  }
}
```

### Basic Consumer Example

```typescript
async function consumeMessages() {
  const consumer = kafkaClient.createConsumer({
    groupId: 'my-group-id',
    configuration: {
      'auto.offset.reset': 'earliest',
    },
  })

  await consumer.subscribe('my-topic')

  try {
    while (true) {
      const message = await consumer.recv()
      if (!message) {
        console.log('Consumer disconnected')
        break
      }

      console.log('Received message:', {
        payload: message.payload.toString(),
        partition: message.partition,
        offset: message.offset,
        headers: Object.entries(message.headers).map(([k, v]) => ({ [k]: v.toString() })),
      })
    }
  } finally {
    await consumer.disconnect()
  }
}
```

### Stream Consumer Example

```typescript
async function streamConsumer() {
  const kafkaStream = kafkaClient.createStreamConsumer({
    groupId: 'my-stream-group',
    enableAutoCommit: true,
    configuration: {
      'auto.offset.reset': 'earliest',
    },
  })

  await kafkaStream.subscribe([
    { topic: 'my-topic' },
    // Or for specific offsets:
    // { topic: 'my-topic', allOffsets: { position: 'Beginning' } }
  ])

  kafkaStream.on('data', (message) => {
    console.log('Message received:', {
      payload: message.payload.toString(),
      offset: message.offset,
      partition: message.partition,
      topic: message.topic,
    })
  })

  kafkaStream.on('error', (error) => {
    console.error('Stream error:', error)
  })

  kafkaStream.on('close', () => {
    console.log('Stream ended')
  })

  // Proper cleanup - use destroy() for streams
  // This ensures all async operations complete before the stream closes
  // kafkaStream.destroy();
}
```

### Batch Stream Consumer (v3.0.0+)

```typescript
async function batchStreamConsumer() {
  // Create batch stream consumer with batch configuration
  const batchStream = kafkaClient.createStreamConsumer({
    groupId: 'my-batch-group',
    enableAutoCommit: true,
    batchSize: 10, // Process up to 10 messages at a time
    batchTimeout: 1000, // Wait up to 1000ms for batch to fill
  })

  await batchStream.subscribe([{ topic: 'my-topic' }])

  // Get batch configuration
  const config = batchStream.getBatchConfig()
  console.log('Batch config:', config) // { batchSize: 10, batchTimeout: 1000 }

  batchStream.on('data', (message) => {
    // Messages are delivered individually but fetched in batches
    console.log('Message received:', message.payload.toString())
  })

  batchStream.on('error', (error) => {
    console.error('Stream error:', error)
  })
}
```

### Proper Stream Cleanup

When working with stream consumers, always use `destroy()` for proper cleanup. This ensures all async operations complete before the stream closes and prevents errors after cleanup.

```typescript
/**
 * Properly cleans up a stream consumer
 */
async function cleanupStreamConsumer(streamConsumer) {
  if (!streamConsumer) return

  return new Promise((resolve) => {
    // If already destroyed, resolve immediately
    if (streamConsumer.destroyed) {
      resolve()
      return
    }

    // Wait for close event which fires after destroy is complete
    streamConsumer.once('close', () => {
      resolve()
    })

    // Destroy the stream - this triggers _destroy() which handles
    // unsubscribe and disconnect properly
    streamConsumer.destroy()
  })
}

// Usage
const stream = kafkaClient.createStreamConsumer({ groupId: 'my-group' })
await stream.subscribe('my-topic')

// ... process messages ...

// Cleanup
await cleanupStreamConsumer(stream)
```

### Enabling OpenTelemetry Instrumentation (v3.0.0+)

OpenTelemetry instrumentation is now in a separate package for reduced bundle size.

```typescript
import { KafkaClient } from 'kafka-crab-js'
import { enableOtelInstrumentation, endSpan } from 'kafka-crab-js-otel'
import { NodeTracerProvider } from '@opentelemetry/sdk-trace-node'
import { SimpleSpanProcessor } from '@opentelemetry/sdk-trace-base'
import { AsyncHooksContextManager } from '@opentelemetry/context-async-hooks'
import { context } from '@opentelemetry/api'

// Set up OpenTelemetry
const provider = new NodeTracerProvider()
provider.addSpanProcessor(new SimpleSpanProcessor(exporter))
provider.register()
context.setGlobalContextManager(new AsyncHooksContextManager().enable())

// Enable kafka-crab-js instrumentation
enableOtelInstrumentation({
  captureMessagePayload: true,
  captureMessageHeaders: true,
})

// Create client with diagnostics enabled
const client = new KafkaClient({
  brokers: 'localhost:9092',
  clientId: 'otel-example',
  diagnostics: true, // Required for OTEL
})

// Producer - spans are created automatically
const producer = client.createProducer()
await producer.send({
  topic: 'otel-topic',
  messages: [
    {
      payload: Buffer.from('hello world'),
      headers: { 'custom-header': 'value' },
    },
  ],
})

// Consumer - call endSpan() when done processing
const consumer = client.createConsumer({ groupId: 'otel-group' })
await consumer.subscribe('otel-topic')
const message = await consumer.recv()
// ... process message ...
endSpan(message) // End the processing span
```

### Consumer with Retry Logic

```typescript
async function consumerWithRetry() {
  const MAX_RETRIES = 5
  const RETRY_DELAY = 5000 // 5 seconds
  let retryCount = 0

  async function createConsumer() {
    const kafkaStream = kafkaClient.createStreamConsumer({
      groupId: 'retry-example-group',
      enableAutoCommit: true,
      configuration: {
        'auto.offset.reset': 'earliest',
      },
    })

    await kafkaStream.subscribe([{ topic: 'my-topic' }])

    return kafkaStream
  }

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
    let kafkaStream
    try {
      kafkaStream = await createConsumer()
      retryCount = 0 // Reset retry count on successful connection

      kafkaStream.on('data', (message) => {
        console.log('Message received:', {
          payload: message.payload.toString(),
          offset: message.offset,
          partition: message.partition,
          topic: message.topic,
        })
      })

      kafkaStream.on('error', async (error) => {
        console.error('Stream error:', error)
        // Use destroy() for proper cleanup before retry
        kafkaStream.destroy()
        handleRetry()
      })

      kafkaStream.on('close', () => {
        console.log('Stream ended')
      })
    } catch (error) {
      console.error('Error setting up consumer:', error)
      if (kafkaStream) {
        kafkaStream.destroy()
      }
      handleRetry()
    }
  }

  await setupConsumerWithRetry()
}
```

## API Reference

### KafkaClient

Main client class that creates producers and consumers.

#### Constructor

```typescript
new KafkaClient(kafkaConfiguration: KafkaConfiguration)
```

#### Configuration Options

```typescript
interface KafkaConfiguration {
  brokers: string // Comma-separated list of brokers
  clientId?: string // Client identifier
  logLevel?: 'debug' | 'info' | 'warning' | 'error' // Default: 'info'
  brokerAddressFamily?: 'v4' | 'v6' // IP version, default: 'v4'
  securityProtocol?: string // Default: 'Plaintext'
  diagnostics?: boolean // v3.0.0+: Enable diagnostics channel for OTEL
  configuration?: {
    // Additional librdkafka configuration
    [key: string]: string
  }
}
```

#### Methods

- **createProducer(config: ProducerConfiguration): KafkaProducer**
  Creates a Kafka producer

- **createConsumer(config: ConsumerConfiguration): KafkaConsumer**
  Creates a Kafka consumer

- **createStreamConsumer(config: StreamConsumerConfiguration): KafkaStreamReadable | KafkaBatchStreamReadable**
  Creates a stream-based Kafka consumer. Returns `KafkaBatchStreamReadable` if `batchSize > 1` is specified.

### KafkaProducer

#### Configuration

```typescript
interface ProducerConfiguration {
  configuration?: {
    // Additional producer configuration
    [key: string]: string
  }
}
```

#### Methods

- **send(record: ProducerRecord): Promise<RecordMetadata[]>**
  Sends messages to Kafka and returns offset information

```typescript
interface ProducerRecord {
  topic: string
  messages: Array<{
    key?: Buffer
    payload: Buffer
    headers?: Record<string, Buffer>
  }>
}
```

### KafkaConsumer

#### Configuration

```typescript
interface ConsumerConfiguration {
  groupId: string // Consumer group ID
  enableAutoCommit?: boolean // Whether to auto-commit offsets
  configuration?: {
    // Additional consumer configuration
    'auto.offset.reset'?: 'earliest' | 'latest'
    [key: string]: string
  }
}
```

#### Methods

- **subscribe(topics: string | TopicPartitionConfig[]): Promise<void>**
  Subscribe to Kafka topics

- **recv(): Promise<Message | null>**
  Receive next message (returns null when disconnected)

- **recvBatch(maxMessages: number, timeoutMs: number): Promise<Message[]>**
  Receive a batch of messages

- **disconnect(): Promise<void>**
  Disconnect the consumer

- **commit(topic: string, partition: number, offset: number, commit: CommitMode): Promise<void>**
  Manually commit an offset

- **commitMessage(message: Message, commit: CommitMode): Promise<void>**
  v2.1.0+: Simplified commit that automatically handles offset increment

### KafkaStreamReadable / KafkaBatchStreamReadable

Extends Node.js Readable stream interface for Kafka consumption.

#### Stream Consumer Configuration

```typescript
interface StreamConsumerConfiguration extends ConsumerConfiguration {
  batchSize?: number // If > 1, creates KafkaBatchStreamReadable
  batchTimeout?: number // Timeout for batch collection (default: 1000ms)
  streamOptions?: ReadableOptions
}
```

#### Methods

- **subscribe(topics: string | TopicPartitionConfig[]): Promise<void>**
  Subscribe to Kafka topics

- **commit(topic: string, partition: number, offset: number, commit: CommitMode): Promise<void>**
  Manually commit an offset

- **commitMessage(message: Message, commit: CommitMode): Promise<void>**
  Simplified commit

- **unsubscribe(): void**
  Unsubscribe from topics

- **disconnect(): Promise<void>**
  Disconnect the consumer

- **destroy(): void**
  Properly destroy the stream (recommended for cleanup)

- **getBatchConfig(): { batchSize: number; batchTimeout: number }**
  (KafkaBatchStreamReadable only) Get batch configuration

#### Events

- **data**: Emitted for each Kafka message
- **error**: Emitted on errors
- **close**: Emitted when the stream ends

### Message Format

```typescript
interface Message {
  topic: string
  partition: number
  offset: number
  timestamp: number
  payload: Buffer
  key?: Buffer
  headers: Record<string, Buffer>
}
```

## Performance Benchmarks

_Benchmarks run on macOS with Apple M1 chip processing 50,000 messages (December 2024)_

| Client            | Mode      | Ops/sec     |
| ----------------- | --------- | ----------- |
| kafkajs           | -         | 834         |
| node-rdkafka      | evented   | 24,923      |
| kafka-crab-js     | serial    | 43,214      |
| node-rdkafka      | stream    | 49,805      |
| **kafka-crab-js** | **batch** | **205,985** |

Performance characteristics:

- **52x faster than kafkajs** in serial mode, **247x faster in batch mode**
- **4.8x improvement** with batch processing over serial mode
- Lock-free data structures minimize memory overhead
- Zero-contention concurrent operations

## Best Practices

1. **Resource Management**
   - Always use `destroy()` when done with a stream consumer
   - Use try/finally blocks to ensure proper cleanup
   - Handle process signals (SIGINT, etc.) to gracefully shut down

2. **Error Handling**
   - Implement proper error handling and retry mechanisms
   - For critical applications, consider using the retry pattern shown above
   - Check for null returns from `recv()` to detect disconnections

3. **Performance Tuning**
   - Use batch stream consumers for high-throughput applications
   - Configure batch sizes and commit intervals appropriately
   - Monitor memory usage, especially when processing large messages

4. **Offset Management**
   - Use `enableAutoCommit: true` for simple use cases
   - For more control, set to false and manually commit offsets
   - Use `commitMessage()` for simplified offset handling

5. **Stream Cleanup**
   - Always call `destroy()` on stream consumers, not `disconnect()` directly
   - Wait for the `close` event before considering cleanup complete
   - This prevents async errors after the stream has ended
