# API Reference

Complete API reference for kafka-crab-js v3.0.0+.

## Table of Contents

- [KafkaClient](#kafkaclient)
- [KafkaProducer](#kafkaproducer)
- [KafkaConsumer](#kafkaconsumer)
- [KafkaStreamReadable](#kafkastreamreadable)
- [KafkaBatchStreamReadable](#kafkabatchstreamreadable)
- [Types](#types)
- [OpenTelemetry (kafka-crab-js-otel)](#opentelemetry-kafka-crab-js-otel)

---

## KafkaClient

Main client class that creates producers and consumers.

### Constructor

```typescript
new KafkaClient(config: KafkaConfiguration)
```

### KafkaConfiguration

| Property              | Type                                        | Default       | Description                                      |
| --------------------- | ------------------------------------------- | ------------- | ------------------------------------------------ |
| `brokers`             | `string`                                    | **required**  | Comma-separated list of broker addresses         |
| `clientId`            | `string`                                    | `"rdkafka"`   | Client identifier                                |
| `logLevel`            | `'debug' \| 'info' \| 'warning' \| 'error'` | `'info'`      | Log level                                        |
| `brokerAddressFamily` | `'v4' \| 'v6'`                              | `'v4'`        | IP address family                                |
| `securityProtocol`    | `SecurityProtocol`                          | `'Plaintext'` | Security protocol                                |
| `diagnostics`         | `boolean`                                   | `false`       | **v3.0.0+**: Enable diagnostics channel for OTEL |
| `configuration`       | `Record<string, any>`                       | `{}`          | Additional librdkafka configuration              |

### Methods

#### createProducer

```typescript
createProducer(config?: ProducerConfiguration): KafkaProducer
```

Creates a Kafka producer instance.

#### createConsumer

```typescript
createConsumer(config: ConsumerConfiguration): KafkaConsumer
```

Creates a Kafka consumer instance.

#### createStreamConsumer

```typescript
createStreamConsumer(config: StreamConsumerConfiguration): KafkaStreamReadable | KafkaBatchStreamReadable
```

Creates a stream-based Kafka consumer. Returns `KafkaBatchStreamReadable` if `batchSize > 1` is specified.

### Example

```typescript
import { KafkaClient } from 'kafka-crab-js'

const client = new KafkaClient({
  brokers: 'localhost:9092',
  clientId: 'my-app',
  logLevel: 'info',
  diagnostics: true, // Enable for OTEL instrumentation
})
```

---

## KafkaProducer

Kafka producer for sending messages.

### ProducerConfiguration

| Property        | Type                  | Default | Description                       |
| --------------- | --------------------- | ------- | --------------------------------- |
| `queueTimeout`  | `number`              | `5000`  | Queue timeout in milliseconds     |
| `autoFlush`     | `boolean`             | `true`  | Enable automatic message flushing |
| `configuration` | `Record<string, any>` | `{}`    | Additional producer configuration |

### Methods

#### send

```typescript
send(record: ProducerRecord): Promise<RecordMetadata[]>
```

Sends messages to Kafka and returns metadata about the sent messages.

##### ProducerRecord

```typescript
interface ProducerRecord {
  topic: string
  messages: Array<{
    key?: Buffer | string
    payload: Buffer
    headers?: Record<string, Buffer | string>
  }>
}
```

##### RecordMetadata

```typescript
interface RecordMetadata {
  topic: string
  partition: number
  offset: number
  error?: string
}
```

#### disconnect

```typescript
disconnect(): Promise<void>
```

Disconnects the producer.

### Example

```typescript
const producer = client.createProducer({
  configuration: {
    'batch.size': 16384,
    'compression.type': 'snappy',
    'enable.idempotence': true,
  },
})

const result = await producer.send({
  topic: 'my-topic',
  messages: [
    {
      key: Buffer.from('key-1'),
      payload: Buffer.from(JSON.stringify({ id: 1 })),
      headers: { 'correlation-id': Buffer.from('123') },
    },
  ],
})

await producer.disconnect()
```

---

## KafkaConsumer

Kafka consumer for receiving messages.

### ConsumerConfiguration

| Property               | Type                  | Default      | Description                           |
| ---------------------- | --------------------- | ------------ | ------------------------------------- |
| `groupId`              | `string`              | **required** | Consumer group ID                     |
| `enableAutoCommit`     | `boolean`             | `true`       | Enable automatic offset commits       |
| `fetchMetadataTimeout` | `number`              | `60000`      | Timeout for fetching metadata (ms)    |
| `maxBatchMessages`     | `number`              | `1000`       | Maximum messages in a batch operation |
| `configuration`        | `Record<string, any>` | `{}`         | Additional consumer configuration     |

### Methods

#### subscribe

```typescript
subscribe(topics: string | TopicPartitionConfig[]): Promise<void>
```

Subscribe to one or more topics.

#### recv

```typescript
recv(): Promise<Message | null>
```

Receive the next message. Returns `null` when disconnected.

#### recvBatch

```typescript
recvBatch(maxMessages: number, timeoutMs: number): Promise<Message[]>
```

Receive a batch of messages.

#### commit

```typescript
commit(topic: string, partition: number, offset: number, mode: CommitMode): Promise<void>
```

Manually commit an offset. Note: You must pass `offset + 1` to commit the message.

#### commitMessage

```typescript
commitMessage(message: Message, mode: CommitMode): Promise<void>
```

**v2.1.0+**: Simplified commit that automatically handles the offset increment.

#### seek

```typescript
seek(topic: string, partition: number, offset: OffsetModel, timeout?: number): void
```

Seek to a specific offset.

#### unsubscribe

```typescript
unsubscribe(): void
```

Unsubscribe from topics.

#### disconnect

```typescript
disconnect(): Promise<void>
```

Disconnect the consumer.

### Example

```typescript
const consumer = client.createConsumer({
  groupId: 'my-group',
  enableAutoCommit: false,
  configuration: {
    'auto.offset.reset': 'earliest',
  },
})

await consumer.subscribe([{ topic: 'my-topic', createTopic: true }])

while (true) {
  const message = await consumer.recv()
  if (!message) break

  console.log(message.payload.toString())
  await consumer.commitMessage(message, 'Sync')
}

await consumer.disconnect()
```

---

## KafkaStreamReadable

Stream-based consumer that extends Node.js Readable interface. Created when `batchSize` is not specified or equals 1.

### StreamConsumerConfiguration

Extends `ConsumerConfiguration` with:

| Property        | Type              | Default                | Description            |
| --------------- | ----------------- | ---------------------- | ---------------------- |
| `streamOptions` | `ReadableOptions` | `{ objectMode: true }` | Node.js stream options |

### Methods

All methods from `KafkaConsumer`, plus:

#### destroy

```typescript
destroy(error?: Error): void
```

Properly destroy the stream. **This is the recommended way to clean up stream consumers.**

### Events

| Event   | Description              |
| ------- | ------------------------ |
| `data`  | Emitted for each message |
| `error` | Emitted on errors        |
| `close` | Emitted when stream ends |

### Proper Cleanup Pattern

```typescript
async function cleanupStreamConsumer(stream) {
  if (!stream || stream.destroyed) return

  return new Promise((resolve) => {
    stream.once('close', resolve)
    stream.destroy()
  })
}
```

### Example

```typescript
const stream = client.createStreamConsumer({
  groupId: 'my-stream-group',
  enableAutoCommit: true,
})

await stream.subscribe('my-topic')

stream.on('data', (message) => {
  console.log(message.payload.toString())
})

stream.on('error', (error) => {
  console.error('Stream error:', error)
  stream.destroy()
})

stream.on('close', () => {
  console.log('Stream closed')
})

// Cleanup
await cleanupStreamConsumer(stream)
```

---

## KafkaBatchStreamReadable

Batch stream consumer that extends `KafkaStreamReadable`. Created when `batchSize > 1` is specified.

### BatchStreamConsumerConfiguration

Extends `StreamConsumerConfiguration` with:

| Property       | Type     | Default      | Description                           |
| -------------- | -------- | ------------ | ------------------------------------- |
| `batchSize`    | `number` | **required** | Number of messages to fetch per batch |
| `batchTimeout` | `number` | `1000`       | Timeout for batch collection (ms)     |

### Additional Methods

#### getBatchConfig

```typescript
getBatchConfig(): { batchSize: number; batchTimeout: number }
```

Returns the current batch configuration.

### Example

```typescript
const batchStream = client.createStreamConsumer({
  groupId: 'my-batch-group',
  batchSize: 10,
  batchTimeout: 500,
})

await batchStream.subscribe('my-topic')

// Get batch configuration
const config = batchStream.getBatchConfig()
console.log(config) // { batchSize: 10, batchTimeout: 500 }

// Messages are still delivered individually via 'data' event
// but are fetched in batches for better performance
batchStream.on('data', (message) => {
  console.log(message.payload.toString())
})

// Cleanup
batchStream.destroy()
```

---

## Types

### Message

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

### TopicPartitionConfig

```typescript
interface TopicPartitionConfig {
  topic: string
  allOffsets?: OffsetModel
  partitionOffset?: PartitionOffset[]
  createTopic?: boolean // Create topic if it doesn't exist
  numPartitions?: number // Number of partitions (when creating)
  replicas?: number // Number of replicas (when creating)
}
```

### OffsetModel

```typescript
interface OffsetModel {
  position: 'Beginning' | 'End' | 'Stored'
  offset?: number // For specific offset
}
```

### CommitMode

```typescript
type CommitMode = 'Sync' | 'Async'
```

### SecurityProtocol

```typescript
type SecurityProtocol = 'Plaintext' | 'Ssl' | 'SaslPlaintext' | 'SaslSsl'
```

---

## OpenTelemetry (kafka-crab-js-otel)

**v3.0.0+**: OpenTelemetry instrumentation has been moved to a separate package.

### Installation

```bash
npm install kafka-crab-js-otel @opentelemetry/api
```

### Basic Usage

```typescript
import { KafkaClient } from 'kafka-crab-js'
import { enableOtelInstrumentation, endSpan } from 'kafka-crab-js-otel'

// Enable instrumentation BEFORE creating client
enableOtelInstrumentation({
  captureMessagePayload: true,
  captureMessageHeaders: true,
  metrics: { enabled: true },
})

// Create client with diagnostics enabled
const client = new KafkaClient({
  brokers: 'localhost:9092',
  clientId: 'my-app',
  diagnostics: true, // Required for OTEL
})

// Producer - spans created automatically
const producer = client.createProducer()
await producer.send({ topic: 'my-topic', messages: [...] })

// Consumer - call endSpan() when processing is complete
const consumer = client.createConsumer({ groupId: 'my-group' })
const message = await consumer.recv()
// ... process message ...
endSpan(message)
```

### enableOtelInstrumentation Options

| Option                       | Type                                     | Default | Description                  |
| ---------------------------- | ---------------------------------------- | ------- | ---------------------------- |
| `tracerProvider`             | `TracerProvider`                         | global  | Custom tracer provider       |
| `captureMessagePayload`      | `boolean`                                | `false` | Include payload in spans     |
| `captureMessageHeaders`      | `boolean`                                | `true`  | Include headers in spans     |
| `maxPayloadSize`             | `number`                                 | `1024`  | Max payload bytes to capture |
| `enableBatchInstrumentation` | `boolean`                                | `true`  | Instrument batch operations  |
| `ignoreTopics`               | `string[] \| (topic: string) => boolean` | `[]`    | Topics to exclude            |
| `metrics.enabled`            | `boolean`                                | `false` | Enable metrics collection    |
| `metrics.meterProvider`      | `MeterProvider`                          | global  | Custom meter provider        |
| `messageHook`                | `(span, message) => void`                | -       | Hook for custom attributes   |
| `producerHook`               | `(span, record, metadata) => void`       | -       | Hook for producer spans      |

### API Functions

| Function                            | Description                     |
| ----------------------------------- | ------------------------------- |
| `enableOtelInstrumentation(config)` | Enable instrumentation          |
| `getOtelAdapter()`                  | Get singleton adapter           |
| `resetOtelAdapter()`                | Reset adapter (testing)         |
| `endSpan(message)`                  | End processing span             |
| `getKafkaInstrumentation()`         | Get instrumentation instance    |
| `resetKafkaInstrumentation()`       | Reset instrumentation (testing) |

### Spans Created

| Span Name         | Kind     | Description                |
| ----------------- | -------- | -------------------------- |
| `send <topic>`    | PRODUCER | Producer send operation    |
| `poll <topic>`    | CONSUMER | Consumer receive operation |
| `process <topic>` | CONSUMER | Message processing         |
| `batch receive`   | CONSUMER | Batch receive operation    |
| `batch process`   | CONSUMER | Batch processing           |

### Metrics

| Metric                                | Type      | Description         |
| ------------------------------------- | --------- | ------------------- |
| `messaging.client.operation.duration` | Histogram | Operation duration  |
| `messaging.client.sent.messages`      | Counter   | Messages sent       |
| `messaging.client.consumed.messages`  | Counter   | Messages consumed   |
| `messaging.process.duration`          | Histogram | Processing duration |

---

## librdkafka Configuration

kafka-crab-js supports all librdkafka configuration options via the `configuration` object.

See: [librdkafka Configuration](https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.html)

### Common Producer Options

```typescript
{
  'batch.size': 16384,
  'linger.ms': 5,
  'compression.type': 'snappy',  // none, gzip, snappy, lz4, zstd
  'enable.idempotence': true,
  'acks': 'all',
  'retries': 5,
  'message.timeout.ms': 30000,
}
```

### Common Consumer Options

```typescript
{
  'auto.offset.reset': 'earliest',  // earliest, latest
  'enable.auto.commit': true,
  'auto.commit.interval.ms': 5000,
  'session.timeout.ms': 30000,
  'heartbeat.interval.ms': 10000,
  'max.poll.interval.ms': 300000,
}
```
