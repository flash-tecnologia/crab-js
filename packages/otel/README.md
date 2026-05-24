# kafka-crab-js-otel

OpenTelemetry instrumentation for [kafka-crab-js](https://www.npmjs.com/package/kafka-crab-js) - a high-performance Kafka client for Node.js built with Rust.

> **Note:** This package is required for OpenTelemetry support starting with kafka-crab-js v3.0.0. OTEL instrumentation was moved from the core package to reduce bundle size and make it opt-in.

## Features

- 🔭 **Distributed Tracing** - Automatic span creation for producer and consumer operations
- 📊 **Metrics Collection** - Kafka-specific metrics following OpenTelemetry semantic conventions
- 🔗 **Context Propagation** - Automatic trace context injection/extraction in message headers
- ⚡ **Zero Overhead When Disabled** - Uses Node.js `diagnostics_channel` for near-zero cost when OTEL is not active
- 🎯 **Configurable** - Fine-grained control over tracing behavior, metrics, and payload capture

## Installation

```bash
npm install kafka-crab-js-otel
# or
pnpm add kafka-crab-js-otel
# or
yarn add kafka-crab-js-otel
```

### Peer Dependencies

This package requires the following peer dependencies:

```bash
npm install kafka-crab-js @opentelemetry/api
```

For full functionality, you'll also want:

```bash
npm install @opentelemetry/sdk-node @opentelemetry/sdk-trace-node @opentelemetry/sdk-metrics
```

## Quick Start

```javascript
import { KafkaClient } from 'kafka-crab-js'
import { enableOtelInstrumentation, endSpan } from 'kafka-crab-js-otel'

// 1. Enable OTEL instrumentation (call this BEFORE creating KafkaClient)
enableOtelInstrumentation({
  captureMessagePayload: true,
  captureMessageHeaders: true,
})

// 2. Create KafkaClient with diagnostics enabled
const client = new KafkaClient({
  brokers: 'localhost:9092',
  clientId: 'my-client',
  diagnostics: true, // Required for OTEL to receive events
})

// 3. Use producer/consumer as normal - spans are created automatically
const producer = client.createProducer()
await producer.send({
  topic: 'my-topic',
  messages: [{ payload: Buffer.from('Hello!') }],
})

// 4. For consumers, call endSpan() when message processing is complete
const consumer = client.createConsumer({ groupId: 'my-group' })
await consumer.subscribe('my-topic')

const message = await consumer.recv()
// ... process message ...
endSpan(message) // End the processing span

// 5. For stream consumers, call endSpan() in event handler and use destroy() for cleanup
const stream = client.createStreamConsumer({ groupId: 'my-stream-group' })
await stream.subscribe('my-topic')

stream.on('data', (message) => {
  try {
    // ... process message ...
  } finally {
    endSpan(message)
  }
})

// Proper cleanup for streams
stream.destroy()
```

## Configuration

### `enableOtelInstrumentation(config)`

Enable OTEL instrumentation with the given configuration:

```typescript
enableOtelInstrumentation({
  // Tracing options
  tracerProvider: myTracerProvider,     // Optional: custom tracer provider
  captureMessagePayload: false,         // Include message payload in spans (default: false)
  captureMessageHeaders: true,          // Include message headers in spans (default: true)
  maxPayloadSize: 1024,                 // Max payload bytes to capture (default: 1024)
  enableBatchInstrumentation: true,     // Instrument batch operations (default: true)

  // Topic filtering
  ignoreTopics: ['__consumer_offsets'], // Topics to exclude from tracing
  // Or use a function:
  ignoreTopics: (topic) => topic.startsWith('_'),

  // Metrics
  metrics: {
    enabled: true,                       // Enable metrics collection (default: false)
    meterProvider: myMeterProvider,      // Optional: custom meter provider
    includePartitionId: true,            // Include partition in labels (default: false)
    serverAddress: 'localhost',          // Broker address for attribution
    serverPort: 9092,                    // Broker port for attribution
    histogramBuckets: [0.005, 0.01, ...], // Custom latency buckets
  },

  // Hooks for custom attributes
  messageHook: (span, message) => {
    span.setAttribute('custom.key', message.key?.toString())
  },
  producerHook: (span, record, metadata) => {
    span.setAttribute('custom.partition', metadata?.partition)
  },
})
```

## API Reference

### Main Functions

| Function                            | Description                                       |
| ----------------------------------- | ------------------------------------------------- |
| `enableOtelInstrumentation(config)` | Enable OTEL instrumentation with configuration    |
| `getOtelAdapter()`                  | Get the singleton OtelAdapter instance            |
| `resetOtelAdapter()`                | Reset the adapter (for testing)                   |
| `endSpan(message)`                  | End the processing span for a consumed message    |
| `withMessageContext(message, fn)`   | Run framework code under the message OTEL context |
| `withBatchContext(batch, fn)`       | Run framework code under the batch OTEL context   |

### Instrumentation Functions

| Function                      | Description                               |
| ----------------------------- | ----------------------------------------- |
| `getKafkaInstrumentation()`   | Get the KafkaCrabInstrumentation instance |
| `resetKafkaInstrumentation()` | Reset instrumentation (for testing)       |

### Utility Functions

| Function                                      | Description                        |
| --------------------------------------------- | ---------------------------------- |
| `getTracer(name?)`                            | Get a tracer instance              |
| `createProducerSpan(tracer, topic, config)`   | Create a producer span             |
| `createConsumerSpan(tracer, message, config)` | Create a consumer span             |
| `createBatchSpan(tracer, messages, config)`   | Create a batch processing span     |
| `injectTraceContext(headers)`                 | Inject trace context into headers  |
| `extractTraceContext(headers)`                | Extract trace context from headers |
| `shouldIgnoreTopic(topic, config)`            | Check if topic should be ignored   |

### Constants

| Export                       | Description                                          |
| ---------------------------- | ---------------------------------------------------- |
| `KAFKA_SEMANTIC_CONVENTIONS` | OpenTelemetry semantic convention attribute names    |
| `KAFKA_OPERATION_TYPES`      | Operation type values (send, receive, process, etc.) |
| `KAFKA_OPERATION_NAMES`      | Operation name values                                |
| `KAFKA_SPAN_NAMES`           | Span name templates                                  |
| `KAFKA_METRICS`              | Metric names                                         |
| `KAFKA_DEFAULTS`             | Default configuration values                         |

## Metrics

When metrics are enabled, the following metrics are collected:

| Metric                                | Type      | Description                          |
| ------------------------------------- | --------- | ------------------------------------ |
| `messaging.client.operation.duration` | Histogram | Producer/consumer operation duration |
| `messaging.client.sent.messages`      | Counter   | Number of messages sent              |
| `messaging.client.consumed.messages`  | Counter   | Number of messages consumed          |
| `messaging.process.duration`          | Histogram | Message processing duration          |

## Spans

The instrumentation creates the following spans:

| Span Name         | Kind     | Description                |
| ----------------- | -------- | -------------------------- |
| `send <topic>`    | PRODUCER | Producer send operation    |
| `poll <topic>`    | CONSUMER | Consumer receive operation |
| `process <topic>` | CONSUMER | Message processing         |
| `batch receive`   | CONSUMER | Batch receive operation    |
| `batch process`   | CONSUMER | Batch processing           |

## Integration with OpenTelemetry SDK

### With Console Exporter (Development)

```javascript
import { NodeSDK } from '@opentelemetry/sdk-node'
import { ConsoleSpanExporter } from '@opentelemetry/sdk-trace-node'
import { enableOtelInstrumentation } from 'kafka-crab-js-otel'

const sdk = new NodeSDK({
  traceExporter: new ConsoleSpanExporter(),
})
sdk.start()

enableOtelInstrumentation()
```

### With OTLP Exporter (Production)

```javascript
import { NodeSDK } from '@opentelemetry/sdk-node'
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-grpc'
import { OTLPMetricExporter } from '@opentelemetry/exporter-metrics-otlp-grpc'
import { PeriodicExportingMetricReader } from '@opentelemetry/sdk-metrics'
import { enableOtelInstrumentation } from 'kafka-crab-js-otel'

const sdk = new NodeSDK({
  traceExporter: new OTLPTraceExporter({ url: 'http://localhost:4317' }),
  metricReader: new PeriodicExportingMetricReader({
    exporter: new OTLPMetricExporter({ url: 'http://localhost:4317' }),
  }),
})
sdk.start()

enableOtelInstrumentation({
  metrics: { enabled: true },
})
```

## Stream Consumer Best Practices

When using stream consumers with OTEL instrumentation:

```javascript
import { KafkaClient } from 'kafka-crab-js'
import { enableOtelInstrumentation, endSpan } from 'kafka-crab-js-otel'

enableOtelInstrumentation()

const client = new KafkaClient({
  brokers: 'localhost:9092',
  clientId: 'my-client',
  diagnostics: true,
})

// Batch stream consumer for high-throughput
const batchStream = client.createStreamConsumer({
  groupId: 'my-batch-group',
  batchSize: 10,
  batchTimeout: 500,
})

await batchStream.subscribe('my-topic')

batchStream.on('data', (message) => {
  try {
    console.log(message.payload.toString())
  } finally {
    endSpan(message)
  }
})

// Proper cleanup - use destroy() for streams
async function cleanup() {
  return new Promise((resolve) => {
    if (batchStream.destroyed) {
      resolve()
      return
    }
    batchStream.once('close', resolve)
    batchStream.destroy()
  })
}

// Handle shutdown
process.on('SIGINT', async () => {
  await cleanup()
  process.exit(0)
})
```

## Performance

kafka-crab-js with OTEL instrumentation maintains excellent performance:

| Mode             | Ops/sec            | Notes                    |
| ---------------- | ------------------ | ------------------------ |
| Serial (no OTEL) | 43,214             | Baseline                 |
| Batch (no OTEL)  | 205,985            | 4.8x improvement         |
| With OTEL        | Near-zero overhead | Uses diagnostics_channel |

_Benchmarks run on macOS with Apple M1 chip (December 2024)_

## Examples

See the [examples/kafka](https://github.com/inaiat/kafka-crab-js/tree/main/examples/kafka) directory for complete examples:

- `otel-tracing-example.mjs` - Tracing with custom spans
- `otel-metrics-example.mjs` - Metrics collection setup
- `otel-grafana-validation.mjs` - Full Grafana/Tempo/Prometheus integration

## Migration from v2.x

If you're migrating from kafka-crab-js v2.x where OTEL was built-in:

**Before (v2.x):**

```javascript
const client = new KafkaClient({
  brokers: 'localhost:9092',
  otel: {
    serviceName: 'my-service',
    metrics: { enabled: true },
  },
})
```

**After (v3.x):**

```javascript
import { enableOtelInstrumentation, endSpan } from 'kafka-crab-js-otel'

enableOtelInstrumentation({
  metrics: { enabled: true },
})

const client = new KafkaClient({
  brokers: 'localhost:9092',
  diagnostics: true,
})

// Don't forget to call endSpan() for consumers!
const message = await consumer.recv()
endSpan(message)
```

## License

MIT
