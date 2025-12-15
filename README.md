# kafka-crab-js

A high-performance Kafka client for Node.js built with Rust (via napi-rs).

## Packages

This monorepo contains two packages:

| Package | Description | npm |
|---------|-------------|-----|
| [kafka-crab-js](./packages/kafka-crab-js) | Core Kafka client with producer, consumer, and streaming APIs | [![npm](https://img.shields.io/npm/v/kafka-crab-js)](https://www.npmjs.com/package/kafka-crab-js) |
| [kafka-crab-js-otel](./packages/kafka-crab-js-otel) | OpenTelemetry instrumentation (tracing & metrics) | [![npm](https://img.shields.io/npm/v/kafka-crab-js-otel)](https://www.npmjs.com/package/kafka-crab-js-otel) |

## Quick Start

### Basic Usage (Core Package Only)

```bash
npm install kafka-crab-js
```

```javascript
import { KafkaClient } from 'kafka-crab-js'

const client = new KafkaClient({
  brokers: 'localhost:9092',
  clientId: 'my-app',
})

// Producer
const producer = client.createProducer()
await producer.send({
  topic: 'my-topic',
  messages: [{ payload: Buffer.from('Hello Kafka!') }],
})

// Consumer
const consumer = client.createConsumer({ groupId: 'my-group' })
await consumer.subscribe('my-topic')
const message = await consumer.recv()
console.log(message.payload.toString())
```

### With OpenTelemetry Instrumentation

```bash
npm install kafka-crab-js kafka-crab-js-otel @opentelemetry/api @opentelemetry/sdk-node
```

```javascript
import { KafkaClient } from 'kafka-crab-js'
import { enableOtelInstrumentation, endSpan } from 'kafka-crab-js-otel'

// Enable OTEL before creating client
enableOtelInstrumentation({
  serviceName: 'my-kafka-service',
  metrics: { enabled: true },
})

// Create client with diagnostics enabled
const client = new KafkaClient({
  brokers: 'localhost:9092',
  clientId: 'my-app',
  diagnostics: true, // Required for OTEL instrumentation
})

// Spans are created automatically for producer/consumer operations
const producer = client.createProducer()
await producer.send({ topic: 'my-topic', messages: [...] })

// For consumers, call endSpan() when processing is complete
const message = await consumer.recv()
// ... process message ...
endSpan(message)
```

## Features

### Core Package (`kafka-crab-js`)

- 🦀 **Rust Performance** - Native Kafka bindings via librdkafka
- 📨 **Producer** - Send messages with keys, headers, and partitioning
- 📥 **Consumer** - Manual commit, auto-commit, and offset management
- 🌊 **Streams** - Node.js Readable streams for message consumption
- 🔄 **Batch Operations** - Efficient batch produce and consume
- 🔌 **Diagnostics Channel** - Integration point for observability

### OTEL Package (`kafka-crab-js-otel`)

- 🔭 **Distributed Tracing** - Automatic span creation with context propagation
- 📊 **Metrics** - Producer/consumer metrics following OTel semantic conventions
- ⚡ **Zero Overhead** - Uses `diagnostics_channel` for near-zero cost when disabled
- 🎯 **Configurable** - Control payload capture, topic filtering, custom hooks

## Documentation

- [Core Package README](./packages/kafka-crab-js/README.md) - Full API documentation
- [OTEL Package README](./packages/kafka-crab-js-otel/README.md) - Instrumentation setup and configuration
- [Examples](./example/) - Complete working examples

## Examples

| Example | Description |
|---------|-------------|
| [otel-tracing-example.mjs](./example/otel-tracing-example.mjs) | Distributed tracing with custom spans |
| [otel-metrics-example.mjs](./example/otel-metrics-example.mjs) | Metrics collection and export |
| [otel-grafana-validation.mjs](./example/otel-grafana-validation.mjs) | Full Grafana/Tempo/Prometheus integration |

## Development

```bash
# Install dependencies
pnpm install

# Build all packages
pnpm build

# Run tests
pnpm test

# Lint
pnpm lint

# Format
pnpm fmt
```

## Requirements

- Node.js >= 20
- Rust toolchain (for building from source)
- librdkafka (bundled)

## License

MIT

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.
