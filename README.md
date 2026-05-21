# Kafka Crab JS

Native Kafka performance for Node.js, with a TypeScript API small enough to use directly.

Kafka Crab JS uses Rust, NAPI-RS, and librdkafka to bring Kafka's mature native client behavior into Node.js services.
The goal is not to hide Kafka behind a new abstraction. The goal is to keep the API familiar while reducing JavaScript
heap pressure, improving high-throughput consumer paths, and preserving access to librdkafka tuning when production
workloads need it.

[![kafka-crab-js npm beta](https://img.shields.io/badge/npm%20beta-v4.0.0--beta.3-blue)](https://www.npmjs.com/package/kafka-crab-js)
[![kafka-crab-js-otel npm](https://img.shields.io/npm/v/kafka-crab-js-otel)](https://www.npmjs.com/package/kafka-crab-js-otel)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Why This Exists

KafkaJS is a good fit for many Node.js services. Kafka Crab JS is designed for the point where one or more of these
constraints becomes important:

| Need                             | What This Monorepo Provides                                                            |
| -------------------------------- | -------------------------------------------------------------------------------------- |
| Higher consumer throughput       | Native `recvBatch`, Web Stream batch consumers, and stream paths backed by librdkafka. |
| Lower JavaScript memory pressure | Kafka protocol work, polling, and batch collection happen outside the JS heap.         |
| Explicit offset control          | Manual commit helpers commit the next Kafka offset correctly.                          |
| Production Kafka tuning          | librdkafka configuration is passed through without a custom allowlist.                 |
| Observability without core bloat | The core emits diagnostics-channel events; OTEL lives in a separate package.           |
| Node.js integration              | ESM, CommonJS, direct APIs, Node.js `Readable` streams, and native Web Streams.        |

For simple low-volume consumers, a pure JavaScript client may be enough. Kafka Crab JS is most useful when consumer
throughput, heap pressure, batching, native Kafka behavior, or OpenTelemetry separation are part of the decision.

## Benchmark Snapshot

The repository benchmark runs each selected scenario in an isolated Node.js process and reports throughput, lifecycle
memory, and GC during the measured message window. In the local snapshot below, batch scenarios were normalized to an
effective batch size of `16384`.

| Scenario                            |       Throughput | Relative | RSS delta | Peak heap |
| ----------------------------------- | ---------------: | -------: | --------: | --------: |
| `kafka-crab-js v4 (stream, batch)`  | `1,089,284 op/s` | `100.0%` | `167 MiB` |  `36 MiB` |
| `@platformatic/kafka`               |   `732,207 op/s` |  `67.2%` | `218 MiB` |  `94 MiB` |
| `KafkaJS (eachBatch)`               |   `676,576 op/s` |  `62.1%` | `193 MiB` |  `70 MiB` |
| `kafka-crab-js v4 (stream, serial)` |   `544,658 op/s` |  `50.0%` |  `72 MiB` |  `12 MiB` |
| `KafkaJS (eachMessage)`             |   `505,815 op/s` |  `46.4%` | `182 MiB` |  `77 MiB` |

The takeaway is directional, not universal: v4 batch led raw throughput in this run, while v4 serial led memory
efficiency. Kafka benchmarks move with CPU power mode, broker state, message shape, partitions, and fetch settings, so
repeat the benchmark in your own environment before making capacity claims.

See [BENCHMARKS.md](./BENCHMARKS.md) for the latest captured benchmark run. The
[core package benchmark section](./packages/kafka-crab-js/README.md#performance-benchmarks) and
[benchmark package](./packages/benchmark) cover methodology, GC metrics, and profiling commands.

## Packages

### Published Packages

| Package                                   | Description                                                                 | npm                                                                                                                     |
| ----------------------------------------- | --------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| [kafka-crab-js](./packages/kafka-crab-js) | Core Kafka client with producer, consumer, batch, Node stream, and Web APIs | [![npm beta](https://img.shields.io/badge/npm%20beta-v4.0.0--beta.3-blue)](https://www.npmjs.com/package/kafka-crab-js) |
| [kafka-crab-js-otel](./packages/otel)     | Optional OpenTelemetry instrumentation for the core diagnostics channels    | [![npm](https://img.shields.io/npm/v/kafka-crab-js-otel)](https://www.npmjs.com/package/kafka-crab-js-otel)             |

### Workspace Tools

| Package                           | Description                                                                                                |
| --------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| [benchmark](./packages/benchmark) | Isolated-process consumer benchmark with memory, GC, throughput charts, and V8 CPU/heap profiling scripts. |
| [examples](./packages/examples)   | Runnable producer, consumer, stream, retry, OpenTelemetry, and Grafana examples.                           |

## Install

```bash
npm install kafka-crab-js
```

```bash
pnpm add kafka-crab-js
```

Install OpenTelemetry support only when you need tracing or metrics:

```bash
npm install kafka-crab-js kafka-crab-js-otel @opentelemetry/api @opentelemetry/sdk-node
```

## Quick Start

```ts
import { KafkaClient } from 'kafka-crab-js'

const client = new KafkaClient({
  brokers: 'localhost:9092',
  clientId: 'orders-service',
  securityProtocol: 'Plaintext',
})

const producer = client.createProducer()

await producer.send({
  topic: 'orders',
  messages: [
    {
      key: Buffer.from('order-123'),
      payload: Buffer.from(JSON.stringify({ id: 'order-123', status: 'created' })),
      headers: {
        'content-type': Buffer.from('application/json'),
      },
    },
  ],
})

const consumer = client.createConsumer({
  groupId: 'orders-worker',
  enableAutoCommit: false,
  configuration: {
    'auto.offset.reset': 'earliest',
  },
})

await consumer.subscribe('orders')

try {
  const message = await consumer.recv()
  if (message) {
    const order = JSON.parse(message.payload.toString('utf8'))
    console.log(order)

    await consumer.commitMessage(message, 'Sync')
  }
} finally {
  await consumer.disconnect()
}
```

For throughput-oriented consumers, use batch or stream batch APIs:

```ts
const webConsumer = client.createWebStreamConsumer({
  groupId: 'orders-batch-worker',
  batchSize: 1024,
  batchTimeout: 10,
  enableAutoCommit: false,
  configuration: {
    'auto.offset.reset': 'earliest',
  },
})

if (webConsumer.mode === 'batch') {
  await webConsumer.consumer.subscribe('orders')
  const reader = webConsumer.stream.getReader()

  try {
    const { value: batch, done } = await reader.read()
    if (!done && batch) {
      for (const message of batch) {
        await processOrder(message)
      }
    }
  } finally {
    await reader.cancel()
    await webConsumer.consumer.disconnect()
  }
}
```

## Core Features

- Producer API with keys, headers, delivery metadata, configurable `autoFlush`, and manual `flush()`.
- Direct consumer API with `recv()`, `recvBatch()`, manual commit, pause/resume, seek, assignment, and consumer events.
- Node.js `Readable` stream consumers for existing stream pipelines.
- Native Web Stream consumers with serial and batch modes.
- Batch receive APIs that reduce native-to-JS boundary crossings.
- `commitMessage()` helper that commits `message.offset + 1`.
- Advanced librdkafka configuration passthrough through `configuration`.
- Diagnostics-channel instrumentation that stays optional and powers `kafka-crab-js-otel`.
- Prebuilt native binaries for supported macOS and Linux targets.

## OpenTelemetry

The core package is not coupled to OpenTelemetry. It emits diagnostics-channel events by default, and
`kafka-crab-js-otel` subscribes to those events.

```ts
import { KafkaClient } from 'kafka-crab-js'
import { enableOtelInstrumentation, endSpan } from 'kafka-crab-js-otel'

enableOtelInstrumentation({
  metrics: { enabled: true },
})

const client = new KafkaClient({
  brokers: 'localhost:9092',
  clientId: 'orders-worker',
  diagnostics: true,
})

const consumer = client.createConsumer({ groupId: 'orders-worker' })
await consumer.subscribe('orders')

const message = await consumer.recv()
if (message) {
  try {
    await processOrder(message)
  } finally {
    endSpan(message)
  }
}
```

Read the [OTEL package README](./packages/otel/README.md) for tracing, metrics, hooks, and exporter setup.

## Version 4 Migration Note

`CommitMode`, `KafkaEventName`, `PartitionPosition`, and `SecurityProtocol` are TypeScript-only exports in v4. Use
string literal values at runtime:

```ts
import type { CommitMode, SecurityProtocol } from 'kafka-crab-js'

const commitMode: CommitMode = 'Sync'
const securityProtocol: SecurityProtocol = 'Plaintext'
```

See the [core migration notes](./packages/kafka-crab-js/README.md#v4-type-only-runtime-exports) for details.

## Documentation

- [Core package README](./packages/kafka-crab-js/README.md): full API reference, tuning, troubleshooting, and benchmark analysis.
- [OpenTelemetry package README](./packages/otel/README.md): instrumentation setup and configuration.
- [Benchmark snapshot](./BENCHMARKS.md): captured consumer throughput, lifecycle memory, and GC results.
- [Benchmark README](./packages/benchmark/README.md): methodology, environment variables, memory mode, GC, and profiling.
- [Examples README](./packages/examples/README.md): runnable examples for core and OTEL usage.

## Development

```bash
# Install dependencies
vp install

# Build all packages
vp run build

# Run tests
vp run test

# Run integration tests
vp run test:integration

# Format, lint, package checks, and tests
vp check
```

Useful focused commands:

```bash
vp run --filter kafka-crab-js build
vp run --filter kafka-crab-js test
vp run --filter kafka-crab-js test:integration

podman compose up -d
cd packages/benchmark
vp run setup:consumer
vp run benchmark
```

## Requirements

- Node.js `>= 22` for published packages.
- A Kafka broker reachable from the Node.js process.
- Rust toolchain when building native bindings from source.
- No separate librdkafka install is required for published binaries.

## License

MIT
