# kafka-crab-js

**Native Kafka performance. A JavaScript API.**

Build Kafka services in JavaScript and TypeScript with Rust, NAPI-RS, and librdkafka
handling the native transport. Publish with delivery confirmations, consume through
Web Streams or Node.js streams, and process batches without writing native code.

[![npm](https://img.shields.io/npm/v/kafka-crab-js)](https://www.npmjs.com/package/kafka-crab-js)
[![Node.js 24](https://img.shields.io/badge/Node.js-24-339933)](https://nodejs.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

[Quick start](#quick-start) · [Benchmarks](#benchmarks) · [Choose your API](#choose-your-api) ·
[API reference](docs/api.md) · [Examples](../../examples/kafka/README.md) ·
[Production recommendations](#production-recommendations)

## Why kafka-crab-js

Kafka should move your events without consuming your entire JavaScript heap.
`kafka-crab-js` puts Kafka transport and batch collection in native code while
keeping application logic in familiar async JavaScript.

- **Batch throughput that makes a difference.** The current development snapshot
  delivered **1.66 million messages/s**, **1.98× KafkaJS `eachBatch`**, in the
  [reproducible consumer comparison](../../BENCHMARKS.md).
- **More room in the JavaScript heap.** That batch scenario peaked at **29.0 MiB
  of JS heap versus KafkaJS's 71.7 MiB**. Native memory still contributes to RSS;
  the comparison reports both.
- **An API for each processing style.** Use `Message[]` Web Stream batches for
  bulk writes, individual messages for sequential handlers, Node streams for
  pipelines, or `recvBatch()` for your own receive loop.
- **Explicit delivery and offset control.** Await producer results, commit after
  successful processing, and preserve keys, headers, and tombstones across batching.
- **librdkafka configuration when you need it.** Configure TLS/SASL, compression,
  idempotent production, fetch behavior, and prefetch through the native client.
- **Observability without bundling an OTEL SDK into the core.** Diagnostic channels
  integrate with the optional [`kafka-crab-js-otel`](../kafka-crab-js-otel/README.md) package.

The measurements above describe this source tree's development build, not the
currently installed npm release. The
[artifact fingerprints and methodology](../../BENCHMARKS.md#methodology) identify the tested build.

## Install

```sh
npm install kafka-crab-js
# or
pnpm add kafka-crab-js
```

Use **Node.js 24**. Prebuilt binaries cover macOS x64/arm64 and Linux x64/arm64
with glibc or musl. Published binaries include librdkafka; supported installations
do not need a separate Rust toolchain or librdkafka installation. ESM, CommonJS,
and TypeScript declarations are included.

```js
import { KafkaClient } from 'kafka-crab-js'
// CommonJS: const { KafkaClient } = require('kafka-crab-js')
```

This is a native Node.js client. Web Streams support refers to Node's Web Stream
API; browser, WASM, and Windows builds are not provided. Documentation on the main
branch describes the current source; use a matching release tag for older packages.

### Upgrading to 5.0

Version 5.0 requires Node.js 24. The previous release supported Node.js 22 and
newer; update service runtimes and container images before upgrading.

Producer and consumer entrypoints remain available. Review the stricter
[commit and shutdown contracts](docs/api.md#commits-and-processing-order): manual
Async commits require a live event listener, mixed group subscriptions and manual
assignments are rejected, and Node.js consumer streams require object mode.
Failed sends expose per-call delivery details through `SendFailureError`; use
those details when deciding retries after partial delivery.

## Quick start

Run a Kafka broker and create an `orders` topic before starting these examples.
Use `KAFKA_BROKERS` for a comma-separated broker list; the examples default to
`localhost:9092`. Save the following files as `.mjs` and run with Node.js 24.

### Publish with confirmation

`producer.mjs`:

```js
import { KafkaClient } from 'kafka-crab-js'

const client = new KafkaClient({
  brokers: process.env.KAFKA_BROKERS ?? 'localhost:9092',
  clientId: 'orders-producer',
})

const producer = client.createProducer({
  configuration: {
    'enable.idempotence': true,
    acks: 'all',
    'compression.type': 'lz4',
  },
})

const deliveries = await producer.send({
  topic: 'orders',
  messages: [
    {
      key: Buffer.from('order-42'),
      payload: Buffer.from(JSON.stringify({ orderId: 'order-42', total: 129.9 })),
      headers: { 'content-type': Buffer.from('application/json') },
    },
  ],
})

console.log('Delivered:', deliveries)
```

By default, `send()` waits for delivery results and rejects on failure. For
high-volume publishing, send multiple records together and bound concurrent sends.
If you opt into `autoFlush: false`, call `flush()` regularly and before shutdown.
Records can be transmitted before a manual flush; it collects confirmations.
See [producer delivery semantics](docs/api.md#delivery-and-partial-failures).

### Consume batches and commit after processing

`consumer.mjs`:

```js
import { KafkaClient } from 'kafka-crab-js'

const client = new KafkaClient({
  brokers: process.env.KAFKA_BROKERS ?? 'localhost:9092',
  clientId: 'orders-consumer',
})

const { consumer, stream } = client.createWebStreamConsumer({
  groupId: 'orders-workers',
  enableAutoCommit: false,
  batchSize: 64,
  batchTimeout: 5,
  configuration: {
    'enable.auto.offset.store': false,
    'auto.offset.reset': 'earliest',
  },
})

const reader = stream.getReader()
const stop = () => {
  void reader.cancel('shutdown').catch(console.error)
}
process.once('SIGINT', stop)
process.once('SIGTERM', stop)

try {
  await consumer.subscribe('orders')

  while (true) {
    const { value: messages, done } = await reader.read()
    if (done) break

    const processed = new Map()
    for (const message of messages) {
      // Replace this with an awaited, idempotent database write or handler.
      if (message.isTombstone) {
        console.log('Delete:', message.key?.toString())
      } else {
        console.log('Order:', JSON.parse(message.payload.toString()))
      }
      processed.set(JSON.stringify([message.topic, message.partition]), message)
    }

    // Commit once per topic/partition, only after the entire batch succeeds.
    for (const message of processed.values()) {
      await consumer.commitMessage(message, 'Sync')
    }
  }
} finally {
  process.off('SIGINT', stop)
  process.off('SIGTERM', stop)
  try {
    await reader.cancel()
  } finally {
    reader.releaseLock()
    await consumer.disconnect()
  }
}
```

Run `node consumer.mjs` and then `node producer.mjs` in another terminal. Consumers
sharing a group split assigned partitions; use another group for an independent
subscription. `earliest` applies when there is no valid committed offset.

A failed handler prevents that batch's commits, so records can be replayed. Make
side effects idempotent and never commit past unfinished work in the same partition.
The signal handler cancels future reads while the already handed-out batch finishes;
cancellation can abandon prefetched work. See [shutdown semantics](docs/api.md#position-flow-control-and-shutdown)
when the application needs to drain collected work instead.

## Choose your API

| Your workload                                     | Start with                                                         | Delivered to your code                         |
| ------------------------------------------------- | ------------------------------------------------------------------ | ---------------------------------------------- |
| Bulk database writes, ingestion, analytics        | `createWebStreamConsumer({ batchSize: 64, batchTimeout: 5, ... })` | `Message[]` per read                           |
| Sequential business logic, message-level handling | `createWebStreamConsumer({ groupId, ... })`                        | One `Message` per read, with internal prefetch |
| Existing Node.js pipelines                        | `createStreamConsumer({ groupId, batchSize: 64, ... })`            | Individual `Message` objects in object mode    |
| A custom polling or scheduling loop               | `createConsumer()` + `recvBatch(size, timeoutMs)`                  | Up to `size` messages per call                 |
| One direct blocking receive                       | `createConsumer()` + `recv()`                                      | One message, or `null` after disconnect        |

Start with a batch size of **64 and a 5 ms timeout** for evaluation, then measure
with your payload sizes and handler. Larger batches amortize per-delivery work;
smaller batches reduce collection delay and the amount of work held by the handler.
The throughput comparison uses 4,096/2 ms for its small-message scenario, which is
not a universal production preset.

For serial Web Streams, `serialPrefetchSize` and `serialPrefetchTimeout` control
internal collection without changing the one-message public interface. A Node
batch stream still emits individual messages; choose the Web batch API when your
application needs arrays. Await handlers with a reader loop, async iteration, or
a pipeline sink so downstream processing applies backpressure.

## Benchmarks

**September 12, 2026 · Apple M4 · Node 24.20.0 · local Kafka · 120 runs per scenario.**
Two full runs used opposite crab pair orders, fresh processes per block, explicit
warmup, and all samples. Throughput is total measured messages / total measured
time. Heap and RSS are maximum sampled lifecycle values across four processes.

| Consumer                      | Interface                    |    Messages/s | Peak JS heap |      Peak RSS |
| ----------------------------- | ---------------------------- | ------------: | -----------: | ------------: |
| **kafka-crab-js development** | **Web Stream batch**         | **1,663,382** | **29.0 MiB** | **266.3 MiB** |
| @platformatic/kafka 2.11.0    | Message stream               |       998,122 |     87.0 MiB |     256.7 MiB |
| **kafka-crab-js development** | **Web Stream serial**        |   **915,664** | **21.9 MiB** | **178.9 MiB** |
| KafkaJS 2.2.4                 | `eachBatch`                  |       841,194 |     71.7 MiB |     238.9 MiB |
| KafkaJS 2.2.4                 | `eachMessage`                |       655,365 |     77.8 MiB |     245.6 MiB |
| KafkaJS 2.2.4                 | `eachMessage`, concurrency 3 |       646,613 |     83.2 MiB |     246.7 MiB |

For this workload, current batch throughput was **1.98× KafkaJS `eachBatch`**;
current serial was **1.40× KafkaJS `eachMessage`**. Platformatic's message stream
was faster than crab serial. Crab batch used less JS heap than the measured JS clients
but more total RSS; native allocations are outside the JS heap.

These are consumer delivery rates for small, already published messages, without
application processing, manual commit costs, or end-to-end producer latency.
They are not a universal ranking. The topic has three partitions, two nonempty;
KafkaJS concurrency 3 therefore does not imply three active message partitions.
Crab uses its default fetch queue backoff of 20 ms. Cluster observations and other
measurement limits are recorded in the full report.

[Full results, percentiles, configuration, and reproduction](../../BENCHMARKS.md) ·
[Comparison with the previous release](../../BENCHMARKS.md#comparison-with-the-previous-release) ·
[Portable evidence: all 960 measurements](docs/rfc/review/evidence/consumer-comparison-2026-09-12.json)

## How it compares

Choose the client that matches your processing model and deployment constraints.

| Decision                   | kafka-crab-js                                                                | KafkaJS                                                     | @platformatic/kafka                                       |
| -------------------------- | ---------------------------------------------------------------------------- | ----------------------------------------------------------- | --------------------------------------------------------- |
| Implementation             | Rust + NAPI-RS + librdkafka                                                  | JavaScript                                                  | TypeScript/JavaScript                                     |
| Consumer style             | Direct receive, Node streams, serial/batch Web Streams                       | `eachMessage` and `eachBatch` callbacks                     | Node streams and events                                   |
| Best reason to evaluate it | Native batch throughput, explicit delivery/offset control, librdkafka tuning | JavaScript-only deployment and callback-oriented processing | JavaScript-only deployment and stream-oriented processing |
| Configuration model        | Typed wrapper plus librdkafka properties                                     | KafkaJS options                                             | Platformatic options                                      |
| Installation consideration | Prebuilt native targets, Node 24                                             | No native addon                                             | No native addon                                           |

Competitor API descriptions come from the official
[KafkaJS documentation](https://kafka.js.org/docs/consuming) and
[Platformatic repository](https://github.com/platformatic/kafka). The comparison
covers these interfaces, not every feature or every client in the Kafka ecosystem.

`kafka-crab-js` is a strong fit for ingestion workers, event-driven services, and
bulk processing where you can benefit from native batch delivery. If you need
Kafka transactions, a built-in schema registry client, or a broad administrative
API, account for the current public API's scope before migrating.

### Coming from KafkaJS

| KafkaJS concept                            | kafka-crab-js equivalent                                       |
| ------------------------------------------ | -------------------------------------------------------------- |
| `new Kafka({ brokers: [...] })`            | `new KafkaClient({ brokers: 'host:9092,...' })`                |
| `kafka.producer()`                         | `client.createProducer()`                                      |
| `producer.send({ messages: [{ value }] })` | `producer.send({ messages: [{ payload: Buffer.from(...) }] })` |
| `eachMessage`                              | Serial Web Stream + an awaited message handler                 |
| `eachBatch`                                | Batch Web Stream + an awaited batch handler                    |
| Commit after successful work               | `consumer.commitMessage(message, 'Sync')`                      |

This is an API migration, not an import-only replacement. Review message encoding,
partition ordering, commit policy, retries, and shutdown. Start by replaying a
representative topic into an independent group and compare throughput, latency,
RSS, and verified processing outcomes. The [API reference](docs/api.md) describes
individual contracts and partial-failure behavior.

## Production recommendations

**Commit after the work succeeds.** Disable automatic commits/offset storage when
application completion defines delivery. Commit the last successfully processed
message per topic/partition. Keep side effects idempotent so replay is safe. For
Async commits, observe `CommitCallback`; resolving the scheduling promise is not
broker confirmation.

**Budget memory across the entire process.** The current native batch queue's
32 MiB accounted-byte budget includes headers and keys, with an oversized-batch
exception. It does not cap RSS or librdkafka's separate prefetch queues. Bound
application concurrency and retained messages, and monitor RSS alongside JS heap.

**Use a smaller prefetch profile for large headers and reader stalls.** Evaluate
these consumer properties against your own workload:

```js
const configuration = {
  'queued.max.messages.kbytes': 65536,
  'queued.min.messages': 256,
  'fetch.max.bytes': 8 * 1024 * 1024,
  'max.partition.fetch.bytes': 1024 * 1024,
  'fetch.queue.backoff.ms': 20,
}
```

In the [concurrent workload](docs/rfc/review/performance.md#concurrent-workload-with-natural-gc--2026-09-12),
640,000 new records totaling 12.736 GiB passed byte/order checks and broker-verified
commits with zero final backlog. Using this profile reduced the worst observed
consumer RSS by **50.1% serial / 54.3% batch** versus the 100,000-message threshold.
Normal p95 stayed around 60 ms; p99 and recovery time increased. Each consumer ran
for two minutes on macOS with natural GC. This validates a specific profile, not
long-term stability, a hard memory ceiling, or production failure recovery.

**Tune for your application.** Larger fetches can help small-message throughput;
large batches can raise memory and latency. Use the
[benchmark harness](../../benchmarks/kafka/README.md) and your own handler to find
the useful tradeoff. Keep confirmed send/commit costs in end-to-end measurements.

**Connect and shut down deliberately.** Use TLS/SASL appropriate to your cluster,
keep credentials in a secret store, and retain certificate validation. Finish
accepted work before committing/closing. Await sends and manual flushes; cancel
and release Web readers or destroy Node streams and await close. Details and
examples are in the [API reference](docs/api.md).

## Observability

Install `kafka-crab-js-otel` when you need OpenTelemetry tracing and metrics.
Configure your SDK/exporter, enable instrumentation before creating clients, and
keep client diagnostics enabled. Consumer processing spans should end after the
handler completes.

[OTEL setup](../kafka-crab-js-otel/README.md) ·
[Runnable tracing and metrics examples](../../examples/kafka/README.md)

## Documentation and development

- [API reference](docs/api.md): configuration, producer, consumer, streams, commits, and shutdown.
- [Examples](../../examples/kafka/README.md): direct APIs, streams, retries, and observability.
- [Benchmarks](../../BENCHMARKS.md): measured comparisons and reproducible commands.
- [Engineering docs](docs/README.md): RFCs, validation evidence, and remaining release checks.
- [Issues and feature requests](https://github.com/flash-tecnologia/crab-js/issues): share reproductions and proposals.

From the repository root:

```sh
pnpm install
pnpm --filter kafka-crab-js build
pnpm --filter kafka-crab-js test
RUN_KAFKA_INTEGRATION=true pnpm --filter kafka-crab-js test:integration
```

Building from source requires Rust and the native build prerequisites. Kafka must
be running for integration tests. See [test infrastructure](js-tests/integration/README.md)
for the Compose setup and broker requirements. The [release review](docs/rfc/review/README.md) tracks
validation separately from the performance claims above.

## License

[MIT](https://opensource.org/licenses/MIT)
