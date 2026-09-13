# kafka-crab-js-otel

Trace Kafka messages from producer to application processing, with OpenTelemetry
instrumentation for [kafka-crab-js](../kafka-crab-js/README.md).

The adapter subscribes to Node.js diagnostic channels. Your application chooses
its OpenTelemetry SDK, sampler and exporters; the Kafka client keeps telemetry
optional. Use the same integration with direct receives, Web Streams or Node.js
streams.

- Producer, receive and processing spans, with errors and Kafka attributes.
- Trace propagation that preserves binary application headers.
- Batch spans linked to producer origins, including batches containing unrelated traces.
- Operation latency, processing latency and message counters.
- Topic filtering, custom attributes and explicit processing completion.

## Install

```sh
pnpm add kafka-crab-js kafka-crab-js-otel @opentelemetry/api
```

This package targets Node.js 24. It declares `kafka-crab-js >=3` and
`@opentelemetry/api >=1.9` as peers. An application must also configure an
OpenTelemetry SDK to export telemetry. Without a provider, the OpenTelemetry API
uses its no-op implementation.

The examples below use these SDK packages:

```sh
pnpm add @opentelemetry/sdk-trace-node @opentelemetry/sdk-trace-base
```

## Quick start

This example sends and processes one message. Provision the `orders` topic first.
For a service, keep the receive loop running and apply the same processing and
shutdown boundaries.

```js
import { NodeTracerProvider } from '@opentelemetry/sdk-trace-node'
import { ConsoleSpanExporter, SimpleSpanProcessor } from '@opentelemetry/sdk-trace-base'
import { KafkaClient } from 'kafka-crab-js'
import { enableOtelInstrumentation, endSpan, withMessageContext } from 'kafka-crab-js-otel'

const provider = new NodeTracerProvider({
  spanProcessors: [new SimpleSpanProcessor(new ConsoleSpanExporter())],
})
provider.register()

const adapter = enableOtelInstrumentation()
const client = new KafkaClient({
  brokers: 'localhost:9092',
  clientId: 'orders-service',
  diagnostics: true,
})
const producer = client.createProducer()
const consumer = client.createConsumer({
  groupId: 'orders-service',
  enableAutoCommit: false,
  configuration: {
    'enable.auto.offset.store': false,
    'auto.offset.reset': 'earliest',
  },
})

try {
  await producer.send({
    topic: 'orders',
    messages: [{ key: Buffer.from('order-1'), payload: Buffer.from('{"id":1}') }],
  })
  await consumer.subscribe('orders')
  const message = await consumer.recv()
  if (message) {
    try {
      await withMessageContext(message, async () => {
        // Await business work here. Child spans inherit this message's context.
        console.log(message.isTombstone ? 'Deleted' : message.payload.toString())
      })
      await consumer.commitMessage(message, 'Sync')
      endSpan(message)
    } catch (error) {
      endSpan(message, error instanceof Error ? error : new Error(String(error)))
      throw error
    }
  }
} finally {
  try {
    await consumer.disconnect()
  } finally {
    adapter.disable()
    await provider.shutdown()
  }
}
```

Register the SDK and enable the adapter before Kafka operations begin. Diagnostic
channels are enabled by default in the current Kafka client; `diagnostics: false`
turns them off even when the OTEL adapter is enabled.

For production, replace the console exporter with your collector exporter and use
`BatchSpanProcessor`. Configure sampling in the SDK. See the
[OpenTelemetry JavaScript tracing guide](https://opentelemetry.io/docs/languages/js/instrumentation/#traces).

## Processing and context ownership

`endSpan(message, error?)` records processing completion. It does **not** commit an
offset or acknowledge delivery. Use `withMessageContext(message, async () => ...)`
and await its result when downstream work needs to inherit the processing span.
The context helpers do not end spans automatically.

| Consumer API                                                 | Application receives                       | Complete processing with   |
| ------------------------------------------------------------ | ------------------------------------------ | -------------------------- |
| `consumer.recv()`                                            | `Message` or `null`                        | `endSpan(message, error?)` |
| `consumer.recvBatch(size, timeoutMs)`                        | `Message[]`                                | `endSpan(batch, error?)`   |
| `client.createWebStreamConsumer({ groupId })`                | One `Message` per read                     | `endSpan(message, error?)` |
| `client.createWebStreamConsumer({ groupId, batchSize: 64 })` | One `Message[]` per read                   | `endSpan(batch, error?)`   |
| `client.createStreamConsumer(...)`                           | Individual `Message` objects in both modes | `endSpan(message, error?)` |

An empty direct batch has no processing span. The native `recvStream()` and
`recvBatchStream()` methods bypass the public stream wrappers; use the client
factories above for wrapper instrumentation.

For batches, use `withBatchContext(batch, fn)` for work shared by the batch, and
`withMessageContext(message, fn)` for work specific to one message. For example,
given a batch returned by `recvBatch()` or a public Web Stream:

```js
import { endSpan, withBatchContext, withMessageContext } from 'kafka-crab-js-otel'

async function processBatch(batch, processMessage) {
  try {
    await withBatchContext(batch, async () => {
      for (const message of batch) {
        await withMessageContext(message, () => processMessage(message))
      }
    })
    endSpan(batch)
  } catch (error) {
    endSpan(batch, error instanceof Error ? error : new Error(String(error)))
    throw error
  }
}
```

Commit only after business work succeeds. For a batch, commit the last completed
message in **each topic/partition**, without skipping unfinished work. See the
[core commit contract](../kafka-crab-js/docs/api.md#commits-and-processing-order).
With Node.js streams, prefer awaited async iteration or a pipeline sink: an
`async` `'data'` listener does not make the stream wait for processing.

Batch and message completion helpers are idempotent. In batch mode, ending all
message helpers also completes the batch; processing spans remain open until the
batch completes. Passing an error marks the batch and its message spans as failed.
Finish processing and end spans before disabling the adapter or shutting down the
SDK. Cancellation is not proof that prefetched messages were processed, and does
not replace explicit span completion.

## Trace propagation

The configured global propagator injects the producer context into outgoing
headers. Application header buffers retain their bytes. Incoming headers are
read without being rewritten. Missing or invalid trace context starts an
independent processing trace instead of inheriting unrelated ambient work.

When all messages share one producer span, the batch continues that trace and
message spans are children of the batch. With multiple producer origins, the
batch starts an aggregate trace with links to those origins, while each message
continues its own producer trace and links back to the batch. Topic filtering is
applied before choosing those origins. This uses
[OpenTelemetry messaging span links](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/)
to represent multiple causes without replacing individual message contexts.

## Configuration

```ts
import { enableOtelInstrumentation, type OtelAdapterConfig } from 'kafka-crab-js-otel'

const config: OtelAdapterConfig = {
  ignoreTopics: (topic) => topic.startsWith('__'),
  captureMessageHeaders: true,
  captureMessagePayload: false,
  maxPayloadSize: 1024,
  metrics: {
    enabled: true,
    includePartitionId: false,
  },
  messageHook: (span, message) => {
    span.setAttribute('app.message.kind', message.isTombstone ? 'delete' : 'value')
  },
  producerHook: (span, _record, metadata) => {
    if (metadata) span.setAttribute('app.first_delivery.partition', metadata.partition)
  },
}
enableOtelInstrumentation(config)
```

| Option                                 | Default                   | Behavior                                                                                                                           |
| -------------------------------------- | ------------------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| `tracerProvider`                       | Global provider           | Provider used to create Kafka spans. Register a global context manager and propagator separately when using a custom provider.     |
| `ignoreTopics`                         | None                      | Topic name array or predicate returning `true` to exclude a topic from tracing and metrics.                                        |
| `captureMessageHeaders`                | `true`                    | Records header count and up to 20 header names, never header values.                                                               |
| `captureMessagePayload`                | `false`                   | Records message body size, never payload contents. For sends, this applies to single-message records.                              |
| `maxPayloadSize`                       | `1024` bytes              | Body-size attributes are omitted above this threshold; it does not truncate Kafka messages.                                        |
| `messageHook`                          | None                      | Synchronous hook running under each message's processing context.                                                                  |
| `producerHook`                         | None                      | Synchronous hook at send start; runs again with the first metadata entry when delivery metadata is returned, before the span ends. |
| `metrics.enabled`                      | `false`                   | Opts into metric collection.                                                                                                       |
| `metrics.meterProvider`                | Global provider           | Provider used to create metric instruments.                                                                                        |
| `metrics.includePartitionId`           | `true`                    | Adds partition labels when known; disable to reduce metric cardinality.                                                            |
| `metrics.serverAddress` / `serverPort` | None                      | Broker attribution for metrics.                                                                                                    |
| `metrics.histogramBuckets`             | Standard duration buckets | Positive, strictly increasing boundaries in seconds; SDK support determines how instrument advice is applied.                      |

Kafka keys, topic names, consumer groups and broker attributes can appear in
spans independently of the capture options. Review these fields when keys contain
sensitive data; hooks can replace key attributes before export. Payload and header
values are not included by the built-in capture options.

Batch tracing is enabled whenever the adapter is enabled. `enableBatchInstrumentation`
and `decorateMessages` belong to the legacy `KafkaCrabInstrumentation` helper API;
they are not options for `enableOtelInstrumentation()`.

## Metrics and operation boundaries

Install and configure a metrics SDK before enabling collection:

```sh
pnpm add @opentelemetry/sdk-metrics
```

```js
import { metrics } from '@opentelemetry/api'
import { ConsoleMetricExporter, MeterProvider, PeriodicExportingMetricReader } from '@opentelemetry/sdk-metrics'
import { enableOtelInstrumentation } from 'kafka-crab-js-otel'

const meterProvider = new MeterProvider({
  readers: [
    new PeriodicExportingMetricReader({
      exporter: new ConsoleMetricExporter(),
      exportIntervalMillis: 10000,
    }),
  ],
})
metrics.setGlobalMeterProvider(meterProvider)
enableOtelInstrumentation({ metrics: { enabled: true } })
// After Kafka processing and span completion: await meterProvider.shutdown().
```

| Metric                                | Type               | Meaning                                                                                         |
| ------------------------------------- | ------------------ | ----------------------------------------------------------------------------------------------- |
| `messaging.client.operation.duration` | Histogram, seconds | Duration of send and nonempty/failed receive operations.                                        |
| `messaging.client.sent.messages`      | Counter            | Messages attempted by completed send calls, including failed calls with `error.type`.           |
| `messaging.client.consumed.messages`  | Counter            | Messages returned by receives, grouped by their actual topic/partition labels.                  |
| `messaging.process.duration`          | Histogram, seconds | Time from processing-span creation to completion; batch mode records one observation per batch. |

A batch spanning partitions has no single partition label on its operation
histogram. A batch spanning topics also omits the destination name. A send receives
one partition label only when all returned delivery metadata identifies that
partition. These aggregate durations are not duplicated across message counters.

| Span              | Kind     | Boundary                                                                                               |
| ----------------- | -------- | ------------------------------------------------------------------------------------------------------ |
| `send <topic>`    | Producer | The `send()` call, including delivery waiting when auto-flush is enabled.                              |
| `poll <topic>`    | Consumer | Receive operation; `poll kafka` when there is no single known topic.                                   |
| `process <topic>` | Consumer | Application processing; batches also create an aggregate span, named `process kafka` for mixed topics. |

A successful send span in manual-flush mode means the call completed; it does not
prove broker delivery. `flush()` is not separately instrumented. A failed send may
have partially delivered messages: use the core client's
[send-failure contract](../kafka-crab-js/docs/api.md#delivery-and-partial-failures), not telemetry counters,
to decide retries. Commit, rebalance and admin operations do not currently have
automatic spans in this adapter.

## API reference

| Export                                                                     | Purpose                                                                         |
| -------------------------------------------------------------------------- | ------------------------------------------------------------------------------- |
| `enableOtelInstrumentation(config?)`                                       | Configure and enable the singleton adapter; enables it again after `disable()`. |
| `getOtelAdapter(config?)`                                                  | Get/create the singleton adapter and optionally update its configuration.       |
| `adapter.disable()`                                                        | Unsubscribe from diagnostic channels after processing finishes.                 |
| `resetOtelAdapter()`                                                       | Dispose/reset the adapter, primarily for tests.                                 |
| `endSpan(target, error?)`                                                  | Complete a message or batch processing span.                                    |
| `getMessageContext(message)` / `getBatchContext(batch)`                    | Resolve the context attached to a public message or batch.                      |
| `withMessageContext(message, fn)` / `withBatchContext(batch, fn)`          | Execute and return a callback under that context, including promises.           |
| `createProducerSpan(tracer, record, options?)`                             | Create a manual producer span for a `ProducerRecord`.                           |
| `createConsumerSpan(tracer, message, options?)`                            | Create a manual consumer span.                                                  |
| `createBatchSpan(tracer, batchSize, options?)`                             | Create a manual span for a numeric message count.                               |
| `injectTraceContext(headers?, context?)` / `extractTraceContext(headers?)` | Propagate context through Kafka headers.                                        |
| `getKafkaInstrumentation()` / `resetKafkaInstrumentation()`                | Legacy helper integration and test reset.                                       |

Manual span factories require explicit completion and do not instrument Kafka
calls themselves. Exported `KAFKA_SEMANTIC_CONVENTIONS`, `KAFKA_METRICS` and
`KAFKA_SPAN_NAMES` provide the attribute, metric and span naming constants.

## Upgrading to 2.0

Version 2.0 requires Node.js 24. Version 1.2.1 declared support for Node.js 22 and
newer; update service runtimes and container images before upgrading.

Producer and consumer APIs remain the same. Batch traces now preserve independent
producer origins using span links, and incoming message headers are no longer
rewritten. Dashboards should expect mixed-origin batch spans in a separate trace
and omit partition labels on operations that span multiple partitions. Header
capture records names and counts by default, never values.

## Performance and validation

Tracing cost depends on sampling, hooks, per-message spans, the SDK and exporter.
Measure the telemetry configuration used by your service. The
[consumer comparisons](../../BENCHMARKS.md) measure the core client with diagnostics
disabled; those numbers do not measure OTEL overhead.

From the repository root:

```sh
pnpm --filter kafka-crab-js-otel test
KAFKA_REQUIRED=true pnpm --filter kafka-crab-js-otel test:integration
pnpm --filter kafka-crab-js-otel lint
pnpm --filter kafka-crab-js-otel build
```

Integration tests use `KAFKA_BROKERS` (default `localhost:9092`). They fail if Kafka
is unavailable when `KAFKA_REQUIRED=true` is set, as in CI; otherwise they skip.
They cover binary headers, empty values, tombstones and trace continuity through direct, Web Stream and Node.js Stream consumers in both modes.

The monorepo pins pnpm in the root `packageManager` field. Run `corepack enable`
once and use Node.js 24; Corepack selects the pinned version for local commands.
CI installs the same version with `pnpm/action-setup` and uses a frozen lockfile.

The OTEL release workflow checks that `kafka-crab-js-otel@<version>` matches the
package manifest, builds the native core, runs lint, formatting, unit tests and
required Kafka integration tests, then packs the OTEL artifact. Publishing depends
on those checks and uses that same tarball. Instrumentation name and version are
read from `package.json` during the build.

See [runnable telemetry examples](../../examples/kafka) for collector integration.

## License

MIT
