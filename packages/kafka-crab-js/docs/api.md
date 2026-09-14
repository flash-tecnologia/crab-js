# Kafka API reference

This reference describes the current source tree. For an installed release, use
the documentation at its matching tag. Start with the [package guide](../README.md)
for installation, working examples, API selection, and benchmark results.
Generated declarations in `dist/` are the definitive TypeScript signatures.

## KafkaClient

```ts
import { KafkaClient } from 'kafka-crab-js'

const client = new KafkaClient({
  brokers: 'localhost:9092,localhost:9093',
  clientId: 'orders-service',
  diagnostics: true,
})
```

| Client option         | Default       | Purpose                                                                           |
| --------------------- | ------------- | --------------------------------------------------------------------------------- |
| `brokers`             | Required      | Comma-separated bootstrap broker addresses.                                       |
| `clientId`            | `'rdkafka'`   | Identifier used by Kafka and diagnostics.                                         |
| `securityProtocol`    | `'Plaintext'` | `'Plaintext'`, `'Ssl'`, `'SaslPlaintext'`, or `'SaslSsl'`.                        |
| `logLevel`            | `'error'`     | Native logging level, for example `'debug'`, `'info'`, `'warning'`, or `'error'`. |
| `brokerAddressFamily` | `'v4'`        | Address family; use `'any'` where appropriate for the deployment.                 |
| `diagnostics`         | `true`        | Enable diagnostic-channel instrumentation; OTEL is a separate opt-in package.     |
| `configuration`       | None          | Additional librdkafka properties; values are converted to strings.                |

Create a new client/producer/consumer to apply configuration changes. Advanced
properties pass through to librdkafka for validation. Prefer producer- or
consumer-specific properties on the corresponding factory rather than sharing
incompatible settings between both.

| Factory                            | Result                                                        |
| ---------------------------------- | ------------------------------------------------------------- |
| `createProducer(options?)`         | `KafkaProducer`                                               |
| `createConsumer(options)`          | `KafkaConsumer`                                               |
| `createWebStreamConsumer(options)` | `{ mode, consumer, stream }`, with serial or batch Web Stream |
| `createStreamConsumer(options)`    | Node.js `KafkaStreamReadable` or `KafkaBatchStreamReadable`   |

### Secure connections

```ts
const secureClient = new KafkaClient({
  brokers: process.env.KAFKA_BROKERS!,
  clientId: 'orders-service',
  securityProtocol: 'SaslSsl',
  configuration: {
    'sasl.mechanism': 'PLAIN',
    'sasl.username': process.env.KAFKA_USERNAME!,
    'sasl.password': process.env.KAFKA_PASSWORD!,
  },
})
```

Load credentials from the deployment's secret store and configure trusted CA
certificates for the broker. The mechanism must match the cluster; `PLAIN` above
is an example over TLS. See the pinned
[librdkafka configuration reference](https://github.com/confluentinc/librdkafka/blob/v2.12.1/CONFIGURATION.md)
for certificate and authentication properties.

## KafkaProducer

```ts
const producer = client.createProducer({
  queueTimeout: 5000,
  autoFlush: true,
  configuration: {
    'enable.idempotence': true,
    acks: 'all',
    'compression.type': 'lz4',
  },
})
```

`queueTimeout` defaults to 5,000 ms and controls queue/flush waiting.
`autoFlush` defaults to `true`. Kafka delivery timeouts such as
`message.timeout.ms` are separate librdkafka configuration.

| Method                      | Result and contract                                                                                           |
| --------------------------- | ------------------------------------------------------------------------------------------------------------- |
| `send({ topic, messages })` | `Promise<RecordMetadata[]>`; with auto-flush, waits for delivery results and rejects delivery/flush failures. |
| `flush()`                   | Waits for pending deliveries; returns collected results in manual mode, or an empty array in auto-flush mode. |
| `inFlightCount()`           | Current native in-flight count.                                                                               |
| `getLastDeliveryResults()`  | Last collected result snapshot; concurrent operations can overwrite it.                                       |

A message accepts `payload?: Buffer`, `key?: Buffer`,
`headers?: Record<string, Buffer>`, and `isTombstone?: boolean`.
Encode JSON/strings explicitly with `Buffer.from(...)`.

### Delivery and partial failures

Each auto-flush send owns its delivery confirmations. Handle rejection at the
send that failed. Errors exposed through `KafkaClient.createProducer()` may carry
`SendFailureError` fields `enqueuedCount`, `totalCount`, `confirmedCount`, and
`confirmedMessages`. These are optional, per-send details copied from the native
error payload. A confirmation is a delivery result; inspect its `error` field
before treating it as successful.

Do not associate concurrent sends with `getLastDeliveryResults()`: its shared
last-result slot is diagnostic, not a correlation mechanism. An error can leave
some messages successfully delivered. Blindly retrying the whole application
operation can duplicate side effects. Producer idempotence handles supported
producer retries; it does not make application processing exactly once.

### Manual flush

```ts
const bufferedProducer = client.createProducer({ autoFlush: false })
await bufferedProducer.send({
  topic: 'orders',
  messages: [{ payload: Buffer.from('first') }, { payload: Buffer.from('second') }],
})
const results = await bufferedProducer.flush()
console.log(results)
```

With `autoFlush: false`, `send()` enqueues records and returns an empty array.
Records can reach Kafka before `flush()`; this setting defers waiting for and
collecting delivery results, not transmission itself. Bound your outstanding
work and flush regularly and during shutdown. Await all auto-flush sends before
exiting. The current producer API has no `disconnect()` or `close()` method.

### Tombstones

```ts
await producer.send({
  topic: 'compacted-orders',
  messages: [{ key: Buffer.from('order-42'), isTombstone: true }],
})
```

Omitting `payload` produces a tombstone. An empty `Buffer` is an ordinary empty
value. Combining a payload with `isTombstone: true` is rejected, preserving the
caller's bytes instead of silently discarding them. On consumption, check
`isTombstone`; payload length alone cannot distinguish the two cases.

## KafkaConsumer

All consumer factories accept these options:

| Consumer option        | Default                          | Purpose                                                                                                          |
| ---------------------- | -------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| `groupId`              | Required                         | Kafka consumer group identifier.                                                                                 |
| `enableAutoCommit`     | librdkafka configuration/default | If provided, overrides `configuration['enable.auto.commit']`; omission preserves the advanced setting.           |
| `fetchMetadataTimeout` | 2,000 ms                         | Metadata timeout; zero falls back to the default, positive values clamp to 1–300,000 ms, negative values reject. |
| `configuration`        | None                             | Additional consumer librdkafka properties.                                                                       |

Current consumers supply `fetch.queue.backoff.ms=20` only when neither client nor
consumer configuration supplies it. Registry version 4.1.3 uses librdkafka's
1,000 ms default. These defaults must be identified when comparing versions.

### Subscription and assignment

```ts
const consumer = client.createConsumer({
  groupId: 'orders-workers',
  enableAutoCommit: false,
  configuration: {
    'enable.auto.offset.store': false,
    'auto.offset.reset': 'earliest',
  },
})
await consumer.subscribe('orders')
```

A string or an array of plain `{ topic }` entries uses group subscription.
Explicit offsets use manual assignment:

```ts
await consumer.subscribe([
  { topic: 'orders', allOffsets: { position: 'Beginning' } },
  { topic: 'payments', partitionOffset: [{ partition: 0, offset: { offset: 100 } }] },
])
```

Every entry in one call must use the same mode. Mixed group/manual entries reject.
Manual assignment covers exactly the specified partitions, or all partitions for
`allOffsets`; it does not use group balancing to distribute that assignment.
Metadata validates topic/partition references. Invalid entries and explicitly
empty partition lists reject without replacing the prior assignment.

For local setup, an entry can include `createTopic: true`, `numPartitions`, and
`replicas`. Topic-creation failures are returned after remaining topics have been
attempted and subscription applied; a rejected call can therefore leave the
consumer subscribed. Provision production topics through your infrastructure
workflow. This helper is not a general administrative API.

### Receiving messages

| Method                                          | Result                                                                             |
| ----------------------------------------------- | ---------------------------------------------------------------------------------- |
| `recv()`                                        | `Promise<Message \| null>`; waits for a message, returns `null` when disconnected. |
| `recvBatch(size, timeoutMs)`                    | `Promise<Message[]>`; up to `size`, with partial/empty batches allowed on timeout. |
| `recvStream(prefetchSize?, prefetchTimeoutMs?)` | Native `ReadableStream<Message>`.                                                  |
| `recvBatchStream(size, timeoutMs)`              | Native `ReadableStream<Message[]>`.                                                |
| `recvBatchStreamCompact(size, timeoutMs)`       | Compact native metadata batches; intended for wrapper expansion.                   |

Use one receive loop/stream per consumer. For application code, prefer
`createWebStreamConsumer()` to get the supported public `Message`/`Message[]`
representation and wrapper instrumentation. Empty direct batches mean no data
was collected in that call, not necessarily that the consumer has disconnected.

### Commits and processing order

`commitMessage(message, 'Sync')` commits `message.offset + 1`.
`commit(topic, partition, nextOffset, 'Sync')` takes the next offset directly.
Sync resolves after the broker result. Commit only successfully processed offsets
and never advance past unfinished work in the same partition. For a successfully
processed batch, commit the last processed message in each topic/partition.

Set both `enableAutoCommit: false` and `'enable.auto.offset.store': false` when
application completion controls commits. Prefetch does not imply completed work.
Reprocessing after failure is possible; make downstream side effects idempotent.

`'Async'` schedules the commit. Resolution of the method's promise is not broker
acknowledgment. Register `onEvents()` before scheduling, keep it alive, and observe
the matching `CommitCallback`, including its error. Native callback polling runs
independently of message receiving. Closing before the callback is observed is
best effort. Event delivery is bounded and can report skipped events if the
subscriber lags; use Sync when the application needs a simple confirmed boundary.

```ts
consumer.onEvents((error, event) => {
  if (error) console.error(error)
  if (event?.name === 'CommitCallback') {
    console.log(event.payload.tpl, event.payload.error)
  }
})
```

Other event names are `'PreRebalance'` and `'PostRebalance'`. They are string
literal types, not runtime enum objects. `payload.tpl` contains topic/partition
offsets; `payload.action` and `payload.error` are optional.

### Position, flow control, and shutdown

| Method                                          | Behavior                                                                                 |
| ----------------------------------------------- | ---------------------------------------------------------------------------------------- |
| `assignment()`                                  | Current assigned topic/partition entries.                                                |
| `getSubscription()`                             | Current subscription entries.                                                            |
| `getConfig()`                                   | Consumer configuration.                                                                  |
| `pause()` / `resume()`                          | Pause/resume fetching on all assigned partitions; already buffered work can still exist. |
| `seek(topic, partition, offsetModel, timeout?)` | Reposition an assigned partition; timeout defaults to 1,500 ms, maximum 300,000 ms.      |
| `unsubscribe()`                                 | Remove the subscription.                                                                 |
| `disconnect()`                                  | Terminal async teardown; create a new consumer to reconnect.                             |

For native batch streams, disconnect stops new collection and gives collected
work a bounded 1,500 ms drain window. Keep reading during this drain if the
application intends to process it. Work still blocked after the deadline is
reported and discarded from the local queue. Kafka records are not deleted;
replay depends on retained records and the application's committed offsets.

Stream cancellation is an interruption and can abandon in-flight collection.
It is appropriate when stopping immediately. For orderly shutdown, finish the
application's accepted work, commit the corresponding offsets, then close resources.
Do not treat cancellation as proof that every prefetched message was processed.

## Web Streams

```ts
const batchSource = client.createWebStreamConsumer({
  groupId: 'orders-batch',
  enableAutoCommit: false,
  batchSize: 64,
  batchTimeout: 5,
  configuration: { 'enable.auto.offset.store': false },
})
// batchSource.mode === 'batch'; stream chunks are Message[].
```

| Web Stream option       | Default  | Behavior                                                                      |
| ----------------------- | -------- | ----------------------------------------------------------------------------- |
| `batchSize`             | `1`      | Greater than 1 selects `Message[]` chunks; otherwise serial `Message` chunks. |
| `batchTimeout`          | 1,000 ms | Collection timeout in batch mode.                                             |
| `serialPrefetchSize`    | `64`     | Internal prefetch in serial mode; chunk size remains one message.             |
| `serialPrefetchTimeout` | `1` ms   | Serial prefetch collection timeout.                                           |

A numeric literal batch size selects the corresponding TypeScript return type.
For a dynamic size, narrow on `source.mode` before using `source.stream`.
Acquire a reader, subscribe through `source.consumer`, await each read/handler,
and release both reader and consumer in `finally`. The
[quick start](../README.md#quick-start) includes signal handling and manual commits.

Both public modes use the compact batch transport with JS expansion; serial
flattens deliveries into individual messages. The native regular/compact batch
queues have a 32 MiB accounted-byte budget and 256-batch capacity. Accounting
includes payloads, keys, headers, and compact metadata. A single oversized batch
may exceed the budget to guarantee progress. This is not an RSS limit: librdkafka
prefetch, a batch under construction, handed-out JS objects, and allocator regions
are separate. The lower-level native `recvStream()` path has its own buffering;
do not assume the same byte-budget contract for every receive API.

## Node.js streams

```ts
const nodeStream = client.createStreamConsumer({
  groupId: 'orders-node-stream',
  batchSize: 64,
  batchTimeout: 5,
  streamOptions: { highWaterMark: 16 },
})
```

Both `KafkaStreamReadable` and `KafkaBatchStreamReadable` emit individual `Message`
objects. `batchSize > 1` batches the internal transport; it does not emit arrays.
Object mode is required, and `objectMode: false` rejects. An explicit
`highWaterMark` is respected; it counts stream objects, not total buffered bytes.
`KafkaBatchStreamReadable.getBatchConfig()` returns `{ batchSize, batchTimeout }`.
Batch configuration is chosen at creation; create a new stream to change modes.

Use awaited async iteration or an awaited pipeline sink for asynchronous handlers.
An `async` listener on `'data'` does not make the event emitter await processing
and can create uncontrolled concurrency or commit ordering errors.

Streams expose `subscribe`, `seek`, `commit`, `commitMessage`, `unsubscribe`,
`disconnect`, `rawConsumer()`, and the underlying `kafkaConsumer`. Prefer
`destroy()` and await `'close'` for complete Node stream teardown. Destruction
cancels the source reader, unsubscribes, and awaits consumer disconnect. If awaiting
`'close'` manually, register the listener before calling `destroy()`; a `destroyed`
flag alone does not mean teardown has finished. Handle stream errors too.

## Types

```ts
import type { Message, RecordMetadata, SendFailureError } from 'kafka-crab-js'
```

| Type                   | Fields                                                                                         |
| ---------------------- | ---------------------------------------------------------------------------------------------- |
| `Message`              | `topic`, `partition`, `offset`, `payload: Buffer`, optional `key`, `headers`, `isTombstone`.   |
| `RecordMetadata`       | `topic`, `partition`, `offset`, optional `error: { code, message }`.                           |
| `OffsetModel`          | Optional `offset: number` or `position: 'Beginning' \| 'End' \| 'Stored' \| 'Invalid'`.        |
| `TopicPartitionConfig` | `topic`, optional `allOffsets`, `partitionOffset`, `createTopic`, `numPartitions`, `replicas`. |

Offsets are exposed as JavaScript numbers. Applications using offsets outside the
safe integer range must account for that representation limit. `Message` currently
has no public timestamp field. Headers use Buffer values and an object map;
applications needing repeated headers with identical names must account for that
representation rather than assuming an ordered list of duplicate keys.

`CommitMode`, `KafkaEventName`, `PartitionPosition`, and `SecurityProtocol` are
exported types with string literal values, not runtime enums. Use `'Sync'`, for
example, rather than `CommitMode.Sync`.

## OpenTelemetry

Install `kafka-crab-js-otel` separately, configure an OpenTelemetry SDK/exporter,
and call `enableOtelInstrumentation()` before creating clients. Keep
`diagnostics: true` on the client. Call `endSpan(message)` after application
processing when using consumer processing spans. See the
[OTEL package guide](../../kafka-crab-js-otel/README.md) and
[working examples](../../../examples/kafka/README.md#opentelemetry-examples).

## Scope and further reading

The supported runtime is Node.js 24 with the published native targets. Web Streams
here are Node-hosted APIs, not browser or WASM support. The public API does not
currently expose Kafka transactions, a schema registry client, or a full admin
client. Add application serializers, schema tooling, and provisioning as needed.

- [Production recommendations](../README.md#production-recommendations)
- [Benchmark results and reproduction](../../../BENCHMARKS.md)
- [Examples](../../../examples/kafka/README.md)
- [RFCs and current release review](rfc/README.md)
