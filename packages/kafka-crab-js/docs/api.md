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

| Factory                            | Result                                                          |
| ---------------------------------- | --------------------------------------------------------------- |
| `createProducer(options?)`         | `KafkaProducer`                                                 |
| `createAdmin(options?)`            | `KafkaAdmin`; administrative queries and topic provisioning     |
| `createConsumer(options)`          | `KafkaConsumer`                                                 |
| `createWebStreamConsumer(options)` | `{ mode, consumer, stream }`, with serial or batch Web Stream   |
| `createStreamConsumer(options)`    | Node.js `KafkaStreamReadable` or `KafkaBatchStreamReadable`     |
| `deleteRecords(topicPartitions)`   | `Promise<TopicPartition[]>`; deletes records before each offset |

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

## KafkaAdmin

```ts
const admin = client.createAdmin({ timeoutMs: 5000 })
try {
  const results = await admin.createTopics({
    topics: [{ topic: 'orders', numPartitions: 3, replicationFactor: 1 }],
  })
  for (const result of results) {
    if (result.error) throw new Error(`${result.topic}: ${result.error.message}`)
  }
  const watermarks = await admin.fetchTopicOffsets('orders')
  const committed = await admin.fetchOffsets({ groupId: 'orders-service', topics: ['orders'] })
  const groups = await admin.describeGroups(['orders-service'])
} finally {
  await admin.close()
}
```

The admin inherits the client's brokers, TLS/SASL credentials, and configuration.
`timeoutMs` defaults to 5,000 ms and bounds each operation, including all requested
topics/partitions. Blocking native queries run outside the JavaScript thread.

| Method                                      | Result and contract                                                                                                           |
| ------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| `createTopics({ topics, waitForLeaders? })` | `Promise<AdminTopicResult[]>`; explicit partitions/replication, optional per-topic `config`. Waits for leaders by default.    |
| `fetchTopicMetadata(topics)`                | `Promise<AdminTopicMetadata[]>`; partitions, leaders, replicas, ISR, and per-topic/partition errors.                          |
| `fetchTopicOffsets(topic)`                  | `Promise<AdminTopicOffset[]>`; inclusive `low` and exclusive `high` watermarks per partition.                                 |
| `fetchOffsets({ groupId, topics })`         | `Promise<AdminGroupOffsets[]>`; committed next offsets per partition.                                                         |
| `describeGroups(groupIds)`                  | `Promise<AdminGroupDescription[]>`; state, protocol, and members with raw assignment/metadata buffers. Missing groups reject. |
| `close()`                                   | `Promise<void>`; idempotent. Rejects new operations; already started operations may finish.                                   |

Watermarks and committed offsets are decimal strings to preserve Kafka's full
64-bit precision. A partition without a committed offset has no `offset` field;
it is not converted to zero or resolved to a watermark. Watermark failures have
an `error` and no `low`/`high`. Inspect result errors (`{ code, message }`) before
using a topic/partition; transport/deadline failures reject the operation.

Queries do not subscribe, join groups, commit, reset offsets, or auto-create
topics. `createTopics` reports partial success per topic. An existing topic has
`created: false` without an error and is not modified. Explicit replication
factors are sent unchanged to Kafka; insufficient brokers produce an error.
Topic creation does not require a broker configuration lookup. If waiting for
leaders times out, successful creations are not rolled back.

The existing `consumer.subscribe([{ createTopic: true, replicas }])` path retains
its replication fallback to the available broker count and its existing request
timeouts. Strict replication and `timeoutMs` apply to the new admin API.

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
`headers?: Record<string, Buffer>`, `isTombstone?: boolean`, and `partition?: number`.
An explicit partition must be a non-negative 32-bit integer and overrides the
configured partitioner. Omit it to retain normal key/partitioner routing.
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

| Consumer option        | Default                          | Purpose                                                                                                              |
| ---------------------- | -------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| `groupId`              | Required                         | Kafka consumer group identifier.                                                                                     |
| `enableAutoCommit`     | librdkafka configuration/default | If provided, overrides `configuration['enable.auto.commit']`; omission preserves the advanced setting.               |
| `fetchMetadataTimeout` | 2,000 ms                         | Metadata timeout; zero falls back to the default, positive values clamp to 1–300,000 ms, negative values reject.     |
| `configuration`        | None                             | Additional consumer librdkafka properties.                                                                           |
| `managed`              | `false`                          | Experimental. Enables [managed consumption](#managed-consumption-experimental) for this consumer; fixed at creation. |
| `managedOptions`       | See below                        | Experimental. Limits of the managed native state; requires `managed: true`.                                          |

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
  {
    topic: 'payments',
    partitionOffset: [{ partition: 0, offset: { offset: 100 } }],
  },
])
```

Every entry in one call must use the same mode. Mixed group/manual entries reject.
Manual assignment covers exactly the specified partitions, or all partitions for
`allOffsets`; it does not use group balancing to distribute that assignment.
Metadata validates topic/partition references. Invalid entries and explicitly
empty partition lists reject without replacing the prior assignment.

### Deleting records

```ts
await client.deleteRecords([
  {
    topic: 'orders-dlq',
    partitionOffset: [{ partition: 0, offset: { offset: 42 } }],
  },
])
```

`deleteRecords` uses Kafka's `DeleteRecords` admin operation. For each partition,
Kafka deletes records with offsets lower than the requested offset; passing `42`
therefore removes records through offset `41`. The operation requires explicit,
non-negative offsets and returns the broker's resulting low-water marks. This is a
physical deletion from the log and requires the Kafka ACL permission for deleting
records. It is not equivalent to committing a consumer offset.

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

| Method                                                         | Behavior                                                                                              |
| -------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| `assignment()`                                                 | Current assigned topic/partition entries.                                                             |
| `getSubscription()`                                            | Current subscription entries.                                                                         |
| `getConfig()`                                                  | Consumer configuration.                                                                               |
| `pause()` / `resume()`                                         | Pause/resume fetching on all assigned partitions; already buffered work can still exist.              |
| `pausePartitions(partitions)` / `resumePartitions(partitions)` | Control only the supplied `{ topic, partition }` entries. Already buffered work can still exist.      |
| `seek(topic, partition, offsetModel, timeout?)`                | Reposition an assigned partition; timeout defaults to 1,500 ms, maximum 300,000 ms.                   |
| `unsubscribe()`                                                | Remove the subscription.                                                                              |
| `disconnect(options?)`                                         | Terminal async teardown; waits for native close. Optional `{ signal: AbortSignal }` cancels the wait. |

Partition flow control leaves other assigned partitions running. The entries use
the exported `TopicPartitionId` type. Empty topic names and negative partition
numbers are rejected; librdkafka reports unassigned partition errors. These methods
cannot retract messages already fetched into application queues.

Use `consumer.disconnect({ signal: AbortSignal.timeout(5_000) })` to bound
one caller's wait, or pass an `AbortController`'s signal. Without a signal, the
call waits for native closure. Aborting rejects with the exact `signal.reason`;
it does not undo the terminal transition or cancel native cleanup. This also
applies to a signal that was already aborted when passed. Another caller can
still await `consumer.disconnect()` to confirm closure. Abort listeners are
removed when the wait settles. Stream consumers accept the same options.

Repeated or concurrent `disconnect()` calls are safe. Pending receives are
interrupted and `assignment()` returns an empty list after disconnection. Graceful
disconnect serves rebalance events while waiting for native closure. Dynamic group
members leave the group; static membership follows librdkafka's protocol rules.
A killed process still relies on the broker's session timeout. With rust-rdkafka 0.39, its
destructor can log a redundant queue-close error after an explicit native close;
the disconnect promise already confirms closure before this diagnostic.

For native batch streams, disconnect stops new collection and gives collected
work a bounded 1,500 ms drain window. Keep reading during this drain if the
application intends to process it. Work still blocked after the deadline is
reported and discarded from the local queue. Kafka records are not deleted;
replay depends on retained records and the application's committed offsets.

Stream cancellation is an interruption and can abandon in-flight collection.
It is appropriate when stopping immediately. For orderly shutdown, finish the
application's accepted work, commit the corresponding offsets, then close resources.
Do not treat cancellation as proof that every prefetched message was processed.

A broker transport failure can error a stream. Handle the rejected read or async
iteration, cancel the failed reader, and disconnect its consumer. Create a new
consumer with the same `groupId` and subscribe without overriding the committed
offsets to resume. With application-controlled commits, keep auto-commit and
auto-offset-store disabled and commit only completed work. Work processed before
a failure but not committed can be replayed, so downstream side effects should
be idempotent.

### Managed consumption (experimental)

Managed consumption is an opt-in contract from
[RFC-0017](rfc/proposed/0017-managed-consumption-rebalance/). It is a prototype: the API can
change, it is built and tested on darwin-arm64 only, and it has not been validated for
release. Check `MANAGED_CONSUMPTION_VERSION` (currently `1`) before relying on it.

It ties each message, and each commit, to the incarnation of the partition assignment that
delivered it. Assignment transitions and messages arrive as one sequence whose order is
defined natively, so a message can never be attributed to a later assignment.

```ts
import { KafkaClient, MANAGED_ERROR_CODES, type ManagedConsumptionError } from 'kafka-crab-js'

const consumer = client.createConsumer({ groupId: 'orders', managed: true })
await consumer.subscribe('orders')

for (;;) {
  const record = await consumer.recvManaged(1_000)
  if (record === null) break // Disconnected
  if (record.kind === 'message') {
    await handle(record.message)
    try {
      // The next offset to consume, under the identity that delivered the message.
      await consumer.commitManaged(
        record.message.topic,
        record.message.partition,
        record.assignment,
        record.message.offset + 1,
      )
    } catch (error) {
      const { code } = error as ManagedConsumptionError
      if (code !== MANAGED_ERROR_CODES.STALE_ASSIGNMENT) throw error
      // The partition was revoked: the next owner replays this message.
    }
  }
}
```

Activation is fixed when the consumer is created. Managed mode disables
`enable.auto.commit` and `enable.auto.offset.store` and rejects a configuration that enables
either. A managed consumer rejects `recv`, `recvBatch`, the stream methods and factories,
`commit`, `commitMessage`, `seek`, `unsubscribe` and manual assignment with an
`[ERR_MANAGED_MODE]` error; a legacy consumer rejects the methods below. `onEvents` keeps
working as an observational listener and is not part of the ordering.

| Method                                                    | Behavior                                                                                                                              |
| --------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `managed`                                                 | Getter: whether the consumer was created with `managed: true`.                                                                        |
| `recvManaged(timeoutMs?)`                                 | Next record in native order. Only one call may be pending.                                                                            |
| `cancelManagedReceive()`                                  | Resolves the pending receive as `{ kind: 'idle', reason: 'cancelled' }`; returns whether one was pending. No record is lost.          |
| `commitManaged(topic, partition, assignment, nextOffset)` | Commits the next offset to consume under an assignment identity. Resolves with the broker result. Never retried automatically.        |
| `isManagedAssignmentActive(topic, partition, assignment)` | Whether the identity owns the partition right now, read from native state. It can become stale immediately.                           |
| `managedState()`                                          | Diagnostic snapshot: `status`, `failure`, `assignments`, queued control records, retained messages and bytes, pause, pending commits. |

`recvManaged` resolves one of:

| Record                                               | Meaning                                                                                                                                        |
| ---------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `{ kind: 'message', message, assignment }`           | An unchanged `Message` and the identity that owned its partition when it was admitted.                                                         |
| `{ kind: 'assign', partitions }`                     | Each entry has `topic`, `partition` and its `assignment`. The list can be empty. A retained partition has `previousAssignment === assignment`. |
| `{ kind: 'revoke', partitions, lost }`               | Each entry names the invalidated identity in `previousAssignment`. `lost` is librdkafka's assignment-lost flag.                                |
| `{ kind: 'idle', reason: 'timeout' \| 'cancelled' }` | No record. Not end-of-stream.                                                                                                                  |
| `null`                                               | The consumer was disconnected while the call was pending.                                                                                      |

Every transition that preceded a message is returned before it, and transitions are returned
on an idle topic. An identity is an opaque string. It stays the same while the partition is
continuously owned, including across a cooperative rebalance that retains it, and changes
whenever the partition is revoked and assigned again. When an identity is revoked, messages
under it that have not been returned yet are released; the next owner fetches them again.

Errors from the managed methods are rejections with real properties
(`ManagedConsumptionError`); `isManagedConsumptionError(error)` narrows them.

| `error.code`                     | Meaning                                                                                         |
| -------------------------------- | ----------------------------------------------------------------------------------------------- |
| `ERR_MANAGED_STALE_ASSIGNMENT`   | The identity was revoked. Nothing was sent. Do not resubmit the commit under a newer identity.  |
| `ERR_MANAGED_UNKNOWN_ASSIGNMENT` | The identity is malformed or belongs to another consumer.                                       |
| `ERR_MANAGED_COMMIT_FAILED`      | Kafka rejected or failed the commit; `kafkaCode` and `kafkaErrorCode` identify the error.       |
| `ERR_MANAGED_COMMIT_UNKNOWN`     | The request was dispatched but no result was observed, for example because the consumer closed. |
| `ERR_MANAGED_RECEIVE_PENDING`    | Another `recvManaged` is pending.                                                               |
| `ERR_MANAGED_RECEIVE_FAILED`     | A consumption error from librdkafka, with `kafkaCode`. Not terminal: call `recvManaged` again.  |
| `ERR_MANAGED_CLOSED`             | The consumer is disconnecting or disconnected.                                                  |
| `ERR_MANAGED_CONTROL_OVERFLOW`   | Terminal. More transitions were pending than `maxControlRecords`.                               |
| `ERR_MANAGED_CONTROL_FAILURE`    | Terminal. The native control state became incoherent, or librdkafka reported a fatal error.     |
| `ERR_MANAGED_INVALID_ARGUMENT`   | Invalid argument.                                                                               |
| `ERR_MANAGED_MODE`               | A managed method was called on a legacy consumer.                                               |

Commit errors also carry `outcomeUnknown`. When it is `true` the broker may still have applied
the commit, so it is not proof of failure. A Kafka error code alone does not say whether the
partition was kept: `IllegalGeneration` and `RebalanceInProgress` also occur on partitions that
a cooperative rebalance retains. Use `isManagedAssignmentActive` or the next `commitManaged`
to find out.

**What the identity does not guarantee.** It is a local authorization inside one consumer, not
fencing by the broker:

- A commit handed to librdkafka before a revocation cannot be recalled and may be applied
  after the partition moved. It covers work that was already completed, so it cannot skip
  messages, but it can move the committed offset backwards and cause replay.
- Kafka does not check partition ownership on a commit. Other members, and legacy consumers
  in the same group, are not constrained by it.
- It does not know whether a handler finished. Commit only completed work.

A terminal failure invalidates every identity before JavaScript is told, clears the native
buffers and makes the consumer leave the group so its partitions are released. Disconnect the
consumer and create a new one.

`managedOptions` bounds the state owned by the binding:

| Option                | Default | Behavior                                                                                 |
| --------------------- | ------- | ---------------------------------------------------------------------------------------- |
| `maxControlRecords`   | 1,024   | Transitions waiting for `recvManaged` (1–65,536). Exceeding it is the terminal overflow. |
| `maxBufferedMessages` | 1,024   | Messages retained natively (1–1,000,000).                                                |
| `maxBufferedBytes`    | 32 MiB  | Bytes retained natively. One message larger than the limit is still admitted.            |

The binding polls natively whether or not `recvManaged` is called, so rebalances are served
and authorization is updated while the application is busy. At a message limit it stops taking
messages; if the application frees no room for 100 ms it pauses the owned partitions and
resumes them at half of the limit. The limits can be exceeded by a few messages around a
pause, and they do not cover librdkafka's own fetch queues or messages already returned.
`pause`, `resume`, `pausePartitions` and `resumePartitions` keep working; only owned
partitions can be paused and a pause ends when the partition is revoked.

`disconnect()` resolves a pending `recvManaged` with `null`, rejects later managed calls and
clears the buffers. A commit already dispatched settles on its own with the broker result or
`ERR_MANAGED_COMMIT_UNKNOWN`; disconnecting does not cancel it.

With diagnostics enabled, only `message` records publish receive events and open a
processing span, closed with `record.message.endSpan()`. Transitions and idle results publish
nothing.

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
