# RFC-0012: Per-message `oneshot` delivery tracking

- Status: Implemented (2026-09-10)
- Related item: M06
- Priority: High

## Motivation

The previous state map provided concurrent access, but callbacks and timeouts shared a
global map entry. A per-message `tokio::sync::oneshot` channel gives one delivery result a
single owner and makes a late callback harmless when its receiver has been dropped.

## Decision

Carry a `oneshot::Sender` in librdkafka's `DeliveryOpaque`; keep the receiver with the send or
flush operation; and define explicit timeout, cancellation, partial-failure, and `autoFlush: false`
semantics. Do not adopt the design based on the isolated benchmark alone.

## Evidence

The historical isolated prototype excluded Kafka, NAPI, async receiver waits and real flush
behavior. Its benchmark artifact is no longer present in this repository, so its throughput
claims are not used as current evidence. See the consolidated
[performance review](../../review/performance.md) for maintained evidence and limitations.

## Acceptance criteria

Prove no lost confirmations or orphaned callbacks under timeout races, concurrent sends and
flushes, partial enqueue, and shutdown. Compare throughput, p95/p99, and memory against the
current implementation using a real broker.

## Implementation

`CollectingContext` is stateless (partitioner only). Each message carries a
`Box<oneshot::Sender<DeliveryResultData>>` as its librdkafka `DeliveryOpaque`;
the delivery callback moves the result into the channel, and a dropped receiver
(timeout, shutdown) turns late callbacks into harmless no-ops. `send()` owns its
receivers in auto mode; manual mode stashes them in a mutex-guarded pending list
drained by `flush()`, one receiver per confirmation, so concurrent operations
cannot steal each other's results. One queue-timeout budget covers flush plus
gathering; unconfirmed results are abandoned by dropping receivers — no map, no
IDs, no expiry pass. `dashmap` and `nanoid` left the dependency tree. The
`getLastDeliveryResults()` compat API and the `enqueued X of Y, confirmed Z`
message are unchanged; the JS per-instance send/flush serialization was removed
as its race no longer exists.

## Verification (2026-09-10)

- `pnpm test` 43/43, including late-callback, partial-enqueue, concurrent-flush,
  native-concurrency, and tombstone-rejection regressions.
- Real Kafka (`localhost:9092`): lifecycle 12/12 and manual-commit 9/9, whose
  producer paths cover sync/async commits, restarts, batches, and partial sends.
- Producer bench (`benchmarks/kafka/producer.ts`, added for this change): v4 vs previous
  release across autoFlush/manual modes, 20k messages × 3 runs each — throughput parity
  (~971 op/sec all four, ±0.1%) and identical batch p50/p95/p99 (~103/106/108 ms),
  flush-dominated as expected.
- The timeout-storm probe does not prove native cleanup: later review found sequential
  sends against a refused connection and in-flight messages still present at the final
  sample. Retained RSS alone is not evidence that all pending deliveries were released.
- `cargo clippy --all-targets --offline`, `cargo fmt --check`, `pnpm lint`,
  `pnpm fmt:check`: clean.
