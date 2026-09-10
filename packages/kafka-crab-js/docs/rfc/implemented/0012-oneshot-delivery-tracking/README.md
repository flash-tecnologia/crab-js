# RFC-0011: Evaluate per-message `oneshot` delivery tracking

- Status: Implemented (2026-09-10)
- Related item: M06
- Priority: High

## Motivation

The current state map provides concurrent access, but each callback and timeout still shares a
global map entry. A per-message `tokio::sync::oneshot` channel could give one delivery result a
single owner and make a late callback harmless when its receiver has been dropped.

## Proposal

Carry a `oneshot::Sender` in librdkafka's `DeliveryOpaque`; keep the receiver with the send or
flush operation; and define explicit timeout, cancellation, partial-failure, and `autoFlush: false`
semantics. Do not adopt the design based on the isolated benchmark alone.

## Evidence

The [isolated benchmark](../../../../benchmarks/delivery-tracking/ANALISE.md) measured 1.34×–3.08×
the throughput of a single `DashMap` for 256-message batches without expiration, with the
largest difference at eight producer lanes. It excludes Kafka, NAPI, async receiver waits,
and real flush behavior.

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
- Timeout storm (`benchmark:producer:storm`, 300 × 64 KiB timed-out sends, blackhole):
  no retained RSS growth in either implementation (previous clears entries on flush
  error instead of leaking; v4 drops receivers by construction). The dramatic-leak
  story does not reproduce against release 4.1.3 — the win stays structural (no shared
  state to reason about, no steal class, −14 dependency crates), not a measured delta.
- `cargo clippy --all-targets --offline`, `cargo fmt --check`, `pnpm lint`,
  `pnpm fmt:check`: clean.
