# RFC-0011: Evaluate per-message `oneshot` delivery tracking

- Status: Proposed
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
