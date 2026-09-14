# RFC-0001: Native stream cancellation and lifecycle

- Status: Implemented
- Review item: M01
- Priority: High

## Problem

Cancelling a JavaScript `ReadableStream` could leave the native Tokio collector waiting for
Kafka messages. The abandoned collector could consume messages produced after cancellation,
compete with later reads, and remain alive until another channel operation exposed closure.
Termination cause determines the delivery guarantee: explicit cancellation is an interrupt
(an in-flight collection is abandoned and its partial batch may be discarded), while
disconnect without cancellation drains already-collected data to a live reader.

## Decision

Propagate reader cancellation into the native collection task. Use a bounded channel for
prefetched batches and observe cancellation, disconnect, queue closure, and timeouts while
collecting and sending both regular and compact batches.

## Implementation

`recv_stream`, `recv_batch_stream`, and `recv_batch_stream_compact` wrap the native stream with
a cancellation callback backed by `tokio::sync::watch`. The collection loops use biased
`select!` branches and bounded `mpsc` channels. The contract is split by termination cause:
cancel drops an in-flight collection (serial items buffered by earlier collects are still
emitted, since they were pulled before cancellation); disconnect makes a bounded delivery
attempt (one batch timeout) for already-collected batches to a live reader. Only a stalled
reader past that grace period loses collected messages, surfaced as a warning with the count.
`reader.cancel()` on the wrapper propagates the watch signal and tears down the inner reader;
it does not wait for prefetch delivery.

## Evidence

Two disconnect-drain regressions prove a partial compact batch and serial prefetch reach a live
reader after `disconnect()`. General regression coverage exercises idle cancellation, pending
reads, partial batches, serial streams, and prefetch queues in
[regressions.test.ts](../../../../js-tests/unit/regressions.test.ts).

## Follow-up

Measure memory and task lifetime with sustained backlog and slow readers against a real broker.
