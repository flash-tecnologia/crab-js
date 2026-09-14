# Deterministic regression: disconnect during a blocked native batch send

This isolated Rust test suite pins the drainage guarantee in the common and
compact native batch streams, including the byte-budget reserve that precedes
the handoff. It leaves production code and the public API unchanged.

## Run

From `packages/kafka-crab-js`:

```sh
cargo test --offline --manifest-path js-tests/native-drain/Cargo.toml
```

Current result: **18/18 passing**:

- 6 steady-state handoff cases (resume, blocked-disconnect, and stalled-reader
  for both stream variants).
- 6 byte-wait cases (disconnect stays bounded, resume still delivers in order,
  and cancel ends promptly, for both variants).
- 3 broadcast overflow pins for the consumer event channel policy.
- 3 `ByteBudget` unit tests.

The existing `pnpm test` command does not run this standalone crate.

## How synchronization works

1. Fill a real Tokio channel of capacity four with offsets 0–127 in four batches.
2. Hand offsets 128–159 to the native reserve/handoff block as an already-collected fifth batch.
3. Poll the actual `mpsc::Sender::send` future. Signal the test only when that future returns
   `Pending` and channel capacity is zero.
4. For the failure cases, send the watch disconnect signal before letting the reader resume.
5. Drain the live reader, await task completion, and compare every delivered offset with 0–159.

The byte-wait cases additionally fill a 4-unit `ByteBudget` so the fifth batch parks in the
reserve first. The future is polled once to prove it is `Pending` before disconnect is sent;
no sleeps are used for ordering. Timeouts only detect a hung test.

The current-thread runtime and the production `biased` select make the competing ready
branches deterministic. Two-second timeouts only detect a hung test;
they are not synchronization mechanisms.

The controls resume the reader without disconnect: all 160 offsets arrive with no drop report.
With disconnect, the blocked batch containing 128–159 is still delivered (grace period active
while the reader is live). With a stalled reader, the task terminates at the 1500ms grace
expiry and reports exactly one warning with `dropped = 32`, verified by a capturing
`tracing` subscriber (no sleeps; the count assertion reads the structured event field).
The byte-wait resume cases prove the same delivery when disconnect arrives during the
reserve; the cancel cases prove cancellation during the reserve ends promptly with no
drain warning.

## Broadcast overflow policy

Three tests pin the `tokio::sync::broadcast` semantics the event channel relies on
(pinned `=1.53.1`): 100 requested slots round up to 128, overflow overwrites the oldest
retained events with an exact `Lagged(skipped)` report, late subscribers see no history,
and `send()` only fails with no receivers.

## Relationship to production code

`build.rs` extracts the reserve-through-handoff block directly from `recv_batch_stream_internal` and
`recv_batch_stream_compact_internal` in `src/kafka/consumer/kafka_consumer.rs` at build time.
It inserts the block unchanged into a one-iteration harness, preserving its `break` behavior.
Cargo rebuilds the generated code when that source file changes. Structural changes that
invalidate the extraction fail loudly and require reviewing the harness.

The observed sender delegates to a real Tokio sender and only reports a pending poll.
Message values are offset vectors instead of NAPI message objects. This avoids linking the
production `cdylib` into a standalone test and provides exact ownership accounting without
Kafka delivery timing or modifications to the production source.
The handoff now carries the cached byte count with the batch. The observed sender verifies
that this count matches the synthetic payload before forwarding it to the test channel.

This proves the behavior of the actual **reserve and queue handoff block**, not the complete Kafka/NAPI
pipeline. It does not exercise partial collection, serial prefetch, explicit reader cancellation,
deadline policy, or broker commits. The real-Kafka suite remains complementary:
[stream-lifecycle-real.test.mjs](../integration/stream-lifecycle-real.test.mjs).

To fix this regression, the production disconnect path must retain the already-collected
blocked batch and finish its handoff to a live reader. A bounded drainage policy must report
incomplete delivery if the reader never resumes; cancellation may remain an interrupt.
