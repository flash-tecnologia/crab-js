# Deterministic regression: disconnect during a blocked native batch send

This isolated Rust test suite demonstrates the missing drainage guarantee in the common and
compact native batch streams. It leaves production code and the public API unchanged.

## Run

From `packages/kafka-crab-js`:

```sh
cargo test --offline --manifest-path js-tests/native-drain/Cargo.toml
```

Current result: **6/6 passing** (resume, blocked-disconnect, and stalled-reader cases for both
stream variants). The existing `pnpm test` command does not run this standalone crate.

## How synchronization works

1. Fill a real Tokio channel of capacity four with offsets 0–127 in four batches.
2. Hand offsets 128–159 to the native send block as an already-collected fifth batch.
3. Poll the actual `mpsc::Sender::send` future. Signal the test only when that future returns
   `Pending` and channel capacity is zero.
4. For the failure cases, send the watch disconnect signal before letting the reader resume.
5. Drain the live reader, await task completion, and compare every delivered offset with 0–159.

The current-thread runtime and the production `biased` select make the competing ready
branches deterministic. There are no sleeps. Two-second timeouts only detect a hung test;
they are not synchronization mechanisms.

The controls resume the reader without disconnect: all 160 offsets arrive with no drop report.
With disconnect, the blocked batch containing 128–159 is still delivered (grace period active
while the reader is live). With a stalled reader, the task terminates at the 1500ms grace
expiry and reports exactly one warning with `dropped = 32`, verified by a capturing
`tracing` subscriber (no sleeps; the count assertion reads the structured event field).

## Relationship to production code

`build.rs` extracts the send/select block directly from `recv_batch_stream_internal` and
`recv_batch_stream_compact_internal` in `src/kafka/consumer/kafka_consumer.rs` at build time.
It inserts the block unchanged into a one-iteration harness, preserving its `break` behavior.
Cargo rebuilds the generated code when that source file changes. Structural changes that
invalidate the extraction fail loudly and require reviewing the harness.

The observed sender delegates to a real Tokio sender and only reports a pending poll.
Message values are offset vectors instead of NAPI message objects. This avoids linking the
production `cdylib` into a standalone test and provides exact ownership accounting without
Kafka delivery timing or modifications to the production source.

This proves the behavior of the actual **queue handoff block**, not the complete Kafka/NAPI
pipeline. It does not exercise partial collection, serial prefetch, explicit reader cancellation,
deadline policy, or broker commits. The real-Kafka suite remains complementary:
[stream-lifecycle-real.test.mjs](../integration/stream-lifecycle-real.test.mjs).

To fix this regression, the production disconnect path must retain the already-collected
blocked batch and finish its handoff to a live reader. A bounded drainage policy must report
incomplete delivery if the reader never resumes; cancellation may remain an interrupt.
