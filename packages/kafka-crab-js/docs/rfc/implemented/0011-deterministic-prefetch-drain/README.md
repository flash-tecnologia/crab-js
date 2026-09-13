# RFC-FIX-0001: Deterministic drainage of prefetched batches

- Status: Implemented
- Related item: M01
- Priority: High

## Problem

`disconnect()` can race with a native batch that has already been collected but is blocked
while being handed to the JavaScript reader. The current send loop observes the disconnect
signal and exits before that pending handoff completes. The collected batch is then dropped.
This makes the delivery guarantee depend on scheduler timing and prevents us from proving
which prefetched messages were delivered.

The existing isolated regression reproduces the problem for both regular and compact batch
streams. Four batches containing offsets `0..127` fill the queue; a fifth, already-collected
batch containing `128..159` is blocked on send. After the disconnect signal, only `0..127`
are observed. Without disconnect, all 160 offsets arrive.

## Intended contract

The implementation must distinguish termination causes:

- `reader.cancel()` is an interrupt and may abandon an in-flight collection according to the
  M01 cancellation contract.
- `disconnect()` must make a bounded attempt to deliver every batch already collected before
  the disconnect was observed, including a batch whose send is currently pending.
- If the reader does not resume within the bounded grace period, the undelivered count must be
  reported through the documented warning/error path. The task must still terminate.

The contract must not claim that librdkafka messages still held outside the native collection
boundary were delivered; only batches acknowledged by the handoff are covered.

## Proposed fix

1. Record the handoff state for each collected batch before entering the send future.
2. On disconnect, stop collecting new Kafka messages but keep the pending batch owned by the
   drainage state machine.
3. Finish the pending handoff, or return it to the bounded drainage queue, before closing the
   output stream. Apply one explicit deadline to the complete drainage attempt.
4. Report the number of batches/messages that could not be handed off after the deadline.
5. Use the same state machine in regular and compact batch streams so their guarantees match.

The disconnect signal must not win by simply breaking the send loop while a batch is pending.
Cancellation remains a separate, immediate path.

## Acceptance criteria

- The deterministic native-drain test passes for regular and compact streams.
- A disconnect test with a blocked handoff observes all offsets from every batch collected before
  disconnect, in order and without duplicates.
- A stalled reader proves bounded termination and verifies the reported undelivered count.
- A real-Kafka integration test demonstrates the same behavior with backlog and a slow reader,
  without sleeps being used as synchronization.
- Existing cancellation tests continue to show that cancellation interrupts collection and does
  not keep a native task consuming future messages.
- The RFC is moved to `docs/rfc/implemented` only after the source, unit/regression, and
  integration evidence agree with this contract.

## Implementation and Verification Evidence

The drainage state machine was implemented in `src/kafka/consumer/kafka_consumer.rs` for both
`recv_batch_stream_internal` and `recv_batch_stream_compact_internal`:

1. **Pinned Send and Grace Timer**: The batch send future is pinned and polled in a loop alongside
   a 1500ms bounded grace timer. When `disconnect()` occurs, the grace timer activates without
   dropping the in-flight batch. If the reader resumes, the handoff finishes cleanly.
2. **Bounded Termination on Stalled Reader**: If the reader remains stalled beyond the grace timeout,
   the timer expires, the batch is abandoned with a warning, and the task terminates promptly without deadlock.
3. **Cancellation Priority**: Cancellation (`cancel_receiver`) immediately aborts the send handoff
   without waiting for drainage.
4. **Disconnect Version Isolation**: Using a local cloned receiver for `collect_batch_messages` prevents
   swallowing the disconnect event before the handoff loop evaluates termination.

**Verification Results (2026-09-09, re-verified after review fixes):**

- `cargo test --offline --manifest-path js-tests/native-drain/Cargo.toml`: **6/6 passed** (regular & compact resume, blocked disconnect batch preservation, and stalled reader bounded termination). The harness needed two fixes to be real: the shim send future is `Box::pin`ned (Tokio polls it via `&mut`, as in production) and the graceful-expiry `warn!` is covered by a capturing `tracing` subscriber that asserts exactly one warning with `dropped = 32` on stall and zero warnings on resume.
- `pnpm test`: **43/43 passed** across all unit and regression suites without timeouts, including two disconnect-drain regressions (partial compact batch and serial prefetch reach a live reader).
- Real Kafka (`localhost:9092`): `stream-lifecycle-real.test.mjs` **12/12** (slow/backlog/cancel/prefetch/partial across serial, batch, compact) and `consumer-manual-commit.test.mjs` **9/9** after replacing rebalance waits (never emitted under manual assignment) with assignment polls.
- `cargo clippy --all-targets --offline`: **0 warnings**.
- `cargo fmt --check`: **Clean**.
- `pnpm lint` and `pnpm fmt:check`: **0 warnings, 0 errors across all files**.

**Re-verification (2026-09-10, conformance fixes F01–F04):** the harness now extracts the
reserve-through-handoff block, after F01 showed the isolated handoff missed a disconnect consumed
by the byte-budget wait. `cargo test --offline --manifest-path js-tests/native-drain/Cargo.toml`:
**18/18** (6 steady-state handoff, 6 byte-wait disconnect/resume/cancel × regular/compact,
3 broadcast policy pins, 3 `ByteBudget` unit tests). `pnpm test`: **54/54** against the rebuilt
binding. Clippy, `cargo fmt`, lint, and `fmt:check` clean. Real-Kafka integration was not
re-run here (no broker available); see the conformance review §10 for the remaining gaps.

## Non-goals

This RFC does not change cancellation semantics, commit behavior, producer delivery tracking,
or the byte-based backpressure policy covered by other RFCs.
