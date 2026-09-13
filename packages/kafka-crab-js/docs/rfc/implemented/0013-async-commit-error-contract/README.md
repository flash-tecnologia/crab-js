# RFC-0013: Asynchronous commit error contract

- Status: Implemented; F06 corrected and covered by real-Kafka regressions (2026-09-12)
- Related item: M07
- Priority: Medium

## Problem

An asynchronous commit error is delivered through a bounded broadcast channel. If no listener
exists, the application never observes the error.

## Decision

Reject `commit(..., 'Async')` and `commitMessage(..., 'Async')` unless `onEvents()` has been
registered on that consumer. Use `'Sync'` when the caller does not consume `CommitCallback`.

Authorization tracks live event tasks, not historic registration: each `onEvents()` call spawns
one forwarding task, and the active-task count gates Async commits. After `disconnect()` the
tasks stop and new Async commits are rejected, since no observer remains. `disconnect()` is
terminal for the consumer; a later `onEvents()` would stop immediately and does not re-authorize
Async commits. Validation and native enqueue share the lifecycle lock with disconnect;
once disconnect wins, new Async commits cannot be scheduled.

Manual Async commits use `rd_kafka_commit_queue` with a dedicated reply queue, created
lazily per consumer. A Tokio task polls ready events with timeout zero every 10 ms,
at most 128 events per tick, and forwards the complete result through the existing
context broadcast. This does not depend on `recv()` or consume records from the main
queue. The task holds a weak queue reference; disconnect releases the owned queue,
and queued native events are destroyed through RAII. The native client stays alive
until its queue reference is released. `'Sync'` continues to await the broker response
in `spawn_blocking`; `'Async'` resolves after native scheduling.

Overflow policy follows `tokio::sync::broadcast` (pinned `=1.53.1`): capacity 100 is rounded up
to 128 slots; a send on a full channel overwrites the oldest retained events instead of dropping
the new one. `send()` itself only fails when no receiver exists. A lagging listener observes the
loss as `Lagged(skipped)` on its next read, surfaced as a warning with the skipped count, and
keeps receiving newer events. A late subscriber only sees events sent after its `resubscribe()`
and never replays history. No bound is claimed for the full path through the `ThreadsafeFunction`
to JavaScript execution; a slow JavaScript callback can fall behind the native channel.

Callbacks for commits already scheduled when disconnect begins are best-effort: tasks exiting on
disconnect may miss events that had not been read yet, including unobserved `Lagged` losses.
The native reply queue and JavaScript callback queue are not covered by the consumer
message-byte budget. Applications needing confirmation before shutdown must await the
matching event or use Sync.

## Evidence

The release review reproduced missing manual `CommitCallback` events before the fix.
The new queue fixes that wiring. Real-Kafka tests now require success and
`UnknownTopicOrPartition` callbacks with topic, partition and offset intact, without
receive polling; 32 concurrent commits preserve all results; a commit/disconnect
race settles and leaves Async disabled. The existing async integration also requires
one successful callback for every scheduled commit. See
[F06 and its evidence](../../review/validation.md).

Regressions in [regressions.test.ts](../../../../js-tests/unit/regressions.test.ts) require the
no-listener path to fail with `InvalidArg`, allow Async after `onEvents()`, reject Async
`commit()` and `commitMessage()` after `disconnect()`, and keep rejecting with multiple
listeners registered before disconnect. Broadcast semantics (128 slots from 100 requested,
oldest-overwrite, exact `Lagged` count, no history for late subscribers) are pinned by
[overflow tests](../../../../js-tests/native-drain/src/lib.rs) against the pinned Tokio version.
Implementation: [commit_queue.rs](../../../../src/kafka/consumer/commit_queue.rs),
[kafka_consumer.rs](../../../../src/kafka/consumer/kafka_consumer.rs) and
[context.rs](../../../../src/kafka/consumer/context.rs).
