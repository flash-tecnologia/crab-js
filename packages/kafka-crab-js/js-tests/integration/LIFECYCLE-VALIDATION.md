# Real Kafka lifecycle validation — 2026-09-09

## Scope and reproduction

Broker: `localhost:9092`, real Kafka (no librdkafka mock). Tests use unique `lifecycle-*`
topics, a shared key to keep each case on one partition, and producer acknowledgments as
the reference for IDs and offsets. No production source or existing integration tests were changed.
The existing `dist/` and native binding were used; no rebuild was performed during this validation.

```sh
KAFKA_BROKERS=localhost:9092 node --test --test-concurrency=1 js-tests/integration/stream-lifecycle-real.test.mjs
```

The new suite runs 12 cases, each in its own process. The parent imposes a 35-second process
deadline, including cleanup, to catch native stalls even if JavaScript timers cannot execute.
Successful cases exit naturally: they do not call `process.exit` or force successful termination.
The fixture emits `LIFECYCLE_RESULT` JSON diagnostics for each case.

## Results

Three consecutive runs of the final 12-case suite passed: **36/36**, no skips, expected failures,
or child-process deadlines. Each complete run took approximately 10.5–10.6 seconds.

| Scenario                                             | Modes                  | Result                                                                                                            |
| ---------------------------------------------------- | ---------------------- | ----------------------------------------------------------------------------------------------------------------- |
| Slow consumption, 1,024 messages                     | Serial, batch, compact | All IDs and offsets received in order, without duplicates; stream closes after disconnect                         |
| Backlog, 1,024 messages, reader paused for 500 ms    | Serial, batch, compact | Delivered prefix verified; remaining records recovered with a new consumer at the next application-visible offset |
| Cancel pending read, then produce three more records | Serial, batch, compact | Pending read finishes; direct receive gets all three later records                                                |
| Disconnect with serial prefetch                      | Serial                 | All 16 records delivered, then `done: true`                                                                       |
| Disconnect during partial-batch load                 | Batch, compact         | Both records delivered, then `done: true`                                                                         |

With backlog, all three runs delivered 32 messages through the serial stream and 224 through
each batch stream before stream completion. The remainder was recovered from Kafka by explicit
offset assignment. These counts are observations, not new API guarantees or buffer-size assertions.
Both auto-commit and auto-offset-store were disabled for the original consumers; recovery does
not demonstrate persistence of committed group offsets or application processing success.

For the backlog cases, `disconnect()` resolved in roughly 0.06–0.26 ms, while the stream reached
`done` about 0.99–1.99 ms after starting the disconnect. For partial batches, stream completion
took about 0.54–0.85 ms. These are local observations, not performance thresholds.
RSS diagnostics are point-in-time process samples, not leak measurements or peak memory.

## Additional existing integration suites

- `web-stream-consumer.test.mjs`: **11/11 passed**, including the parent/setup reported by
  Node's test runner. Run with `--test-force-exit --test-timeout=90000`; this run validates the
  assertions but does not establish natural teardown of that existing suite.
- `consumer-manual-commit.test.mjs`: four cases failed after waiting for `PostRebalance`:
  sync commit, async commit, restart persistence, and batch processing. They subscribe with
  `allOffsets`, which selects manual assignment, then wait for a group rebalance event.
  These failures occur before exercising the intended commit assertions. The basic commit
  and stream commit cases passed. The process remained alive after reporting failures and
  was terminated; this suite is **not green**.
- Lint passed for the new test code.

## What remains unproven

The API does not expose a native batch-collected or queue-full acknowledgment. The partial
and backlog cases therefore use a deliberate 500 ms delay; they are load regressions, not
deterministic scheduling proofs. Even the first serial item does not expose the native batch
length, although the prefetch case uses exactly one batch of acknowledged input.

The suite verifies application-visible prefixes and recoverability. It cannot distinguish
records still buffered by librdkafka from records already collected into a native batch and
discarded during disconnect. Thus it does **not** prove lossless native-prefetch drainage.

Before promising guaranteed drainage:

- Add test synchronization for collection completion and blocked queue sends.
- Exercise disconnect while a native send is blocked and assert the exact collected IDs.
- Expose drainage completion separately from the request to disconnect, with explicit failure
  or discard accounting when its deadline expires.
- Test a permanently stalled reader, deadline expiration, and concurrent cancellation.
- Validate long-duration memory/task cleanup and committed-offset restart behavior.

Unique test topics remain on the local broker; no existing topics were deleted or modified.
