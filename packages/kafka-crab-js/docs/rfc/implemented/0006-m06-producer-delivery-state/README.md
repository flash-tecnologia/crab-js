# RFC-0006: Producer delivery state and partial failures

- Status: Implemented
- Review item: M06
- Priority: High

## Problem

Blocking flushes could occupy Tokio workers. Timeout callbacks could create orphaned results,
and partial queue failures did not identify accepted or confirmed messages clearly.

## Decision

Represent each delivery as a stateful entry (`Pending` or `Delivered`) in one `DashMap`.
Callbacks update only an occupied entry, so expired IDs are discarded. Snapshot target IDs for
manual flushes, preserve concurrent sends, expose structured partial-send details, and run
blocking librdkafka flushes through `spawn_blocking`. The JavaScript wrapper additionally
serializes `send()` and `flush()` per producer instance so the shared recovery slot cannot
mix concurrent batches.

## Evidence

Regressions cover a single Tokio worker, late callbacks, partial queue acceptance, and recovery
of confirmed metadata in [regressions.test.ts](../../../../js-tests/unit/regressions.test.ts).
A dedicated native-concurrency test bypasses the wrapper chain to prove entry isolation, and a
partial-failure test against a live mock broker proves confirmed-metadata recovery.
The implementation is in [kafka_producer.rs](../../../../src/kafka/producer/kafka_producer.rs).

## Follow-up

Superseded for tracking by [RFC-0012](../0012-oneshot-delivery-tracking/). The structured
partial-send contract and `spawn_blocking` flush remain.
