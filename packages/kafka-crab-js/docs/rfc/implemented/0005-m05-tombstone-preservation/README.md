# RFC-0005: Tombstones and empty buffers

- Status: Implemented
- Review item: M05
- Priority: High

## Problem

Kafka null payloads (tombstones) and zero-length payloads are distinct, but converting both to
an empty JavaScript buffer loses compaction semantics.

## Decision

Always expose a `Buffer`; mark null payloads with `isTombstone`. Compact batches carry aligned
`tombstones` metadata. Reject contradictory producer input that supplies both a tombstone flag
and a payload.

## Evidence

Mock-librdkafka regressions cover direct batches, compact batches, diagnostics, empty payloads,
absent payloads, and regular payloads in [regressions.test.ts](../../../../js-tests/unit/regressions.test.ts).
The data model and codec live in [model.rs](../../../../src/kafka/producer/model.rs) and
[kafka_util.rs](../../../../src/kafka/kafka_util.rs).

## Follow-up

Document the `isTombstone` contract for every public receive API and preserve aligned array
lengths when compact representations evolve.
