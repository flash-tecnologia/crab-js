# RFC-0004: Multi-topic and multi-partition assignment

- Status: Implemented
- Review item: M04
- Priority: High

## Problem

Manual assignments covering more than one topic or partition could be overwritten when the
consumer built the assignment incrementally.

## Decision

Build one `TopicPartitionList` containing every explicit topic and partition, then call
`assign` once. Reject ambiguous mixed subscription modes instead of silently changing semantics.

## Evidence

The regression asserts that two topics and three explicit partitions remain assigned together;
a second regression requires mixed manual/subscribe lists to be rejected with `InvalidArg`. The
implementation is in [kafka_consumer.rs](../../../../src/kafka/consumer/kafka_consumer.rs).

## Follow-up

Add real-broker coverage for `allOffsets`, stored offsets, and mixed partition offset models.

The 2026-09-12 release review reproduced F07: with automatic topic creation disabled,
`allOffsets` for a valid topic and a missing topic resolves successfully with only
the valid topic assigned. An empty explicit `partitionOffset` entry is also ignored
when another entry supplies partitions. Both defects are now corrected: empty explicit
lists reject, and metadata must contain the requested topic, no topic/partition error,
and at least one partition. Validation completes before `assign()`, preserving the
previous assignment on failure. Real-Kafka regressions cover both failures and that
preservation policy; see [the release review and reproduction](../../review/validation.md).
