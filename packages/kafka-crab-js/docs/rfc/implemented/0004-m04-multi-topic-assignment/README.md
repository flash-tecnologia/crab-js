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
