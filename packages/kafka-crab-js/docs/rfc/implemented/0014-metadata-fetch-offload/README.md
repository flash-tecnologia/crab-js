# RFC-0014: Offload blocking metadata operations

- Status: Implemented
- Related items: M07 and M10
- Priority: Medium

## Problem

Topic creation and metadata lookup use librdkafka operations that may block a Tokio worker,
even after producer flush moved to `spawn_blocking`.

## Decision

Run partition-metadata assembly for manual `assign()` in one `spawn_blocking` task per
`subscribe()`, and offload admin `fetch_metadata` the same way. Topic creation stays a single
admin flow; one blocking task per subscribe avoids a burst of blocking work when many topics
are requested.

## Evidence

Existing M07/M04 subscribe regressions still cover fatal create, mixed mode, and multi-topic
assign. Subscription and manual assignment application are serialized with the terminal
`disconnect()` transition: metadata fetch remains outside the lifecycle lock, then the assignment
is revalidated immediately before it is applied. The regression
`disconnect stays terminal while manual subscribe fetches metadata` in
[consumer-manual-commit.test.mjs](../../../../js-tests/integration/consumer-manual-commit.test.mjs)
passed with Kafka real and verifies that a completed metadata operation cannot restore an
assignment after disconnect. Implementation: [kafka_consumer.rs](../../../../src/kafka/consumer/kafka_consumer.rs)
and [kafka_admin.rs](../../../../src/kafka/kafka_admin.rs).

The persisted M14 regression runs with one Tokio worker and verifies that an unrelated receive
completes while manual-assignment metadata is still pending. The full matrix of administrative
offload, timeout, cancellation, multiple topics, fatal errors and already-existing topics remains
open; see [validation](../../review/validation.md).
