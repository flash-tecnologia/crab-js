# RFC-0007: Fatal administrative error propagation

- Status: Implemented
- Review item: M07
- Priority: Medium

## Problem

`subscribe({ createTopic: true })` could report success after a fatal topic-creation failure,
leaving callers unaware that the requested setup did not complete.

## Decision

Inspect individual admin results, tolerate only an already-existing topic, aggregate fatal
creation errors with topic names, and propagate the aggregate to the caller. The subscription
is installed before the aggregate error is returned, so rejection does not imply unsubscribed:
callers must not retry `subscribe()` blindly nor assume cleanup happened. Commit callbacks
also preserve asynchronous errors for their documented event path.

## Evidence

The regression uses an unreachable broker and requires a rejected subscription whose message
contains the requested topic. The paths are implemented in
[kafka_admin.rs](../../../../src/kafka/kafka_admin.rs),
[kafka_consumer.rs](../../../../src/kafka/consumer/kafka_consumer.rs), and
[context.rs](../../../../src/kafka/consumer/context.rs).

## Follow-up

Define behavior when an asynchronous commit has no listener; see
[RFC-0013](../0013-async-commit-error-contract/).
