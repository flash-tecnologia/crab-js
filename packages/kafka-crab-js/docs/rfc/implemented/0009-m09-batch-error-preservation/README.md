# RFC-0009: Batch message and error ordering

- Status: Implemented
- Review item: M09
- Priority: Medium

## Problem

If a batch received a valid message and then an error during its fill phase, returning the
partial batch could silently discard the error.

## Decision

Return already collected messages first and retain the subsequent error as `pending_error`.
The next receive or stream pull surfaces that error immediately, without another Kafka poll.

## Evidence

Tests cover direct batch and compact stream behavior with partition EOF enabled: the valid
message is delivered, then the error is raised on the following read. The collector and state
handling are in [kafka_consumer.rs](../../../../src/kafka/consumer/kafka_consumer.rs).

## Follow-up

Keep direct and compact collectors aligned when new error types are added, and add broker-based
tests for transport and authorization errors.
