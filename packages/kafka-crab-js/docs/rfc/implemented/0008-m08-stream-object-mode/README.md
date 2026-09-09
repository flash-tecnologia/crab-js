# RFC-0008: Node stream object mode

- Status: Implemented
- Review item: M08
- Priority: Medium

## Problem

Custom stream options could accidentally disable `objectMode`, causing Kafka message objects
to be treated as byte chunks and breaking stream consumers.

## Decision

Force object mode for Kafka streams and preserve caller-controlled options such as
`highWaterMark`. Explicit `objectMode: false` is rejected.

## Evidence

Regressions verify object mode with a custom high-water mark on serial and batch paths. The
implementation is covered by [kafka-client.ts](../../../../js-src/kafka-client.ts) and
[base-kafka-stream-readable.ts](../../../../js-src/streams/base-kafka-stream-readable.ts).

## Follow-up

Add an end-to-end flow assertion that reads an actual message object through each Node stream
variant, in addition to checking stream flags.
