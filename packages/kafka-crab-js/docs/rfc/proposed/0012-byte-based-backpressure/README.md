# RFC-0012: Byte-based backpressure

- Status: Proposed
- Related item: M01
- Priority: Medium

## Motivation

Limiting the stream queue to a fixed number of batches bounds item count, but memory usage still
varies with payload size, headers, keys, and compact metadata.

## Proposal

Define a byte budget in addition to the batch-count limit. Account for native queued messages,
prefetch buffers, compact arrays, and JavaScript stream high-water marks. Choose a policy for a
single message larger than the budget and expose useful queue metrics.

## Acceptance criteria

Demonstrate bounded RSS under mixed message sizes and sustained slow consumption. Cancellation,
partial batches, and oversized messages must still terminate within the existing lifecycle
contract without silently dropping data.
