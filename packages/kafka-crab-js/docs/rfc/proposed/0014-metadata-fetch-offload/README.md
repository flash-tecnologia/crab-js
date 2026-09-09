# RFC-0014: Offload blocking metadata operations

- Status: Proposed
- Related items: M07 and M10
- Priority: Medium

## Motivation

Topic creation and metadata lookup use librdkafka operations that may block. Even with producer
flush moved to `spawn_blocking`, synchronous metadata calls can still occupy an async worker.

## Proposal

Move blocking metadata and administrative calls to `spawn_blocking`, preserving cancellation,
timeouts, error aggregation, and the current subscription ordering. Avoid unbounded blocking-task
creation when many topics are requested.

## Acceptance criteria

With one Tokio worker, a metadata request must not delay unrelated timers or receives beyond the
specified tolerance. Tests must cover timeout, cancellation, multiple topics, fatal errors, and
already-existing topics.
