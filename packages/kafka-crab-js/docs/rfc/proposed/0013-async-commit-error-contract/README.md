# RFC-0013: Asynchronous commit error contract

- Status: Proposed
- Related item: M07
- Priority: Medium

## Motivation

An asynchronous commit error is delivered through a bounded broadcast channel. If no listener
exists, the application may never observe the error, which makes success and failure semantics
unclear.

## Proposal

Choose and document one contract: return a confirmation future, expose a per-consumer error
stream with defined buffering, or reject asynchronous mode when no listener is registered.
Preserve topic, partition, offset, and broker error details.

## Acceptance criteria

Tests must cover a listener attached before commit, a late listener, a full error buffer, consumer
disconnect, and broker failure. No commit failure may disappear without an explicit documented
drop policy.
