# RFC index

## Purpose

These RFCs split the M01–M10 review into independently reviewable decisions. They separate
completed work from follow-up improvements while preserving the evidence and limitations of
each validation.

## Status model

For the consolidated audit, see [conformance](review/README.md),
[validation](review/validation.md), and [performance](review/performance.md).

- **Implemented**: the current source and automated checks contain the described behavior.
- **Proposed**: an improvement or validation that remains open; no production change is implied.

An implemented RFC can still list follow-up work. In particular, a passing unit or mock test
does not replace validation against a real Kafka broker under load.

## Implemented

| RFC                                                           | Review item | Subject                                      |
| ------------------------------------------------------------- | ----------- | -------------------------------------------- |
| [0001](implemented/0001-m01-stream-cancellation/)             | M01         | Native stream cancellation and lifecycle     |
| [0002](implemented/0002-m02-auto-commit-precedence/)          | M02         | Auto-commit precedence                       |
| [0003](implemented/0003-m03-sensitive-configuration-logging/) | M03         | Sensitive configuration logging              |
| [0004](implemented/0004-m04-multi-topic-assignment/)          | M04         | Multi-topic and multi-partition assignment   |
| [0005](implemented/0005-m05-tombstone-preservation/)          | M05         | Tombstones and empty buffers                 |
| [0006](implemented/0006-m06-producer-delivery-state/)         | M06         | Producer delivery state and partial failures |
| [0007](implemented/0007-m07-admin-error-propagation/)         | M07         | Fatal topic-creation errors                  |
| [0008](implemented/0008-m08-stream-object-mode/)              | M08         | Node stream object mode                      |
| [0009](implemented/0009-m09-batch-error-preservation/)        | M09         | Batch message/error ordering                 |
| [0010](implemented/0010-m10-metadata-timeout-validation/)     | M10         | Metadata timeout validation                  |
| [0011](implemented/0011-deterministic-prefetch-drain/)        | M01         | Deterministic drainage of prefetched batches |
| [0012](implemented/0012-oneshot-delivery-tracking/)           | M06         | Per-message `oneshot` delivery tracking      |
| [0013](implemented/0013-async-commit-error-contract/)         | M07         | Asynchronous commit error contract           |
| [0014](implemented/0014-metadata-fetch-offload/)              | M07/M10     | Offload blocking metadata operations         |
| [0016](implemented/0016-byte-based-backpressure/)             | M01         | Bound native stream memory by bytes          |

RFC-0006 describes the earlier `DashMap` design; RFC-0012 superseded that tracking model.

## Proposed follow-ups

| RFC                                                      | Related item | Subject                                           |
| -------------------------------------------------------- | ------------ | ------------------------------------------------- |
| [0015](proposed/0015-real-kafka-performance-validation/) | M01/M06      | Validate behavior and performance with real Kafka |

Byte-based backpressure was originally numbered 0012 while proposed; that number is used by
oneshot tracking. The implemented byte-budget work is RFC-0016; its remaining
acceptance criteria are retained there. Each decision now has one canonical RFC.

## Review conventions

The RFCs use the current branch as their implementation reference. Test counts and benchmark
results are snapshots, not permanent guarantees. When a proposed RFC is implemented, update
its status, add the relevant test evidence, and link the implementation change from the RFC.
