# RFC index

## Purpose

These RFCs split the M01–M10 review into independently reviewable decisions. They separate
completed work from follow-up improvements while preserving the evidence and limitations of
each validation.

## Status model

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

## Proposed follow-ups

| RFC                                                      | Related item | Subject                                           |
| -------------------------------------------------------- | ------------ | ------------------------------------------------- |
| [0011](proposed/0011-oneshot-delivery-tracking/)         | M06          | Evaluate per-message `oneshot` delivery tracking  |
| [0012](proposed/0012-byte-based-backpressure/)           | M01          | Bound memory by bytes as well as batches          |
| [0013](proposed/0013-async-commit-error-contract/)       | M07          | Define the asynchronous commit error contract     |
| [0014](proposed/0014-metadata-fetch-offload/)            | M07/M10      | Offload blocking metadata operations              |
| [0015](proposed/0015-real-kafka-performance-validation/) | M01/M06      | Validate behavior and performance with real Kafka |

## Open fixes

| RFC                                                | Related item | Subject                                      |
| -------------------------------------------------- | ------------ | -------------------------------------------- |
| [FIX-0001](fix/0001-deterministic-prefetch-drain/) | M01          | Deterministic drainage of prefetched batches |

## Review conventions

The RFCs use the current branch as their implementation reference. Test counts and benchmark
results are snapshots, not permanent guarantees. When a proposed RFC is implemented, update
its status, add the relevant test evidence, and link the implementation change from the RFC.
