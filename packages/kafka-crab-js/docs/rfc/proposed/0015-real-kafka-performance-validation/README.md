# RFC-0015: Real-Kafka performance and lifecycle validation

- Status: Proposed
- Related items: M01 and M06
- Priority: High

## Motivation

The delivery-tracking benchmark is intentionally isolated. It cannot establish behavior under
Kafka delivery callbacks, network delay, broker backpressure, JavaScript garbage collection, or
the actual NAPI boundary.

## Proposal

Add a repeatable CI or opt-in benchmark using the repository's Kafka container. Compare the
previous release with the implemented `oneshot` tracking under slow consumers, sustained backlog,
timeouts, partial queue failures, concurrent sends, and manual flush.

## Acceptance criteria

Record throughput, p50/p95/p99 latency, process RSS, external/native memory, in-flight counts,
cleanup time, and recovered delivery metadata. Run enough repetitions to report variability.
The candidate must preserve all M01/M06 contracts before performance determines adoption.

## Partial evidence

The opt-in `benchmark:consumer:live` workload runs a current producer and consumer
concurrently in separate processes, using natural GC, public serial/batch streams,
three Kafka partitions, production bursts, downstream stalls, content validation,
and broker-confirmed processing commits. The default-versus-bounded-prefetch
comparison completed eight 120-second blocks on 2026-09-12; see the canonical
[measurements and limitations](../../review/performance.md#concurrent-workload-with-natural-gc--2026-09-12).

This does not close the RFC: comparison with the previous release under timeouts,
partial queue failures, concurrent sends, and manual flush remains necessary.
