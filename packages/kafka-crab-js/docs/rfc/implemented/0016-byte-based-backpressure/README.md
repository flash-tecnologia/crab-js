# RFC-0016: Byte-based backpressure

- Status: Implemented
- Related item: M01
- Priority: Medium

## Problem

The native stream queue was limited to four batches. Memory still grew with payload size.

## Decision

Keep a batch-count cap alongside the 32 MiB wired-byte budget on Web batch/compact streams (see follow-up below for
the depth correction). A single batch larger than the budget is still delivered (no silent drop). Collection waits on
the budget during steady state; disconnect drain does not wait on the byte budget so shutdown stays bounded.
Serial prefetch remains one in-flight native batch.

Wired bytes count payload, key, topic, header names and values (including shared/compact header
state, dictionary indexes, and tombstone flags), plus a small per-message overhead. Shared buffers
are counted once, not once per message sharing them.

## Scope

The budget covers queued native batches on the batch/compact Web streams only. Outside it:

- The batch currently being collected (allocated before the reserve).
- Messages already handed to JavaScript (bytes release when the item leaves the receiver).
- librdkafka buffers, JavaScript queues, and auxiliary structures.
- The serial `recvStream` prefetch path, which tracks bytes without enforcing the budget.
- JavaScript `highWaterMark` behavior, which is unchanged.

The 32 MiB figure therefore bounds the native prefetch queue accounting, not total process RSS.
The pinned librdkafka 2.12.1 has separate fetch-queue accounting. Its fetch operation
sets `rko_len` from the record value length, and the queue sums that value; headers
and keys are not included in this counter. Consequently, reducing
`queued.max.messages.kbytes` alone does not bound header-heavy prefetch memory.
See [fetch operation](https://github.com/confluentinc/librdkafka/blob/v2.12.1/src/rdkafka_op.c),
[queue accounting](https://github.com/confluentinc/librdkafka/blob/v2.12.1/src/rdkafka_queue.h)
and [fetch thresholds](https://github.com/confluentinc/librdkafka/blob/v2.12.1/src/rdkafka_fetcher.c).
These are distinct from the wrapper's byte accounting, which includes headers.
Trace events now expose reserves, blocking, resumption and releases. A real-Kafka
mixed-size/header run with a slow reader verified the queue bound and repeated
blocking/recovery; process RSS still grew and is not bounded by this budget.
The original proposal also covered collection buffers, compact arrays and JavaScript
high-water marks; those are not claimed as implemented by the native queue budget.

## Remaining acceptance criteria

The macOS measurement now includes 28 cycles and over 15 minutes per mode, with
native allocation summaries after cleanup. General RSS limits are still not
claimed; validate other operating systems, partition counts and workloads without
forced GC. Queue metrics show actual byte-pressure blocking and recovery for mixed
and header-heavy workloads. Cover cancellation, partial
batches, oversized messages and header-heavy pressure without silently dropping data beyond
the documented lifecycle contract. See the [validation review](../../review/validation.md).

## Evidence

On 2026-09-12, the sustained diagnostic consumed 3,072 messages (481.5 MiB of
payload/header data) per mode in isolated processes, with slow reads and five
pauses. Both regular and compact modes delivered all payloads, headers, tombstones
and offsets intact, blocked/resumed 88 times, and stayed below 32 MiB of queued
wire bytes. RSS reached about 510 MiB and rose during the run; this is not evidence
of a general RSS plateau. See [validation](../../review/validation.md) for the
configuration, observations and the limited late-window RSS guard.

The revised diagnostic subsequently completed 56 cycles across regular/compact
processes, validating 172,032 records and 26.33 GiB of data. Both queues remained
below 32 MiB and blocked/resumed 2,464 times per mode. Live malloc allocations after
cleanup stayed near 3.7 MiB; macOS retained hundreds of MiB in empty allocator
regions. A separate matrix validated 135,168 more records and showed that limiting
prefetch by message count helps the header-heavy workload, where reducing only
the librdkafka byte threshold did not. These findings and the optional measured
configuration are in the [memory investigation](../../review/performance.md).
The earlier three-window RSS assertion was replaced with descriptive measurements;
it was not evidence of a universal RSS plateau. Runtime defaults are unchanged.

Existing M01 stream regressions continue to cover cancel, disconnect drain, and compact/serial
paths. The drain handoff block is unchanged so [native-drain](../../../../js-tests/native-drain/)
still extracts it. Native-drain now extracts the reserve-through-handoff block and proves a
disconnect arriving during the byte wait still terminates within grace plus tolerance, a resume
still delivers the pending batch in order, and cancel during the wait ends promptly, for both
regular and compact modes. Header-heavy regressions in
[regressions.test.ts](../../../../js-tests/unit/regressions.test.ts) deliver multi-KiB headers
through a slow reader on both regular and compact streams, including shared-header encoding.
Implementation: [byte_budget.rs](../../../../src/kafka/consumer/byte_budget.rs)
and [kafka_consumer.rs](../../../../src/kafka/consumer/kafka_consumer.rs).

## Follow-up: prefetch depth (2026-09-10)

The 4-batch count cap bound before the byte budget for small messages (4 x 64
messages = ~256 messages of lookahead) and starved small-batch streams on
fetch bubbles: v4 serial measured 176k op/sec vs 818k for the previous release
at 200k messages. The count cap is now 256 batches so the byte budget governs;
measured 614k op/sec from depth alone. The remaining gap was librdkafka's
`fetch.queue.backoff.ms` (upstream default 1000ms): once the local queue hits
`queued.min.messages`, fetch pauses ~1s while a small native bank drains in
milliseconds. Consumers now default it to 20ms unless set explicitly; measured
976k op/sec for v4 serial vs 845k previous at 200k messages, same suite.
