# Conformance review

Consolidated on 2026-09-11; release review updated on 2026-09-13.
Scope: `kafka-crab-js`, its tests, CI, and benchmarks.
The numbered [RFCs](../README.md) record engineering decisions. This review consolidates
implemented fixes, evidence, and remaining acceptance criteria. Execution results are
local snapshots, not certification of every behavior supported by the library.

- [Functional validation and commands](validation.md).
- [Performance, memory, and evidence](performance.md).

## Assessment

**F06 and F07 are fixed and passed their regressions against real Kafka.** F01–F05
remain fixed. The candidate passed its build, 55 unit tests, 18 native tests, and
123 integration tests. The release version is now `5.0.0`; the preceding CI fix
passed all six platform targets on commit `9142681`. Full conformance and a general
memory ceiling are not claimed; the open criteria are listed below.

Reviews were performed against a working tree with local changes. Commit hashes alone
cannot reconstruct the tested state. Final evidence JSONs include source and loaded
binding hashes.

| Finding                                           | Fix                                                                                                           | Evidence and scope                                                                                                          |
| ------------------------------------------------- | ------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| F01: disconnect while waiting for the byte budget | Preserves the observed shutdown signal and starts bounded drainage in regular/compact streams                 | Disconnect race covered by the deterministic harness; blocking/resumption also verified under real Kafka pressure           |
| F02: headers omitted from byte accounting         | Accounts for names/values, compact representations, dictionaries, and tombstones                              | A real batch workload with only 3 MiB of payload and 478.5 MiB of headers blocked/resumed 88 times while preserving content |
| F03: Async commit allowed without a live observer | Requires an active event task and rejects after disconnect                                                    | M13 regressions and real broker rejection callbacks covered after F06                                                       |
| F04: inconsistent overflow contract               | Documents 128 effective slots, overwriting oldest events, `Lagged(skipped)`, and no history for new listeners | Broadcast tests; this is not a bound on the entire path to the JavaScript callback                                          |
| F05: assignment restored after disconnect         | A terminal flag and lifecycle lock coordinate subscribe/assign with disconnect                                | Real Kafka regression; metadata lookup stays outside the lock                                                               |
| F06: manual Async commit without a callback       | Explicit response queue with polling independent of consumption; enqueue serialized with disconnect           | Complete success/error callbacks without recv, 32 concurrent commits, and disconnect races passed                           |
| F07: a topic silently omitted from assignment     | Validates metadata per topic/partition and rejects explicit empty partition lists before assign               | Both failures reject and preserve the previous assignment against real Kafka                                                |

Before F05, a pending metadata lookup could finish after disconnect and reinstall
partitions. Assignment now rechecks the terminal state under the same lock used for
shutdown. The regression is in
[consumer-manual-commit.test.mjs](../../../js-tests/integration/consumer-manual-commit.test.mjs).

## Release blockers found and fixed

Before F06, `KafkaConsumer.commit()` called `StreamConsumer.commit()`, which uses
`rd_kafka_commit` in the pinned dependency (`rdkafka 0.39.0`, librdkafka 2.12.1).
Registering `onEvents()` only forwarded the context broadcast. That path did not
connect the manual commit response to the expected event: the Async call passed
null callback/reply-queue pointers, while rust-rdkafka enabled the event interface.
Source inspection agreed with the reproduction: a valid offset independently confirmed
at the broker and an invalid commit with `COMMITFAIL` both produced no
`CommitCallback`. The reproduction also made 30 `recvBatch(1, 100)` calls, so missing
polling alone did not explain it.

The native result now preserves topic, partition, offset, and error through a
dedicated queue. Tests require real success/failure callbacks. `commit_queue.rs`
uses the [librdkafka explicit commit queue API](https://github.com/confluentinc/librdkafka/blob/v2.12.1/src/rdkafka_offset.c),
with RAII cleanup and no record consumption from the main queue. The previous
integration test allowed zero events; it now requires a callback for every Async
commit. A separate regression covers delivery without receive polling.

Before F07, `add_topic_partitions_to_tpl()` did not validate per-topic metadata errors
or require partitions for every entry. It now rejects omitted topics, topic/partition
metadata errors, and explicit empty lists. Both failures preserve the previous
assignment; real Kafka tests reproduce the formerly silent partial assignments.

Reproductions and execution results are recorded in [validation.md](validation.md).

## Remaining acceptance criteria

| ID          | RFC                                                         | Evidence and remaining work                                                                                                                                                                                                                            |
| ----------- | ----------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| V01         | [0016](../implemented/0016-byte-based-backpressure/)        | 56 cycles/26.33 GiB, 15.5 minutes per mode: queue below 32 MiB, without proportional growth in live allocations after collection. Empty allocator regions dominated residual RSS on macOS. This does not establish a general ceiling or validate Linux |
| V02         | [0011](../implemented/0011-deterministic-prefetch-drain/)   | A deterministic real-Kafka barrier proving a blocked handoff and every collected offset; some current tests still use sleeps                                                                                                                           |
| V03         | [0013](../implemented/0013-async-commit-error-contract/)    | Success, contextual broker rejection, and commit/disconnect concurrency covered; delivery after disconnect remains best-effort by contract                                                                                                             |
| V04         | [0014](../implemented/0014-metadata-fetch-offload/)         | F07 fixed; the administrative matrix still needs coverage beyond manual metadata, per-topic errors, and the single-worker case                                                                                                                         |
| Performance | [0015](../proposed/0015-real-kafka-performance-validation/) | Concurrent live traffic with natural GC and optional prefetch measured in eight 120-second blocks; comparison with the previous release under timeouts, partial failures, concurrent sends, and manual flush remains open                              |

F06 and F07 close the reproduced M07/RFC-0013 and M04 discrepancies.
RFC-0012's `oneshot` tracking supersedes RFC-0006's earlier `DashMap` design while
preserving isolation between concurrent sends. An implemented RFC does not
necessarily close every acceptance criterion in this table.

## Protections retained

- Terminal disconnect, cancellation propagation, and time-bounded drainage.
- A 32 MiB byte budget and 256-batch native Web batch/compact queue limit,
  including headers and the oversized-batch exception. This does not cap total RSS.
- Per-send confirmations/failure details and a live observer for Async commits.
- Limit validation, tombstone preservation, and sensitive-configuration log handling.

Library defaults remain unchanged. The matrix identifies optional prefetch settings
for large payloads/headers and slow readers, documented in [performance.md](performance.md).
Native snapshots identify empty allocator regions as the dominant contribution to
residual RSS on macOS; no allocator intervention was added.

The [concurrent workload with natural GC](performance.md#concurrent-workload-with-natural-gc--2026-09-12)
validated 640,000 records with three partitions per block. A prefetch threshold of
256 reduced peak RSS by 50–54%, with slower backlog recovery. This supports an
optional profile, not a universal RSS ceiling.

## CI and maintenance

The workflow runs the native harness and lifecycle, manual-commit, and send-failure
integrations. Publication also depends on `integration-kafka`. The
[remote workflow passed](https://github.com/flash-tecnologia/crab-js/actions/runs/34785588953)
on commit `9142681`, before the version bump.

`package.json` and Cargo metadata declare `5.0.0`. This is a major release because
the supported runtime changes from Node.js 22 and newer to Node.js 24.
The workflow rejects tags that do not match the manifest version in the `lint` job,
which publication depends on. Matching and mismatched tags were tested locally.
Creating a tag does not update the package version. The local packaging dry run
neither ran `prepublishOnly` nor tested installation of native packages on all six platforms.

Manual probes live in `js-tests/diagnostics`; test-owned fixtures live in
`js-tests/fixtures`. The package allowlist excludes both, documentation, and benchmarks.
`dist/diagnostics` is public instrumentation and remains in the package.

Future maintenance includes separating codec/collection/lifecycle internals after the
contracts stabilize and updating older development instructions. These are not
reproduced functional defects. Offset recovery after a broker-backed restart and the
relationship between prefetch, buffers, and high-water marks remain relevant to
sustained validation, without claiming a bound on total process memory.
