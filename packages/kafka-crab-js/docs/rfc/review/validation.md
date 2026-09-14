# Functional validation

Records from local reviews on 2026-09-11 through 2026-09-13. Open items are listed in
the [conformance assessment](README.md); performance is covered in [performance.md](performance.md).
Failures discovered during review are preserved as historical evidence below.

## CI finalization and architecture regression — 2026-09-13

[PR #53's initial CI run](https://github.com/flash-tecnologia/crab-js/actions/runs/34783723496)
exposed M09/M13 timeouts on ARM64 and a missing Vite+ binding on macOS x64.
Forced garbage collection reproduced a **45,162 ms** event-loop stall after an
Async commit to an unavailable coordinator followed by disconnect. A native stack
sample located the wait in the Node-API finalizer; rust-rdkafka's consumer destructor
polls until native close completes.

The shared consumer owner now schedules that destructor on the captured Tokio
runtime's blocking pool, including when the last reference belongs to a stream or
commit queue. Native resources remain owned until close finishes; this changes
where cleanup runs, not the drainage policy or a bound on total RSS.

The new `consumer-finalizer-probe.mjs` uses a 6-second session timeout and verifies
that the JavaScript consumer was actually collected. The same probe measured
**5,986 ms with the original CI artifact** and **53 ms with the fix**, including its
50 ms timer. Its unit regression requires completion below 1 second.

On macOS ARM64, local validation passed **56 JavaScript unit tests, 4 Rust unit tests,
18 native drainage tests, and 30 real-Kafka lifecycle/commit/send-failure tests**,
plus the build, lint, formatting and Clippy. The Vite+ x64 startup failure was also
reproduced under Node x64; reinstalling with `pnpm install --cpu=x64 --frozen-lockfile`
fixed it. The Kafka and PDF binding-test workflows now select optional dependencies
using the matrix's Node architecture. These local results precede the updated remote
platform matrix.

## Consolidated evidence

| Check                                                          | Last recorded result before documentation cleanup                                                           |
| -------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| Rust + JavaScript build                                        | Passed                                                                                                      |
| Unit suite                                                     | 55/55                                                                                                       |
| Native drainage harness                                        | 18/18                                                                                                       |
| Selected integrations: lifecycle, manual commit, send failures | 25/25, no skips after moving V03 to a manual probe                                                          |
| Oversized regular and compact                                  | 80/80 messages of 512 KiB in one 40 MiB batch, in each mode                                                 |
| Manual metadata with one Tokio worker                          | A 50 ms receive finished in 52.302 ms; metadata took 601.326 ms and was still pending when receive finished |
| Packaging dry run                                              | 64 files; no tests, docs, or benchmarks                                                                     |

These checks were not all executed simultaneously. Earlier transient failures in
M13 and M09 mock startup were followed by a passing suite. These results do not
cover other topologies, authentication, or sustained traffic.

## Reproduce the checks

From `packages/kafka-crab-js`, with Kafka on `localhost:9092` when needed:

```sh
pnpm build
pnpm test
rtk proxy cargo test --locked --manifest-path js-tests/native-drain/Cargo.toml
KAFKA_LOG_LEVEL=error pnpm exec node --test js-tests/integration/consumer-manual-commit.test.mjs js-tests/integration/producer-send-failure.test.mjs js-tests/integration/stream-lifecycle-real.test.mjs
```

The F05 test attaches `Promise.allSettled` immediately when starting subscribe, so
it observes a rejection racing with disconnect without causing an unhandled rejection.
Some lifecycle cases still use sleeps and therefore do not close V02.

## Retained probes

### Oversized batches — V01

```sh
pnpm exec node js-tests/diagnostics/byte-pressure.mjs batch
pnpm exec node js-tests/diagnostics/byte-pressure.mjs compact
```

Each invocation creates a unique topic and produces 80 messages of 512 KiB in one
partition. It checks a single batch, offsets, payloads, and headers. Both modes passed.
Memory sampling in this original probe includes production; it does not isolate the
consumer or prove an RSS budget or sustained pressure. At that stage, header pressure
and queue metrics remained open; later measurements are documented below and in
the [memory investigation](performance.md#rss-investigation-under-byte-pressure--2026-09-12).

### Commit error — V03

```sh
KAFKA_LOG_LEVEL=error pnpm exec node --test js-tests/diagnostics/async-commit-error.mjs
```

The trigger schedules an Async commit to a nonexistent partition while retaining
polling. The historical result was a 10-second timeout, without proof of broker
rejection. It was inconclusive, not a pass or proof of a lost callback. An earlier
invocation whose filter did not select the parent suite was discarded as evidence.
The probe lives outside the default suite and is not a CI gate.

The 2026-09-12 review below adds a Sync control, independent offset queries, and
`COMMITFAIL` logging: F06 became a reproduced defect rather than that earlier
inconclusive timeout. The historical result no longer represents the current assessment.

### Metadata — V04

```sh
TOKIO_WORKER_THREADS=1 pnpm exec node --import tsx js-tests/fixtures/metadata-offload-probe.mjs
```

The fixture is used by the M14 unit regression. An unavailable endpoint keeps metadata
pending while a separate consumer against a mock completes receive. The regression
requires receive below 200 ms, metadata lasting at least 400 ms, and proven overlap.
It covers the manual path, not the full administrative/cancellation matrix.

Manual diagnostics are documented in
[js-tests/diagnostics](../../../js-tests/diagnostics/README.md). Reorganization does
not turn inconclusive probes into passing tests or remove regressions.

## Verification after documentation cleanup

The unit suite passed again, 55/55. Benchmark workspace typechecking, serial/batch
ABBA smoke checks, and the memory diagnostic passed at their new paths. Smoke checks
use one short repetition and do not establish performance. The packaging dry run
still contained 64 files. The 23 documents checked had no broken local links, and the
two final evidence JSONs were moved without changing their bytes. Integrations and
oversized probes were not repeated during that cleanup because their implementations
had not changed.

## Release review — 2026-09-12

Local runtime: Node 24.20.0, macOS arm64, Kafka at localhost:9092. This review did not
change library runtime code. It examined the local diff and both branch commits
since `05ba800`.

| Check performed in this review                                      | Result                                                  |
| ------------------------------------------------------------------- | ------------------------------------------------------- |
| Unit tests                                                          | 55/55                                                   |
| Native harness                                                      | 18/18                                                   |
| All 13 `integration/*.test.mjs` entry points, in three groups       | 118/118, no skips or cancellations                      |
| Lint, `cargo fmt -- --check`, clippy with `-D warnings`, diff check | Passed                                                  |
| ESM and CJS: import, consumer creation, disconnect                  | Passed                                                  |
| `npm pack --dry-run --json --ignore-scripts`                        | 64 files, manifest 4.1.3; no docs, tests, or benchmarks |

These passing tests **did not close F06/F07**: the callback and partial-metadata cases
reproduced below were not required by existing assertions. No publication or remote
validation of the six platforms was performed. The full build had passed in the
previous stage; runtime code did not change between that build and these tests.

The CI integration group (lifecycle, manual commit, send failure) passed 25/25.
Producer, consumer, Web, Node stream, and both cleanup suites passed 56/56. The two
batch-stream files, batch limits, and Kafka suites passed 37/37. The latter groups
used per-test deadlines of 60 and 120 seconds respectively; none reached the limit.
The last group used `RUN_KAFKA_INTEGRATION=true`.

### F06: Async commit without an event

Local probe: `/tmp/crab-release-commit-probe.mjs`.
Results: `/tmp/crab-release-commit-probe-final.json` and
`/tmp/crab-release-commit-probe.log`. Files under `/tmp` are temporary local evidence,
not versioned fixtures.

Procedure: produce one record in a unique topic; register `onEvents`; manually assign
and consume it; schedule an Async commit; wait 2.5 seconds; make 30
`recvBatch(1, 100)` polls; query offsets with KafkaJS; repeat the same commit in Sync
mode as a control. Repeat for a valid partition and partition 999.

- Valid partition: offset `1` independently confirmed by KafkaJS **before** the Sync
  control, with zero events.
- Invalid partition: native `COMMITFAIL ... Unknown topic or partition` log, zero
  events before/after polling; Sync rejected with `UnknownTopicOrPartition`.
- Disconnect happened after collection, and there were no competing listeners.

Resolving an Async commit after scheduling is expected. The defect was failure to
surface the error through the promised `CommitCallback`. Counting listeners did not
connect the native commit result to the broadcast.

### F07: silent partial assignment

Local probe: `/tmp/crab-release-assignment-probe.mjs`.
Result: `/tmp/crab-release-assignment-probe.json`.

With one valid and one nonexistent topic, and `allow.auto.create.topics=false`:

- Nonexistent topic alone with `allOffsets: Beginning`: rejected, empty assignment.
- Valid + nonexistent topic, both with `allOffsets: Beginning`: resolved without
  error and assigned only the valid topic.
- A valid topic with an explicit partition + a second entry with `partitionOffset: []`:
  also resolved without error and omitted the second entry.

The missing checks were per-topic metadata validation and rejection of empty manual
entries. The regression needed to require an error and verify preservation of the
previous assignment, rather than accepting any nonempty final assignment.

## Fixes and revalidation — 2026-09-12

F06 and F07 are fixed. New regressions were first run against the previous binding:
Async success/error, concurrent commits, partial metadata, and empty lists failed as
expected. All passed after the fixes. The Async commit test that previously allowed
zero events now requires every confirmation.

| Check after the fixes                       | Result                                                                       |
| ------------------------------------------- | ---------------------------------------------------------------------------- |
| Rust + ESM/CJS + declaration build          | Passed                                                                       |
| Unit tests                                  | 55/55                                                                        |
| Native harness                              | 18/18                                                                        |
| All integrations                            | 123/123, zero skips/cancellations                                            |
| Lint, clippy `-D warnings`, Rust formatting | Passed                                                                       |
| Manual invalid-commit probe                 | Passed; `UnknownTopicOrPartition` callback with topic, partition, and offset |
| Oversized regular and compact               | Both delivered 80 messages/40 MiB in a single batch                          |
| CI version guard                            | Matching tag accepted; mismatched tag rejected                               |
| Packaging dry run without scripts           | 64 files; not a native package publication/installation test                 |

Full-suite command, including the new tests already selected by the integration job:

```sh
RUN_KAFKA_INTEGRATION=true pnpm exec node --test --test-timeout=120000 js-tests/integration/*.test.mjs
```

`commit_queue.rs` retains broker responses in a dedicated queue and forwards events
without consuming records. Async scheduling uses the lifecycle lock; the task uses
a weak reference, and disconnect releases the queue. Regressions cover 32 concurrent
commits and 32 commits racing with disconnect. Delivery of already-pending responses
after disconnect remains best-effort. Neither broadcast overflow policy nor the
message-queue limit was extended to the entire callback pipeline.

Metadata must now resolve every requested topic without topic/partition errors.
Explicit empty lists reject. Both failures preserve the previous assignment,
verified against real Kafka.

### Initial mixed byte-pressure run

```sh
pnpm exec node js-tests/diagnostics/byte-pressure.mjs all sustained
```

Data was produced in a separate process, with an isolated consumer for each mode:
3,072 messages and 481.5 MiB of payloads/headers per mode, approximately 31 seconds
of slow reading, and five longer pauses. All offsets, payloads, headers, and
tombstones were checked. This workload used a 65,536 KiB librdkafka queue threshold
and 20 ms backoff.

| Mode    | Peak native queued bytes  | Blocks/resumptions | Peak RSS          | RSS spread over the last three pauses |
| ------- | ------------------------- | ------------------ | ----------------- | ------------------------------------- |
| Regular | 31,575,552 (< 33,554,432) | 88 / 88            | 536,018,944 bytes | 44,793,856 bytes                      |
| Compact | 31,561,164 (< 33,554,432) | 88 / 88            | 533,987,328 bytes | 17,072,128 bytes                      |

This closes the missing measurement of blocking/resumption under real byte/header
pressure for that scenario. It does not establish a general RSS limit or long-term
growth behavior: RSS increased during execution despite a small JS heap, reaching
approximately 510 MiB. The old guard tolerated a 64 MiB spread across the last three
pauses and was not evidence of leak freedom.

Temporary local evidence:
`/var/folders/0s/j_q0xhmj0cb7p_z7hyl7l9h00000gp/T/crab-byte-pressure-gX4JYz/results.json`
(input and full logs are in the same directory). Other logs:
`/tmp/crab-release-fixes-{build-final,integration,unit,native}.log`,
`/tmp/crab-release-async-diagnostic.log`, and `/tmp/crab-release-oversized-{batch,compact}.log`.
No publication occurred. The manifest version awaits selection and remote CI must
still validate the six targets.

Source/binding hashes and raw byte-pressure data:
`/tmp/crab-release-fixes-evidence.json`.

### Subsequent prolonged investigation

The diagnostic was corrected to avoid allocating extra large validation buffers and
to measure after releasing reader/consumer references. The last-three-pauses guard
was removed: the current version records the trend without treating it as proof of
bounded RSS. Logs go directly to files, and completed modes are checkpointed.

The new run completed 56 cycles, over 15.5 minutes per mode, plus 44 cycles across
22 configuration cases: 307,200 validated records. Native snapshots identified empty
allocator regions as the dominant contribution to residual RSS on macOS, with about
3.7 MiB of live allocations after collection. A message-count threshold helped the
large-header workload, whereas lowering the byte threshold alone did not. Data,
limitations, and the measured optional profile are in the
[memory investigation](performance.md#rss-investigation-under-byte-pressure--2026-09-12).
Summary: `/tmp/crab-memory-investigation.json`.
The initial run above is historical and does not describe the corrected methodology.

### Concurrent traffic with natural GC

Eight 120-second blocks subsequently validated 640,000 new records and 2,258 Sync
commits with producer and consumer running simultaneously in separate processes.
Final broker lag and producer in-flight counts were zero in every block. The
[workload report](performance.md#concurrent-workload-with-natural-gc--2026-09-12)
records configuration, results, and limits. This adds live traffic evidence without
closing the full RFC-0015 fault matrix.

## Documentation validation — 2026-09-12

The Kafka documentation was consolidated in English. The package README now links
to one maintained API reference; obsolete wiki examples and historical headline
speed ratios were replaced with current, qualified comparisons.

Two full consumer benchmark suites completed with opposite crab pair orders,
retaining 960 measurements. The published aggregate rates were recalculated from
every message count and elapsed time. All 32 retained blocks matched the original
measurements, memory, GC, and execution metadata; original JSON SHA-256 values
were checked. See the [portable snapshot](evidence/consumer-comparison-2026-09-12.json)
and [methodology](../../../../../BENCHMARKS.md#methodology), including observed
Kafka controller changes and unchanged topic offsets/partition metadata.

The README producer and consumer examples were extracted and run against a unique
local Kafka topic. Only the import path, topic, and group were substituted to use
the current build and isolate the check. The producer received partition 1/offset 0;
the consumer processed the expected order and the broker confirmed committed offset 1.
SIGTERM then produced a clean exit (code 0), with no producer/consumer errors.
The administrative KafkaJS helper emitted its existing `TimeoutNegativeWarning`;
it did not prevent topic creation or independent commit verification.

Strict TypeScript checking passed for the README examples and the API walkthrough.
All 216 local links in the reviewed Kafka/shared documentation resolved, including
heading anchors. Markdown tables, formatting, and `git diff --check` passed.
The language scan found no remaining Portuguese prose in repository Markdown.
No library runtime behavior changed for this documentation update.

Local original captures and example-check output:
`/var/folders/0s/j_q0xhmj0cb7p_z7hyl7l9h00000gp/T/crab-docs-english-l6b4j96_/`.
