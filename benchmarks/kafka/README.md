# Benchmark Suite

This directory contains the reproducible Kafka consumer/producer harness and diagnostics.
For the current comparison with KafkaJS and Platformatic, start with
[BENCHMARKS.md](../../BENCHMARKS.md). Use this guide to repeat measurements or change
a workload; use the [package guide](../../packages/kafka-crab-js/README.md) for application examples.

## Live producer and consumer workload

```sh
LIVE_SECONDS=120 LIVE_RATE=500 LIVE_BURST_RATE=1000 \
  pnpm benchmark:consumer:live
```

This sends new records while a consumer processes them, using separate processes
on a real Kafka cluster (`KAFKA_BROKERS`, default `localhost:9092`). It is a
synthetic workload, not a replay of production traffic or a maximum-throughput
benchmark. It does not require the `setup:consumer` dataset.

By default, serial and public batch Web Streams each run four fresh-process
blocks: default prefetch, bounded prefetch, bounded prefetch, default prefetch.
Each block publishes for 120 seconds at 500 records/s, with two 20-second bursts
at 1,000 records/s. Sends contain 100 or 200 records every 200 ms and await broker
delivery confirmation. The first 15 seconds are warmup. The consumer stops
draining at seconds 30–40 and 75–85 to model a downstream outage while the producer
and native prefetch continue running. Durations scale with `LIVE_SECONDS`.

Records cycle through 70% with 2 KiB payloads/128 B headers, 20% with 32 KiB
payloads/512 B headers, 5% with 1 KiB payloads/128 KiB headers, and 5% tombstones
with 128 KiB headers. Keys and a 20-byte sequence/timestamp header are additional.
All payload/header-data bytes, keys, tombstones, sequences, partition order, and
offsets are checked. Three CRC32-routed keys cover three partitions. Processed
offsets are committed synchronously about once per second and checked against
broker commits and final high watermarks. Each block gets a unique topic with
replication factor 1, one-hour retention and 64 MiB segments. No existing topic is
reset or deleted. The producer uses idempotence, `acks=all`, and no compression;
RF1 does not test replicated durability or failover.

The two consumer configurations differ only in `queued.min.messages`: 100,000
(the librdkafka default) versus 256. Both use a 64 MiB fetch-queue threshold,
8 MiB fetch, 1 MiB per-partition fetch, 20 ms queue backoff, and 10 ms fetch wait.
Batch size and serial prefetch are 64, with a 5 ms timeout. These are explicit
workload settings, not a change to library defaults or a total RSS limit.

Run without `--expose-gc`: GC is natural, logging is at error level, and telemetry
samples memory separately in the two processes every 100 ms. JSON includes
end-to-end latency from actual creation and from scheduled production, producer
acknowledgement latency, scheduling delay, event-loop delay, CPU, GC, backlog
recovery, configuration, cluster metadata, and loaded JS/native hashes. Normal
latency excludes warmup, consumer stalls, and one stall-length recovery window;
overall latency includes these periods. Recovery means that every partition has
processed through the schedule at the end of its stall. Idle RSS after two seconds
is not evidence of live retained allocations. A case fails if the producer cannot
maintain its schedule after warmup (p99 delay > 200 ms, maximum > 1 s), production
exceeds the planned duration by over 1 s, or integrity/commit checks fail.

Use `LIVE_MODES=serial` or `batch` and `LIVE_ORDER=default,bounded` for a shorter
comparison; `LIVE_SECONDS=12` scales the workload for a smoke check. Rates must be
positive multiples of 20. The script prints a unique temporary evidence directory,
writes worker logs directly to disk, checkpoints completed blocks, and preserves
failure details. The full default run takes approximately 17 minutes.

## Setup

1. Install benchmark dependencies:

```bash
vp install
```

2. Start the repository benchmark Kafka cluster from the repository root:

```bash
podman compose up -d
# or, when Docker is available:
docker compose up -d
```

3. Prepare the benchmark data:

```bash
cd benchmarks/kafka
vp run setup:consumer
```

`setup:consumer` uses `kafka-crab-js` to create `BENCHMARK_TOPIC` when it does not exist, then produces the benchmark
messages. It does not delete an existing topic. Use a custom `BENCHMARK_TOPIC` when you want an isolated data set.

## Running Benchmarks

Run measurement-boundary tests with `pnpm test` in this directory (no broker required).

```bash
vp run benchmark
```

The default consumer benchmark runs in isolated memory mode, using the built package exports (`dist/index.js`).
Build the current library first. Each scenario runs in two fresh child processes; current/previous pairs follow
previous → current → current → previous, matching the isolated ABBA tests. It reports mean/median/p05/p95
throughput, lifecycle memory, and message-window GC. When previous kafka-crab-js
scenarios are included, a separate table compares this workspace `kafka-crab-js` with previous `kafka-crab-js@4.1.3`
(this is not the old “Vs previous” column, which was only “vs the row above”). A yellow note is printed when
p95/p05 ≥ 1.5, because the mean is then a poor summary. `BENCHMARK_ONLY=v4-*` and `current-*` still map to `crab-*`.

The default measurement window is now `steady`: each consumer warms up for at least `BENCHMARK_ITERATIONS`
messages, rounded up to a whole delivery, then measures the next `BENCHMARK_ITERATIONS`. A batch already received
and converted at the boundary is excluded from the measured count. This changes the methodology; do not compare
these rates directly with historical first-message tables. Use `BENCHMARK_MEASUREMENT_WINDOW=first-message` for
the old window (no warmup). Raw runs also record latency from scenario start to first delivery (including setup),
first delivery size, actual warmup count, total received messages and delivery size range. This is consumer drain
throughput; prefetch may already have filled queues and this is not an end-to-end producer latency measurement.

Default scenarios:

- `crab-serial`
- `kafkajs-serial`
- `kafkajs-serial-concurrent`
- `platformatic-kafka`
- `crab-batch`
- `kafkajs-batch`

KafkaJS is reported separately as serial `eachMessage`, concurrent `eachMessage`, and `eachBatch`.
The previous kafka-crab-js scenarios are hidden by default. Set `BENCHMARK_SHOW_PREVIOUS=1` or select
`previous-serial` / `previous-batch` with `BENCHMARK_ONLY` when you want them in the comparison.
Both previous and workspace scenarios use `createWebStreamConsumer()` with the same benchmark configuration.

For throughput-only comparison with isolated scenario blocks:

```bash
vp run benchmark:isolated
```

For the old same-process stress run:

```bash
vp run benchmark:sequential
```

Sequential mode runs every selected scenario in one Node.js process. This is useful as a stress test, but process state
from previous clients can affect later scenarios. The isolated modes are the fairer comparison because native Kafka
clients, librdkafka state, libuv handles, allocator arenas, and RSS start clean for each scenario.

To run only selected library families:

```bash
BENCHMARK_LIBS=crab,platformatic-kafka vp run benchmark
BENCHMARK_LIBS=crab vp run benchmark
```

`BENCHMARK_LIBS` supports `crab`, `kafkajs`, and `platformatic-kafka`. By default it selects all benchmark scenarios
owned by those libraries.

For explicit scenario debugging, use `BENCHMARK_ONLY`:

```bash
BENCHMARK_ONLY=kafkajs-batch vp run benchmark
```

To force memory mode explicitly:

```bash
vp run benchmark:memory
```

Memory mode starts a fresh child Node.js process per scenario block. This avoids carrying V8 heap pages, native allocator
arenas, Kafka metadata, sockets, and Buffer pools from one client into the next client's measurement.
Each child imports the scenario client library after the memory baseline is captured, so memory deltas include client
module loading and the actual consumption run.

The most useful knobs are:

- `BENCHMARK_ITERATIONS=100000` controls how many consumed messages are measured per scenario.
- `BENCHMARK_RUNS=5` controls measured runs per child process (or per scenario in same-process mode).
- `BENCHMARK_MEASUREMENT_WINDOW=steady|first-message` selects the window; default `steady`.
- `BENCHMARK_WARMUP_MESSAGES` defaults to `BENCHMARK_ITERATIONS` in steady mode. `0` still excludes the first
  nonempty delivery. Whole deliveries are used, so the actual count can exceed the requested count. Ignored in
  first-message mode. Warmup and measured messages belong to the same consumer; each run creates a new consumer.
- `BENCHMARK_PAIR_ORDER=current-first` switches ABBA to BAAB in both full and isolated comparators.
  The default is `previous-first`. Alternate this across repetitions to balance which version occupies the middle.
- `BENCHMARK_KAFKA_SNAPSHOT=0` disables the common metadata/sample preparation in both full and ABBA modes;
  default `1`. Keep this setting equal when comparing the two runners.
- `BENCHMARK_BLOCKS=2` controls fresh processes per scenario in isolated modes. With 30 runs and two blocks,
  the table contains 60 measured runs per scenario. Use `1` to restore a single process per scenario.
- `BENCHMARK_FORCE_GC=1` triggers `globalThis.gc()` before each run when Node is started with `--expose-gc`.
- `BENCHMARK_SCENARIO_TIMEOUT_MS=120000` fails a scenario instead of waiting forever when the topic is missing data.
- `BENCHMARK_FETCH_MIN_BYTES=1` controls the minimum bytes requested by each consumer fetch.
- `BENCHMARK_FETCH_WAIT_MS=10` controls the broker-side fetch wait timeout.
- `BENCHMARK_FETCH_QUEUE_BACKOFF_MS` optionally sets librdkafka's fetch queue backoff for both previous and
  workspace kafka-crab-js (integer `0..300000`). Unset preserves each version's default: previous 4.1.3 uses
  `1000ms`, the workspace uses `20ms`. Set `20` to compare both with the same backoff. KafkaJS and Platformatic
  are unaffected. JSON records the explicit value, or `null` when inherited; it is not a native config dump.
- `BENCHMARK_MAX_BYTES=2048` controls both the total fetch cap and per-partition fetch cap where each client exposes
  those settings separately.
- `BENCHMARK_KAFKAJS_EACH_MESSAGE_CONCURRENCY=3` controls the concurrent KafkaJS `eachMessage` comparison scenario.
- `BENCHMARK_BATCH_SIZE=4096` controls batch stream size. Values above `16384` are normalized to `16384` so batch
  scenarios use a comparable effective size.
- `BENCHMARK_BATCH_TIMEOUT_MS=2` controls kafka-crab-js batch collection timeout.
- `BENCHMARK_SERIAL_PREFETCH_SIZE=64` controls kafka-crab-js serial Web Stream prefetching.
- `BENCHMARK_SERIAL_PREFETCH_TIMEOUT_MS=5` controls the serial Web Stream prefetch timeout.
- `BENCHMARK_SHOW_PREVIOUS=1` includes the installed previous kafka-crab-js scenarios in default selections.
- `BENCHMARK_SHUFFLE_SCENARIOS=1` randomizes the initial scenario order. Isolated mode keeps current/previous
  pairs together and reverses each subsequent block; same-process mode shuffles every run. Default order is stable.
- `BENCHMARK_MEMORY=1` runs the isolated memory benchmark. This is the default.
- `BENCHMARK_MEMORY=0` disables memory mode and allows the same-process or throughput-only isolated modes.
- `BENCHMARK_ISOLATED=1` runs the throughput-only table using the same isolated block schedule.
- `BENCHMARK_COLORS=0` disables ANSI colors in the benchmark tables.
- `BENCHMARK_CHARTS=0` disables the terminal comparison charts printed after the benchmark tables.
- `BENCHMARK_MEMORY_SAMPLE_MS=100` controls how frequently memory is sampled inside each child process.
- `BENCHMARK_MEMORY_SETTLE_MS=100` controls the delay after each run before the next run or final retained-memory sample.
- `BENCHMARK_SERIAL_TIMING=off|cpu|gaps` enables optional timing for the current and previous serial scenarios
  in isolated modes. `off` is the default. `cpu` records process and main-thread CPU at the existing measurement
  boundaries; `gaps` also timestamps each observed message. Memory/GC hooks and teardown remain enabled.
  Requires Node with `process.threadCpuUsage` support. Other scenarios are not instrumented.
  `mainThreadNonCpuMs` includes scheduling/GC/blocking and must not be interpreted as Kafka I/O time.
- `BENCHMARK_RESULT_PATH=/tmp/consumer.json` saves aggregated results and all raw `blocks` in either isolated mode.
  Each block contains its runs, memory, GC and optional timing. Crab serial/batch blocks also capture loaded
  package paths and binding hashes by default (`BENCHMARK_CAPTURE_RUNTIME=0` disables this post-measurement capture).
  CPU totals are also printed. Gap details are in the JSON.
  Each invocation reserves a new file before Kafka work and checkpoints each block, retaining partial results on
  failure. An existing path is rejected. When omitted, a unique destination under the OS temporary directory is
  printed and used automatically. Blocks include timestamps, host load, relevant runtime environment, harness
  hashes and lifecycle samples. Timing instrumentation affects results, especially the allocations
  used by `gaps`; compare against `off` and do not treat instrumented memory numbers as release measurements.
- `BENCHMARK_SETUP_MESSAGES=100000` controls how many messages `setup:consumer` produces.
- `BENCHMARK_SETUP_BATCH_SIZE=10000` controls setup producer batch size.
- `BENCHMARK_TOPIC_PREPARE_TIMEOUT_MS=30000` controls the metadata/admin timeout used while ensuring the setup topic
  exists.
- `BENCHMARK_TOPIC=benchmarks` controls the shared topic used by setup and consumers.
- `BENCHMARK_PARTITIONS=3` controls the topic partition count used by setup when the topic is created.
- `KAFKA_BROKERS=localhost:9092` overrides the broker list.

The topic must contain measured messages plus warmup, with room for rounding warmup to whole deliveries.
`setup:consumer` defaults to measured count + warmup target + max(measured count, 16384) messages. With custom
large broker batches, supply a larger `BENCHMARK_SETUP_MESSAGES`. The benchmark fails if it cannot finish the window.

## Producer Benchmarks

`producer.ts` compares producer throughput and per-send latency between the workspace
`kafka-crab-js` and the installed previous release, using the public `createProducer` API on
both sides:

```bash
vp run benchmark:producer
```

Scenarios: `crab-producer` and `previous-producer` (`autoFlush`), plus `crab-producer-manual` and
`previous-producer-manual` (`autoFlush: false` with an explicit `flush()` per batch). Previous
scenarios follow the same selection rules as the consumer bench (`BENCHMARK_SHOW_PREVIOUS`,
`BENCHMARK_ONLY`, `BENCHMARK_LIBS=crab`). Throughput, lifecycle memory, GC, and a per-send
latency table (mean/p50/p95/p99/max) are reported with the same isolated-process methodology.

Producer-specific knobs (consumer knobs do not apply, except `BENCHMARK_RUNS`,
`BENCHMARK_SCENARIO_TIMEOUT_MS`, and the memory/GC sampling knobs):

- `BENCHMARK_PRODUCER_TOPIC=benchmarks-producer` controls the topic produced into (created on
  first run; kept separate from the consumer topic).
- `BENCHMARK_PRODUCER_ITERATIONS=20000` controls how many produced messages are measured per
  scenario.
- `BENCHMARK_PRODUCER_BATCH_SIZE=100` controls messages per `send()`; each send is flushed
  before the next starts, so batch latency includes the broker round trip.
- `vp run benchmark:producer:quick` runs 2,000 messages once per autoFlush scenario.
- `vp run benchmark:producer:isolated` runs the throughput-only comparison.
- `vp run benchmark:producer:sequential` runs all scenarios in one process (stress mode).

For timeout-storm memory retention (no broker needed; every flush times out against a
blackhole, so confirmations are late or never arrive):

```bash
vp run benchmark:producer:storm
```

It reports retained RSS/external deltas after forced GC per implementation. Knobs:
`BENCHMARK_STORM_SENDS=300`, `BENCHMARK_STORM_MESSAGE_BYTES=65536`,
`BENCHMARK_STORM_QUEUE_TIMEOUT_MS=200`, `BENCHMARK_STORM_SETTLE_MS=500`.

## Profiling

For serial consumer memory across create/consume/cancel/disconnect/GC cycles, run each
implementation in a separate process on a prepared topic with at least 200,000 messages:

```bash
BENCHMARK_TOPIC=benchmarks MEMORY_RESULT_PATH=/tmp/serial-current.json node --expose-gc diagnostics/consumer-memory.mjs current
BENCHMARK_TOPIC=benchmarks MEMORY_RESULT_PATH=/tmp/serial-previous.json node --expose-gc diagnostics/consumer-memory.mjs previous
```

`BENCHMARK_RUNS` defaults to 30 and `BENCHMARK_ITERATIONS` to 200000. On macOS,
`NATIVE_MEMORY=1` adds baseline/final `vmmap -summary` snapshots (requires `rtk`).
The JSON includes all lifecycle samples and a final weak-reference reachability check.
This is a memory diagnostic, not a throughput comparison or a complete native leak detector.

For a serial comparison with an environment fingerprint and reversed order:

```sh
BENCHMARK_ITERATIONS=20000 BENCHMARK_RUNS=30 \
  BENCHMARK_ABBA_RESULT_PATH=/tmp/serial-abba.json pnpm benchmark:serial:abba
```

The runner uses the real `consumer.ts` memory child, preserving its memory/GC hooks and cleanup.
Both libraries now use built JavaScript by default; the source alias was removed from the benchmark tsconfig.
`TSX_TSCONFIG_PATH=./tsconfig.runtime.json` remains compatible with older reproduction commands but is no longer
required. `tsx` runs the benchmark harness; library imports resolve through package exports to `dist/index.js`.
Check each block's runtime package entry in the JSON.
It runs previous → current → current → previous in fresh processes, first with timing off and then
with CPU timing (`BENCHMARK_ABBA_MODES=off,cpu` by default; `gaps` is also supported).
`BENCHMARK_RUNS` applies to **each block**, so defaults yield 240 runs across eight processes.
The report contains every run, aggregate and forward/reversed pair comparisons, Node/OS/CPU details,
Git commit/status and source hashes, resolved package entries and actual loaded native binding hashes.
Runtime fingerprints are collected after the lifecycle memory snapshot.

A separate process reads Kafka metadata, available offsets and selected topic/broker settings before
and after the blocks. Before measurement it samples up to 32 messages per partition, storing sizes
and digests only. It does not produce messages or commit offsets. Sampling can warm caches and is not
a description of every message subsequently consumed. Config queries that fail are recorded explicitly.
The full isolated benchmark uses this same preparation, unless snapshots are disabled in both runners.
No topic is created; it must already contain enough messages for the requested count.

The output path must not exist. The runner checkpoints the same JSON after each block and records
failure if a child fails. `BENCHMARK_ABBA_TIMEOUT_MS` bounds each benchmark child (default 600000 ms).
Source and artifact consistency and before/after offsets are recorded; inspect them before comparing
results. Equal offsets do not establish identical broker load or cache state. This is a comparison of
serial blocks, not the full eight-scenario release benchmark. It does not capture per-function CPU profiles.

For the same isolated comparison of batch streams, use `benchmark:batch:abba`:

```sh
TSX_TSCONFIG_PATH=./tsconfig.runtime.json BENCHMARK_ITERATIONS=20000 BENCHMARK_RUNS=30 \
  BENCHMARK_ABBA_RESULT_PATH=/tmp/batch-abba-built.json pnpm benchmark:batch:abba
```

This uses previous-batch → crab-batch → crab-batch → previous-batch, four fresh processes and
120 runs with the defaults above. It preserves the configured batch size/timeout (4096/2 ms by default),
memory sampler and GC observer. Batch supports `BENCHMARK_ABBA_MODES=off` only; serial CPU/gap hooks
do not apply. Build the current library first when using `tsconfig.runtime.json`.

Profiling scripts use Node.js built-in profilers and write artifacts to `.profiles/`. They force `BENCHMARK_MEMORY=0`
so the profile captures the selected scenario directly instead of mostly profiling the isolated-process orchestrator.

Each script defaults to `BENCHMARK_ONLY=crab-batch`. Override it to inspect another scenario:

```bash
BENCHMARK_ONLY=crab-serial vp run benchmark:profile:cpu
BENCHMARK_ONLY=platformatic-kafka vp run benchmark:profile:gc
```

Available profiling scripts:

- `benchmark:profile:cpu` writes `.profiles/benchmark.cpuprofile`, which can be opened in Chrome DevTools or another
  V8 CPU profile viewer.
- `benchmark:profile:heap` writes `.profiles/benchmark.heapprofile`, useful for sampled heap allocation analysis.
- `benchmark:profile:v8` writes `.profiles/v8-processed.txt`, a processed `--prof` tick profile for terminal review.
- `benchmark:profile:gc` writes `.profiles/gc-trace.log`, including V8 GC timing and heap-space details.

Profiling changes timing and should be used for diagnosis, not for headline throughput numbers. Use the normal
benchmark scripts for comparisons, then profile one scenario at a time when the result points to a bottleneck.

## Methodology

This benchmark is a direct throughput benchmark. It compares concrete consumer APIs, not one abstract "library score".
Message-oriented and batch-oriented scenarios can appear in the same chart by design.

The default chart includes:

- message-oriented APIs for kafka-crab-js, KafkaJS, and `@platformatic/kafka`
- batch-oriented APIs for kafka-crab-js and KafkaJS

Previous kafka-crab-js rows are included only when `BENCHMARK_SHOW_PREVIOUS=1` is set or when previous scenarios are
selected explicitly with `BENCHMARK_ONLY`.

Libraries are included with the APIs available in this benchmark harness. A library can have more than one row when it
has more than one relevant consumption style. A library without a batch scenario is not forced into one.

Each scenario consumes from the beginning of the shared `BENCHMARK_TOPIC` topic with auto-commit disabled. The harness
records one complete run over `BENCHMARK_ITERATIONS` messages, then repeats the complete-run measurement
`BENCHMARK_RUNS` times per child, for `BENCHMARK_BLOCKS` children per scenario. Isolated children start clean, but they still share the broker and topic: a full
eight-scenario suite is a noisier (and more realistic) gate than `BENCHMARK_ONLY=previous-batch,crab-batch` on a cold
cluster. Use the full suite for release numbers; use `BENCHMARK_ONLY` to iterate on a single fix.

This is intentionally different from a microbenchmark that records one sample per message. Kafka consumers are bursty:
fetch wait time, librdkafka queues, Node's event loop, JIT, and GC can all move individual message timings around. The
stable number for this benchmark is aggregate throughput over a large measured window, with tolerance calculated across
whole runs. The `Runs` column means measured runs, not consumed messages.

Memory mode reports lifecycle memory for each isolated scenario. The memory sampler starts after process baseline GC
and includes module import, client creation, subscribe, measured consumption, cleanup, and final retained memory. GC
metrics use the selected measurement window; warmup GC is excluded in steady mode.

Across isolated blocks, throughput/percentiles use every run without filtering. GC counts and durations are
summed (maximum pause is a maximum); each memory column is the maximum per-process value, never a sum of
RSS across processes. The JSON keeps raw blocks for auditing these aggregates. The ABBA commands use the
same throughput calculation. Matching methodology does not guarantee identical measurements in separate executions.

Memory mode reports:

- `Peak RSS`: maximum sampled lifecycle RSS, including the final post-GC sample. Sampling can miss brief peaks.
  Raw blocks also contain `osPeakRssBytes` (OS process-lifetime high-water mark) and `sampledProcessingPeak` with
  `processingSamples`; the latter is `null` if the timer never ran during a measured window. These are distinct metrics.
- `Peak RSS delta`: `Peak RSS` minus the child process baseline after startup GC.
- `Peak heap`: maximum JS `heapUsed`.
- `Peak external`: maximum V8-tracked external memory, including much Buffer/native memory.
- `Peak ArrayBuffer`: maximum ArrayBuffer/Buffer backing store memory.
- `Retained RSS`: final RSS after scenario cleanup and forced GC, minus the child process baseline.
- `GC comparison`: V8 GC events observed with `perf_hooks` during the same selected windows used for
  throughput, excluding setup, subscribe, disconnect, and the benchmark's forced GC before each run. Use `GC time`,
  `GC share`, and `Max pause` for quick comparison.

Use `rss`, `external`, and `arrayBuffers` when comparing native clients; `heapUsed` alone misses most native-side cost.
Use `benchmark:profile:gc` when you need the full V8 `--trace-gc` log instead of the summarized comparison table.

Default tuning follows the Platformatic Kafka benchmark shape:

- `fetch.min.bytes=1` / `minBytes=1`
- `fetch.wait.max.ms=10` / `maxWaitTime=10`
- `BENCHMARK_MAX_BYTES=2048`
- `BENCHMARK_BATCH_TIMEOUT_MS=2`
- KafkaJS receives both `maxBytes` and `maxBytesPerPartition` from `BENCHMARK_MAX_BYTES`.
- kafka-crab-js receives `fetch.max.bytes`, `message.max.bytes`, `fetch.message.max.bytes`, and
  `max.partition.fetch.bytes` from `BENCHMARK_MAX_BYTES`.
- Platformatic Kafka uses its `maxBytes` option for both total fetch and partition fetch limits internally.
- KafkaJS `eachBatch` uses `partitionsConsumedConcurrently=3`.
- KafkaJS has two `eachMessage` scenarios: serial concurrency `1`, and concurrent concurrency
  `BENCHMARK_KAFKAJS_EACH_MESSAGE_CONCURRENCY`.
- Previous and workspace kafka-crab-js stream scenarios use `createWebStreamConsumer()` with identical Web
  `ReadableStream` reader loops and explicit tuning. Their native defaults can differ; use
  `BENCHMARK_FETCH_QUEUE_BACKOFF_MS=20` to equalize this backoff instead of comparing version defaults.
- kafka-crab-js batch scenarios use `BENCHMARK_BATCH_SIZE=4096` and `BENCHMARK_BATCH_TIMEOUT_MS=2`.
- Fetch byte limits do not guarantee response size: Kafka can return a first record batch larger than the cap
  to make progress. The application batch size is a message count, independent of the fetch byte limit.
- Batch scenarios use a common effective batch size capped at `16384`, matching the native batch limit. This avoids
  comparing a previous version with a very large Node stream highWaterMark against kafka-crab-js after the requested
  batch size has already been clamped.

The root `docker-compose.yml` exposes a 3-broker benchmark cluster on `127.0.0.1:9092`, `127.0.0.1:9093`, and
`127.0.0.1:9094`. The default benchmark bootstrap broker is `localhost:9092`, which is enough for Kafka metadata
discovery. To pass all brokers explicitly, run with `KAFKA_BROKERS=127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094`.

## Dependencies

The benchmark suite uses separate dependencies to avoid installing heavy native modules in CI/CD:

- `@platformatic/kafka`: Platformatic's Kafka client
- `kafkajs`: Pure JavaScript Kafka client
- `kafka-crab-js-previous`: Alias for the published kafka-crab-js version used by the previous-version scenarios.

The default previous version is declared in `benchmarks/kafka/package.json`. Select any published previous version with:

```bash
pnpm --filter kafka-benchmark add "kafka-crab-js-previous@npm:kafka-crab-js@<version>"
```

The selected version must expose `createWebStreamConsumer()` so the benchmark can use the same API on both sides.

Then compare it explicitly:

```bash
BENCHMARK_ONLY=previous-serial,previous-batch vp run benchmark
```
