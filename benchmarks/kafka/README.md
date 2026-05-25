# Benchmark Suite

This directory contains benchmark tests for kafka-crab-js performance comparison with other Kafka clients.

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

```bash
vp run benchmark
```

The default consumer benchmark runs in isolated memory mode. It starts one child Node.js process per selected scenario
and reports throughput, lifecycle memory, and message-window GC:

- `v4-serial`
- `kafkajs-serial`
- `kafkajs-serial-concurrent`
- `platformatic-kafka`
- `v4-batch`
- `kafkajs-batch`

KafkaJS is reported separately as serial `eachMessage`, concurrent `eachMessage`, and `eachBatch`.
kafka-crab-js v3 scenarios are hidden by default. Set `BENCHMARK_SHOW_V3=1` or select `v3-serial` / `v3-batch` with
`BENCHMARK_ONLY` when you want them in the comparison.

For throughput-only comparison with one child Node.js process per selected scenario:

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

Memory mode starts one child Node.js process per selected scenario. This avoids carrying V8 heap pages, native allocator
arenas, Kafka metadata, sockets, and Buffer pools from one client into the next client's measurement.
Each child imports the scenario client library after the memory baseline is captured, so memory deltas include client
module loading and the actual consumption run.

The most useful knobs are:

- `BENCHMARK_ITERATIONS=100000` controls how many consumed messages are measured per scenario.
- `BENCHMARK_RUNS=5` controls how many complete measured runs are collected per scenario.
- `BENCHMARK_FORCE_GC=1` triggers `globalThis.gc()` before each run when Node is started with `--expose-gc`.
- `BENCHMARK_SCENARIO_TIMEOUT_MS=120000` fails a scenario instead of waiting forever when the topic is missing data.
- `BENCHMARK_FETCH_MIN_BYTES=1` controls the minimum bytes requested by each consumer fetch.
- `BENCHMARK_FETCH_WAIT_MS=10` controls the broker-side fetch wait timeout.
- `BENCHMARK_MAX_BYTES=2048` controls both the total fetch cap and per-partition fetch cap where each client exposes
  those settings separately.
- `BENCHMARK_KAFKAJS_EACH_MESSAGE_CONCURRENCY=3` controls the concurrent KafkaJS `eachMessage` comparison scenario.
- `BENCHMARK_BATCH_SIZE=4096` controls batch stream size. Values above `16384` are normalized to `16384` so batch
  scenarios use a comparable effective size.
- `BENCHMARK_BATCH_TIMEOUT_MS=2` controls kafka-crab-js batch collection timeout.
- `BENCHMARK_SHOW_V3=1` includes kafka-crab-js v3 scenarios in default selections.
- `BENCHMARK_MEMORY=1` runs the isolated memory benchmark. This is the default.
- `BENCHMARK_MEMORY=0` disables memory mode and allows the same-process or throughput-only isolated modes.
- `BENCHMARK_ISOLATED=1` runs the throughput-only benchmark with one child Node.js process per selected scenario.
- `BENCHMARK_COLORS=0` disables ANSI colors in the benchmark tables.
- `BENCHMARK_CHARTS=0` disables the terminal comparison charts printed after the benchmark tables.
- `BENCHMARK_MEMORY_SAMPLE_MS=100` controls how frequently memory is sampled inside each child process.
- `BENCHMARK_MEMORY_SETTLE_MS=100` controls the delay after each run before the next run or final retained-memory sample.
- `BENCHMARK_SETUP_MESSAGES=100000` controls how many messages `setup:consumer` produces.
- `BENCHMARK_SETUP_BATCH_SIZE=10000` controls setup producer batch size.
- `BENCHMARK_TOPIC_PREPARE_TIMEOUT_MS=30000` controls the metadata/admin timeout used while ensuring the setup topic
  exists.
- `BENCHMARK_TOPIC=benchmarks` controls the shared topic used by setup and consumers.
- `BENCHMARK_PARTITIONS=3` controls the topic partition count used by setup when the topic is created.
- `KAFKA_BROKERS=localhost:9092` overrides the broker list.

`BENCHMARK_SETUP_MESSAGES` must be at least `BENCHMARK_ITERATIONS`. When it is not set, `setup:consumer` uses
`BENCHMARK_ITERATIONS` as its default.

## Profiling

Profiling scripts use Node.js built-in profilers and write artifacts to `.profiles/`. They force `BENCHMARK_MEMORY=0`
so the profile captures the selected scenario directly instead of mostly profiling the isolated-process orchestrator.

Each script defaults to `BENCHMARK_ONLY=v4-batch`. Override it to inspect another scenario:

```bash
BENCHMARK_ONLY=v4-serial vp run benchmark:profile:cpu
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

- message-oriented APIs for kafka-crab-js v4, KafkaJS, and `@platformatic/kafka`
- batch-oriented APIs for kafka-crab-js v4 and KafkaJS

kafka-crab-js v3 rows are included only when `BENCHMARK_SHOW_V3=1` is set or when v3 scenarios are selected explicitly
with `BENCHMARK_ONLY`.

Libraries are included with the APIs available in this benchmark harness. A library can have more than one row when it
has more than one relevant consumption style. A library without a batch scenario is not forced into one.

Each scenario consumes from the beginning of the shared `BENCHMARK_TOPIC` topic with auto-commit disabled. The harness
records one complete run over `BENCHMARK_ITERATIONS` messages, then repeats the complete-run measurement
`BENCHMARK_RUNS` times per scenario.

This is intentionally different from a microbenchmark that records one sample per message. Kafka consumers are bursty:
fetch wait time, librdkafka queues, Node's event loop, JIT, and GC can all move individual message timings around. The
stable number for this benchmark is aggregate throughput over a large measured window, with tolerance calculated across
whole runs. The `Runs` column means measured runs, not consumed messages.

Memory mode reports lifecycle memory for each isolated scenario. The memory sampler starts after process baseline GC
and includes module import, client creation, subscribe, measured consumption, cleanup, and final retained memory. GC
metrics use the narrower first-to-last-message window described below.

Memory mode reports:

- `Peak RSS`: maximum resident set size seen in the isolated child process.
- `Peak RSS delta`: `Peak RSS` minus the child process baseline after startup GC.
- `Peak heap`: maximum JS `heapUsed`.
- `Peak external`: maximum V8-tracked external memory, including much Buffer/native memory.
- `Peak ArrayBuffer`: maximum ArrayBuffer/Buffer backing store memory.
- `Retained RSS`: final RSS after scenario cleanup and forced GC, minus the child process baseline.
- `GC comparison`: V8 GC events observed with `perf_hooks` during the same first-to-last-message windows used for
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
- kafka-crab-js v4 stream scenarios use `createWebStreamConsumer()` with Web `ReadableStream` reader loops.
- kafka-crab-js batch scenarios use `BENCHMARK_BATCH_SIZE=4096` and `BENCHMARK_BATCH_TIMEOUT_MS=2`.
- Batch scenarios use a common effective batch size capped at `16384`, matching kafka-crab-js v4's native batch limit.
  This avoids comparing v3 with a very large Node stream highWaterMark against v4 after v4 has already clamped the
  requested batch size.

The root `docker-compose.yml` exposes a 3-broker benchmark cluster on `127.0.0.1:9092`, `127.0.0.1:9093`, and
`127.0.0.1:9094`. The default benchmark bootstrap broker is `localhost:9092`, which is enough for Kafka metadata
discovery. To pass all brokers explicitly, run with `KAFKA_BROKERS=127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094`.

## Dependencies

The benchmark suite uses separate dependencies to avoid installing heavy native modules in CI/CD:

- `@platformatic/kafka`: Platformatic's Kafka client
- `kafkajs`: Pure JavaScript Kafka client
- `kafka-crab-js-v3`: Alias for `kafka-crab-js@3.1.0`, used only by the v3 comparison scenarios
