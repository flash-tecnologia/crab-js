# Benchmark Suite

This directory contains benchmark tests for kafka-crab-js performance comparison with other Kafka clients.

## Setup

1. Install benchmark dependencies:

```bash
vp install
```

2. Make sure you have Kafka running (use the integration test setup):

```bash
cd ../__test__/integration
docker-compose up -d
# or
podman-compose up -d
```

3. Prepare the benchmark data:

```bash
vp run setup:consumer
```

## Running Benchmarks

```bash
vp run benchmark
```

The consumer benchmark is intentionally small and follows the same shape as Platformatic's consumer benchmark. It reads
from the shared benchmark topic and prints all selected scenarios in one chart:

- `v3-serial`
- `v4-serial`
- `kafkajs-serial`
- `platformatic-kafka`
- `v3-batch`
- `v4-batch`
- `kafkajs-batch`

KafkaJS is reported separately as `eachMessage` and `eachBatch`.

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

To compare memory, run the isolated memory benchmark:

```bash
vp run benchmark:memory
```

Memory mode starts one child Node.js process per selected scenario and skips v3 kafka-crab-js scenarios. This avoids
carrying V8 heap pages, native allocator arenas, Kafka metadata, sockets, and Buffer pools from one client into the next
client's measurement.

The most useful knobs are:

- `BENCHMARK_ITERATIONS=100000` controls how many consumed messages are measured per scenario.
- `BENCHMARK_WARMUP_MESSAGES=0` controls how many messages are consumed before timing starts.
- `BENCHMARK_WARMUP_RUNS=1` controls how many complete runs are discarded before measured runs start.
- `BENCHMARK_RUNS=5` controls how many complete measured runs are collected per scenario.
- `BENCHMARK_FORCE_GC=1` triggers `globalThis.gc()` before each run when Node is started with `--expose-gc`.
- `BENCHMARK_SCENARIO_TIMEOUT_MS=120000` fails a scenario instead of waiting forever when the topic is missing data.
- `BENCHMARK_MAX_BYTES=2048` controls fetch byte caps.
- `BENCHMARK_BATCH_SIZE=4096` controls kafka-crab-js batch stream size.
- `BENCHMARK_MEMORY=1` runs the isolated memory benchmark instead of the regular same-process throughput table.
- `BENCHMARK_MEMORY_SAMPLE_MS=100` controls how frequently memory is sampled inside each child process.
- `BENCHMARK_MEMORY_SETTLE_MS=100` controls the delay after each run before the next run or final retained-memory sample.
- `BENCHMARK_SETUP_MESSAGES=100000` controls how many messages `setup:consumer` produces.
- `BENCHMARK_SETUP_BATCH_SIZE=10000` controls setup producer batch size.
- `BENCHMARK_TOPIC=benchmarks` controls the shared topic used by setup and consumers.
- `BENCHMARK_PARTITIONS=3` controls the topic partition count used by setup.
- `KAFKA_BROKERS=localhost:9092` overrides the broker list.

`BENCHMARK_SETUP_MESSAGES` must be at least `BENCHMARK_WARMUP_MESSAGES + BENCHMARK_ITERATIONS`. When it is not set,
`setup:consumer` uses that sum as its default.

## Methodology

This benchmark is a direct throughput benchmark. It compares concrete consumer APIs, not one abstract "library score".
Message-oriented and batch-oriented scenarios can appear in the same chart by design.

The default chart includes:

- message-oriented APIs for kafka-crab-js v3, kafka-crab-js v4, KafkaJS, and `@platformatic/kafka`
- batch-oriented APIs for kafka-crab-js v3, kafka-crab-js v4, and KafkaJS

Libraries are included with the APIs available in this benchmark harness. A library can have more than one row when it
has more than one relevant consumption style. A library without a batch scenario is not forced into one.

Each scenario consumes from the beginning of the shared `BENCHMARK_TOPIC` topic with auto-commit disabled. The harness
first consumes `BENCHMARK_WARMUP_MESSAGES` without timing, then records one complete run over `BENCHMARK_ITERATIONS`
messages. The benchmark discards `BENCHMARK_WARMUP_RUNS` complete runs, then repeats the complete-run measurement
`BENCHMARK_RUNS` times per scenario.

This is intentionally different from a microbenchmark that records one sample per message. Kafka consumers are bursty:
fetch wait time, librdkafka queues, Node's event loop, JIT, and GC can all move individual message timings around. The
stable number for this benchmark is aggregate throughput over a large measured window, with tolerance calculated across
whole runs. In the cronometro output, the `Samples` column means measured runs, not consumed messages.

Memory mode reports:

- `Peak RSS`: maximum resident set size seen in the isolated child process.
- `Peak RSS delta`: `Peak RSS` minus the child process baseline after startup GC.
- `Peak heap`: maximum JS `heapUsed`.
- `Peak external`: maximum V8-tracked external memory, including much Buffer/native memory.
- `Peak ArrayBuffer`: maximum ArrayBuffer/Buffer backing store memory.
- `Retained RSS`: final RSS after scenario cleanup and forced GC, minus the child process baseline.

Use `rss`, `external`, and `arrayBuffers` when comparing native clients; `heapUsed` alone misses most native-side cost.

Default tuning follows the Platformatic Kafka benchmark shape:

- `fetch.min.bytes=1` / `minBytes=1`
- `fetch.wait.max.ms=10` / `maxWaitTime=10`
- `BENCHMARK_MAX_BYTES=2048`
- KafkaJS uses `partitionsConsumedConcurrently=3`.
- kafka-crab-js v4 serial prefetch uses `64` messages and `5ms`.
- kafka-crab-js batch scenarios use `BENCHMARK_BATCH_SIZE=4096`.

The default broker list is `localhost:9092`, matching the repository integration compose. For the 3-broker benchmark
cluster, run with `KAFKA_BROKERS=localhost:9092,localhost:9093,localhost:9094`.

## Dependencies

The benchmark suite uses separate dependencies to avoid installing heavy native modules in CI/CD:

- `@platformatic/kafka`: Platformatic's Kafka client
- `kafkajs`: Pure JavaScript Kafka client
- `cronometro`: Benchmarking utilities
