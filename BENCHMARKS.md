# Crab JS Kafka Consumer Benchmarks

This document captures a local consumer benchmark run for `kafka-crab-js` v4 against KafkaJS and `@platformatic/kafka`.
The benchmark focuses on consumer throughput, lifecycle memory, and V8 garbage collection during the measured message
window.

These numbers are local benchmark snapshots. They are useful for understanding relative behavior under this workload,
but they are not universal capacity guarantees. Kafka benchmark results move with CPU power mode, broker state, message
shape, partitions, fetch settings, Node.js version, and the host/container runtime.

## Test Environment

The benchmark was run on May 25, 2026, using the repository benchmark harness and the root `docker-compose.yml`
three-broker Kafka cluster.

Host machine: MacBook Pro with Apple M1.

### Kafka Cluster

- Broker image: `apache/kafka:4.0.0`
- Broker count: 3
- Bootstrap brokers: `127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094`
- Topic: `benchmarks`
- Partitions: 3 when the topic is created by setup

### Benchmark Configuration

- Benchmark mode: isolated Node.js process per scenario with lifecycle memory sampling
- Node.js: `v24.16.0`
- Messages consumed per scenario run: 100,000
- Runs per scenario: 5
- Fetch min bytes: 1
- Fetch wait: 10 ms
- Fetch max bytes: 2048
- Partition max bytes: 2048
- kafka-crab-js batch size: 4096
- kafka-crab-js batch timeout: 2 ms
- KafkaJS concurrent `eachMessage` partitions: 3
- Memory sample interval: 100 ms
- Memory settle time: 100 ms

### Commands

```bash
podman compose up -d
```

```bash
cd benchmarks/kafka
KAFKA_BROKERS=127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094 vp run setup:consumer
KAFKA_BROKERS=127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094 vp run benchmark
```

## Libraries And Scenarios Tested

The default benchmark compares consumer APIs with broadly similar semantics:

- `kafka-crab-js@4.0.0-beta.3`:
  - `kafka-crab-js v4 (stream, serial)`: Web Stream serial mode, emitting one `Message` at a time.
  - `kafka-crab-js v4 (stream, batch)`: Web Stream batch mode, emitting `Message[]` chunks.
- `kafkajs@2.2.4`:
  - `KafkaJS (eachMessage)`: standard KafkaJS message handler.
  - `KafkaJS (eachMessage, concurrent)`: KafkaJS `eachMessage` with concurrent partition consumption.
  - `KafkaJS (eachBatch)`: KafkaJS batch handler.
- `@platformatic/kafka@2.1.0`:
  - `@platformatic/kafka`: Platformatic consumer stream.

The `KafkaJS (eachBatch)` row is an additional KafkaJS batch baseline. The Platformatic upstream benchmark's `KafkaJS`
row is comparable to this document's `KafkaJS (eachMessage)` row, not to `KafkaJS (eachBatch)`.

## Consumer Benchmark

The default isolated-process benchmark reports throughput, lifecycle memory, and retained RSS for each scenario.

| Rank | Scenario                            | Runs |                Result | Tolerance | Vs previous |    Peak RSS |   RSS delta | Retained RSS |  Peak heap |   External | ArrayBuffer |
| ---: | ----------------------------------- | ---: | --------------------: | --------: | ----------: | ----------: | ----------: | -----------: | ---------: | ---------: | ----------: |
|    1 | `KafkaJS (eachMessage, concurrent)` |    5 |   `427,265.29 op/sec` |   `3.71%` |         `-` | `243.9 MiB` | `178.3 MiB` |  `175.5 MiB` | `72.2 MiB` | `14.3 MiB` |   `7.7 MiB` |
|    2 | `KafkaJS (eachMessage)`             |    5 |   `431,519.93 op/sec` |   `8.78%` |    `+1.00%` | `251.1 MiB` | `186.3 MiB` |  `180.8 MiB` | `91.6 MiB` | `13.8 MiB` |  `11.2 MiB` |
|    3 | `KafkaJS (eachBatch)`               |    5 |   `478,411.67 op/sec` |  `13.22%` |   `+10.87%` | `258.7 MiB` | `193.4 MiB` |  `173.5 MiB` | `83.8 MiB` | `16.2 MiB` |  `13.7 MiB` |
|    4 | `kafka-crab-js v4 (stream, serial)` |    5 |   `558,968.71 op/sec` |   `3.41%` |   `+16.84%` | `130.2 MiB` |  `64.7 MiB` |   `63.9 MiB` | `14.6 MiB` |  `2.9 MiB` | `113.0 KiB` |
|    5 | `@platformatic/kafka`               |    5 |   `653,355.35 op/sec` |   `3.39%` |   `+16.89%` | `281.2 MiB` | `216.0 MiB` |  `216.0 MiB` | `87.4 MiB` | `16.3 MiB` |  `12.4 MiB` |
|    6 | `kafka-crab-js v4 (stream, batch)`  |    5 | `1,167,396.71 op/sec` |   `4.40%` |   `+78.68%` | `171.0 MiB` | `106.0 MiB` |  `105.8 MiB` | `29.5 MiB` |  `8.0 MiB` |   `1.0 MiB` |

## Consumer Throughput

`kafka-crab-js v4 (stream, batch)` was the fastest scenario in this run.

| Rank | Scenario                            |                Result | Relative |
| ---: | ----------------------------------- | --------------------: | -------: |
|    1 | `kafka-crab-js v4 (stream, batch)`  | `1,167,396.71 op/sec` | `100.0%` |
|    2 | `@platformatic/kafka`               |   `653,355.35 op/sec` |  `56.0%` |
|    3 | `kafka-crab-js v4 (stream, serial)` |   `558,968.71 op/sec` |  `47.9%` |
|    4 | `KafkaJS (eachBatch)`               |   `478,411.67 op/sec` |  `41.0%` |
|    5 | `KafkaJS (eachMessage)`             |   `431,519.93 op/sec` |  `37.0%` |
|    6 | `KafkaJS (eachMessage, concurrent)` |   `427,265.29 op/sec` |  `36.6%` |

### Throughput Interpretation

- `kafka-crab-js v4 (stream, batch)` was about `78.7%` faster than `@platformatic/kafka`.
- `kafka-crab-js v4 (stream, batch)` was about `144.0%` faster than `KafkaJS (eachBatch)`.
- `kafka-crab-js v4 (stream, serial)` was about `29.5%` faster than `KafkaJS (eachMessage)`.
- KafkaJS concurrent `eachMessage` did not improve this workload; the serial KafkaJS `eachMessage` scenario was about
  `1.0%` faster.

The batch result has a `4.40%` tolerance, so the exact gap should be treated as a snapshot. The ranking and magnitude
are still strong enough to show that the v4 batch stream path is the best throughput path in this workload.

## Lifecycle Memory

Lifecycle memory includes module loading, client creation, subscription, consumption, cleanup, and retained RSS after
cleanup. It is intentionally wider than the message-only throughput window because it captures the process cost of each
client library in an isolated child process.

| Scenario                            |    Peak RSS |   RSS delta | Retained RSS |  Peak heap |   External | ArrayBuffer |
| ----------------------------------- | ----------: | ----------: | -----------: | ---------: | ---------: | ----------: |
| `kafka-crab-js v4 (stream, serial)` | `130.2 MiB` |  `64.7 MiB` |   `63.9 MiB` | `14.6 MiB` |  `2.9 MiB` | `113.0 KiB` |
| `kafka-crab-js v4 (stream, batch)`  | `171.0 MiB` | `106.0 MiB` |  `105.8 MiB` | `29.5 MiB` |  `8.0 MiB` |   `1.0 MiB` |
| `KafkaJS (eachMessage, concurrent)` | `243.9 MiB` | `178.3 MiB` |  `175.5 MiB` | `72.2 MiB` | `14.3 MiB` |   `7.7 MiB` |
| `KafkaJS (eachMessage)`             | `251.1 MiB` | `186.3 MiB` |  `180.8 MiB` | `91.6 MiB` | `13.8 MiB` |  `11.2 MiB` |
| `KafkaJS (eachBatch)`               | `258.7 MiB` | `193.4 MiB` |  `173.5 MiB` | `83.8 MiB` | `16.2 MiB` |  `13.7 MiB` |
| `@platformatic/kafka`               | `281.2 MiB` | `216.0 MiB` |  `216.0 MiB` | `87.4 MiB` | `16.3 MiB` |  `12.4 MiB` |

### Memory Interpretation

- `kafka-crab-js v4 (stream, batch)` used about `45.2%` less RSS delta than `KafkaJS (eachBatch)`.
- `kafka-crab-js v4 (stream, batch)` used about `50.9%` less RSS delta than `@platformatic/kafka`.
- `kafka-crab-js v4 (stream, serial)` had the lowest RSS delta, retained RSS, peak heap, external memory, and
  ArrayBuffer usage among the measured scenarios.
- `@platformatic/kafka` was the fastest non-crab competitor, but had the highest lifecycle RSS delta and retained RSS in
  this run.

## Memory Efficiency

Memory efficiency is calculated as throughput divided by RSS delta MiB. Higher is better.

| Rank | Scenario                            |         Efficiency |   RSS delta | Relative |
| ---: | ----------------------------------- | -----------------: | ----------: | -------: |
|    1 | `kafka-crab-js v4 (stream, batch)`  | `11016 op/sec/MiB` | `106.0 MiB` | `100.0%` |
|    2 | `kafka-crab-js v4 (stream, serial)` |  `8635 op/sec/MiB` |  `64.7 MiB` |  `78.4%` |
|    3 | `@platformatic/kafka`               |  `3025 op/sec/MiB` | `216.0 MiB` |  `27.5%` |
|    4 | `KafkaJS (eachBatch)`               |  `2474 op/sec/MiB` | `193.4 MiB` |  `22.5%` |
|    5 | `KafkaJS (eachMessage, concurrent)` |  `2396 op/sec/MiB` | `178.3 MiB` |  `21.8%` |
|    6 | `KafkaJS (eachMessage)`             |  `2316 op/sec/MiB` | `186.3 MiB` |  `21.0%` |

The two kafka-crab-js v4 scenarios are the most memory-efficient results in this run. The batch stream path leads total
throughput and memory efficiency at the same time; the serial stream path is the lowest-memory option when a
message-by-message API is required.

## Garbage Collection

GC metrics are measured during the message window used for throughput. Lower total GC time and lower GC share are
better.

| Rank | Scenario                            |    GC time | GC share | Events | Avg pause |  Max pause | Minor | Major | Incr | Forced |
| ---: | ----------------------------------- | ---------: | -------: | -----: | --------: | ---------: | ----: | ----: | ---: | -----: |
|    1 | `kafka-crab-js v4 (stream, batch)`  | `21.69 ms` |  `5.06%` |     22 | `0.99 ms` |  `2.81 ms` |     8 |     7 |    7 |      0 |
|    2 | `kafka-crab-js v4 (stream, serial)` | `30.46 ms` |  `3.41%` |    185 | `0.16 ms` |  `0.45 ms` |   185 |     0 |    0 |      0 |
|    3 | `KafkaJS (eachBatch)`               | `63.05 ms` |  `6.03%` |     39 | `1.62 ms` |  `4.11 ms` |    25 |     7 |    7 |      0 |
|    4 | `@platformatic/kafka`               | `74.27 ms` |  `9.71%` |     39 | `1.90 ms` |  `8.75 ms` |    25 |     7 |    7 |      0 |
|    5 | `KafkaJS (eachMessage, concurrent)` | `83.77 ms` |  `7.16%` |     65 | `1.29 ms` |  `2.85 ms` |    49 |     8 |    8 |      0 |
|    6 | `KafkaJS (eachMessage)`             | `90.15 ms` |  `7.78%` |     62 | `1.45 ms` | `11.45 ms` |    46 |     8 |    8 |      0 |

### GC Interpretation

- `kafka-crab-js v4 (stream, batch)` had the lowest total GC time.
- `kafka-crab-js v4 (stream, serial)` had the lowest GC share, average pause, and max pause, with many small minor
  collections.
- KafkaJS scenarios spent more time in GC and showed higher peak heap and ArrayBuffer usage in this workload.
- `@platformatic/kafka` had the highest GC share among the measured scenarios.

## Summary

The benchmark shows two clear kafka-crab-js v4 strengths:

- Use `kafka-crab-js v4 (stream, batch)` when raw consumer throughput and memory efficiency are the priority.
- Use `kafka-crab-js v4 (stream, serial)` when the application needs message-by-message processing with the lowest RSS
  delta and heap pressure.

In this run, `kafka-crab-js v4 (stream, batch)` led throughput, memory efficiency, and total GC time. `@platformatic/kafka`
remained a strong non-crab competitor, but used more lifecycle RSS, heap, and GC time than both kafka-crab-js v4
scenarios.
