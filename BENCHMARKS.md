# Crab JS Kafka Consumer Benchmarks

This document captures a local consumer benchmark run for `kafka-crab-js` v4 against KafkaJS and `@platformatic/kafka`.
The benchmark focuses on consumer throughput, lifecycle memory, and V8 garbage collection during the measured message
window.

These numbers are local benchmark snapshots. They are useful for understanding relative behavior under this workload,
but they are not universal capacity guarantees. Kafka benchmark results move with CPU power mode, broker state, message
shape, partitions, fetch settings, Node.js version, and the host/container runtime.

## Test Environment

The benchmark was run on May 12, 2026, using the repository benchmark harness and the root `docker-compose.yml`
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
- Node.js: `v24.15.0`
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
- `@platformatic/kafka@1.33.0`:
  - `@platformatic/kafka`: Platformatic consumer stream.

The `KafkaJS (eachBatch)` row is an additional KafkaJS batch baseline. The Platformatic upstream benchmark's `KafkaJS`
row is comparable to this document's `KafkaJS (eachMessage)` row, not to `KafkaJS (eachBatch)`.

## Consumer Throughput

`kafka-crab-js v4 (stream, batch)` was the fastest scenario in this run.

| Rank | Scenario                            |           Result | Tolerance | Relative |
| ---: | ----------------------------------- | ---------------: | --------: | -------: |
|    1 | `kafka-crab-js v4 (stream, batch)`  | `804,316 op/sec` |  `10.07%` | `100.0%` |
|    2 | `KafkaJS (eachBatch)`               | `601,307 op/sec` |   `4.24%` |  `74.8%` |
|    3 | `@platformatic/kafka`               | `527,737 op/sec` |   `5.63%` |  `65.6%` |
|    4 | `kafka-crab-js v4 (stream, serial)` | `449,827 op/sec` |   `9.67%` |  `55.9%` |
|    5 | `KafkaJS (eachMessage)`             | `333,847 op/sec` |  `13.72%` |  `41.5%` |
|    6 | `KafkaJS (eachMessage, concurrent)` | `292,268 op/sec` |  `26.82%` |  `36.3%` |

### Throughput Interpretation

- `kafka-crab-js v4 (stream, batch)` was about `33.8%` faster than `KafkaJS (eachBatch)`.
- `kafka-crab-js v4 (stream, batch)` was about `52.4%` faster than `@platformatic/kafka`.
- `kafka-crab-js v4 (stream, serial)` was about `34.7%` faster than `KafkaJS (eachMessage)`.
- KafkaJS concurrent `eachMessage` did not improve this workload and had the highest variance.

The batch result has a `10.07%` tolerance, so the exact gap should be treated as a snapshot. The ranking and magnitude
are still strong enough to show that the v4 batch stream path is the best throughput path in this workload.

## Lifecycle Memory

Lifecycle memory includes module loading, client creation, subscription, consumption, cleanup, and retained RSS after
cleanup. It is intentionally wider than the message-only throughput window because it captures the process cost of each
client library in an isolated child process.

| Scenario                            |    Peak RSS |   RSS delta | Retained RSS |  Peak heap |   External | ArrayBuffer |
| ----------------------------------- | ----------: | ----------: | -----------: | ---------: | ---------: | ----------: |
| `kafka-crab-js v4 (stream, batch)`  | `164.8 MiB` |  `83.8 MiB` |   `83.6 MiB` | `20.3 MiB` |  `5.7 MiB` |  `47.9 KiB` |
| `kafka-crab-js v4 (stream, serial)` | `145.7 MiB` |  `65.1 MiB` |   `64.7 MiB` | `13.9 MiB` |  `2.9 MiB` |  `47.9 KiB` |
| `@platformatic/kafka`               | `229.8 MiB` | `149.0 MiB` |  `148.9 MiB` | `73.2 MiB` |  `6.2 MiB` |   `2.2 MiB` |
| `KafkaJS (eachBatch)`               | `260.2 MiB` | `179.0 MiB` |  `179.0 MiB` | `69.5 MiB` | `15.1 MiB` |   `9.3 MiB` |
| `KafkaJS (eachMessage)`             | `247.3 MiB` | `166.6 MiB` |  `150.6 MiB` | `60.9 MiB` |  `9.6 MiB` |   `7.1 MiB` |
| `KafkaJS (eachMessage, concurrent)` | `260.1 MiB` | `179.2 MiB` |  `160.8 MiB` | `78.4 MiB` | `11.5 MiB` |   `8.8 MiB` |

### Memory Interpretation

- `kafka-crab-js v4 (stream, batch)` used about `53.2%` less RSS delta than `KafkaJS (eachBatch)`.
- `kafka-crab-js v4 (stream, batch)` used about `43.8%` less RSS delta than `@platformatic/kafka`.
- `kafka-crab-js v4 (stream, serial)` had the lowest RSS delta and peak heap among the measured scenarios.
- KafkaJS scenarios allocated substantially more heap and ArrayBuffer memory in this workload.

## Memory Efficiency

Memory efficiency is calculated as throughput divided by RSS delta MiB. Higher is better.

| Rank | Scenario                            |        Efficiency |   RSS delta | Relative |
| ---: | ----------------------------------- | ----------------: | ----------: | -------: |
|    1 | `kafka-crab-js v4 (stream, batch)`  | `9604 op/sec/MiB` |  `83.8 MiB` | `100.0%` |
|    2 | `kafka-crab-js v4 (stream, serial)` | `6912 op/sec/MiB` |  `65.1 MiB` |  `72.0%` |
|    3 | `@platformatic/kafka`               | `3542 op/sec/MiB` | `149.0 MiB` |  `36.9%` |
|    4 | `KafkaJS (eachBatch)`               | `3359 op/sec/MiB` | `179.0 MiB` |  `35.0%` |
|    5 | `KafkaJS (eachMessage)`             | `2004 op/sec/MiB` | `166.6 MiB` |  `20.9%` |
|    6 | `KafkaJS (eachMessage, concurrent)` | `1631 op/sec/MiB` | `179.2 MiB` |  `17.0%` |

The two kafka-crab-js v4 scenarios are the most memory-efficient results in this run. The batch stream path leads total
throughput and memory efficiency at the same time; the serial stream path is the lower-memory option when a
message-by-message API is required.

## Garbage Collection

GC metrics are measured during the message window used for throughput. Lower total GC time and lower GC share are
better.

| Rank | Scenario                            |     GC time | GC share | Events | Avg pause |  Max pause |
| ---: | ----------------------------------- | ----------: | -------: | -----: | --------: | ---------: |
|    1 | `kafka-crab-js v4 (stream, batch)`  |  `29.12 ms` |  `4.68%` |     23 | `1.27 ms` |  `2.41 ms` |
|    2 | `kafka-crab-js v4 (stream, serial)` |  `44.12 ms` |  `3.97%` |    195 | `0.23 ms` |  `0.83 ms` |
|    3 | `@platformatic/kafka`               |  `56.29 ms` |  `5.94%` |     46 | `1.22 ms` |  `8.22 ms` |
|    4 | `KafkaJS (eachBatch)`               |  `78.50 ms` |  `9.44%` |     37 | `2.12 ms` |  `6.47 ms` |
|    5 | `KafkaJS (eachMessage)`             |  `98.38 ms` |  `6.57%` |     56 | `1.76 ms` | `17.47 ms` |
|    6 | `KafkaJS (eachMessage, concurrent)` | `111.44 ms` |  `6.51%` |     65 | `1.71 ms` |  `8.39 ms` |

### GC Interpretation

- `kafka-crab-js v4 (stream, batch)` had the lowest total GC time.
- `kafka-crab-js v4 (stream, serial)` had many small minor collections, but the shortest max pause and the lowest GC
  share.
- KafkaJS scenarios spent more time in GC and showed higher peak heap and ArrayBuffer usage.

## Summary

The benchmark shows two clear kafka-crab-js v4 strengths:

- Use `kafka-crab-js v4 (stream, batch)` when raw consumer throughput and memory efficiency are the priority.
- Use `kafka-crab-js v4 (stream, serial)` when the application needs message-by-message processing with low heap and RSS
  pressure.

In this run, `kafka-crab-js v4 (stream, batch)` led throughput, memory efficiency, and total GC time. `@platformatic/kafka`
remained a strong non-crab competitor, but used more lifecycle RSS and heap than both kafka-crab-js v4 scenarios.
