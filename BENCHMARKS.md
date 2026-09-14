# Kafka consumer benchmarks

The September 12, 2026 development build delivered **1.66 million messages/s in
batch mode**, **1.98× KafkaJS `eachBatch`**, in this small-message consumer workload.
Serial delivered **916 thousand messages/s**, **1.40× KafkaJS `eachMessage`**.
Platformatic's message stream was faster than crab serial. Current batch used
less JavaScript heap than the measured JavaScript clients, with higher total RSS.

This page reports a measured configuration, not a universal client ranking. It
replaces the May 2026 first-message snapshot as the current comparison. The old
[chart](packages/kafka-crab-js/assets/consumer-benchmark-snapshot.svg) is retained
only as a historical artifact and does not represent these results.

[Get started](packages/kafka-crab-js/README.md) ·
[Run the harness](benchmarks/kafka/README.md) ·
[Download the evidence](packages/kafka-crab-js/docs/rfc/review/evidence/consumer-comparison-2026-09-12.json)

## Consumer throughput

All values are messages per second. Each row includes **120 runs of 20,000 measured
messages**, after warmup, across four separate processes. Aggregate throughput is
total measured messages divided by total measured time. Median/p05/p95 use the
nearest rank of individual run throughputs; p05/p95 describe throughput dispersion,
not per-message latency or a confidence interval.

The tables below compare kafka-crab-js with KafkaJS and Platformatic. Baselines
from the previous release appear only in the
[version comparison](#comparison-with-the-previous-release). The six scenarios
below contribute 720 runs; the two previous-release baselines add 240 runs to
the complete 960-run capture.

| Consumer / interface                        | Aggregate msg/s |    Median |       p05 |       p95 |
| ------------------------------------------- | --------------: | --------: | --------: | --------: |
| kafka-crab-js development · Web batch       |       1,663,382 | 1,824,367 | 1,201,649 | 2,041,085 |
| @platformatic/kafka 2.11.0 · message stream |         998,122 | 1,025,488 |   805,694 | 1,230,523 |
| kafka-crab-js development · Web serial      |         915,664 |   969,084 |   648,395 | 1,026,582 |
| KafkaJS 2.2.4 · eachBatch                   |         841,194 |   875,562 |   620,807 |   939,487 |
| KafkaJS 2.2.4 · eachMessage                 |         655,365 |   689,455 |   496,290 |   732,725 |
| KafkaJS 2.2.4 · eachMessage, concurrency 3  |         646,613 |   680,237 |   496,134 |   735,390 |

The development snapshot led the measured batch scenarios. The 1.67× ratio versus
Platformatic compares batch delivery with message-at-a-time
stream delivery. Use the matching API comparison when choosing a processing model.
No claim is made about unmeasured clients or production application throughput.

## Memory

Values are MiB. Each column takes the maximum per-process value across the four
blocks, so values in the same row can come from different processes. RSS includes
JavaScript, native allocations, and allocator-retained regions. JS heap is only
one part of it; external buffers are not included in `heapUsed`.

| Consumer / interface                        | Peak RSS | Peak RSS delta | Retained RSS delta | Peak JS heap |
| ------------------------------------------- | -------: | -------------: | -----------------: | -----------: |
| kafka-crab-js development · Web batch       |    266.3 |          197.6 |              197.5 |         29.0 |
| @platformatic/kafka 2.11.0 · message stream |    256.7 |          187.8 |              187.8 |         87.0 |
| kafka-crab-js development · Web serial      |    178.9 |          110.5 |              110.4 |         21.9 |
| KafkaJS 2.2.4 · eachBatch                   |    238.9 |          170.3 |              170.3 |         71.7 |
| KafkaJS 2.2.4 · eachMessage                 |    245.6 |          177.3 |              177.3 |         77.8 |
| KafkaJS 2.2.4 · eachMessage, concurrency 3  |    246.7 |          178.1 |              178.0 |         83.2 |

Current batch peaked at 29.0 MiB of JS heap versus KafkaJS batch's 71.7 MiB, about
60% lower in this capture. Its RSS was 266.3 MiB versus 238.9 MiB. Current serial
used 178.9 MiB RSS versus 245.6 MiB for KafkaJS `eachMessage`.

These are **sampled lifecycle maxima**, including final post-GC observations,
not exact peaks inside the measured message window. The 100 ms sampler can miss
short processing windows; JSON retains processing samples separately, with `null`
when none exists, plus the OS RSS high-water mark. Retained delta is post-teardown,
post-GC RSS minus baseline. It does not identify live objects or prove a leak.

## Garbage collection

Observed GC within the measured message windows, summed across all 120 runs per
scenario. Share is summed GC duration / summed active measurement duration.
Forced GC runs before measurement and during lifecycle collection; no forced GC
was observed within the measured windows.

| Consumer / interface                        | GC time (ms) | GC share | Events | Largest pause (ms) | Forced events |
| ------------------------------------------- | -----------: | -------: | -----: | -----------------: | ------------: |
| kafka-crab-js development · Web batch       |        62.01 |    4.29% |     61 |               2.29 |             0 |
| @platformatic/kafka 2.11.0 · message stream |       201.87 |    8.39% |    120 |               3.86 |             0 |
| kafka-crab-js development · Web serial      |        88.15 |    3.36% |    625 |               0.47 |             0 |
| KafkaJS 2.2.4 · eachBatch                   |       206.21 |    7.23% |    217 |               3.90 |             0 |
| KafkaJS 2.2.4 · eachMessage                 |       396.02 |   10.81% |    295 |               3.87 |             0 |
| KafkaJS 2.2.4 · eachMessage, concurrency 3  |       408.63 |   11.01% |    303 |               4.53 |             0 |

The workload measures delivery with a minimal counter handler. GC duration is not
a direct measure of end-to-end latency, and a lower percentage can also reflect a
longer processing window.

## Comparison with the previous release

The baseline is the published **kafka-crab-js 4.1.3** release. It is used here to
measure changes between versions. Both modes use Web Streams, the same measured
message count, and 120 runs per version with opposite process pair orders.

| Mode   | Previous 4.1.3 (msg/s) | Current development (msg/s) | Throughput change |
| ------ | ---------------------: | --------------------------: | ----------------: |
| Serial |                744,174 |                     915,664 |           +23.04% |
| Batch  |              1,544,012 |                   1,663,382 |            +7.73% |

| Mode   | Previous peak RSS | Current peak RSS | Previous peak JS heap | Current peak JS heap |
| ------ | ----------------: | ---------------: | --------------------: | -------------------: |
| Serial |         203.4 MiB |        178.9 MiB |              19.0 MiB |             21.9 MiB |
| Batch  |         212.0 MiB |        266.3 MiB |              26.1 MiB |             29.0 MiB |

Current serial improved throughput and reduced peak RSS; current batch improved
throughput with higher peak RSS. Total observed GC time changed from 117.97 to
88.15 ms in serial and from 64.45 to 62.01 ms in batch. All per-process memory,
GC, and individual throughput measurements remain in the evidence JSON.

This comparison preserves each version's fetch queue backoff default: 1,000 ms
previous and 20 ms current. It measures the versions with those defaults, rather
than isolating implementation changes under equal backoff. Reproduction below
also describes an equal-backoff control.

At capture time, the development manifest still declared 4.1.3. It is a different,
unpublished build from the registry baseline; the artifact fingerprints below
identify each one. The current results must not be attributed to the published
4.1.3 package.

## Methodology

Captured September 12, 2026, from 15:46:15 to 15:50:00 UTC:

| Property                | Captured value                                                                                       |
| ----------------------- | ---------------------------------------------------------------------------------------------------- |
| Host                    | Apple M4, 10 CPUs, 16 GiB RAM, Darwin 25.6.0, arm64                                                  |
| Runtime                 | Node.js 24.20.0; `node --expose-gc --import tsx consumer.ts`                                         |
| Kafka                   | Three local brokers; topic `benchmarks`, replication factor 1                                        |
| Topic offsets           | Partition 0: 0–166,665; partition 1: 0–333,335; partition 2: empty                                   |
| Sample                  | 32 records per nonempty partition; 73/75-byte values, 11-byte keys, one 38-byte header entry         |
| Fetch settings          | Minimum 1 byte; maximum 2,048 bytes; per-partition 2,048 bytes for crab/KafkaJS; wait 10 ms          |
| Crab backoff            | Version defaults: previous 1,000 ms, development 20 ms; recorded override is `null`                  |
| Batch / serial prefetch | Crab batch 4,096/2 ms; serial prefetch 64/5 ms                                                       |
| KafkaJS concurrency     | `eachMessage`: 1 and 3; `eachBatch`: 3                                                               |
| Measurement             | `steady`; target 20,000 warmup messages, then 20,000 measured messages per run                       |
| Isolation               | Two full suites × eight scenarios × two processes × 30 runs = 960 runs                               |
| Ordering                | First suite ABBA, second BAAB for current/previous crab pairs; other scenarios in fixed positions    |
| Memory                  | 100 ms sampling and settling; forced GC before runs and during lifecycle collection                  |
| Handler                 | Count delivered messages; no business logic, manual commits, or payload validation in the timed loop |
| Diagnostics             | Crab diagnostic instrumentation disabled; no OTEL exporter or serial timing instrumentation          |

The warmup delivery is excluded in its entirety, including messages beyond the
warmup target. The timer for the measured window starts after that boundary and
covers subsequent delivery. First-delivery latency/size, actual warmup, received
counts, and delivery-size ranges are preserved separately. Setup, warmup, and
shutdown are not part of the throughput denominator. Auto-commit is disabled.

The harness loads built `dist/index.js` entries. `tsx` runs the benchmark, not
current package source via a path alias. The current build is labeled
**development**; the [previous-release comparison](#comparison-with-the-previous-release)
explains the baseline version. The evidence retains JS/harness SHA-256 values
and these native fingerprints:

| Artifact                             | SHA-256                                                            |
| ------------------------------------ | ------------------------------------------------------------------ |
| Development macOS arm64 binding      | `65d1cdd282a6f810164e8c0b02806615a767f25ef47b53d7178fcfbc946054c2` |
| Previous-release macOS arm64 binding | `6cf38ecb025c7c2615297bd91a264a76cceefa53c79d1a24b4357c06f6fe7351` |

Competitor versions were read from installed packages: KafkaJS 2.2.4 and
`@platformatic/kafka` 2.11.0. The harness fingerprints crab runtime artifacts;
it does not capture equivalent per-block competitor module hashes.

### Interpretation limits

All measurements are retained; none were trimmed as outliers. Four processes per
scenario do not represent 120 independent machines. The local host/brokers were
shared, topic preparation can warm caches, and competitor order was not randomized.
The Kafka controller changed from broker 2 to 3 in suite A and 1 to 3 in suite B;
partition metadata, leaders, offsets, and captured configuration matched before
and after each suite. The effect of those controller changes was not isolated.
The evidence preserves both snapshots rather than assuming an idle cluster.

Only two topic partitions contained records. This limits interpretations of
partition concurrency. Fetch limits and batch interfaces are not semantically
identical between libraries; equivalent byte/wait options are used where exposed.
The default comparison intentionally preserves crab's different backoff defaults;
set an explicit override to run a separate equal-backoff control.

These results do not establish cross-platform superiority, exactly-once delivery,
replicated durability, failure recovery, OTEL overhead, or long-term memory
stability. Older `first-message` figures excluded the first receive/conversion
from timing while counting its messages; do not combine them with this window.
The [engineering history](packages/kafka-crab-js/docs/rfc/review/performance.md)
retains earlier losses, methodology changes, and remaining acceptance criteria.

## Reproduce

From the repository root, install dependencies and build the current package:

```sh
pnpm install --frozen-lockfile
pnpm --filter kafka-crab-js build
```

Start local Kafka with the repository's `docker-compose.yml`, or set
`KAFKA_BROKERS` for your test cluster. In a separate test environment, prepare the
dataset with `pnpm --filter kafka-benchmark setup:consumer`. Existing topic data
and broker layout affect results; inspect the captured metadata rather than
assuming they match the topic above. See [setup guidance](benchmarks/kafka/README.md#setup).

Run from `benchmarks/kafka`:

```sh
BENCHMARK_ITERATIONS=20000 BENCHMARK_RUNS=30 BENCHMARK_BLOCKS=2 \
  BENCHMARK_SHOW_PREVIOUS=true BENCHMARK_MEASUREMENT_WINDOW=steady \
  BENCHMARK_PAIR_ORDER=previous-first BENCHMARK_COLORS=false BENCHMARK_CHARTS=false \
  pnpm benchmark

BENCHMARK_ITERATIONS=20000 BENCHMARK_RUNS=30 BENCHMARK_BLOCKS=2 \
  BENCHMARK_SHOW_PREVIOUS=true BENCHMARK_MEASUREMENT_WINDOW=steady \
  BENCHMARK_PAIR_ORDER=current-first BENCHMARK_COLORS=false BENCHMARK_CHARTS=false \
  pnpm benchmark
```

Use Node.js 24 and the pnpm version pinned in the root `package.json`.
The harness generates unique result paths by default. For explicit destinations,
set `BENCHMARK_RESULT_PATH` to a new path for each invocation; existing files are
rejected. The captured suites used distinct `consumer-a.json` and `consumer-b.json`
files. The [portable snapshot](packages/kafka-crab-js/docs/rfc/review/evidence/consumer-comparison-2026-09-12.json)
retains every measurement and block memory/GC, configurations, execution metadata,
crab runtime fingerprints, topic snapshots, and original JSON hashes.

For an equal-backoff control, add `BENCHMARK_FETCH_QUEUE_BACKOFF_MS=20` to both
commands and store the results separately. For single-mode investigations, use
`pnpm benchmark:serial:abba` or `pnpm benchmark:batch:abba`. The same measurement
implementation is used, but separate executions need not yield identical numbers.

## Concurrent traffic and memory under pressure

A separate workload generated new messages while public serial/batch consumers
processed them with natural GC. It validated **640,000 records / 12.736 GiB**,
including large headers and tombstones, across eight 120-second blocks. All data
checks and 2,258 Sync commits passed, with broker-confirmed zero final backlog.

Changing only `queued.min.messages` from 100,000 to 256 reduced the worst sampled
consumer RSS from **316.55 to 157.97 MiB serial** and **341.61 to 156.02 MiB batch**.
Normal p95 stayed around 60 ms. Worst backlog recovery increased from 84 to 833 ms
serial and 101 to 488 ms batch; p99 also increased. This is an optional memory
profile with a measured recovery tradeoff.

The load offered 500 msg/s, with 1,000 msg/s bursts and two ten-second reader stalls.
It measures behavior under that offered load, not maximum capacity. Each consumer
lived for roughly two minutes on macOS. It does not replace a prolonged Linux
soak, application-specific load tests, or the remaining failure matrix.

See the [complete workload report](packages/kafka-crab-js/docs/rfc/review/performance.md#concurrent-workload-with-natural-gc--2026-09-12)
and [reproduction guide](benchmarks/kafka/README.md#live-producer-and-consumer-workload).
The [release review](packages/kafka-crab-js/docs/rfc/review/README.md) tracks release
readiness independently of this consumer throughput comparison.
