# Performance and memory

Assessment as of September 12, 2026: the current development build is faster than
registry version 4.1.3 in the latest steady-window consumer comparison. Batch
throughput leads the measured scenarios, while batch RSS remains higher than the
previous release. The byte-pressure investigation identified retained empty macOS
allocator regions as the dominant contributor to residual RSS in those processes.
A smaller librdkafka message prefetch threshold reduced peak RSS in the concurrent
workload, with slower backlog recovery. This is an optional workload profile.

These results support a release candidate, subject to the remaining
[release checks](README.md#remaining-acceptance-criteria). They do not complete
[RFC-0015](../proposed/0015-real-kafka-performance-validation/). Earlier serial
regressions remain historical observations without a demonstrated root cause.
The improved harness did not reproduce them; that does not invalidate them.

For the reader-facing comparison, see [Benchmarks](../../../../../BENCHMARKS.md).
The current [portable evidence](evidence/consumer-comparison-2026-09-12.json)
contains 960 measurements from two complete runs, with ABBA and BAAB ordering.
Historical sections below use `first-message` until the explicit switch to
`steady`. Their rates are not directly comparable across measurement windows.

## Historical reference evidence

Two original snapshots retain their historical contents:

- [Serial with both builds](evidence/serial-abba-built-20k.json): 240 runs.
- [Batch with both builds](evidence/batch-abba-built-20k.json): 120 runs.

They record the environment, commit and local files, explicit configuration, JS
entry points, loaded native binding hashes, topic metadata/samples, and individual
runs with memory/GC. Script names reflect the directory layout at capture time.

| Scenario                              | 4.1.3 (msg/s) | Current (msg/s) | Current/previous delta |
| ------------------------------------- | ------------: | --------------: | ---------------------: |
| Serial, no additional instrumentation |    614,612.55 |      810,832.31 |                +31.93% |
| Serial, CPU instrumentation           |    629,841.43 |      785,738.11 |                +24.75% |
| Batch, no additional instrumentation  |  1,721,232.63 |    1,710,085.81 |                 −0.65% |

Without additional instrumentation, the serial previous/current and
current/previous pairs produced +40.52% and +23.92%; batch produced +0.75% and
−2.01%. No statistical equivalence test was performed. Aggregate throughput is
total messages divided by total measured time.

Batch peak RSS was 229.52/229.86 MiB in current processes and 181.14/170.98 MiB in
previous processes. Retained RSS above baseline was 156.03/162.45 versus
91.50/101.69 MiB. Total GC across 60 runs per version was 13.19 ms current versus
15.62 ms previous. Retained RSS alone neither identifies live allocations nor
establishes a leak.

## Historical environment and method

- Node 24.20.0 arm64, Apple M4, Darwin 25.6.0; local Kafka.
- 30 runs of 20,000 messages per process, previous → current → current → previous.
- Serial instrumentation `off` and `cpu`; batch `off` only.
- Fetch 2,048 bytes/10 ms, batch 4,096/2 ms, serial prefetch 64/5 ms;
  memory sampling and settling at 100 ms, GC before every run.
- Both libraries loaded `dist/index.js`; `tsx` ran only the harness. Monitored
  sources, artifacts, and topic offsets remained stable within each execution.
- Topic `benchmarks`: high offsets 166,665, 333,335, and 0 for partitions 0, 1,
  and 2; all low offsets were 0. One partition was empty. Offset ranges do not
  certify the number of live records. Samples contained 32 messages from each
  nonempty partition, with 73/75-byte payloads, 11-byte keys, and one header.

Historical current binding: `59f86de79d9943b73efc295b221b24fef22bd64871486602ec9aa37ea98a0f22`.
Previous binding: `6cf38ecb025c7c2615297bd91a264a76cceefa53c79d1a24b4357c06f6fe7351`.

## Reproduction

Build the current package, then run from `benchmarks/kafka`, using fresh output paths:

```sh
TSX_TSCONFIG_PATH=./tsconfig.runtime.json BENCHMARK_ITERATIONS=20000 BENCHMARK_RUNS=30 \
  BENCHMARK_ABBA_RESULT_PATH=/tmp/serial-built.json pnpm benchmark:serial:abba
TSX_TSCONFIG_PATH=./tsconfig.runtime.json BENCHMARK_ITERATIONS=20000 BENCHMARK_RUNS=30 \
  BENCHMARK_ABBA_RESULT_PATH=/tmp/batch-built.json pnpm benchmark:batch:abba
```

These commands use today's default `steady` window. Set
`BENCHMARK_MEASUREMENT_WINDOW=first-message` to inspect the older window; changes
in instrumentation still prevent an exact reproduction of historical results.

The common comparator is `diagnostics/consumer-abba.mjs`.
`diagnostics/consumer-memory.mjs` investigates create/consume/cancel/disconnect/GC
cycles with lifecycle samples, WeakRefs, and optional vmmap captures. See the
[benchmark guide](../../../../../benchmarks/kafka/README.md) for options and limits.

Store new experiments in the temporary directory or `.profiles`. Promote only
decision-supporting evidence into this directory and explain its limits here,
instead of creating a report for every execution.

## Earlier results that constrain the conclusion

- Three user-provided complete runs lost 29.53%, 11.57%, and 10.76% in aggregate
  serial throughput, with lower medians too. Additional GC did not explain the
  loss. Serial RSS moved in different directions across runs. Local gains do not
  erase those observations.
- Early diagnostics used a different loop, then the shared `consumer.ts` harness.
  A tsconfig alias loaded current `js-src/index.ts` against previously published
  JS. The local gain persisted with both builds, but this did not locate the loss.
- Queue capacities of 256, 64, and 4 batches did not resolve residual RSS. In the
  repeated 256-batch control, vmmap fragmentation varied from 75.6 to 12.9 MiB
  while final RSS remained high. Those snapshots did not establish a stable cause.
- `malloc_zone_pressure_relief` released zero bytes. No allocator call or reduced
  production queue was adopted; the experimental helper was removed.
- WeakRefs reached zero and heap/external memory stabilized in memory tests.
  That alone did not exclude native retention; batch RSS remained open at this stage.
- Initial three-run benchmarks mixed runtime phases and were inadequate as a
  release gate. A timeout storm left 300 messages in flight, so it did not prove cleanup.

The host and brokers were shared. Topic sampling could warm caches; order reversal
was by process block. CPU/gap instrumentation adds overhead. No function-level
profile was captured during a losing run. Discarded experiments are not evidence
of conformance or guaranteed gains.

## Alignment of complete and isolated benchmarks

The default command was changed to load built packages without the TypeScript
alias. `BENCHMARK_BLOCKS=2` starts two processes per scenario and reverses the
current/previous pairs. `BENCHMARK_RUNS=30` means 30 runs per process, or 60 per
scenario, as reported in the output. One block supports short smoke runs.

The ABBA comparator now uses the main table's throughput calculation. All samples
contribute; memory takes per-process maxima and GC sums counts/durations while
preserving the largest pause. Exports retain raw blocks and artifacts and reject
inconsistent crab runtime/configuration fingerprints.

This historical check included 480 complete-suite runs and 240 isolated runs,
with 20,000 messages each. Loaded bindings, `dist/index.js` entries, and explicit
library configurations matched across modes. Aggregates were recalculated from
blocks.

| Current relative gain | Complete suite | Isolated |
| --------------------- | -------------: | -------: |
| Serial                |        +46.08% |  +45.61% |
| Batch                 |         +5.06% |   −6.68% |

Serial relative gains aligned, although absolute rates varied. The two current
isolated batch processes reached 1.50 and 1.81 million msg/s. A shared method does
not make separate executions numerically identical. At this stage only the
isolated comparator sampled topic metadata beforehand, potentially warming caches;
this difference was removed later.

Evidence: `/tmp/consumer-alignment.json`. Lint, typecheck, and a one-block smoke
passed. No production protection was removed or changed to obtain these numbers.

## Historical batch tuning: backoff and fetch

`BENCHMARK_FETCH_QUEUE_BACKOFF_MS` applies to both crab versions and appears in
output/JSON. When omitted, version defaults apply: 1,000 ms previous and 20 ms
current. JSON records `null`, not a dump of native defaults. KafkaJS and Platformatic
do not receive this librdkafka-specific option.

Four sequential ABBA experiments each retained all 120 runs of 20,000 messages,
with batch size 4,096 and timeout 2 ms:

| Fetch                   | Previous/current backoff | Previous msg/s | Current msg/s |   Delta |
| ----------------------- | ------------------------ | -------------: | ------------: | ------: |
| 2 KiB                   | 20/20 ms                 |      1,740,653 |     1,783,656 |  +2.47% |
| 1 MiB                   | 20/20 ms                 |      1,460,290 |     2,080,498 | +42.47% |
| 2 KiB, repeated control | Defaults: 1,000/20 ms    |      1,717,180 |     1,874,824 |  +9.18% |
| 1 MiB, repeated         | 20/20 ms                 |      1,881,064 |     2,014,934 |  +7.12% |

The previous version's first run in the first 1 MiB experiment took 185.59 ms,
inflating the aggregate delta. Repeated pairs yielded +8.18% and +6.06%. Current
throughput reached 2.01–2.08 million msg/s with 1 MiB versus 1.78–1.87 million with
2 KiB. This supports testing larger fetches, not replacing the 2 KiB control or
claiming a stable causal improvement: profiles ran in fixed order on a shared host.
Current peak RSS ranged from 236.52 to 246.53 MiB, without a memory solution.

The default-backoff control also gained despite the earlier loss, so equalizing
backoff does not explain the variation by itself. Fetch limits are bytes and can
be exceeded by the first record batch to allow progress; API batch size is messages.

All 480 runs, parameters, stable artifacts, and offsets were checked in
`/tmp/consumer-tuning-comparison.json`. Lint, typecheck, and diff checks passed.
Only the benchmark and documentation changed during this experiment. Deferred
collector allocation and cached queue byte counts were identified as opportunities
here and implemented in the later section below.

## Historical isolated serial matrix: fetch × backoff

A subsequent user-provided full run with 2 KiB fetches and version defaults lost
21.77% in aggregate serial throughput and 26.13% in the median. Four serial ABBA
combinations then used equal backoff in both versions, 30 runs per process, and
20,000 messages per run: 480 measurements, with every sample retained and no extra timing.

| Fetch | Backoff in both versions | Previous msg/s | Current msg/s | Aggregate delta | Median delta |
| ----- | -----------------------: | -------------: | ------------: | --------------: | -----------: |
| 2 KiB |                    20 ms |        621,474 |       814,673 |         +31.09% |      +27.53% |
| 2 KiB |                 1,000 ms |        637,638 |       870,956 |         +36.59% |      +35.67% |
| 1 MiB |                    20 ms |        538,068 |       829,868 |         +54.23% |      +43.33% |
| 1 MiB |                 1,000 ms |        491,166 |       835,103 |         +70.02% |      +52.46% |

Current won all eight process pairs. Previous had 1 MiB runs lasting up to 197.56
and 205.63 ms, amplifying aggregate gains; median gains also remained. Increasing
fetch did not improve serial uniformly, and these results did not justify changing
the 20 ms default.

JS/native artifacts and Node were stable per version; only fetch/backoff varied
in recorded configuration. ABBA order, 60 samples per version/case, aggregates,
offsets, and code stability were checked. Evidence:
`/tmp/consumer-serial-matrix-comparison.json`. Implementation was unchanged.

This matrix did not reproduce the full-suite loss or establish its cause. It ran
sequentially on the shared host, sampled the topic first, and did not include the
case where each version uses a different default backoff. Capturing a losing run
with parameters, artifacts, CPU/wait profiling, and an uninstrumented control
remains necessary to attribute that historical regression.

## Measurement review and improvement opportunities

The path `/tmp/consumer-full-defaults-after-serial-matrix.json` had been overwritten
with a different execution. Its September 11, 2026, 22:32:08.435 UTC contents and
SHA-256 were preserved in `/tmp/consumer-regression-analysis-zuprzki1.json` with
block analysis. These numbers belong to that copy, not the previously reported
+32.19% run.

| Serial in the preserved copy    | Previous | Current |
| ------------------------------- | -------: | ------: |
| Aggregate throughput (msg/s)    |  712,616 | 586,738 |
| Median (msg/s)                  |  784,780 | 606,034 |
| Block 1 (msg/s)                 |  706,996 | 580,917 |
| Block 2 (msg/s)                 |  718,327 | 592,676 |
| Last 10 runs of block 1 (msg/s) |  807,066 | 537,391 |
| Last 10 runs of block 2 (msg/s) |  765,731 | 592,499 |

The loss was 17.66% aggregate and 22.78% median, extending beyond initial runs.
Node, recorded arguments, JS entries, and native hashes matched the winning
`/tmp/consumer-repeat-comparison.json`. Current spent 361.27 ms more in the
measured window with only 4.96 ms of extra observed GC. GC cannot account for the
whole difference; no losing-run CPU profile was available.

The review identified five priorities:

1. **Define the throughput window.** Previously, the timer started after the
   first `reader.read()`, although that delivery's messages entered the numerator.
   A full 4,096-message batch is 20.48% of a 20,000-message run. That fraction is
   not an estimate of artificial speedup and does not explain the serial loss.
   Measure after explicit warmup, exclude the complete boundary delivery, and
   report startup separately.
2. **Preserve comparable evidence.** Refuse existing output paths, fingerprint
   the harness/environment, timestamp blocks, alternate ABBA/BAAB, and apply the
   same topic preparation. Sixty runs from two processes are not 60 independent
   environments.
3. **Profile a losing serial run.** Collect process/main-thread CPU, then gaps
   and function profiles with an uninstrumented control. Compact conversion and
   Web Stream promises are measurement candidates, not demonstrated causes.
4. **Observe memory accurately.** A 100 ms sampler can miss batch windows of
   roughly 10–12 ms. Separate lifecycle/processing/post-GC samples, include the
   final sample, and retain the OS process RSS high-water mark. A faster sampler
   changes overhead and needs its own control.
5. **Optimize while preserving limits.** Carry the reserved byte count with each
   queued batch to avoid a second scan. Allocate collector vectors only when the
   first message arrives. Preserve drain, cancellation, headers, tombstones, and
   the 32 MiB queue budget.

The native byte budget covers accounted queued batches, not total RSS. librdkafka
queues, the collecting batch, JS objects, and allocator memory are outside it.
There was no basis for removing this protection or choosing defaults solely to
reproduce the best observed number.

## Implemented improvements and validation

The default is now `BENCHMARK_MEASUREMENT_WINDOW=steady`: warm up the same consumer
for at least `BENCHMARK_WARMUP_MESSAGES` (default: iterations), rounded to whole
deliveries, then measure the next `BENCHMARK_ITERATIONS`. The delivery crossing
the warmup boundary is excluded from the measured numerator. Every run remains
in the aggregate. `first-message` keeps the old window for controls, with different
instrumentation from the historical harness.

Each run records first-delivery latency from scenario start (including setup and
imports), first-delivery size, actual warmup, received messages, and delivery-size
range. The dataset must cover warmup, measurement, and rounding. Setup adds default
headroom; the benchmark does not modify existing datasets.

Complete and ABBA runs use the same optional metadata/sample preparation. Set
`BENCHMARK_PAIR_ORDER` for BAAB. Results require a fresh path, automatically generated
in the temporary directory if omitted, with per-block checkpoints and `failed`
status on error. Exports include harness hashes, selected environment, timestamps,
and host load. Validation confirmed that an existing file is rejected without
changing its bytes and that a timeout result is preserved.

Memory maxima include the final post-GC read. JSON separates lifecycle snapshots,
processing-window samples, and OS peak RSS. Missing processing samples appear as
`null`. Optional timing captures process/main-thread CPU, delivery gaps, and time
outside main-thread CPU; the latter is not specifically Kafka I/O time.

Rust regular/compact queues now carry `(batch, bytes)` and release the original
reservation without a second scan. Collector vectors allocate on the first
message, avoiding reservations on empty timeouts. The 256-batch capacity, 32 MiB
budget, oversized exception, header/tombstone handling, cancellation, drain,
and 20 ms default backoff remain in place.

Local validation after the build passed six measurement-window tests, 55 package
unit tests, 18 native harness tests, and 12 real-Kafka lifecycle tests (none skipped).
Both oversized diagnostics delivered 80 × 512 KiB messages in one 40 MiB batch,
with intact offsets, payloads, and headers. Typecheck, lint, cargo fmt/clippy, and
diff checks passed. All eight scenarios passed a `steady` smoke; the four crab
scenarios were also checked in `first-message`.

The new window covered 240 pre-optimization runs, 480 complete post-build runs,
and 240 serial BAAB runs (off + gaps), each with 20,000 measured messages plus
warmup. Evidence: `/tmp/consumer-window-validation.json`.

| Complete suite after optimization | Previous msg/s | Current msg/s | Aggregate delta | Median delta |
| --------------------------------- | -------------: | ------------: | --------------: | -----------: |
| Serial                            |        740,357 |       908,848 |         +22.76% |      +25.69% |
| Batch                             |      1,537,957 |     1,633,100 |          +6.19% |      +11.81% |

Current before/after varied −1.82% serial and +3.59% batch. The previous-version
control also varied, and the initial run had only four crab scenarios. This does
not establish a stable causal gain from those optimizations. Current batch sampled
peak RSS was 267.58 MiB versus 207.36 MiB previous; RSS remained unresolved then.

Serial BAAB gained 37.53% without timing and 22.40% with gaps. Instrumented process
CPU was 2,746.57 ms current versus 4,261.09 ms previous; main-thread CPU was
1,315.71 versus 1,547.69 ms. There were 19,999 gaps per run. The regression did
not recur, so this did not establish its historical cause.

## RSS investigation under byte pressure — 2026-09-12

`js-tests/diagnostics/byte-pressure.mjs` now validates every byte using one reusable
192 KiB buffer included in the baseline. Previously, comparison buffers added
481.5 MiB of cumulative allocations per cycle. Collection samples now run after
the cycle function returns, releases its reader, disconnects its consumer, and
removes strong references to both. Multiple GCs and a two-second wait accompany
WeakRefs and `vmmap -summary`/`heap -s` captures. JS reachability and native
allocations are measured separately rather than inferred from RSS.

Logs go directly to disk. The first extended attempt stopped after 16 cycles
because the coordinator's capture buffer reached 64 MiB; completed cycles had no
content failure. That attempt is incomplete. Direct logging was validated in both
modes, including preserved logs/errors for deliberately invalid configuration.

In the corrected extended run, each process repeatedly consumed the same 3,072
messages containing 481.5 MiB of payloads/headers, recreating its consumer each
cycle. The reader waited 250 ms between batches of 32, with five extra one-second
pauses. Configuration: `queued.max.messages.kbytes=65536`,
`queued.min.messages=100000`, `fetch.max.bytes=8388608`,
`max.partition.fetch.bytes=1048576`, and `fetch.queue.backoff.ms=20`.
The two modes ran sequentially for at least 15 minutes each on Node 24.20.0,
macOS 26.6.2/arm64, using the release-review native binding. The 16 GiB host was
using memory compression; captures therefore include resident, compressed, and
empty regions.

| Mode    | Cycles |  Duration | Data consumed |   Peak RSS |  Final RSS | Native queue peak | Blocks/resumes |
| ------- | -----: | --------: | ------------: | ---------: | ---------: | ----------------: | -------------: |
| Batch   |     28 | 15.52 min |     13.17 GiB | 568.92 MiB | 524.25 MiB |         30.11 MiB |  2,464 / 2,464 |
| Compact |     28 | 15.52 min |     13.17 GiB | 553.06 MiB | 500.25 MiB |         30.10 MiB |  2,464 / 2,464 |

Batch validated all 86,016 records, with no reachable consumer after collection.
Native `heap` allocations went from 3.93 MiB after the first cycle to 3.74 MiB
at the end. Final vmmap showed 448.6 MiB resident and 55.5 MiB in `SWAPPED` for
`MALLOC_SMALL (empty)`. Retained empty allocator regions were the dominant
contributor to residual RSS in this process, rather than hundreds of MiB of live
messages. The malloc zone's virtual size grew from 510.3 to 546.8 MiB, so this
neither establishes a fixed RSS ceiling nor immediate return of memory to the OS.

Compact also validated 86,016 records with zero reachable consumers. Native
allocations fell from 3.91 to 3.72 MiB. Final `MALLOC_SMALL (empty)` contained
429.4 MiB resident and 82.7 MiB in `SWAPPED`; virtual malloc zone size grew from
506.3 to 550.8 MiB. Both modes ended with 2.92 MiB of external memory after
collection. The diagnostic's retained sample history increases JS heap with cycle
count but holds neither messages nor consumers.

These local results are consistent with empty-region allocator retention, without
proportional growth in live allocations while replaying 26.33 GiB. They do not
generalize to Linux, different message/partition sizes, natural GC, or applications
that retain messages. Library defaults and the native budget were unchanged.

Raw evidence:
`/var/folders/0s/j_q0xhmj0cb7p_z7hyl7l9h00000gp/T/crab-byte-pressure-93d64x/`.
It contains configuration/hashes in `run.json`, checkpoints in `results.json`, logs,
and native captures. The batch log exceeded 100 MiB. Preserved-failure control:
`/var/folders/0s/j_q0xhmj0cb7p_z7hyl7l9h00000gp/T/crab-byte-pressure-hF2mv5/`.

### Configuration and payload-profile comparison

A sequential matrix ran 22 scenarios in separate processes: 11 slow-reader cases
and 11 without pauses (three cycles each), validating another 135,168 records.
Three profiles retained 481.5 MiB and equal total bytes per record but changed the
payload/header split: mixed 192.75/288.75 MiB, payload 478.5/3 MiB, and headers
3/478.5 MiB. Each profile reused its own topic across configurations. All used
8 MiB fetch, 1 MiB per-partition fetch, batches of 32, and 20 ms backoff.

Peak process RSS under pressure, MiB; one slow cycle per configuration:

| Profile | 64 MiB / 100,000 messages | 16 MiB / 100,000 messages | 8 MiB / 100,000 messages | 64 MiB / 256 messages |
| ------- | ------------------------: | ------------------------: | -----------------------: | --------------------: |
| Mixed   |                    471.75 |                    280.00 |                   248.59 |                257.33 |
| Payload |                    331.28 |                    220.97 |                   225.56 |          Not measured |
| Headers |                    652.30 |                    652.56 |                   655.86 |                295.91 |

Reducing the byte setting from 64 to 8 MiB lowered mixed-profile peak RSS by 47.3%.
It did not help the header profile: all 3 MiB of payload fit below even the smallest
setting because librdkafka 2.12.1 fetch queue accounting uses payload length,
excluding headers/keys. See the source analysis in
[RFC-0016](../implemented/0016-byte-based-backpressure/README.md#scope).
With `queued.min.messages=256`, header-profile peak RSS fell 54.6%, from 652.30
to 295.91 MiB. Mixed-profile peak RSS reached 257.33 MiB with that same setting.

All 11 slow cases blocked/resumed the wrapper's native budget 88 times, peaking at
30.11 MiB. The header profile's 3 MiB of payload across the whole dataset confirms
that headers alone can cause wrapper queue pressure. All 3,072 records per cycle
preserved payloads, headers, tombstones, and offsets.

The unthrottled control includes byte validation, trace logging, and first-read
wait; its rates are not comparable to the small-message `steady` benchmark.
Below are medians of three cycle rates and medians of their `reader.read()` wait p95s:

| Profile / configuration            |    msg/s | Read wait p95 |
| ---------------------------------- | -------: | ------------: |
| Mixed / 64 MiB, 100,000 messages   | 2,434.54 |     13.583 ms |
| Mixed / 8 MiB, 100,000 messages    | 2,406.26 |     15.459 ms |
| Mixed / 16 MiB, 100,000 messages   | 2,450.08 |     13.922 ms |
| Mixed / 64 MiB, 256 messages       | 2,459.20 |     13.862 ms |
| Headers / 64 MiB, 100,000 messages | 2,391.96 |     14.444 ms |
| Headers / 64 MiB, 256 messages     | 2,373.67 |     14.759 ms |

The message-threshold adjustment changed median header throughput by −0.76%.
The three runs ranged from 1,591.6 to 2,483.6 msg/s, so this does not demonstrate
no regression generally. Unthrottled header RSS increased from 161.89 to 184.20 MiB;
the observed benefit is under slow-reader accumulation. The library default was
not changed on the basis of this matrix.

For large messages/headers and slow readers, this measured optional profile can
be passed as `createConsumer({ configuration: ... })`:

```js
const configuration = {
  'queued.max.messages.kbytes': 65536,
  'queued.min.messages': 256,
  'fetch.max.bytes': 8 * 1024 * 1024,
  'max.partition.fetch.bytes': 1024 * 1024,
  'fetch.queue.backoff.ms': 20,
}
```

These are prefetch controls, not hard RSS ceilings. Response batches can exceed
thresholds to make progress. This matrix used one partition per process; validate
the profile against the target workload, especially small-message throughput.
For payload-heavy traffic, 16 MiB is another measured candidate; reducing further
to 8 MiB did not consistently help.

Reproduce the header profile and message-threshold control from the Kafka package:

```sh
PRESSURE_PROFILE=headers PRESSURE_QUEUE_MESSAGES=256 \
  pnpm exec node js-tests/diagnostics/byte-pressure.mjs batch sustained
```

Set `PRESSURE_MANIFEST` to an evidence `input.json` to reuse exactly that dataset.
For the unthrottled control, add `PRESSURE_INITIAL_PAUSE_MS=0`,
`PRESSURE_READ_DELAY_MS=0`, `PRESSURE_PAUSE_MS=0`, `PRESSURE_REQUIRE_BLOCKING=0`,
and `PRESSURE_CYCLES=3`. Options and the 15-minute command are in the
[diagnostic guide](../../../js-tests/diagnostics/README.md).

Full matrix, manifests, and per-process logs:
`/var/folders/0s/j_q0xhmj0cb7p_z7hyl7l9h00000gp/T/crab-pressure-matrix-jd4ij2r5/results.json`.
Consolidated summary: `/tmp/crab-memory-investigation.json`.

## Concurrent workload with natural GC — 2026-09-12

A producer published new messages while a consumer processed them, in separate
processes on the three-broker local Kafka cluster. This is synthetic traffic on
real infrastructure, not the production application's traffic or handler. The
consumer used public `createWebStreamConsumer`, error-level logging, and no forced
GC. Its native binding was the same as the preceding investigation:
`65d1cdd282a6f810164e8c0b02806615a767f25ef47b53d7178fcfbc946054c2`.
Script, loaded JS, and binding hashes remained unchanged through the end.
Environment: Node 24.20.0, macOS/arm64, Apple M4, 16 GiB RAM.

Both serial and public batch modes ran `queued.min.messages` in the order
100,000 → 256 → 256 → 100,000, using fresh processes/topics. Eight 120-second
blocks produced **640,000 records, 12.736 GiB of application data, and 16 minutes
of publishing**, plus setup/shutdown. Volume includes payloads, header values,
and keys, excluding protocol overhead. Each topic had three partitions with
leaders on all three brokers, replication factor 1, and one-hour retention.
The producer used idempotence, `acks=all`, and no compression.

The offered rate was 500 msg/s with 1,000 msg/s bursts at seconds 20–40 and 65–85.
The producer sent batches of 100/200 every 200 ms, awaiting acknowledgment of each
send. Every block published its 80,000 records in 119.901–119.910 seconds; maximum
post-warmup scheduling delay was 29.007 ms. Average offered load was 666.67 msg/s,
not a maximum-capacity measurement. An initial 50 ms scheduling calibration could
not sustain its rate because acknowledged `send()` took roughly 105 ms; it was
excluded from the comparison. The final 200 ms smoke validated another 32,000 records.

The repeating profile was 70% 2 KiB payloads/128 B headers, 20% 32 KiB/512 B,
5% 1 KiB/128 KiB, and 5% tombstones with 128 KiB headers, plus a key and a 20-byte
sequence/time header. The consumer checked every payload/data-header byte, key,
tombstone, sequence, per-partition order, and offset using a reusable buffer.
It committed processed offsets synchronously about once per second.

At seconds 30–40 and 75–85 the reader stopped draining, simulating unavailable
downstream processing. Production and prefetch continued. Sampled backlog reached
roughly 10,000 records per interruption, estimated from acknowledgment/consumption
counters sampled at 100 ms. Final backlog was verified directly against broker
offsets and committed positions.

Configurations differed only in `queued.min.messages`. Both used 64 MiB fetch
queue, 8 MiB fetch, 1 MiB per-partition fetch, 20 ms backoff, and 10 ms fetch wait.
Batch size and serial prefetch were 64 with 5 ms timeouts. These were explicit
workload parameters; library defaults remained unchanged.

Ranges cover two blocks per configuration. Recovery is the maximum time after
reader resumption until all partitions have processed the scheduled end time of
the interruption:

| Mode   | queued.min.messages | Peak RSS (MiB) | Normal p95 (ms) | Normal p99 (ms) | Maximum recovery |
| ------ | ------------------: | -------------: | --------------: | --------------: | ---------------: |
| Serial |             100,000 |  311.88–316.55 |           60.03 |     72.06–74.05 |            84 ms |
| Serial |                 256 |  155.64–157.97 |           60.03 |     78.02–79.04 |           833 ms |
| Batch  |             100,000 |  335.63–341.61 |           60.03 |     70.02–71.04 |           101 ms |
| Batch  |                 256 |  151.09–156.02 |     59.01–61.02 |     74.05–84.03 |           488 ms |

Normal p95/p99 measure producer creation to consumer validation, excluding the
first 15 seconds, interruptions, and ten seconds of recovery after each pause:
42,500 observations per block. Global metrics include all 80,000 records and the
roughly ten-second imposed waits. Percentiles from different blocks are not pooled
as if they were a single percentile. Table RSS is the sampled per-process peak;
OS high-water marks in JSON differed by at most 0.03125 MiB. Separately measured
producers peaked at 124.03–148.48 MiB.

All checks passed: **640,000 intact records, 2,258 Sync commits, broker-confirmed
final positions in all 24 partitions across eight topics, zero final backlog,
and no pending sends**. None of the 16 workers logged errors or forced GC. The
KafkaJS administrative helper emitted `TimeoutNegativeWarning` while connecting;
its preserved warning did not prevent metadata, high-watermark, or commit checks.

Comparing the larger peak of each pair, `queued.min.messages=256` reduced RSS by
**50.1% serial and 54.3% batch**. Normal p95 stayed close, but p99 increased and
backlog recovery slowed. This supports an optional profile for this workload with
large messages/headers and reader stalls, not a universal default.

This workload does not validate small-message saturation, real application traffic,
Linux, long-lived consumers, faults, rebalances, or replication. Each consumer lived
roughly two minutes; eight blocks are not a 16-minute soak in one consumer. RSS
after two seconds of natural-GC idle does not measure live allocations or prove
absence of a leak. RFC-0015 remains partial, including the missing comparison
against the previous version under its failure matrix.

Reproduce from `benchmarks/kafka`:

```sh
LIVE_SECONDS=120 LIVE_RATE=500 LIVE_BURST_RATE=1000 \
  pnpm benchmark:consumer:live
```

The [benchmark guide](../../../../../benchmarks/kafka/README.md#live-producer-and-consumer-workload)
documents parameters and failure criteria. Consolidated evidence:
`/tmp/crab-live-evidence.json`. Raw results, configuration, fingerprints, metadata,
and worker logs:
`/var/folders/0s/j_q0xhmj0cb7p_z7hyl7l9h00000gp/T/crab-consumer-live-6lPAin/`.
Complete log: `/tmp/crab-live-full.log`. Final smoke:
`/var/folders/0s/j_q0xhmj0cb7p_z7hyl7l9h00000gp/T/crab-consumer-live-BaYjKR/`.

## Documentation comparison — 2026-09-12

Two new complete suites ran with the current release-review binding, `steady`
measurement, 20,000 warmup messages, 20,000 measured messages, 30 runs per process,
and two processes per scenario per suite. The first used previous-first (ABBA)
and the second current-first (BAAB). All 960 measurements were retained: 120 per
scenario from four processes. No production code changed for this comparison.

| Mode   | Registry 4.1.3 msg/s | Development snapshot msg/s | Aggregate delta |
| ------ | -------------------: | -------------------------: | --------------: |
| Serial |              744,174 |                    915,664 |         +23.04% |
| Batch  |            1,544,012 |                  1,663,382 |          +7.73% |

Current serial/batch sampled peak RSS was 178.94/266.27 MiB; previous was
203.39/212.02 MiB. These are lifecycle maxima across processes, not exact
processing-window peaks. No samples were trimmed. ABBA/BAAB reduces ordering bias
between the two crab versions; competing-library scenarios still run in fixed
positions on one shared host. Both crab manifests still say 4.1.3, so native and
JS hashes identify the development snapshot separately from the registry release.

The [portable evidence](evidence/consumer-comparison-2026-09-12.json) retains all
measurements, process memory/GC, execution metadata, crab runtime fingerprints,
configuration, topic metadata/samples, and original capture hashes. Complete
competitor results and reproduction commands are in
[BENCHMARKS.md](../../../../../BENCHMARKS.md). This comparison supplements the live
workload; it does not measure end-to-end application latency or complete RFC-0015.
