# Manual diagnostics

These probes investigate open RFC acceptance criteria. They are outside the integration
test glob and are not release gates. Their presence is not evidence of conformance.
The package publication allowlist excludes `js-tests`, docs and benchmarks.

## Async commit error (V03)

From `packages/kafka-crab-js`, with a built binding and local Kafka:

```sh
KAFKA_LOG_LEVEL=error node --test js-tests/diagnostics/async-commit-error.mjs
```

The probe commits to a nonexistent partition and logs observed commit callbacks. It
passes after F06: `UnknownTopicOrPartition` arrives with topic, partition and offset.
The release integration suite now covers the broker rejection with a Sync control,
success callbacks without polling, concurrent commits and disconnect. The diagnostic
remains a manual reproduction of the group-subscription path.

## Oversized batch (V01)

```sh
rtk proxy node js-tests/diagnostics/byte-pressure.mjs batch
rtk proxy node js-tests/diagnostics/byte-pressure.mjs compact
```

Each invocation creates a unique topic and produces 80 messages of 512 KiB. It verifies
delivery of one 40 MiB batch, including offsets and headers. This covers the oversized
exception, not sustained queue pressure or bounded process RSS.

## Sustained mixed payload/header pressure (V01)

```sh
pnpm exec node js-tests/diagnostics/byte-pressure.mjs all sustained
```

Produces 3,072 messages (481.5 MiB of payload/header data) in a separate process,
then consumes the same dataset in isolated regular and compact processes. Each
cycle uses slow reads and five longer pauses. Payloads, headers, tombstones and
every offset are verified, reusing one 192 KiB scratch buffer included in the
baseline instead of allocating another large buffer for each validation.

The reader is cancelled/unlocked and the consumer disconnected inside a separate
cycle function. Only after that function returns are GC/settling samples collected.
WeakRefs track JavaScript reachability; they do not prove native deallocation.
Repeated cycles reuse the same dataset and process, but create new consumers.

Rust trace events expose byte-budget reserves, blocking and recovery without a
public API; these traces are disabled at normal log levels. The command requires
repeated blocking/resumption and reserves at or below 32 MiB. RSS windows are
descriptive: the former three-window spread assertion was removed because passing
it does not demonstrate stabilization. Process RSS is not bounded by the wire budget.

Logs, input metadata, source/binding hashes, settings and results are saved in a
unique temporary directory printed by the command. Throughput includes validation
and configured pauses; it is not the consumer benchmark. Read-wait percentiles are
reported separately. Native profiling pauses also affect elapsed times.
Child stdout/stderr go directly to files, so long trace runs do not hit an
in-memory capture limit. Completed modes checkpoint `results.json`; process errors
preserve the log and a `failure.json` entry.

To reuse a manifest and measure at least 15 minutes **per mode**, with native
allocation summaries on macOS:

```sh
PRESSURE_MANIFEST=/path/to/input.json PRESSURE_MIN_SECONDS=900 \
  PRESSURE_NATIVE_MEMORY=1 pnpm exec node js-tests/diagnostics/byte-pressure.mjs all sustained
```

`vmmap -summary` and `heap -s` are captured at baseline, a paused first cycle,
after collection of the first cycle, and final collection. Failures are recorded
in the JSON. Profiling changes the workload: compare timing using runs without it.

| Variable                         |   Default | Meaning                                                                                                       |
| -------------------------------- | --------: | ------------------------------------------------------------------------------------------------------------- |
| `PRESSURE_PROFILE`               |   `mixed` | Seed `mixed`, `payload` or `headers`; total data bytes per record stay equal. Ignored when reusing a manifest |
| `PRESSURE_MANIFEST`              |     unset | Reuse an existing dataset, copied into the evidence directory                                                 |
| `PRESSURE_CYCLES`                |       `1` | Minimum cycles per isolated process                                                                           |
| `PRESSURE_MIN_SECONDS`           |       `0` | Minimum duration; finish the current cycle before stopping                                                    |
| `PRESSURE_QUEUE_KIB`             |   `65536` | librdkafka queue accounting, independent of the wrapper's 32 MiB budget                                       |
| `PRESSURE_QUEUE_MESSAGES`        |  `100000` | `queued.min.messages` fetch threshold; a separate control for header-heavy records                            |
| `PRESSURE_FETCH_BYTES`           | `8388608` | Explicit `fetch.max.bytes`                                                                                    |
| `PRESSURE_PARTITION_FETCH_BYTES` | `1048576` | Explicit `max.partition.fetch.bytes`                                                                          |
| `PRESSURE_INITIAL_PAUSE_MS`      |    `1500` | Pause after opening the stream                                                                                |
| `PRESSURE_READ_DELAY_MS`         |     `250` | Delay between batch reads                                                                                     |
| `PRESSURE_PAUSE_MS`              |    `1000` | Additional pause every 512 records                                                                            |
| `PRESSURE_SETTLE_MS`             |    `2000` | Wait between GC passes outside the consumer's scope                                                           |
| `PRESSURE_REQUIRE_BLOCKING`      |       `1` | Set `0` for unthrottled controls, where actual queue blocking is not required                                 |
| `PRESSURE_NATIVE_MEMORY`         |       `0` | Capture macOS native summaries in the parent-created evidence directory                                       |

For an unthrottled control, set the three pause/delay variables to `0` and
`PRESSURE_REQUIRE_BLOCKING=0`. Keep the same manifest, fetch configuration and
validation. This is a diagnostic comparison, not a replacement for the steady
benchmark, and it does not establish a global RSS limit.

Memory/vmmap and serial/batch comparisons live in the private
[benchmark workspace](../../../../benchmarks/kafka/README.md).
The 15-minute-per-mode soak and queue/profile matrix are recorded in the
[RFC memory investigation](../../docs/rfc/review/performance.md#rss-investigation-under-byte-pressure--2026-09-12).
