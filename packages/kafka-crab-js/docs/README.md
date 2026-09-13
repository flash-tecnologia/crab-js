# kafka-crab-js documentation

Build Kafka services with a JavaScript API backed by Rust and librdkafka.
Start with the [package guide](../README.md) for installation and working examples.

## Using the library

| Guide                                                                 | What you will find                                                              |
| --------------------------------------------------------------------- | ------------------------------------------------------------------------------- |
| [Quick start](../README.md#quick-start)                               | Publish with confirmation and consume with commits after successful processing. |
| [Choose your API](../README.md#choose-your-api)                       | Serial, batch, Node streams, and direct receiving.                              |
| [API reference](api.md)                                               | Configuration, delivery results, commits, assignment, streams, and shutdown.    |
| [Production recommendations](../README.md#production-recommendations) | Memory profiles, backpressure, security, and application delivery policy.       |
| [Benchmarks](../../../BENCHMARKS.md)                                  | Current comparison with KafkaJS and Platformatic, methodology, and evidence.    |
| [Examples](../../../examples/kafka/README.md)                         | Runnable producers, consumers, retries, tracing, and metrics.                   |
| [OpenTelemetry](../../kafka-crab-js-otel/README.md)                   | Optional instrumentation and SDK integration.                                   |

## Engineering and release validation

The [RFC index](rfc/README.md) records numbered engineering decisions, implementation
status, acceptance criteria, and evidence. Implemented means present in the current
source tree; it does not imply every validation criterion is closed or the change
has been published.

- [Conformance review](rfc/review/README.md): implemented fixes and remaining release checks.
- [Functional validation](rfc/review/validation.md): commands, results, regressions, and scope.
- [Performance and memory](rfc/review/performance.md): experiment history, byte pressure, and concurrent traffic.
- [Benchmark harness](../../../benchmarks/kafka/README.md): reproduction and diagnostic options.

Documentation follows the current source. Use the corresponding release tag when
working with an installed version. Historical evidence retains its original artifact
names and measurements; current comparisons explicitly identify the tested build.
