# Crab JS documentation

Native performance for JavaScript applications, with focused packages backed by Rust.

## Package guides

| Package               | Purpose                                              | Documentation                                                      |
| --------------------- | ---------------------------------------------------- | ------------------------------------------------------------------ |
| `kafka-crab-js`       | Kafka producers, consumers, and serial/batch streams | [Get started](../../packages/kafka-crab-js/README.md)              |
| `kafka-crab-js-otel`  | Optional Kafka tracing and metrics                   | [OpenTelemetry guide](../../packages/kafka-crab-js-otel/README.md) |
| `pdf-crab-js`         | Structured PDF generation for Node.js and browsers   | [PDF 1.0 guide](PDF-Guide.md)                                      |
| `html-to-pdf-crab-js` | HTML/CSS to PDF for Node.js and browsers             | [Package guide](../../packages/html-to-pdf-crab-js/README.md)      |

For PDF 1.0 migration, see the [migration guide](../../packages/pdf-crab-js/MIGRATION.md).
The [WASM sample studio](../../examples/wasm-samples/README.md) runs browser examples locally.

## Kafka: native performance, JavaScript APIs

`kafka-crab-js` combines Rust, NAPI-RS, and librdkafka with typed producers,
consumers, Web Streams, and Node.js streams. The current development batch build
reached 1.66 million messages/s in the September 12, 2026 consumer comparison.
Read the [benchmark report](../../BENCHMARKS.md) for versions, competing APIs,
memory tradeoffs, and reproduction; this is a workload-specific result.

- [Installation and quick start](../../packages/kafka-crab-js/README.md#quick-start)
- [Choose a consumer API](../../packages/kafka-crab-js/README.md#choose-your-api)
- [Canonical API reference](../../packages/kafka-crab-js/docs/api.md)
- [Production recommendations](../../packages/kafka-crab-js/README.md#production-recommendations)
- [Runnable examples](../../examples/kafka/README.md)
- [Engineering decisions and release checks](../../packages/kafka-crab-js/docs/README.md)

Kafka API details live with the package so defaults, examples, and contracts have
one maintained reference. The old v3-era examples and December 2024 speed ratios
have been retired from this landing page; use documentation at the matching tag
when maintaining an older release.
