# Crab JS

Native Node.js packages backed by Rust and NAPI-RS.

Crab JS is a monorepo for focused native packages that keep JavaScript APIs small while moving heavy work into Rust.
The root README is intentionally a high-level map. Package APIs, examples, benchmarks, and operational guidance live
next to the project that owns them.

[![kafka-crab-js npm](https://img.shields.io/npm/v/kafka-crab-js.svg)](https://www.npmjs.com/package/kafka-crab-js)
[![kafka-crab-js-otel npm](https://img.shields.io/npm/v/kafka-crab-js-otel)](https://www.npmjs.com/package/kafka-crab-js-otel)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Projects

### Published Packages

| Package                                               | Purpose                                                                                      | Documentation                                      |
| ----------------------------------------------------- | -------------------------------------------------------------------------------------------- | -------------------------------------------------- |
| [kafka-crab-js](./packages/kafka-crab-js)             | Native Kafka client with producer, consumer, batch, Node stream, and Web APIs.               | [README](./packages/kafka-crab-js/README.md)       |
| [pdf-crab-js](./packages/pdf-crab-js)                 | Fast structured PDF generation built on Rust and pdf-writer, with native and WASM builds.    | [README](./packages/pdf-crab-js/README.md)         |
| [html-to-pdf-crab-js](./packages/html-to-pdf-crab-js) | Chromium-free HTML-to-PDF conversion backed by a Rust renderer, with native and WASM builds. | [README](./packages/html-to-pdf-crab-js/README.md) |
| [kafka-crab-js-otel](./packages/kafka-crab-js-otel)   | Optional OpenTelemetry instrumentation for `kafka-crab-js` diagnostics channels.             | [README](./packages/kafka-crab-js-otel/README.md)  |

### Examples

| Example Package                                                | Purpose                                                                 |
| -------------------------------------------------------------- | ----------------------------------------------------------------------- |
| [kafka-examples](./examples/kafka)                             | Producer, consumer, stream, retry, OpenTelemetry, and Grafana examples. |
| [pdf-crab-js-examples](./examples/pdf-crab-js)                 | Node and browser WASM examples for low-level PDF generation.            |
| [html-to-pdf-crab-js-examples](./examples/html-to-pdf-crab-js) | Node and browser WASM examples for HTML-to-PDF rendering.               |

### Benchmarks

| Benchmark Package                     | Purpose                                                                                           |
| ------------------------------------- | ------------------------------------------------------------------------------------------------- |
| [kafka-benchmark](./benchmarks/kafka) | Isolated-process consumer benchmark with memory, GC, throughput charts, and V8 profiling scripts. |
| [pdf-benchmark](./benchmarks/pdf)     | PDF generation benchmark comparing `pdf-crab-js`, `html-to-pdf-crab-js`, and Gotenberg.           |
| [BENCHMARKS.md](./BENCHMARKS.md)      | Latest captured Kafka benchmark snapshot and notes.                                               |

## Package Boundaries

- `kafka-crab-js` owns Kafka producer/consumer APIs and native librdkafka integration.
- `kafka-crab-js-otel` owns OpenTelemetry tracing and metrics. OTEL is intentionally outside the core Kafka package.
- `pdf-crab-js` owns fast explicit PDF construction from structured page and drawing-element inputs.
- `html-to-pdf-crab-js` owns easy HTML/CSS rendering to PDF without a Chromium service. It carries the renderer
  dependency separately from `pdf-crab-js`.
- Browser WASM usage for PDF packages is demonstrated in each example package and requires COOP/COEP headers for
  `SharedArrayBuffer`.

## Install

Install only the package you need:

```bash
pnpm add kafka-crab-js
pnpm add kafka-crab-js-otel
pnpm add pdf-crab-js
pnpm add html-to-pdf-crab-js
```

See each package README for npm/yarn variants, peer dependencies, API usage, and WASM notes.

## Development

Install dependencies from the workspace root:

```bash
vp install
```

Run the whole workspace check:

```bash
vp check
```

Focused commands are documented in the owning project READMEs:

| Task                                     | Documentation                                                            |
| ---------------------------------------- | ------------------------------------------------------------------------ |
| Build/test Kafka core                    | [kafka-crab-js](./packages/kafka-crab-js/README.md)                      |
| Build/test Kafka OTEL                    | [kafka-crab-js-otel](./packages/kafka-crab-js-otel/README.md)            |
| Build/test low-level PDF native and WASM | [pdf-crab-js](./packages/pdf-crab-js/README.md)                          |
| Build/test HTML-to-PDF native and WASM   | [html-to-pdf-crab-js](./packages/html-to-pdf-crab-js/README.md)          |
| Run Kafka examples                       | [examples/kafka](./examples/kafka/README.md)                             |
| Run PDF examples                         | [examples/pdf-crab-js](./examples/pdf-crab-js/README.md)                 |
| Run HTML-to-PDF examples                 | [examples/html-to-pdf-crab-js](./examples/html-to-pdf-crab-js/README.md) |
| Run Kafka benchmarks                     | [benchmarks/kafka](./benchmarks/kafka/README.md)                         |
| Run PDF benchmarks                       | [benchmarks/pdf](./benchmarks/pdf/README.md)                             |

## Requirements

- Node.js `>= 22` for published packages.
- Rust toolchain when building native bindings from source.
- Kafka broker access only for Kafka integration tests, Kafka examples, and Kafka benchmarks.
- No separate librdkafka install is required for published Kafka binaries.

## License

MIT
