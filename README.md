# Crab JS

Native Node.js packages backed by Rust and NAPI-RS.

Crab JS is a monorepo for focused native packages that keep JavaScript APIs small while moving heavy work into Rust.
The root README is intentionally a high-level map. Package APIs, examples, benchmarks, and operational guidance live
next to the project that owns them.

[![kafka-crab-js npm](https://img.shields.io/npm/v/kafka-crab-js)](https://www.npmjs.com/package/kafka-crab-js)
[![kafka-crab-js-otel npm](https://img.shields.io/npm/v/kafka-crab-js-otel)](https://www.npmjs.com/package/kafka-crab-js-otel)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Kafka for JavaScript, powered by Rust

[`kafka-crab-js`](./packages/kafka-crab-js/README.md) brings native Kafka performance
to JavaScript and TypeScript: delivery-confirmed publishing, serial and batch Web
Streams, Node.js pipelines, and direct librdkafka configuration. Prebuilt binaries
support Node.js 24 on macOS and Linux, with optional OpenTelemetry instrumentation.

The September 12, 2026 development build reached **1.66 million messages/s in batch
mode**, **1.98× KafkaJS `eachBatch`**, in the local small-message consumer comparison.
See the [measured versions, memory tradeoffs, and reproduction](./BENCHMARKS.md);
these numbers describe the development snapshot, not the current npm release.

[Get started](./packages/kafka-crab-js/README.md#quick-start) ·
[Choose an API](./packages/kafka-crab-js/README.md#choose-your-api) ·
[Read the API reference](./packages/kafka-crab-js/docs/api.md)

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
| [wasm-samples](./examples/wasm-samples)                        | Interactive Vite studio for structured PDF and HTML-to-PDF examples.    |

### Benchmarks

| Benchmark Package                     | Purpose                                                                                           |
| ------------------------------------- | ------------------------------------------------------------------------------------------------- |
| [kafka-benchmark](./benchmarks/kafka) | Isolated-process consumer benchmark with memory, GC, throughput charts, and V8 profiling scripts. |
| [pdf-benchmark](./benchmarks/pdf)     | Comparable structured-PDF, table, image, and optional HTML/CSS workloads.                         |
| [BENCHMARKS.md](./BENCHMARKS.md)      | Latest captured Kafka benchmark snapshot and notes.                                               |

## Package Boundaries

- `kafka-crab-js` owns Kafka producer/consumer APIs and native librdkafka integration.
- `kafka-crab-js-otel` owns OpenTelemetry tracing and metrics. OTEL is intentionally outside the core Kafka package.
- `pdf-crab-js` owns fast explicit PDF construction from structured page and drawing-element inputs.
- `html-to-pdf-crab-js` owns easy HTML/CSS rendering to PDF without a Chromium service. It carries the renderer
  dependency separately from `pdf-crab-js`.
- The default `pdf-crab-js/browser` and `html-to-pdf-crab-js/browser` builds are threadless and need no cross-origin
  isolation. Optional threaded browser deployments use COOP/COEP as documented by their packages.

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
| Run the interactive WASM studio          | [examples/wasm-samples](./examples/wasm-samples/README.md)               |
| Run Kafka benchmarks                     | [benchmarks/kafka](./benchmarks/kafka/README.md)                         |
| Run PDF benchmarks                       | [benchmarks/pdf](./benchmarks/pdf/README.md)                             |

## Requirements

- Node.js `24` for published packages.
- Rust toolchain when building native bindings from source.
- Kafka broker access only for Kafka integration tests, Kafka examples, and Kafka benchmarks.
- No separate librdkafka install is required for published Kafka binaries.

## Publishing Kafka packages

Kafka and OTEL releases use [npm trusted publishing](https://docs.npmjs.com/trusted-publishers/) with GitHub Actions OIDC.
In each npm package's **Settings → Trusted publishing**, select **GitHub Actions** and configure:

- Organization or user: `flash-tecnologia`
- Repository: `crab-js`
- Environment name: leave empty; these workflows do not use a GitHub environment.
- Allowed actions: enable direct publishing with `npm publish`.
- Workflow filename: use the exact filename below, without `.github/workflows/`.

| npm package                     | Workflow filename |
| ------------------------------- | ----------------- |
| `kafka-crab-js`                  | `CI.yml`          |
| `kafka-crab-js-darwin-x64`       | `CI.yml`          |
| `kafka-crab-js-darwin-arm64`     | `CI.yml`          |
| `kafka-crab-js-linux-x64-gnu`    | `CI.yml`          |
| `kafka-crab-js-linux-x64-musl`   | `CI.yml`          |
| `kafka-crab-js-linux-arm64-gnu`  | `CI.yml`          |
| `kafka-crab-js-linux-arm64-musl` | `CI.yml`          |
| `kafka-crab-js-otel`             | `CI-otel.yml`     |

The publish jobs use the GitHub-hosted `flash-static-ip-ubuntu-24` runner, npm 11, and `id-token: write`. Its runner group
must allow this public repository and the release workflows. Kafka's `prepublishOnly` hook publishes the platform
packages with `--no-gh-release`, so npm publication does not create or upload assets to a GitHub Release.
Kafka and OTEL publishing do not use `NPM_TOKEN`; the PDF workflows still reference that secret.

Release tags must match the package versions: `kafka-crab-js@<version>` and `kafka-crab-js-otel@<version>`.
After correcting npm-side publisher settings, failed publish jobs can be rerun. Workflow or package-script changes
require a release execution containing the updated commit: rerunning an older workflow uses its original tag and SHA.

## License

MIT
