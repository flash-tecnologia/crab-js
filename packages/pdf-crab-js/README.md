# pdf-crab-js

Native Node.js package built with Rust, NAPI-RS, and `pdf-writer`.

## Usage

Node.js should use the native NAPI entrypoint:

```js
import { writeFileSync } from 'node:fs'
import { PdfDocumentBuilder, createPdf } from 'pdf-crab-js'

const pdf = createPdf({
  title: 'Invoice',
  pages: [
    {
      width: 210,
      height: 297,
      elements: [
        {
          type: 'rect',
          x: 20,
          y: 230,
          width: 90,
          height: 32,
          stroke: '#111827',
          strokeWidth: 1,
        },
        {
          type: 'text',
          text: 'Hello PDF',
          x: 28,
          y: 244,
          fontSize: 16,
          fill: '#111827',
        },
      ],
    },
  ],
})

writeFileSync('example.pdf', pdf)
```

The phase 1 public API is intentionally small:

- `createPdf(input): Buffer`
- `createPdfAsync(input): Promise<Buffer>`
- `new PdfDocumentBuilder(input?)`

Use `PdfDocumentBuilder` when the document is produced in chunks and you do not want to build one
large `pages[].elements[]` object graph before crossing the NAPI boundary:

```js
const builder = new PdfDocumentBuilder({ title: 'Chunked PDF' })

builder.startPage({ width: 210, height: 297 })
builder.appendElements([{ type: 'text', text: 'Chunk 1', x: 20, y: 260 }])
builder.appendElements([{ type: 'line', x1: 20, y1: 250, x2: 120, y2: 250 }])
builder.endPage()

writeFileSync('chunked.pdf', builder.finish())
```

Supported elements are `text`, `textBox`, `line`, `rect`, `polygon`, and `path` with straight
segments. Coordinates use the PDF bottom-left origin. Document/page dimensions and coordinates use
`input.unit`, defaulting to `mm`; font sizes and stroke widths are points.

`pdf-writer` is a low-level PDF object/content writer. It does not provide HTML rendering, PDF
parsing, or page-to-SVG rendering, so those former high-level APIs are out of scope for this first
phase.

## WebAssembly

Browser, Deno, Bun, and portable runtimes can use the NAPI-RS WebAssembly build:

```js
import { createPdf } from 'pdf-crab-js/wasm'
```

Browser deployments must enable `SharedArrayBuffer`, which requires these response headers:

```text
Cross-Origin-Embedder-Policy: require-corp
Cross-Origin-Opener-Policy: same-origin
```

## Development

Install dependencies from the workspace root:

```bash
pnpm install --filter pdf-crab-js
```

Build and test:

```bash
pnpm --filter pdf-crab-js build
pnpm --filter pdf-crab-js test
pnpm --filter pdf-crab-js check
pnpm --filter pdf-crab-js lint
pnpm --filter pdf-crab-js fmt:check
```

Build and smoke-test the WebAssembly binding:

```bash
rustup target add wasm32-wasip1-threads
pnpm --filter pdf-crab-js build:wasm
pnpm --filter pdf-crab-js test:wasm
```

Run the local PDF example:

```bash
pnpm --filter pdf-crab-js example
```

The generated file is written to `packages/pdf-crab-js/examples/output/pdf-crab-js-example.pdf`.

Run the PDF table benchmark from the workspace root:

```bash
pnpm --filter pdf-benchmark benchmark
```

The benchmark defaults to a 10-page PDF with 10 table rows per page. Use `PDF_BENCHMARK_RUNS`,
`PDF_BENCHMARK_WARMUP`, `PDF_BENCHMARK_PAGES`, and `PDF_BENCHMARK_WRITE=1` to tune the run or write
the generated PDF to `benchmarks/pdf/output/`.

## Release

Native and WebAssembly package publishing is handled by `napi prepublish -t npm`.
