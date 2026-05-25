# html-to-pdf-crab-js

Native Node.js package built with Rust and NAPI-RS for HTML-to-PDF rendering.

## Usage

Node.js should use the native NAPI entrypoint:

```js
import { writeFileSync } from 'node:fs'
import { createPdfFromHtml } from 'html-to-pdf-crab-js'

const pdf = await createPdfFromHtml({
  html: '<!doctype html><html><body><main><h1>Invoice</h1><p>Generated from HTML.</p></main></body></html>',
  css: 'body { font-family: sans-serif; } h1 { color: #111827; }',
  page: {
    size: 'A4',
    margin: 12,
  },
})

writeFileSync('html-example.pdf', pdf)
```

The public API is intentionally small:

- `createPdfFromHtml(input): Promise<Buffer>`

Use `pdf-crab-js` when the document is already structured as explicit PDF pages and elements.

## WebAssembly

Browser, Deno, Bun, and portable runtimes can use the NAPI-RS WebAssembly build:

```js
import { createPdfFromHtml } from 'html-to-pdf-crab-js/wasm'
```

Browser deployments must enable `SharedArrayBuffer`, which requires these response headers:

```text
Cross-Origin-Embedder-Policy: require-corp
Cross-Origin-Opener-Policy: same-origin
```

## Development

Install dependencies from the workspace root:

```bash
pnpm install --filter html-to-pdf-crab-js
```

Build and test:

```bash
pnpm --filter html-to-pdf-crab-js build
pnpm --filter html-to-pdf-crab-js test
pnpm --filter html-to-pdf-crab-js check
pnpm --filter html-to-pdf-crab-js lint
pnpm --filter html-to-pdf-crab-js fmt:check
```

Run local examples:

```bash
pnpm --filter html-to-pdf-crab-js-examples invoice
pnpm --filter html-to-pdf-crab-js-examples report
pnpm --filter html-to-pdf-crab-js-examples browser
```

Generated files are written to `examples/html-to-pdf-crab-js/output/`.

Build and smoke-test the WebAssembly binding:

```bash
rustup target add wasm32-wasip1-threads
pnpm --filter html-to-pdf-crab-js build:wasm
pnpm --filter html-to-pdf-crab-js test:wasm
```
