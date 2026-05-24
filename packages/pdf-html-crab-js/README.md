# pdf-html-crab-js

Native Node.js package built with Rust and NAPI-RS for HTML-to-PDF rendering.

## Usage

Node.js should use the native NAPI entrypoint:

```js
import { writeFileSync } from 'node:fs'
import { createPdfFromHtml } from 'pdf-html-crab-js'

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
import { createPdfFromHtml } from 'pdf-html-crab-js/wasm'
```

Browser deployments must enable `SharedArrayBuffer`, which requires these response headers:

```text
Cross-Origin-Embedder-Policy: require-corp
Cross-Origin-Opener-Policy: same-origin
```

## Development

Install dependencies from the workspace root:

```bash
pnpm install --filter pdf-html-crab-js
```

Build and test:

```bash
pnpm --filter pdf-html-crab-js build
pnpm --filter pdf-html-crab-js test
pnpm --filter pdf-html-crab-js check
pnpm --filter pdf-html-crab-js lint
pnpm --filter pdf-html-crab-js fmt:check
```

Build and smoke-test the WebAssembly binding:

```bash
rustup target add wasm32-wasip1-threads
pnpm --filter pdf-html-crab-js build:wasm
pnpm --filter pdf-html-crab-js test:wasm
```
