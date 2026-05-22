# pdf-crab-js

Native Node.js package built with Rust, NAPI-RS, and printpdf.

## Usage

```js
import { writeFileSync } from 'node:fs'
import { createPdf } from 'pdf-crab-js'

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

For image, SVG, HTML, parsing, and page rendering work, prefer the async APIs so the native work runs
off the Node.js event loop:

```js
import { writeFileSync } from 'node:fs'
import { createPdfAsync, renderPdfPageToSvgAsync } from 'pdf-crab-js'

const pdf = await createPdfAsync({
  title: 'Rich PDF',
  pages: [
    {
      width: 210,
      height: 297,
      elements: [
        {
          type: 'textBox',
          text: 'Wrapped text with the declarative pdf-crab-js API.',
          x: 20,
          y: 250,
          width: 90,
          fontSize: 12,
        },
        {
          type: 'svg',
          svg: '<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20"><rect width="20" height="20" fill="red"/></svg>',
          x: 20,
          y: 200,
          width: 30,
          height: 30,
        },
      ],
    },
  ],
})

const firstPageSvg = await renderPdfPageToSvgAsync({ pdf, pageIndex: 0 })

writeFileSync('rich.pdf', pdf)
writeFileSync('page.svg', firstPageSvg)
```

The public Node API is Buffer-first. It does not expose the full internal `printpdf` document object
to JavaScript, because converting that structure into JS objects is slower and couples this package
to printpdf internals. Use `parsePdf` for summaries and `renderPdfPageToSvg` when page rendering is
needed.

`printpdf` default features are intentionally disabled in this package. Node builds enable explicit
non-WASM features for HTML, text layout, image codecs, SVG, and Rayon; WASM-only features such as
`js-sys`, `web-sys`, and `wasm-bindgen-futures` are not enabled.

## Development

Install dependencies from the workspace root:

```bash
pnpm install --filter pdf-crab-js
```

Build the native binding:

```bash
pnpm --filter pdf-crab-js build
```

Run the unit tests:

```bash
pnpm --filter pdf-crab-js test
```

Run the local PDF example:

```bash
pnpm --filter pdf-crab-js example
```

The generated file is written to `packages/pdf-crab-js/examples/output/pdf-crab-js-example.pdf`.

Run the `printpdf` HTML example:

```bash
pnpm --filter pdf-crab-js example:tables-html
```

The generated file is written to `packages/pdf-crab-js/examples/output/pdf-crab-js-tables-html.pdf`.

Run the `printpdf` example gallery:

```bash
pnpm --filter pdf-crab-js example:printpdf
```

The gallery reproduces the upstream `printpdf` text, shapes, images, SVG, layers, bookmarks,
multi-page, HTML, table, pagination, and margin examples using the public TypeScript API.
Generated files are written to `packages/pdf-crab-js/examples/output/`.

Run linting and type checks:

```bash
pnpm --filter pdf-crab-js lint
pnpm --filter pdf-crab-js check
```

## Release

Native package publishing is handled by `napi prepublish -t npm`.
