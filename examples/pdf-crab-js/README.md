# pdf-crab-js Examples

Runnable examples for generating PDFs with `pdf-crab-js`.

Run the Node examples from the repository root:

```bash
pnpm --filter pdf-crab-js-examples example
pnpm --filter pdf-crab-js-examples example:table
```

The generated PDFs are written to `examples/pdf-crab-js/output/`.

Run the browser WASM example:

```bash
pnpm --filter pdf-crab-js-examples browser
```

Open `/wasm/` on the dev-server URL printed by Vite. The page previews a structured `CreatePdfInput` object and
renders it into a PDF iframe. The browser example imports the generated
`packages/pdf-crab-js/pdf-crab-js.wasi-browser.js` entry, which loads the local `pdf-crab-js.wasm32-wasi.wasm`
artifact next to it.
