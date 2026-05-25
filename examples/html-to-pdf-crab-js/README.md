# html-to-pdf-crab-js Examples

Runnable examples for generating PDFs from HTML with `html-to-pdf-crab-js`.

Each example keeps the HTML and CSS in standalone files so you can open the HTML directly in a browser and compare it
with the generated PDF output.

The examples pass `assets/Tuffy.ttf` through the `fonts` input. This is required for deterministic WASM browser
rendering because the WASI runtime does not have access to system fonts.

Run from the repository root:

```bash
pnpm --filter html-to-pdf-crab-js-examples invoice
pnpm --filter html-to-pdf-crab-js-examples report
```

The generated PDFs are written to `examples/html-to-pdf-crab-js/output/`.

Run the browser WASM example:

```bash
pnpm --filter html-to-pdf-crab-js-examples browser
```

Open `/wasm/` on the dev-server URL printed by Vite. The page previews `invoice.html` and renders that same HTML/CSS
into a PDF iframe. The browser example imports the generated
`packages/html-to-pdf-crab-js/html-to-pdf-crab-js.wasi-browser.js` entry, which loads the local
`html-to-pdf-crab-js.wasm32-wasi.wasm` artifact next to it.

Preview the source HTML in a browser:

```bash
open examples/html-to-pdf-crab-js/invoice.html
open examples/html-to-pdf-crab-js/report.html
```

You can also run the package-level aliases:

```bash
pnpm --filter html-to-pdf-crab-js example
pnpm --filter html-to-pdf-crab-js example:report
pnpm --filter html-to-pdf-crab-js example:browser
```
