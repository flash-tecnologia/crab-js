# html-to-pdf-crab-js Examples

Runnable examples for generating PDFs from HTML with `html-to-pdf-crab-js`.

Each example keeps the HTML and CSS in standalone files so you can open the HTML directly in a
browser and compare it with the generated PDF output.

The examples pass `assets/Tuffy.ttf` through the `fonts` input. This is required for deterministic
WASM browser rendering because the WASI runtime does not have access to system fonts.

## PDF Results

Invoice document from `invoice.ts`:

![html-to-pdf-crab-js invoice PDF result](./screenshots/html-to-pdf-invoice-example.pdf.png)

Operational report from `report.ts`:

![html-to-pdf-crab-js report PDF result](./screenshots/html-to-pdf-report-example.pdf.png)

## Run

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

Open `/wasm/` on the dev-server URL printed by Vite. The page previews `report.html` and renders
that same HTML/CSS into a PDF iframe. The browser example imports `html-to-pdf-crab-js/browser.js`;
Vite aliases the generated `html-to-pdf-crab-js-wasm32-wasi` package entry to the local WASI browser
build during development.

If you rebuild `html-to-pdf-crab-js` while the Vite dev server is running, restart the dev server
before checking the page again. The package build regenerates `*.wasi-browser.js` and rewrites it to
use async WASM instantiation; stale hot-reload state can still point at the previous generated file.

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

## Screenshot Maintenance

Regenerate the PDFs, then refresh the screenshot thumbnails:

```bash
pnpm --filter html-to-pdf-crab-js-examples invoice
pnpm --filter html-to-pdf-crab-js-examples report
qlmanage -t -s 1200 -o examples/html-to-pdf-crab-js/screenshots examples/html-to-pdf-crab-js/output/html-to-pdf-invoice-example.pdf
qlmanage -t -s 1200 -o examples/html-to-pdf-crab-js/screenshots examples/html-to-pdf-crab-js/output/html-to-pdf-report-example.pdf
```
