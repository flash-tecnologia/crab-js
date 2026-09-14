# pdf-crab-js

Fast, typed PDF generation for Node.js and browser/WASM, powered by Rust, NAPI-RS, and
`pdf-writer`. Build invoices, reports, statements, labels, tables, and other structured
documents without running a browser.

Try the interactive browser demo at [pdf-crab-js.netlify.app](https://pdf-crab-js.netlify.app/).

- Two APIs: declarative `renderPdf(input)` and fluent `PdfDocument`.
- One lazy output contract for bytes, blobs, Web streams, and async iteration.
- Typed tables with automatic widths, pagination, and repeated headers.
- PNG/JPEG images, links, metadata, built-in fonts, and embedded TTF/OTF fonts.
- Top-left coordinates with `mm` or `pt` units.
- Native Node.js builds and single-threaded or threaded browser/WASM builds.

Use [`html-to-pdf-crab-js`](../html-to-pdf-crab-js/README.md) instead when HTML and CSS are the
source of truth. The core package deliberately targets structured PDF primitives rather than a
browser layout engine.

## Contents

- [Requirements and installation](#requirements-and-installation)
- [Quick start](#quick-start)
- [Choosing an API](#choosing-an-api)
- [Output: bytes, blobs, and streams](#output-bytes-blobs-and-streams)
- [Pages, units, and coordinates](#pages-units-and-coordinates)
- [Fluent API](#fluent-api)
- [Text](#text)
- [Tables](#tables)
- [Images](#images)
- [Shapes, paths, and links](#shapes-paths-and-links)
- [Fonts and Unicode](#fonts-and-unicode)
- [Declarative API](#declarative-api)
- [Browser and WASM](#browser-and-wasm)
- [Errors](#errors)
- [Public API surface](#public-api-surface)
- [API changes in 1.0](#api-changes-in-10)
- [pdf-crab-js vs PDFKit](#pdf-crab-js-vs-pdfkit)
- [Current scope](#current-scope)
- [Examples and development](#examples-and-development)

## Requirements and installation

The native Node.js entry point requires Node.js 24.

```bash
npm install pdf-crab-js
```

The package provides ESM and CommonJS entry points:

```ts
import { PdfDocument, PdfError, renderPdf } from 'pdf-crab-js'
```

```js
const { PdfDocument, PdfError, renderPdf } = require('pdf-crab-js')
```

Browser applications must use the explicit browser entry point described in
[Browser and WASM](#browser-and-wasm).

## Quick start

### Fluent document

`PdfDocument` creates the first page automatically and is convenient when content is assembled
sequentially.

```ts
import { writeFile } from 'node:fs/promises'
import { PdfDocument } from 'pdf-crab-js'

type InvoiceRow = {
  description: string
  quantity: number
  total: number
}

const document = new PdfDocument({
  title: 'Invoice 42',
  unit: 'mm',
  size: 'A4',
  margin: 20,
  metadata: {
    author: 'Acme',
    subject: 'Invoice',
  },
})

document
  .font('HelveticaBold')
  .fontSize(20)
  .fillColor('#0f172a')
  .text('Invoice #42')
  .moveDown()
  .font('Helvetica')
  .fontSize(10)
  .fillColor('#475569')
  .text('Generated with pdf-crab-js')
  .moveDown()
  .table<InvoiceRow>({
    columns: [
      { key: 'description', header: 'Description', width: '*' },
      { key: 'quantity', header: 'Qty', width: 22, align: 'right' },
      {
        key: 'total',
        header: 'Total',
        width: 32,
        align: 'right',
        formatter: (value) => `$ ${Number(value).toFixed(2)}`,
      },
    ],
    rows: [{ description: 'Pro plan', quantity: 1, total: 42 }],
    stripe: '#f8fafc',
    border: '#cbd5e1',
    padding: 3,
  })

await writeFile('invoice.pdf', await document.render().bytes())
```

### Declarative document

`renderPdf` is useful when pages and elements already exist as typed data.

```ts
import { writeFile } from 'node:fs/promises'
import { renderPdf, type PdfDocumentInput } from 'pdf-crab-js'

const input = {
  title: 'Report',
  unit: 'mm',
  pages: [
    {
      size: 'A4',
      elements: [
        {
          type: 'rect',
          x: 20,
          y: 20,
          width: 170,
          height: 34,
          fill: '#eff6ff',
          stroke: '#2563eb',
          strokeWidth: 1,
        },
        {
          type: 'text',
          text: 'Declarative report',
          x: 28,
          y: 31,
          font: 'HelveticaBold',
          fontSize: 18,
          fill: '#1e3a8a',
        },
      ],
    },
  ],
} satisfies PdfDocumentInput

await writeFile('report.pdf', await renderPdf(input).bytes())
```

## Choosing an API

Both APIs use the same renderer and return the same `PdfOutput` contract.

| Use case                                      | Recommended API       |
| --------------------------------------------- | --------------------- |
| Templates represented as JSON-like typed data | `renderPdf(input)`    |
| Explicit pages and absolute element placement | `renderPdf(input)`    |
| Sequential report or invoice construction     | `PdfDocument`         |
| Flowing text with cursor movement             | `PdfDocument`         |
| Typed tables with automatic pagination        | `PdfDocument.table()` |
| Fluent shapes, paths, images, and links       | `PdfDocument`         |

The declarative API does not expose `table()`; build typed tables with `PdfDocument`.

## Output: bytes, blobs, and streams

`renderPdf(input)` and `document.render()` return a lazy, single-use `PdfOutput`. Native
serialization does not begin until the output is consumed; fluent content construction and input
validation still happen at their normal API call sites.

| Member                          | Result                       | Typical use                                 |
| ------------------------------- | ---------------------------- | ------------------------------------------- |
| `used`                          | `boolean`                    | Check whether consumption has started       |
| `bytes({ signal })`             | `Promise<Uint8Array>`        | Files, HTTP bodies, or in-memory processing |
| `blob({ signal })`              | `Promise<Blob>`              | Browser preview or download                 |
| `stream({ chunkSize, signal })` | `ReadableStream<Uint8Array>` | Backpressure-aware streaming                |
| `[Symbol.asyncIterator]()`      | `AsyncIterator<Uint8Array>`  | Portable `for await...of` consumption       |

### Write a stream in Node.js

```ts
import { createWriteStream } from 'node:fs'
import { Readable } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import { renderPdf } from 'pdf-crab-js'

const output = renderPdf({
  pages: [{ elements: [{ type: 'text', text: 'Streamed PDF' }] }],
})

await pipeline(Readable.from(output.stream({ chunkSize: 64 * 1024 })), createWriteStream('streamed.pdf'))
```

### Send a PDF over HTTP

`Uint8Array` can be sent directly by Node.js HTTP servers. Set a filename only when the response
should download instead of opening inline.

```ts
import type { ServerResponse } from 'node:http'
import { renderPdf } from 'pdf-crab-js'

async function sendReport(response: ServerResponse) {
  const bytes = await renderPdf({
    pages: [{ elements: [{ type: 'text', text: 'Quarterly report' }] }],
  }).bytes()

  response.writeHead(200, {
    'Content-Type': 'application/pdf',
    'Content-Length': bytes.byteLength,
    'Content-Disposition': 'inline; filename="report.pdf"',
  })
  response.end(bytes)
}
```

For large documents, pipe `output.stream()` as shown above instead of buffering the complete PDF.

### Iterate over chunks

```ts
const output = document.render()

for await (const chunk of output) {
  // chunk is Uint8Array
  await destination.write(chunk)
}
```

### Cancellation

```ts
const controller = new AbortController()
const output = renderPdf(input)

setTimeout(() => controller.abort(), 5_000)
const bytes = await output.bytes({ signal: controller.signal })
```

An output can be consumed exactly once. Calling `bytes()` or `blob()`, pulling the returned stream
for the first time, or requesting the first async-iterator chunk claims it. A second consumption
fails with `PDF_OUTPUT_USED`. Cancellation and early stream termination release the native
serializer.

The default stream chunk size is 64 KiB. The serializer is pull-driven and releases each emitted
page buffer instead of retaining a second complete PDF buffer. Buffered methods necessarily collect
the complete result before returning. A stream emits the PDF header first and starts native
serialization only after the header chunks have been pulled.

## Pages, units, and coordinates

The default `PdfDocument` configuration is:

| Setting         | Default                               |
| --------------- | ------------------------------------- |
| Page size       | A4                                    |
| Layout          | Portrait                              |
| Unit            | `mm`                                  |
| Margin          | 20 physical millimeters on every side |
| Font            | Helvetica                             |
| Font size       | 12 pt                                 |
| Line height     | 14.4 pt                               |
| Fill and stroke | `#000000`                             |
| Stroke width    | 1 pt                                  |

Supported page sizes are `A3`, `A4`, `LETTER`, or a custom `[width, height]` tuple. A custom
tuple uses the configured document unit.

```ts
const document = new PdfDocument({
  unit: 'pt',
  size: [612, 792],
  layout: 'portrait',
  margin: { top: 36, right: 48, bottom: 36, left: 48 },
})

document.addPage({ size: 'A3', layout: 'landscape', margin: 24 })
```

All coordinates use a top-left origin:

- `x` grows to the right.
- `y` grows down the page.
- Coordinates, dimensions, margins, image sizes, and table sizes use `mm` or `pt`.
- Font sizes, line heights, and stroke widths always use PDF points.
- Colors use six-digit hexadecimal values such as `#0f172a`.

`addPage()` without options uses the document defaults. Automatic page breaks preserve the
effective size, layout, and margins of the current page.

The declarative API has no page margin option. Flowing declarative text uses physical 20 mm
margins; absolutely positioned elements use their explicit coordinates.

## Fluent API

`PdfDocument` methods mutate the current document and return `this`, so calls can be chained.

| Member                                   | Purpose                                       |
| ---------------------------------------- | --------------------------------------------- |
| `new PdfDocument(options)`               | Create a document and its first page          |
| `x`, `y`                                 | Read the current flow cursor                  |
| `addPage(options?)`                      | Finish the current page and create another    |
| `text(text, options?)`                   | Add flowing or absolutely positioned text     |
| `text(text, x, y, options?)`             | Positional overload for absolute text         |
| `textBox(text, options)`                 | Alias of `text` for bounded text              |
| `font(name)`                             | Select a built-in or registered font          |
| `registerFont(family, source, options?)` | Register a TTF/OTF font                       |
| `fontSize(points)`                       | Set the current font size                     |
| `fillColor(color)`                       | Set the text/path fill color                  |
| `strokeColor(color)`                     | Set the path stroke color                     |
| `lineWidth(points)`                      | Set the path stroke width                     |
| `save()`, `restore()`                    | Push and restore text/path style state        |
| `moveDown(lines?)`, `moveUp(lines?)`     | Move the flow cursor by line-height multiples |
| `moveTo(x, y)`, `lineTo(x, y)`           | Build a straight-line path                    |
| `closePath()`                            | Close the active path                         |
| `rect(x, y, width, height)`              | Build a rectangular path                      |
| `fill()`, `stroke()`, `fillAndStroke()`  | Paint and clear the active path               |
| `discardPath()`                          | Clear the active path without painting        |
| `image(source, options?)`                | Add a flowing or positioned PNG/JPEG          |
| `table(options)`                         | Add a typed, paginated table                  |
| `link(url, options)`                     | Add a URL annotation                          |
| `render()`                               | Seal the document and return a `PdfOutput`    |

An active path must be painted or discarded before `addPage()` or `render()`. After
`render()`, every mutating method fails with `PDF_DOCUMENT_FINISHED`.

## Text

Text without `x` and `y` starts at the current cursor, wraps to the available width, moves the
cursor, and paginates automatically.

```ts
document.font('HelveticaBold').fontSize(16).text('Monthly report').font('Helvetica').fontSize(10).text(longParagraph, {
  width: 150,
  align: 'justify',
  lineHeight: 14,
  hyphenate: true,
})
```

Absolute placement requires both coordinates:

```ts
document.text('Page 1', {
  x: 160,
  y: 280,
  width: 30,
  align: 'right',
  overflow: 'clip',
})
```

| Option       | Description                                                     |
| ------------ | --------------------------------------------------------------- |
| `x`, `y`     | Provide both for absolute placement; omit both for flowing text |
| `width`      | Wrapping width; required by `align` and `height`                |
| `height`     | Optional bounded height                                         |
| `align`      | `left`, `center`, `right`, or `justify`                         |
| `font`       | Built-in or registered family                                   |
| `fontSize`   | Size in PDF points                                              |
| `fill`       | Text color as `#RRGGBB`                                         |
| `lineHeight` | Line height in PDF points                                       |
| `hyphenate`  | Allow the layout engine to break long words                     |
| `overflow`   | `visible`, `clip`, `ellipsis`, or `paginate`                    |

`textBox()` accepts the same options as `text()`. Alignment and ellipsis are based on actual font
metrics rather than character-count estimates.

## Tables

`table<Row>()` uses typed keys or value functions and automatically advances the document cursor.
Rows can continue on new pages, and headers repeat by default.

```ts
type Row = {
  customer: string
  amount: number
  paid: boolean
}

document.table<Row>({
  columns: [
    { key: 'customer', header: 'Customer', width: '*', minWidth: 60 },
    {
      key: 'amount',
      header: 'Amount',
      width: 32,
      align: 'right',
      formatter: (value) => Number(value).toFixed(2),
    },
    {
      header: 'Status',
      value: (row) => (row.paid ? 'Paid' : 'Pending'),
      width: 'auto',
      background: '#f8fafc',
    },
  ],
  rows,
  width: 170,
  padding: 3,
  border: '#cbd5e1',
  stripe: '#f8fafc',
  repeatHeader: true,
  rowSplit: 'avoid',
})
```

### Table options

| Option         | Description                                                        |
| -------------- | ------------------------------------------------------------------ |
| `columns`      | Required typed column definitions                                  |
| `rows`         | Required row objects                                               |
| `x`, `y`       | Provide both for absolute start position; otherwise use the cursor |
| `width`        | Total table width; defaults to remaining page width                |
| `rowHeight`    | Body row height in the document unit                               |
| `headerHeight` | Header height; defaults to `rowHeight`                             |
| `repeatHeader` | Repeat headers after automatic page breaks; defaults to `true`     |
| `rowSplit`     | `avoid` keeps a row together; `split` allows page fragments        |
| `stripe`       | Alternating body-row background                                    |
| `border`       | Default border color                                               |
| `padding`      | Default cell padding                                               |

### Column options

| Option                         | Description                                                       |
| ------------------------------ | ----------------------------------------------------------------- |
| `key`                          | Typed property key from `Row`                                     |
| `value(row, index)`            | Compute a value instead of using `key`                            |
| `formatter(value, row, index)` | Format the resolved cell value                                    |
| `header`                       | Header label; omit on every column to suppress the header         |
| `width`                        | Fixed number, content-sized `auto`, or shared remaining width `*` |
| `minWidth`, `maxWidth`         | Bounds for measured widths                                        |
| `align`                        | Text alignment                                                    |
| `font`, `fontSize`, `fill`     | Cell text style                                                   |
| `background`                   | Column cell background                                            |
| `padding`                      | Per-column padding override                                       |
| `stroke`, `strokeWidth`        | Per-column border override                                        |

The default table border is `#cbd5e1`, default padding is 2 document units, default body row
height is 24 pt converted to the document unit, and the default header uses a light background and
`HelveticaBold`. Cell values may be strings, numbers, booleans, `null`, or `undefined`; nullish
values render as empty strings.

## Images

PNG and JPEG images are supported in Node.js and browser/WASM. PNG alpha is emitted through a PDF
soft mask; RGB JPEG bytes are embedded without re-encoding.

```ts
import { readFileSync } from 'node:fs'
import { PdfDocument } from 'pdf-crab-js'

const crab = readFileSync('crab.png')
const document = new PdfDocument({ unit: 'mm', margin: 18 })

document.text('Image example').image(crab, {
  fit: [174, 98],
  align: 'center',
  valign: 'center',
})

await document.render().bytes()
```

Node.js accepts `Uint8Array`, `ArrayBuffer`, `Buffer`, or a file path string. Browser/WASM
accepts only `Uint8Array` or `ArrayBuffer`.

| Dimensions             | Behavior                                                   |
| ---------------------- | ---------------------------------------------------------- |
| None                   | Use one PDF point per source pixel                         |
| `width` only           | Preserve aspect ratio and derive height                    |
| `height` only          | Preserve aspect ratio and derive width                     |
| `width` and `height`   | Draw at the exact dimensions                               |
| `fit: [width, height]` | Contain the image inside the box and preserve aspect ratio |

`align` and `valign` position an image inside its `fit` box. Omit `x` and `y` to place the
image at the cursor and advance flow; provide both for absolute placement.

GIF, WebP, SVG, data URLs, HTTP URLs, and `Blob` are not accepted as image sources in 1.0.

See the runnable [image example](../../examples/pdf-crab-js/src/node/image.ts) and its
[rendered preview](../../examples/pdf-crab-js/screenshots/pdf-crab-js-image-example.pdf.png).

## Shapes, paths, and links

### Fluent paths

```ts
document
  .fillColor('#dbeafe')
  .strokeColor('#2563eb')
  .lineWidth(1)
  .rect(20, 20, 80, 30)
  .fillAndStroke()
  .moveTo(20, 65)
  .lineTo(100, 65)
  .lineTo(80, 90)
  .closePath()
  .stroke()
  .link('https://example.com', {
    x: 20,
    y: 100,
    width: 50,
    height: 10,
    color: '#2563eb',
  })
```

Fluent paths contain straight segments. `save()` and `restore()` preserve text/path style state;
they do not introduce a transformation stack.

### Declarative drawing elements

| Element    | Required fields             | Optional fields                            |
| ---------- | --------------------------- | ------------------------------------------ |
| `text`     | `text`                      | Position/bounds and text style             |
| `line`     | `x1`, `y1`, `x2`, `y2`      | `stroke`, `strokeWidth`                    |
| `rect`     | `x`, `y`, `width`, `height` | `fill`, `stroke`, `strokeWidth`            |
| `polygon`  | `points`                    | Fill/stroke style and `winding`            |
| `polyline` | `points`                    | `closed`, fill/stroke style, and `winding` |
| `image`    | `source`, `x`, `y`          | Dimensions, `fit`, `align`, and `valign`   |

`winding` accepts `nonZero` or `evenOdd`.

Pages also accept URL annotations:

```ts
import type { PdfPageInput } from 'pdf-crab-js'

const page = {
  elements: [{ type: 'text', text: 'Open documentation', x: 20, y: 20 }],
  annotations: [
    {
      type: 'link',
      x: 20,
      y: 20,
      width: 42,
      height: 8,
      url: 'https://example.com',
      color: '#2563eb',
    },
  ],
} satisfies PdfPageInput
```

## Fonts and Unicode

Built-in font families cover the standard PDF fonts and common aliases:

- Times, Times Bold, Times Italic, and Times Bold Italic.
- Helvetica/Arial regular, bold, oblique/italic, and bold oblique/italic.
- Courier regular, bold, oblique/italic, and bold oblique/italic.
- Symbol and ZapfDingbats.

Register a TTF or OTF font when text requires glyphs outside the built-in WinAnsi coverage:

```ts
const document = new PdfDocument()

document
  .registerFont('Invoice', new URL('./Invoice.ttf', import.meta.url), {
    weight: 400,
    style: 'normal',
    fallback: 'Helvetica',
  })
  .font('Invoice')
  .text('Ação, café e coração')
```

The declarative equivalent uses `fonts`:

```ts
const output = renderPdf({
  fonts: [
    {
      family: 'Report',
      source: fontBytes,
      fallback: 'Helvetica',
    },
  ],
  pages: [
    {
      elements: [{ type: 'text', text: 'Relatório final', font: 'Report' }],
    },
  ],
})
```

Node.js font sources may be bytes, paths, or file URLs. Browser/WASM font sources must be
`Uint8Array` or `ArrayBuffer`.

Registration `weight` accepts `normal`, `bold`, or an integer from 1 to 1000. `style` accepts
`normal`, `italic`, or `oblique`; `fallback` names a built-in family or a custom family registered
earlier in the document.

Custom fonts are shaped, subset, embedded as CID fonts, and include a `ToUnicode` map for reliable
text extraction. Fallback is explicit and applies only to missing glyph runs. Without a usable
fallback, rendering fails with `PDF_MISSING_GLYPH` rather than silently replacing text.

Version 1.0 covers Latin text, including Portuguese accents. Arabic/RTL and other bidi-heavy scripts
are outside the current contract.

## Declarative API

### Document input

```ts
interface PdfDocumentInput {
  title?: string
  unit?: 'mm' | 'pt'
  metadata?: PdfMetadata
  fonts?: readonly PdfFontInput[]
  pages: readonly PdfPageInput[]
}
```

`pages` is required and must contain at least one page at runtime.

### Page input

```ts
interface PdfPageInput {
  size?: 'A3' | 'A4' | 'LETTER' | readonly [number, number]
  layout?: 'portrait' | 'landscape'
  elements?: readonly PdfElementInput[]
  annotations?: readonly PdfAnnotationInput[]
}
```

Text elements may omit `x` and `y`. Such text flows from the default margin, wraps with measured
font metrics, and can create additional pages. Other declarative elements require explicit
coordinates and do not advance a flow cursor.

### Metadata

`PdfMetadata` accepts `title`, `author`, `creator`, `producer`, `subject`, `keywords`,
and `trapped`.

## Browser and WASM

Use the explicit browser entry point:

```ts
import { renderPdf } from 'pdf-crab-js/browser'

const image = new Uint8Array(await (await fetch('/crab.png')).arrayBuffer())
const blob = await renderPdf({
  pages: [
    {
      elements: [{ type: 'image', source: image, x: 20, y: 20, fit: [170, 100] }],
    },
  ],
}).blob()

const url = URL.createObjectURL(blob)
window.open(url)
```

The default browser build is single-threaded and requires neither `SharedArrayBuffer` nor
cross-origin isolation.

An optional threaded build is available for isolated deployments:

```ts
import { renderPdf } from 'pdf-crab-js/browser/threaded'
```

The threaded entry point requires the normal COOP/COEP cross-origin isolation setup. Both browser
entry points expose the same PDF API, but file path sources are Node-only.

The root package carries the browser artifacts and declares their JavaScript runtimes as production
dependencies. The matching WASI flavor packages are installed automatically as optional dependencies;
keep optional dependencies enabled when using the generated Workerd entry point. The generated
threadless WASI package can also be installed separately for direct access to the low-level binding:

```bash
npm install pdf-crab-js-wasm32-wasip1
```

See the complete [Vite browser studio](../../examples/wasm-samples/README.md) for structured PDF,
HTML-to-PDF, live preview, downloads, and the TypeScript source behind every sample.

### Workerd

For Cloudflare Workers, workerd, or another host that supplies the WebAssembly module itself,
install the root package and use the generated deferred loader:

```bash
npm install pdf-crab-js
```

```ts
import wasmModule from 'pdf-crab-js/wasm.wasm'
import { dispose, instantiate } from 'pdf-crab-js/workerd'

const binding = await instantiate(wasmModule)
const pdf = binding.createPdf({ pages: [{ elements: [] }] })
await dispose()
```

The Workerd loader is deferred and safe to initialize more than once; dispose instances when a
request or isolate is complete.

## Errors

All public failures are normalized to `PdfError`:

```ts
import { PdfError, renderPdf } from 'pdf-crab-js'

try {
  await renderPdf(input).bytes()
} catch (error) {
  if (error instanceof PdfError) {
    console.error(error.code, error.path, error.message)
  }
}
```

| Code                     | Meaning                                                    |
| ------------------------ | ---------------------------------------------------------- |
| `PDF_OUTPUT_USED`        | A single-use `PdfOutput` was consumed more than once       |
| `PDF_DOCUMENT_FINISHED`  | A sealed `PdfDocument` was mutated or rendered again       |
| `PDF_INVALID_ARGUMENT`   | An argument or combination of options is invalid           |
| `PDF_UNSUPPORTED_FORMAT` | An image or other input format is unsupported              |
| `PDF_FONT_NOT_FOUND`     | A selected font is neither built in nor registered         |
| `PDF_MISSING_GLYPH`      | A font and its explicit fallback cannot represent the text |
| `PDF_LAYOUT_ERROR`       | Content cannot satisfy the requested layout                |
| `PDF_ABORTED`            | An `AbortSignal` cancelled rendering                       |

`PdfError.path` identifies the nearest invalid input when available, and `PdfError.cause`
preserves the underlying error.

## Public API surface

The package intentionally exposes only three runtime values:

- `renderPdf(input: PdfDocumentInput): PdfOutput`
- `PdfDocument`
- `PdfError`

The root entry point also exports the following TypeScript types:

- Documents and pages: `PdfDocumentInput`, `PdfDocumentOptions`, `PdfPageInput`,
  `PdfPageOptions`, `PdfPageSize`, `PdfLayout`, `PdfUnit`, `PdfMargins`, and
  `PdfMetadata`.
- Output and errors: `PdfOutput`, `PdfOutputOptions`, `PdfStreamOptions`, and
  `PdfErrorCode`.
- Text and fonts: `PdfTextElement`, `PdfTextOptions`, `PdfTextBoxOptions`,
  `PdfTextStyleOptions`, `PdfTextAlign`, `PdfTextOverflow`, `PdfFontInput`,
  `PdfFontSource`, and `PdfFontRegistrationOptions`.
- Images: `PdfImageElement`, `PdfImageOptions`, `PdfImageSource`, `PdfImageBytes`,
  `PdfImageAlign`, and `PdfImageValign`.
- Drawing and annotations: `PdfElementInput`, `PdfLineElement`, `PdfRectElement`,
  `PdfPolygonElement`, `PdfPolylineElement`, `PdfFillStyleOptions`,
  `PdfStrokeStyleOptions`, `PdfAnnotationInput`, and `PdfLinkOptions`.
- Tables: `PdfTableOptions`, `PdfTableColumn`, `PdfTableColumnWidth`,
  `PdfTableCellValue`, and `PdfTableCellStyle`.

## API changes in 1.0

Version 1.0 intentionally removes the provisional 0.3/draft API instead of keeping compatibility
aliases.

### Entry points and output

| 0.3/draft                       | 1.0                                            |
| ------------------------------- | ---------------------------------------------- |
| `createPdf(input)`              | `await renderPdf(input).bytes()`               |
| `createPdfAsync(input)`         | `await renderPdf(input).bytes()`               |
| `createPdfStream*`              | `renderPdf(input).stream()` or async iteration |
| `document.finish*()`            | `await document.render().bytes()`              |
| `document.stream()`             | `document.render().stream()`                   |
| Immediate `Buffer`/bytes result | Lazy, single-use `PdfOutput`                   |
| Separate sync/async APIs        | One asynchronous output contract               |

### Builders, pages, and coordinates

- `PdfDocumentBuilder`, `startPage`, `appendElements`, and `appendAnnotations` are no longer
  public.
- Use `PdfDocument`, `addPage()`, fluent drawing methods, `link()`, and `render()`.
- `CreatePdfInput` is now `PdfDocumentInput`.
- Page `width`/`height` configuration is now `size` plus optional `layout`.
- Coordinates changed from bottom-left to top-left.
- The first fluent page is created automatically with A4/portrait/`mm`/20 mm defaults.
- `addPage()` and automatic page breaks inherit consistent page configuration.

For old bottom-left coordinates on a page with height `H`:

- Points and line endpoints: `newY = H - oldY`.
- Rectangles, links, and images: `newY = H - oldY - height`.
- Text receives its new top edge instead of the old baseline.

### Text, drawing, tables, and fonts

- `text` and `textBox` now share one options model.
- Text without coordinates flows, wraps, and paginates.
- Absolute text requires both `x` and `y`; alignment or bounded height requires `width`.
- `PdfTextBoxElement` was folded into the declarative `text` element.
- `PdfStyleOptions` was split into text, fill, and stroke style types.
- Declarative `PdfPathElement` became `PdfPolylineElement`; use `polygon` for closed polygons or
  fluent path methods for sequential drawing.
- `PdfDocument.table()` adds typed columns, measured widths, formatting, repeated headers,
  striping, and row pagination.
- Custom TTF/OTF registration, shaping, subsetting, `ToUnicode`, and explicit fallback were added.
- Missing glyphs now fail with `PDF_MISSING_GLYPH`.

### Images, browser, and errors

- PNG and JPEG sources can be bytes; Node.js also accepts file paths.
- Browser callers must use `pdf-crab-js/browser` and pass image/font bytes.
- The default browser/WASM entry is now single-threaded and does not require cross-origin isolation.
- `pdf-crab-js/browser/threaded` is available for isolated deployments.
- Public failures now use typed `PdfError` codes.
- Removed legacy names have no aliases in the public package.

See [MIGRATION.md](./MIGRATION.md) for the concise migration checklist.

## pdf-crab-js vs PDFKit

### API and scope

| Area               | pdf-crab-js                                              | PDFKit                          |
| ------------------ | -------------------------------------------------------- | ------------------------------- |
| Runtime            | Rust core through NAPI-RS or WASM                        | JavaScript                      |
| TypeScript API     | First-party typed inputs and generic tables              | Stream-oriented document API    |
| Coordinates        | Top-left; configurable `mm` or `pt`                      | Top-left; points                |
| Output             | `Uint8Array`, `Blob`, Web stream, async iterable         | Node.js readable stream         |
| Tables             | Typed `table<Row>()` with measured widths and pagination | High-level `document.table()`   |
| Browser            | Dedicated single-threaded and threaded WASM entries      | Browser-bundle workflow         |
| Feature philosophy | Small structured-document contract                       | Broader, mature drawing surface |

Choose pdf-crab-js when typed structured documents, predictable pagination, browser/Node parity,
and throughput are the priority. PDFKit remains a strong choice when an application depends on its
broader drawing surface or existing PDFKit-specific integrations.

### Benchmark snapshot

The benchmark keeps declarative/manual drawing and high-level table APIs in separate rankings. It
also compares buffered output only with buffered output, and fully consumed streams only with
streams. Each scenario runs in its own child process for peak RSS sampling.

In a 1,000-page reference run with 10 rows per page, 3 warmups, and 10 measured runs on Node.js
24.19.0/Darwin arm64, the results were:

| Comparable workload | Buffered speedup | Stream speedup | Lower peak RSS | Smaller PDF |
| ------------------- | ---------------: | -------------: | -------------: | ----------: |
| Declarative/manual  |        **1.24x** |      **1.20x** |     **30-43%** |   **17.5%** |
| High-level table    |        **1.93x** |      **1.95x** |     **40-45%** |   **10.3%** |

Results are environment-dependent. See the
[benchmark documentation](../../benchmarks/pdf/README.md) for the full p50/p95, throughput, memory,
artifact-size, methodology, and reproduction details.

### Reproduce the benchmark

From the repository root:

```bash
PDF_BENCHMARK_PAGES=1000 \
PDF_BENCHMARK_ONLY=pdf-crab,pdf-crab-stream,pdf-crab-document,pdf-crab-document-stream,pdfkit,pdfkit-stream,pdfkit-table,pdfkit-table-stream \
pnpm --filter pdf-benchmark benchmark
```

Use `PDF_BENCHMARK_RUNS`, `PDF_BENCHMARK_WARMUP`, and
`PDF_BENCHMARK_MEMORY_SAMPLE_MS` to tune the run.

## Current scope

Version 1.0 focuses on structured PDFs. The following are intentionally outside the current public
contract:

- Arabic/RTL and other bidi-heavy scripts.
- Forms, encryption, PDF/A, accessibility tags, and outlines.
- SVG input, SVG paths, color fonts, and a PDFKit compatibility adapter.
- GIF, WebP, data URL, HTTP URL, and `Blob` image sources.

## Examples and development

Runnable examples:

- [Declarative PDF](../../examples/pdf-crab-js/src/node/declarative.ts)
- [Fluent table](../../examples/pdf-crab-js/src/node/table.ts)
- [PNG image](../../examples/pdf-crab-js/src/node/image.ts)
- [Node.js and browser streaming](../../examples/pdf-crab-js/stream/README.md)
- [Browser/WASM](../../examples/pdf-crab-js/src/browser/declarative.ts)
- [Interactive Vite browser studio](../../examples/wasm-samples/README.md)

Run them from the repository root:

```bash
pnpm --filter pdf-crab-js-examples example:declarative
pnpm --filter pdf-crab-js-examples example:table
pnpm --filter pdf-crab-js-examples example:image
pnpm --filter pdf-crab-js-examples example:stream
```

Package development:

```bash
pnpm --filter pdf-crab-js build
pnpm --filter pdf-crab-js test
pnpm --filter pdf-crab-js build:wasm
pnpm --filter pdf-crab-js test:wasm
```

The release pipeline also runs `pnpm --filter pdf-crab-js test:package` after gathering the native and
WASM artifacts for all configured targets. This prepares and packs a temporary copy without publishing,
then installs the tarballs in an isolated consumer. It checks ESM, CommonJS, the Workerd loader lifecycle,
and production builds of both browser entry points without relying on workspace dependencies.

Production runtime dependencies use explicit versions so that the `npm publish` workflow ships valid
dependency specifiers. Keep those versions aligned with the WASM toolchain in `pnpm-workspace.yaml`.
