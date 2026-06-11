const PACKAGE_VERSION = '0.2.0'
const RUNTIME_URL = 'https://esm.sh/@napi-rs/wasm-runtime@1.1.4?target=es2022'
const PDF_CRAB_WASM_URL = `https://cdn.jsdelivr.net/npm/pdf-crab-js-wasm32-wasi@${PACKAGE_VERSION}/pdf-crab-js.wasm32-wasi.wasm`
const HTML_TO_PDF_WASM_URL = `https://cdn.jsdelivr.net/npm/html-to-pdf-crab-js-wasm32-wasi@${PACKAGE_VERSION}/html-to-pdf-crab-js.wasm32-wasi.wasm`
const HTML_TO_PDF_FONT_URL = './assets/Tuffy.ttf'
const HASH_BY_MODE = {
  fast: 'pdf-crab-js',
  html: 'html-to-pdf-crab-js',
}
const MODE_BY_HASH = new Map(Object.entries(HASH_BY_MODE).map(([mode, hash]) => [hash, mode]))

const defaultHtml = `<article class="report">
  <header class="hero">
    <div>
      <p class="eyebrow">Quarterly platform brief</p>
      <h1>Crab JS PDF Libraries</h1>
      <p class="subtitle">A richer HTML document rendered in-browser with html-to-pdf-crab-js.</p>
    </div>
    <aside class="stamp">
      <span>Q2</span>
      <strong>2026</strong>
    </aside>
  </header>

  <section class="meta-row">
    <div>
      <span>Prepared for</span>
      <strong>Flash Tecnologia</strong>
    </div>
    <div>
      <span>Generated on</span>
      <strong>2026-05-27</strong>
    </div>
    <div>
      <span>Runtime</span>
      <strong>WASM + NAPI-RS</strong>
    </div>
  </section>

  <section class="metric-row">
    <article>
      <span>Bundle size</span>
      <strong>10 KB</strong>
      <p>PDF output for this report sample.</p>
    </article>
    <article>
      <span>Render mode</span>
      <strong>HTML</strong>
      <p>Semantic markup with custom CSS and embedded font.</p>
    </article>
    <article>
      <span>Deploy target</span>
      <strong>Netlify</strong>
      <p>COOP and COEP headers enabled for SharedArrayBuffer.</p>
    </article>
  </section>

  <section class="split">
    <div class="panel">
      <h2>Highlights</h2>
      <ul>
        <li>Rich HTML layout with cards, tables, badges, and nested sections.</li>
        <li>Browser WASM rendering with a bundled TrueType font.</li>
        <li>Works as a static Netlify deploy with no server runtime.</li>
      </ul>
    </div>
    <div class="panel accent-panel">
      <h2>Delivery notes</h2>
      <p>Use <strong>pdf-crab-js</strong> for fast structured PDF documents and <strong>html-to-pdf-crab-js</strong> when the source already exists as HTML and CSS.</p>
      <div class="tags">
        <span>WASM</span>
        <span>PDF</span>
        <span>HTML</span>
      </div>
    </div>
  </section>

  <section>
    <h2>Capability matrix</h2>
    <table>
      <thead>
        <tr>
          <th>Package</th>
          <th>Best fit</th>
          <th>Input</th>
          <th>Status</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <td>pdf-crab-js</td>
          <td>Fast structured PDFs</td>
          <td>Typed document model</td>
          <td><span class="status-pill">Fast path</span></td>
        </tr>
        <tr>
          <td>html-to-pdf-crab-js</td>
          <td>HTML reports and invoices</td>
          <td>HTML, CSS, fonts, images</td>
          <td><span class="status-pill">Rich path</span></td>
        </tr>
      </tbody>
    </table>
  </section>

  <section class="timeline">
    <h2>Render flow</h2>
    <ol>
      <li><strong>Load WASM</strong><span>Fetch package binary and runtime.</span></li>
      <li><strong>Prepare assets</strong><span>Encode the bundled font for the browser binding.</span></li>
      <li><strong>Create PDF</strong><span>Render HTML and CSS into a downloadable Blob.</span></li>
    </ol>
  </section>

  <footer class="report-footer">
    <span>github.com/flash-tecnologia/crab-js</span>
    <strong>Generated with html-to-pdf-crab-js ${PACKAGE_VERSION}</strong>
  </footer>
</article>`

const defaultCss = `body {
  margin: 0;
  font-family: Tuffy, Arial, Helvetica, sans-serif;
  color: #172033;
  background: #ffffff;
}

.report {
  padding: 30px;
}

.hero {
  display: flex;
  justify-content: space-between;
  gap: 24px;
  padding: 24px;
  background: #172033;
  color: #ffffff;
}

.eyebrow,
.meta-row span,
.metric-row span,
.report-footer span {
  color: #8ea0ba;
  font-size: 10px;
  font-weight: 700;
  letter-spacing: 0;
  margin: 0 0 6px;
  text-transform: uppercase;
}

h1 {
  margin: 0;
  font-size: 34px;
}

h2 {
  margin: 0 0 12px;
  font-size: 15px;
}

.subtitle {
  color: #d9e4f2;
  font-size: 12px;
  margin: 8px 0 0;
}

.stamp {
  min-width: 86px;
  border: 1px solid #6f84a3;
  padding: 14px;
  text-align: center;
}

.stamp span,
.stamp strong {
  display: block;
}

.stamp strong {
  font-size: 24px;
}

.meta-row,
.metric-row,
.split {
  display: flex;
  gap: 12px;
}

.meta-row {
  margin: 18px 0;
}

.meta-row div,
.metric-row article,
.panel {
  border: 1px solid #d9e1ec;
  padding: 14px;
}

.meta-row div,
.metric-row article {
  flex: 1;
}

.meta-row strong,
.metric-row strong {
  display: block;
  color: #172033;
  font-size: 16px;
}

.metric-row article {
  background: #f7f9fc;
}

.metric-row p,
.panel p,
li,
.timeline span {
  color: #526071;
  font-size: 10px;
  line-height: 1.45;
}

.split {
  margin: 18px 0;
}

.panel {
  flex: 1;
}

.accent-panel {
  background: #eef4ff;
  border-color: #b9c8ff;
}

ul {
  margin: 0;
  padding-left: 18px;
}

.tags {
  display: flex;
  gap: 6px;
  margin-top: 12px;
}

.tags span,
.status-pill {
  display: inline-block;
  border-radius: 999px;
  background: #e8f6ef;
  color: #126044;
  font-size: 9px;
  font-weight: 700;
  padding: 4px 8px;
}

table {
  width: 100%;
  border-collapse: collapse;
  font-size: 10px;
}

th {
  background: #172033;
  color: #ffffff;
  padding: 9px;
  text-align: left;
}

td {
  border-bottom: 1px solid #d9e1ec;
  padding: 9px;
}

.timeline {
  margin-top: 18px;
}

.timeline ol {
  margin: 0;
  padding-left: 20px;
}

.timeline li {
  margin-bottom: 8px;
}

.timeline strong,
.timeline span {
  display: block;
}

.report-footer {
  display: flex;
  justify-content: space-between;
  gap: 16px;
  margin-top: 20px;
  border-top: 2px solid #172033;
  padding-top: 12px;
  font-size: 10px;
}`

const elements = {
  accentColor: document.getElementById('accentColor'),
  badgeText: document.getElementById('badgeText'),
  cssInput: document.getElementById('cssInput'),
  customerName: document.getElementById('customerName'),
  docNote: document.getElementById('docNote'),
  docSubtitle: document.getElementById('docSubtitle'),
  docTitle: document.getElementById('docTitle'),
  downloadLink: document.getElementById('downloadLink'),
  fastForm: document.querySelector('[data-panel="fast"]'),
  footerText: document.getElementById('footerText'),
  generateActions: [...document.querySelectorAll('.generate-action')],
  htmlForm: document.querySelector('[data-panel="html"]'),
  htmlInput: document.getElementById('htmlInput'),
  htmlPreviewFrame: document.getElementById('htmlPreviewFrame'),
  itemOneAmount: document.getElementById('itemOneAmount'),
  itemOneLabel: document.getElementById('itemOneLabel'),
  itemTwoAmount: document.getElementById('itemTwoAmount'),
  itemTwoLabel: document.getElementById('itemTwoLabel'),
  modePanels: [...document.querySelectorAll('[data-panel]')],
  modeTabs: [...document.querySelectorAll('[data-mode]')],
  openButton: document.getElementById('openButton'),
  outputTitle: document.getElementById('outputTitle'),
  pageMargin: document.getElementById('pageMargin'),
  pageSize: document.getElementById('pageSize'),
  pdfMobileFallback: document.getElementById('pdfMobileFallback'),
  pdfPanel: document.querySelector('.pdf-panel'),
  pdfPreview: document.getElementById('pdfPreview'),
  runtimeBadge: document.getElementById('runtimeBadge'),
  status: document.getElementById('status'),
}

let currentMode = getModeFromHash() || 'fast'
let currentPdfUrl
let htmlToPdfExportsPromise
let htmlToPdfFontPromise
let pdfCrabExportsPromise

elements.htmlInput.value = defaultHtml
elements.cssInput.value = defaultCss

function setStatus(message, variant = 'info') {
  elements.status.textContent = message
  elements.status.classList.toggle('is-error', variant === 'error')
}

function setRuntimeState() {
  const ready = globalThis.crossOriginIsolated === true && typeof SharedArrayBuffer === 'function'

  elements.runtimeBadge.textContent = ready ? 'Cross-origin isolated' : 'Needs COOP/COEP'
  elements.runtimeBadge.classList.toggle('is-ready', ready)
  elements.runtimeBadge.classList.toggle('is-blocked', !ready)

  for (const action of elements.generateActions) {
    action.disabled = !ready
  }

  if (!ready) {
    setStatus(
      'This threaded WASM build needs SharedArrayBuffer. Deploy with Cross-Origin-Opener-Policy: same-origin and Cross-Origin-Embedder-Policy: require-corp.',
      'error',
    )
  }

  return ready
}

function getModeFromHash(hash = window.location.hash) {
  const anchor = decodeURIComponent(hash.replace(/^#/, '')).toLowerCase()
  return MODE_BY_HASH.get(anchor)
}

function syncHash(mode) {
  const nextHash = `#${HASH_BY_MODE[mode]}`

  if (window.location.hash !== nextHash) {
    window.history.pushState(null, '', nextHash)
  }
}

function switchMode(mode, options = {}) {
  const nextMode = mode === 'html' ? 'html' : 'fast'
  currentMode = nextMode

  for (const tab of elements.modeTabs) {
    const active = tab.dataset.mode === nextMode
    tab.classList.toggle('is-active', active)
    tab.setAttribute('aria-selected', String(active))

    if (active) {
      tab.setAttribute('aria-current', 'page')
    } else {
      tab.removeAttribute('aria-current')
    }
  }

  for (const panel of elements.modePanels) {
    panel.classList.toggle('is-active', panel.dataset.panel === nextMode)
  }

  elements.outputTitle.textContent = nextMode === 'fast' ? 'pdf-crab-js output' : 'html-to-pdf-crab-js output'

  if (options.updateHash) {
    syncHash(nextMode)
  }

  if (setRuntimeState()) {
    setStatus(nextMode === 'fast' ? 'Ready to generate with pdf-crab-js.' : 'Ready to render with html-to-pdf-crab-js.')
  }
}

function refreshHtmlPreview() {
  elements.htmlPreviewFrame.srcdoc = `<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <style>${elements.cssInput.value}</style>
  </head>
  <body>${elements.htmlInput.value}</body>
</html>`
}

function makeWorkerUrl() {
  const workerSource = `
    import { instantiateNapiModuleSync, MessageHandler, WASI } from '${RUNTIME_URL}';

    const handler = new MessageHandler({
      onLoad({ wasmModule, wasmMemory }) {
        const wasi = new WASI({
          print() {
            console.log.apply(console, arguments);
          },
          printErr() {
            console.error.apply(console, arguments);
          },
        });

        return instantiateNapiModuleSync(wasmModule, {
          childThread: true,
          wasi,
          overwriteImports(importObject) {
            importObject.env = {
              ...importObject.env,
              ...importObject.napi,
              ...importObject.emnapi,
              memory: wasmMemory,
            };
          },
        });
      },
    });

    globalThis.onmessage = (event) => {
      handler.handle(event);
    };
  `

  return URL.createObjectURL(new Blob([workerSource], { type: 'text/javascript' }))
}

function createSharedMemory() {
  return new WebAssembly.Memory({
    initial: 4000,
    maximum: 65536,
    shared: true,
  })
}

function wireImports(importObject, sharedMemory) {
  importObject.env = {
    ...importObject.env,
    ...importObject.napi,
    ...importObject.emnapi,
    memory: sharedMemory,
  }
  return importObject
}

function registerNapiExports(instance) {
  for (const name of Object.keys(instance.exports)) {
    if (name.startsWith('__napi_register__')) {
      instance.exports[name]()
    }
  }
}

async function fetchWasm(url, packageName) {
  const response = await fetch(url)

  if (!response.ok) {
    throw new Error(`Unable to download ${packageName} WASM: ${response.status}`)
  }

  return response.arrayBuffer()
}

async function fetchBytes(url, label) {
  const response = await fetch(url, { cache: 'no-store' })

  if (!response.ok) {
    throw new Error(`Unable to download ${label}: ${response.status}`)
  }

  return new Uint8Array(await response.arrayBuffer())
}

function toBase64(bytes) {
  let binary = ''

  for (const byte of bytes) {
    binary += String.fromCodePoint(byte)
  }

  return btoa(binary)
}

async function loadHtmlToPdfFont() {
  if (!htmlToPdfFontPromise) {
    htmlToPdfFontPromise = fetchBytes(HTML_TO_PDF_FONT_URL, 'Tuffy.ttf').then(toBase64)
  }

  return htmlToPdfFontPromise
}

async function loadPdfCrab() {
  if (pdfCrabExportsPromise) {
    return pdfCrabExportsPromise
  }

  pdfCrabExportsPromise = (async () => {
    const [{ getDefaultContext, instantiateNapiModuleSync, WASI }, wasmFile] = await Promise.all([
      import(RUNTIME_URL),
      fetchWasm(PDF_CRAB_WASM_URL, 'pdf-crab-js'),
    ])

    const wasi = new WASI({ version: 'preview1' })
    const sharedMemory = createSharedMemory()
    const workerUrl = makeWorkerUrl()

    const { napiModule } = instantiateNapiModuleSync(wasmFile, {
      context: getDefaultContext(),
      asyncWorkPoolSize: 4,
      wasi,
      onCreateWorker() {
        return new Worker(workerUrl, { type: 'module' })
      },
      overwriteImports(importObject) {
        return wireImports(importObject, sharedMemory)
      },
      beforeInit({ instance }) {
        registerNapiExports(instance)
      },
    })

    return napiModule.exports
  })()

  return pdfCrabExportsPromise
}

async function loadHtmlToPdfCrab() {
  if (htmlToPdfExportsPromise) {
    return htmlToPdfExportsPromise
  }

  htmlToPdfExportsPromise = (async () => {
    const [{ getDefaultContext, instantiateNapiModule, WASI }, wasmFile] = await Promise.all([
      import(RUNTIME_URL),
      fetchWasm(HTML_TO_PDF_WASM_URL, 'html-to-pdf-crab-js'),
    ])

    const wasi = new WASI({ version: 'preview1' })
    const sharedMemory = createSharedMemory()
    const workerUrl = makeWorkerUrl()

    const { napiModule } = await instantiateNapiModule(wasmFile, {
      context: getDefaultContext(),
      asyncWorkPoolSize: 4,
      wasi,
      onCreateWorker() {
        return new Worker(workerUrl, { type: 'module' })
      },
      overwriteImports(importObject) {
        return wireImports(importObject, sharedMemory)
      },
      beforeInit({ instance }) {
        registerNapiExports(instance)
      },
    })

    return napiModule.exports
  })()

  return htmlToPdfExportsPromise
}

function money(value) {
  return new Intl.NumberFormat('en-US', {
    currency: 'USD',
    maximumFractionDigits: 0,
    style: 'currency',
  }).format(value)
}

function readAmount(input) {
  const value = Number.parseFloat(input.value)
  return Number.isFinite(value) ? Math.max(0, value) : 0
}

function readText(input, fallback) {
  const value = input.value.trim()
  return value.length > 0 ? value : fallback
}

function buildFastDocumentInput() {
  const title = readText(elements.docTitle, 'WASM PDF sample')
  const subtitle = readText(elements.docSubtitle, 'Generated with WebAssembly')
  const customer = readText(elements.customerName, 'Netlify Preview')
  const note = readText(elements.docNote, 'Generated in the browser with pdf-crab-js.')
  const badge = readText(elements.badgeText, 'WASM')
  const footer = readText(elements.footerText, `npm: pdf-crab-js@${PACKAGE_VERSION}`)
  const itemOneLabel = readText(elements.itemOneLabel, 'WASM setup')
  const itemTwoLabel = readText(elements.itemTwoLabel, 'PDF generation usage')
  const accent = elements.accentColor.value
  const itemOneAmount = readAmount(elements.itemOneAmount)
  const itemTwoAmount = readAmount(elements.itemTwoAmount)
  const total = itemOneAmount + itemTwoAmount

  return {
    title,
    unit: 'mm',
    metadata: {
      title,
      author: 'pdf-crab-js',
      creator: `pdf-crab-js WASM ${PACKAGE_VERSION}`,
      subject: 'Browser-generated PDF sample',
      keywords: ['pdf-crab-js', 'wasm', 'netlify'],
    },
    pages: [
      {
        width: 210,
        height: 297,
        elements: [
          { type: 'rect', x: 0, y: 0, width: 210, height: 297, fill: '#ffffff' },
          { type: 'rect', x: 16, y: 244, width: 178, height: 34, fill: accent },
          { type: 'text', text: title, x: 24, y: 264, fontSize: 21, fill: '#ffffff' },
          { type: 'text', text: subtitle, x: 24, y: 253, fontSize: 9, fill: '#eef5ff' },
          { type: 'text', text: 'Bill to', x: 18, y: 225, fontSize: 10, fill: '#64748b' },
          { type: 'text', text: customer, x: 18, y: 214, fontSize: 17, fill: '#111827' },
          { type: 'line', x1: 18, y1: 198, x2: 192, y2: 198, stroke: '#cbd5e1', strokeWidth: 1 },
          { type: 'text', text: 'Item', x: 22, y: 184, fontSize: 10, fill: '#64748b' },
          { type: 'text', text: 'Amount', x: 152, y: 184, fontSize: 10, fill: '#64748b' },
          { type: 'text', text: itemOneLabel, x: 22, y: 170, fontSize: 12, fill: '#111827' },
          { type: 'text', text: money(itemOneAmount), x: 152, y: 170, fontSize: 12, fill: '#111827' },
          { type: 'text', text: itemTwoLabel, x: 22, y: 156, fontSize: 12, fill: '#111827' },
          { type: 'text', text: money(itemTwoAmount), x: 152, y: 156, fontSize: 12, fill: '#111827' },
          { type: 'rect', x: 118, y: 119, width: 74, height: 22, fill: '#f1f5f9' },
          { type: 'text', text: 'Total', x: 126, y: 132, fontSize: 10, fill: '#64748b' },
          { type: 'text', text: money(total), x: 152, y: 132, fontSize: 15, fill: '#111827' },
          {
            type: 'polygon',
            points: [
              { x: 18, y: 82 },
              { x: 54, y: 108 },
              { x: 90, y: 82 },
              { x: 54, y: 56 },
            ],
            fill: '#e8f6ef',
            stroke: '#1b7f5a',
            strokeWidth: 1,
            closed: true,
          },
          { type: 'text', text: badge, x: 39, y: 83, fontSize: 12, fill: '#126044' },
          {
            type: 'textBox',
            text: note,
            x: 104,
            y: 65,
            width: 84,
            height: 38,
            fontSize: 10,
            fill: '#334155',
            lineHeight: 13,
          },
          { type: 'line', x1: 18, y1: 34, x2: 192, y2: 34, stroke: '#cbd5e1', strokeWidth: 1 },
          { type: 'text', text: footer, x: 18, y: 22, fontSize: 8, fill: '#64748b' },
        ],
        annotations: [
          {
            type: 'link',
            x: 18,
            y: 17,
            width: 52,
            height: 10,
            url: 'https://github.com/flash-tecnologia/crab-js/tree/main/packages/pdf-crab-js',
            color: '#2f6fed',
          },
        ],
      },
    ],
  }
}

function getHtmlPageInput() {
  return {
    size: elements.pageSize.value,
    margin: Number(elements.pageMargin.value || 0),
  }
}

function copyPdfBytes(pdfBytes) {
  if (ArrayBuffer.isView(pdfBytes)) {
    const view = new Uint8Array(pdfBytes.buffer, pdfBytes.byteOffset, pdfBytes.byteLength)
    const safeBytes = new Uint8Array(view.byteLength)
    safeBytes.set(view)
    return safeBytes
  }

  if (pdfBytes instanceof ArrayBuffer) {
    return new Uint8Array(pdfBytes.slice(0))
  }

  return pdfBytes
}

function slugify(value, fallback) {
  return (
    value
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/^-|-$/g, '') || fallback
  )
}

function setPdfBlob(blob, title, fallbackName) {
  if (currentPdfUrl) {
    URL.revokeObjectURL(currentPdfUrl)
  }

  currentPdfUrl = URL.createObjectURL(blob)
  elements.pdfPanel.classList.add('has-pdf')
  elements.pdfMobileFallback.hidden = false
  elements.pdfPreview.src = currentPdfUrl
  elements.downloadLink.href = currentPdfUrl
  elements.downloadLink.download = `${slugify(title, fallbackName)}.pdf`
  elements.downloadLink.classList.remove('is-disabled')
  elements.openButton.disabled = false
}

async function generateFastPdf(event) {
  event.preventDefault()

  if (!setRuntimeState()) {
    return
  }

  const startedAt = performance.now()
  setStatus('Loading pdf-crab-js WASM...')

  try {
    const { createPdf } = await loadPdfCrab()
    const documentInput = buildFastDocumentInput()
    const pdfBytes = createPdf(documentInput)
    const blob = new Blob([copyPdfBytes(pdfBytes)], { type: 'application/pdf' })
    const elapsedMs = Math.round(performance.now() - startedAt)

    setPdfBlob(blob, documentInput.title, 'pdf-crab-js-sample')
    setStatus(`pdf-crab-js generated: ${blob.size.toLocaleString()} bytes in ${elapsedMs.toLocaleString()} ms.`)
  } catch (error) {
    pdfCrabExportsPromise = undefined
    setStatus(error instanceof Error ? error.message : 'Unable to generate the fast PDF.', 'error')
  }
}

async function generateHtmlPdf(event) {
  event.preventDefault()

  if (!setRuntimeState()) {
    return
  }

  const startedAt = performance.now()
  setStatus('Loading html-to-pdf-crab-js WASM...')

  try {
    const [{ createPdfFromHtml }, font] = await Promise.all([loadHtmlToPdfCrab(), loadHtmlToPdfFont()])
    setStatus('Rendering HTML into PDF...')

    const pdfBytes = await createPdfFromHtml({
      html: `<!doctype html><html><head><meta charset="utf-8"></head><body>${elements.htmlInput.value}</body></html>`,
      css: elements.cssInput.value,
      fonts: [font],
      title: 'html-to-pdf-crab-js demo',
      page: getHtmlPageInput(),
      systemFonts: false,
    })
    const blob = new Blob([copyPdfBytes(pdfBytes)], { type: 'application/pdf' })
    const elapsedMs = Math.round(performance.now() - startedAt)

    setPdfBlob(blob, 'html-to-pdf-crab-js demo', 'html-to-pdf-crab-js-sample')
    setStatus(`html-to-pdf-crab-js generated: ${blob.size.toLocaleString()} bytes in ${elapsedMs.toLocaleString()} ms.`)
  } catch (error) {
    htmlToPdfExportsPromise = undefined
    setStatus(error instanceof Error ? error.message : 'Unable to render the HTML PDF.', 'error')
  }
}

for (const tab of elements.modeTabs) {
  tab.addEventListener('click', (event) => {
    event.preventDefault()
    switchMode(tab.dataset.mode, { updateHash: true })
  })
}

window.addEventListener('hashchange', () => {
  const mode = getModeFromHash()

  if (mode) {
    switchMode(mode)
  }
})

elements.fastForm.addEventListener('submit', generateFastPdf)
elements.htmlForm.addEventListener('submit', generateHtmlPdf)
elements.htmlInput.addEventListener('input', refreshHtmlPreview)
elements.cssInput.addEventListener('input', refreshHtmlPreview)
elements.openButton.addEventListener('click', () => {
  if (currentPdfUrl) {
    window.open(currentPdfUrl, '_blank', 'noopener')
  }
})

setRuntimeState()
refreshHtmlPreview()
switchMode(currentMode)
