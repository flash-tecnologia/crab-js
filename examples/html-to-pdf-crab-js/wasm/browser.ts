import type { CreatePdfFromHtmlInput } from 'html-to-pdf-crab-js/browser.js'
import { Buffer as BrowserBuffer } from 'buffer'
import './browser.css'

type ElementConstructor<T extends Element> = new (...args: never[]) => T
type BrowserCreatePdfFromHtmlInput = Omit<CreatePdfFromHtmlInput, 'fonts' | 'images'> & {
  fonts?: Uint8Array[]
  images?: {
    name: string
    data: Uint8Array
  }[]
}
type WasmCreatePdfFromHtmlInput = Omit<CreatePdfFromHtmlInput, 'fonts' | 'images'> & {
  fonts?: string[]
  images?: {
    name: string
    data: string
  }[]
}
type CreatePdfFromHtmlWasi = (input: WasmCreatePdfFromHtmlInput) => Promise<Uint8Array> | Uint8Array

globalThis.Buffer ??= BrowserBuffer

const { createPdfFromHtml: createPdfFromHtmlWasi } = await import('html-to-pdf-crab-js/browser.js')

function isCreatePdfFromHtmlWasi(value: unknown): value is CreatePdfFromHtmlWasi {
  return typeof value === 'function'
}

function getCreatePdfFromHtmlBinding(): CreatePdfFromHtmlWasi {
  if (!isCreatePdfFromHtmlWasi(createPdfFromHtmlWasi)) {
    throw new TypeError('WASI browser binding is missing createPdfFromHtml')
  }

  return createPdfFromHtmlWasi
}

const createPdfFromHtmlBinding = getCreatePdfFromHtmlBinding()

function requiredElement<T extends Element>(selector: string, constructor: ElementConstructor<T>): T {
  const element = document.querySelector(selector)

  if (!(element instanceof constructor)) {
    throw new Error(`Browser example markup is missing ${selector}`)
  }

  return element
}

const renderButton = requiredElement('#render-pdf', HTMLButtonElement)
const downloadLink = requiredElement('#download-pdf', HTMLAnchorElement)
const pdfPreview = requiredElement('#pdf-preview', HTMLIFrameElement)
const pdfSize = requiredElement('#pdf-size', HTMLElement)
const status = requiredElement('#status', HTMLElement)

let currentPdfUrl: string | undefined

async function readText(path: string, accept: string): Promise<string> {
  const response = await fetch(path, {
    cache: 'no-store',
    headers: {
      Accept: accept,
    },
  })

  if (!response.ok) {
    throw new Error(`Failed to fetch ${path}: ${response.status} ${response.statusText}`)
  }

  return response.text()
}

async function readBuffer(path: string): Promise<Uint8Array> {
  const response = await fetch(path, { cache: 'no-store' })

  if (!response.ok) {
    throw new Error(`Failed to fetch ${path}: ${response.status} ${response.statusText}`)
  }

  return new Uint8Array(await response.arrayBuffer())
}

function setStatus(message: string): void {
  status.textContent = message
}

function setDownloadUrl(url: string): void {
  downloadLink.href = url
  downloadLink.download = 'html-to-pdf-report-browser-example.pdf'
  downloadLink.setAttribute('aria-disabled', 'false')
}

function createPdfBlob(pdf: Uint8Array): Blob {
  const arrayBuffer = new ArrayBuffer(pdf.byteLength)
  new Uint8Array(arrayBuffer).set(pdf)

  return new Blob([arrayBuffer], { type: 'application/pdf' })
}

function htmlForPdf(input: string): string {
  return input.replace('<link rel="stylesheet" href="./report.css" />', '')
}

function toBase64(value: Uint8Array): string {
  let binary = ''

  for (const byte of value) {
    binary += String.fromCodePoint(byte)
  }

  return btoa(binary)
}

function normalizeInput(input: BrowserCreatePdfFromHtmlInput): WasmCreatePdfFromHtmlInput {
  return {
    ...input,
    fonts: input.fonts?.map(toBase64),
    images: input.images?.map((image) => ({
      ...image,
      data: toBase64(image.data),
    })),
  }
}

async function createPdfFromHtml(input: BrowserCreatePdfFromHtmlInput): Promise<Uint8Array> {
  const pdf = await createPdfFromHtmlBinding(normalizeInput(input))

  if (!(pdf instanceof Uint8Array)) {
    throw new TypeError('WASI browser binding returned a non-binary result')
  }

  return pdf
}

async function renderPdf(): Promise<void> {
  renderButton.disabled = true
  setStatus('Rendering report.html with the WASM package...')

  try {
    const [html, css, font] = await Promise.all([
      readText('../report.html', 'text/html'),
      readText('../report.css', 'text/css'),
      readBuffer('../assets/Tuffy.ttf'),
    ])

    const pdf = await createPdfFromHtml({
      css,
      fonts: [font],
      html: htmlForPdf(html),
      page: {
        landscape: true,
        margin: 10,
        size: 'A4',
      },
      systemFonts: false,
      tagged: true,
      title: 'HTML-to-PDF Rendering Report (Browser WASM)',
    })

    if (currentPdfUrl) {
      URL.revokeObjectURL(currentPdfUrl)
    }

    currentPdfUrl = URL.createObjectURL(createPdfBlob(pdf))
    pdfPreview.src = currentPdfUrl
    pdfSize.textContent = `${pdf.byteLength.toLocaleString()} bytes`
    setDownloadUrl(currentPdfUrl)
    setStatus('Rendered with html-to-pdf-crab-js/wasm.')
  } catch (error) {
    setStatus(error instanceof Error ? error.message : 'Failed to render PDF')
  } finally {
    renderButton.disabled = false
  }
}

if (!globalThis.crossOriginIsolated) {
  setStatus('This browser example requires COOP/COEP headers for SharedArrayBuffer. Use the package browser script.')
} else {
  renderButton.addEventListener('click', () => {
    renderPdf().catch((error: unknown) => {
      setStatus(error instanceof Error ? error.message : 'Failed to render PDF')
    })
  })
  renderPdf().catch((error: unknown) => {
    setStatus(error instanceof Error ? error.message : 'Failed to render PDF')
  })
}
