import type { CreatePdfInput } from 'pdf-crab-js/browser.js'
import { Buffer as BrowserBuffer } from 'buffer'
import './browser.css'

type CreatePdfWasi = (input: CreatePdfInput) => Promise<Uint8Array> | Uint8Array
type ElementConstructor<T extends Element> = new (...args: never[]) => T

globalThis.Buffer ??= BrowserBuffer

const { createPdf: createPdfWasi } = await import('pdf-crab-js/browser.js')

function isCreatePdfWasi(value: unknown): value is CreatePdfWasi {
  return typeof value === 'function'
}

function getCreatePdfBinding(): CreatePdfWasi {
  if (!isCreatePdfWasi(createPdfWasi)) {
    throw new TypeError('WASI browser binding is missing createPdf')
  }

  return createPdfWasi
}

const createPdfBinding = getCreatePdfBinding()

const documentInput: CreatePdfInput = {
  metadata: {
    creator: 'pdf-crab-js WASM browser example',
    producer: 'pdf-crab-js',
    title: 'pdf-crab-js WASM browser example',
  },
  title: 'pdf-crab-js WASM browser example',
  unit: 'mm',
  pages: [
    {
      height: 297,
      width: 210,
      elements: [
        {
          fill: '#f8fafc',
          height: 34,
          stroke: '#0f172a',
          strokeWidth: 1,
          type: 'rect',
          width: 174,
          x: 18,
          y: 240,
        },
        {
          fill: '#0f172a',
          font: 'HelveticaBold',
          fontSize: 18,
          text: 'pdf-crab-js',
          type: 'text',
          x: 26,
          y: 260,
        },
        {
          fill: '#334155',
          fontSize: 10,
          text: 'Rendered in the browser with the NAPI-RS WASI build',
          type: 'text',
          x: 26,
          y: 250,
        },
        {
          stroke: '#2563eb',
          strokeWidth: 1.5,
          type: 'line',
          x1: 18,
          x2: 192,
          y1: 226,
          y2: 226,
        },
        {
          fill: '#eff6ff',
          height: 36,
          stroke: '#2563eb',
          strokeWidth: 1,
          type: 'rect',
          width: 72,
          x: 24,
          y: 170,
        },
        {
          fill: '#111827',
          fontSize: 12,
          text: 'Same createPdf input shape',
          type: 'text',
          x: 104,
          y: 192,
        },
        {
          fill: '#475569',
          fontSize: 9,
          text: 'The generated WASI browser entry loads pdf-crab-js.wasm32-wasi.wasm.',
          type: 'text',
          x: 104,
          y: 182,
        },
        {
          fill: '#16a34a',
          font: 'HelveticaBold',
          fontSize: 11,
          text: 'Browser WASM',
          type: 'text',
          x: 26,
          y: 186,
        },
      ],
    },
  ],
}

function requiredElement<T extends Element>(selector: string, constructor: ElementConstructor<T>): T {
  const element = document.querySelector(selector)

  if (!(element instanceof constructor)) {
    throw new Error(`Browser example markup is missing ${selector}`)
  }

  return element
}

const renderButton = requiredElement('#render-pdf', HTMLButtonElement)
const downloadLink = requiredElement('#download-pdf', HTMLAnchorElement)
const inputSource = requiredElement('#input-source', HTMLPreElement)
const inputSize = requiredElement('#input-size', HTMLElement)
const pdfPreview = requiredElement('#pdf-preview', HTMLIFrameElement)
const pdfSize = requiredElement('#pdf-size', HTMLElement)
const status = requiredElement('#status', HTMLElement)

let currentPdfUrl: string | undefined

function setStatus(message: string): void {
  status.textContent = message
}

function setDownloadUrl(url: string): void {
  downloadLink.href = url
  downloadLink.download = 'pdf-crab-js-browser-wasm-example.pdf'
  downloadLink.setAttribute('aria-disabled', 'false')
}

function createPdfBlob(pdf: Uint8Array): Blob {
  const arrayBuffer = new ArrayBuffer(pdf.byteLength)
  new Uint8Array(arrayBuffer).set(pdf)

  return new Blob([arrayBuffer], { type: 'application/pdf' })
}

async function createPdf(input: CreatePdfInput): Promise<Uint8Array> {
  const pdf = await createPdfBinding(input)

  if (!(pdf instanceof Uint8Array)) {
    throw new TypeError('WASI browser binding returned a non-binary result')
  }

  return pdf
}

async function renderPdf(): Promise<void> {
  renderButton.disabled = true
  setStatus('Rendering structured PDF input with the WASM package...')

  try {
    const pdf = await createPdf(documentInput)

    if (currentPdfUrl) {
      URL.revokeObjectURL(currentPdfUrl)
    }

    currentPdfUrl = URL.createObjectURL(createPdfBlob(pdf))
    pdfPreview.src = currentPdfUrl
    pdfSize.textContent = `${pdf.byteLength.toLocaleString()} bytes`
    setDownloadUrl(currentPdfUrl)
    setStatus('Rendered with pdf-crab-js/wasm.')
  } catch (error) {
    setStatus(error instanceof Error ? error.message : 'Failed to render PDF')
  } finally {
    renderButton.disabled = false
  }
}

const formattedInput = JSON.stringify(documentInput, null, 2)
inputSource.textContent = formattedInput
inputSize.textContent = `${new TextEncoder().encode(formattedInput).byteLength.toLocaleString()} bytes`

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
