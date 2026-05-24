import { Buffer as __Buffer } from 'buffer'

globalThis.Buffer ??= __Buffer

const __binding = await import('html-to-pdf-crab-js-wasm32-wasi')

export default __binding.default ?? __binding
export const createPdfFromHtml = __binding.createPdfFromHtml
