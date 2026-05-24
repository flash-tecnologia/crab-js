import { Buffer as __Buffer } from 'buffer'

globalThis.Buffer ??= __Buffer

const __binding = await import('pdf-html-crab-js-wasm32-wasi')

export default __binding.default ?? __binding
export const createPdfFromHtml = __binding.createPdfFromHtml
