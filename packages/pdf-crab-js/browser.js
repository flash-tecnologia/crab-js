import { Buffer as __Buffer } from 'buffer'

globalThis.Buffer ??= __Buffer

const __binding = await import('pdf-crab-js-wasm32-wasi')

export default __binding.default ?? __binding
export const createPdf = __binding.createPdf
export const createPdfAsync = __binding.createPdfAsync
export const createPdfFromHtmlWithFulgur = __binding.createPdfFromHtmlWithFulgur
export const PdfDocumentBuilder = __binding.PdfDocumentBuilder
