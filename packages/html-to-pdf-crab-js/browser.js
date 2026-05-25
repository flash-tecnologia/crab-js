import { Buffer as __Buffer } from 'buffer'

globalThis.Buffer ??= __Buffer

const __binding = await import('html-to-pdf-crab-js-wasm32-wasi')

export default __binding.default ?? __binding

function toBase64(value) {
  return __Buffer.from(value).toString('base64')
}

function normalizeInput(input) {
  return {
    ...input,
    fonts: input.fonts?.map(toBase64),
    images: input.images?.map((image) => ({
      ...image,
      data: toBase64(image.data),
    })),
  }
}

export function createPdfFromHtml(input) {
  return Promise.resolve(__binding.createPdfFromHtml(normalizeInput(input)))
}
