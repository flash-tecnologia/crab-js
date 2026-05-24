import { equal, ok } from 'node:assert/strict'

import { createPdfAsync, createPdfFromHtmlWithFulgur } from '../index.js'

const pdf = await createPdfAsync({
  title: 'WASM PDF smoke',
  pages: [
    {
      width: 120,
      height: 120,
      elements: [{ type: 'text', text: 'WASM PDF smoke', x: 12, y: 90 }],
    },
  ],
})

ok(pdf instanceof Uint8Array)
equal(Buffer.from(pdf.subarray(0, 5)).toString('utf8'), '%PDF-')
equal(Buffer.from(pdf).toString('utf8').trimEnd().endsWith('%%EOF'), true)
ok(pdf.byteLength > 800)
ok(Buffer.from(pdf).toString('latin1').includes('WASM PDF smoke'))

const htmlPdf = await createPdfFromHtmlWithFulgur({
  html: '<!doctype html><html><body><h1>WASM Fulgur PDF smoke</h1><p>Rendered from HTML.</p></body></html>',
  page: {
    margin: 12,
    size: 'A4',
  },
})

ok(htmlPdf instanceof Uint8Array)
equal(Buffer.from(htmlPdf.subarray(0, 5)).toString('utf8'), '%PDF-')
equal(Buffer.from(htmlPdf).toString('utf8').trimEnd().endsWith('%%EOF'), true)
ok(htmlPdf.byteLength > 1000)
