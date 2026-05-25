import { equal, ok } from 'node:assert/strict'

import { createPdfFromHtml } from '../index.js'

const htmlPdf = await createPdfFromHtml({
  html: '<!doctype html><html><body><h1>WASM HTML PDF smoke</h1><p>Rendered from HTML.</p></body></html>',
  page: {
    margin: 12,
    size: 'A4',
  },
})

ok(htmlPdf instanceof Uint8Array)
equal(Buffer.from(htmlPdf.subarray(0, 5)).toString('utf8'), '%PDF-')
equal(Buffer.from(htmlPdf).toString('utf8').trimEnd().endsWith('%%EOF'), true)
ok(htmlPdf.byteLength > 1000)
