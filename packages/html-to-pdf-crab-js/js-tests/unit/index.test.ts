import { equal, match, ok, rejects } from 'node:assert/strict'
import { test } from 'vite-plus/test'

import * as pdfHtmlCrab from '../../index.js'
import { createPdfFromHtml } from '../../index.js'

function assertPdfBuffer(pdf: Buffer): void {
  ok(Buffer.isBuffer(pdf))
  equal(pdf.subarray(0, 5).toString('utf8'), '%PDF-')
  equal(pdf.toString('utf8').trimEnd().endsWith('%%EOF'), true)
}

test('public API exports expose only the HTML renderer', () => {
  const publicApi = pdfHtmlCrab as Record<string, unknown>

  equal(typeof createPdfFromHtml, 'function')
  equal(publicApi.createPdfFromHtmlWithFulgur, undefined)
  equal(publicApi.createPdf, undefined)
  equal(publicApi.createPdfAsync, undefined)
  equal(publicApi.PdfDocumentBuilder, undefined)
})

test('createPdfFromHtml returns a PDF buffer for simple HTML', async () => {
  const pdf = await createPdfFromHtml({
    html: '<!doctype html><html><body><h1>HTML PDF</h1><p>Hello from HTML.</p></body></html>',
    page: {
      margin: 12,
      size: 'A4',
    },
    title: 'HTML PDF',
  })

  assertPdfBuffer(pdf)
  ok(pdf.byteLength > 1000)
})

test('createPdfFromHtml accepts inline CSS', async () => {
  const pdf = await createPdfFromHtml({
    css: 'body { font-family: sans-serif; } h1 { color: #2563eb; }',
    html: '<!doctype html><html><body><main><h1>Styled HTML PDF</h1><p>Inline CSS smoke.</p></main></body></html>',
    page: {
      margin: { top: 24, right: 24, bottom: 24, left: 24, unit: 'pt' },
      size: { width: 300, height: 300, unit: 'pt' },
    },
  })

  assertPdfBuffer(pdf)
  ok(pdf.byteLength > 1000)
})

test('createPdfFromHtml validates blank HTML', async () => {
  await rejects(
    async () => createPdfFromHtml({ html: '   ' }),
    (error) => {
      match((error as Error).message, /html must not be empty/)
      return true
    },
  )
})
