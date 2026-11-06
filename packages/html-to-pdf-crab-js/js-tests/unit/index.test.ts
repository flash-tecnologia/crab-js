import { equal, match, ok, rejects } from 'node:assert/strict'
import { createRequire } from 'node:module'
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

test('CommonJS consumers can require the package and render HTML', async () => {
  const require = createRequire(import.meta.url)
  const { createPdfFromHtml: renderHtmlToPdf } = require('html-to-pdf-crab-js') as typeof pdfHtmlCrab

  const pdf = await renderHtmlToPdf({ html: '<h1>CommonJS PDF</h1>' })

  assertPdfBuffer(pdf)
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

test('createPdfFromHtml accepts a language for PDF/UA without an HTML lang attribute', async () => {
  const pdf = await createPdfFromHtml({
    html: '<!doctype html><html><body><h1>Language PDF</h1></body></html>',
    lang: 'pt-BR',
    pdfUa: true,
    title: 'Language PDF',
  })

  assertPdfBuffer(pdf)
  match(pdf.toString('latin1'), /\/Lang \(pt-BR\)/)
  match(pdf.toString('latin1'), /\/StructTreeRoot/)
})

test('createPdfFromHtml prioritizes the configured language over HTML lang', async () => {
  const pdf = await createPdfFromHtml({
    html: '<!doctype html><html lang="en"><body><p>Language override.</p></body></html>',
    lang: 'pt-BR',
  })

  assertPdfBuffer(pdf)
  match(pdf.toString('latin1'), /\/Lang \(pt-BR\)/)
})

test('createPdfFromHtml falls back to HTML lang when the configured language is omitted or blank', async () => {
  for (const lang of [undefined, '   ']) {
    const pdf = await createPdfFromHtml({
      html: '<!doctype html><html lang="en"><body><h1>HTML language</h1></body></html>',
      lang,
      pdfUa: true,
      title: 'HTML language',
    })

    assertPdfBuffer(pdf)
    match(pdf.toString('latin1'), /\/Lang \(en\)/)
  }
})

test('createPdfFromHtml rejects PDF/UA without a configured or HTML language', async () => {
  await rejects(
    async () =>
      createPdfFromHtml({
        html: '<!doctype html><html><body><h1>No language</h1></body></html>',
        pdfUa: true,
        title: 'No language',
      }),
    /PDF\/UA requires a document language/,
  )
})
