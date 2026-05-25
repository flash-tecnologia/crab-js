import { equal, match, ok, throws } from 'node:assert/strict'
import { test } from 'vite-plus/test'

import * as pdfCrab from '../../index.js'
import { createPdf, createPdfAsync, PdfDocumentBuilder } from '../../index.js'

function assertPdfBuffer(pdf: Buffer): void {
  ok(Buffer.isBuffer(pdf))
  equal(pdf.subarray(0, 5).toString('utf8'), '%PDF-')
  equal(pdf.toString('utf8').trimEnd().endsWith('%%EOF'), true)
}

function createPdfUnchecked(input: unknown): Buffer {
  return createPdf(input as never)
}

test('public API exports expose the pdf-writer phase surface', () => {
  const publicApi = pdfCrab as Record<string, unknown>

  equal(typeof createPdf, 'function')
  equal(typeof createPdfAsync, 'function')
  equal(typeof PdfDocumentBuilder, 'function')
  equal(publicApi.createPdfFromHtml, undefined)
  equal(publicApi.createPdfFromHtmlWithFulgur, undefined)
  equal(publicApi.createPdfFromHtmlAsync, undefined)
  equal(publicApi.parsePdf, undefined)
  equal(publicApi.renderPdfPageToSvg, undefined)
  equal(publicApi.renderPdfPageToSvgAsync, undefined)
})

test('createPdf returns a PDF buffer for one page with text', () => {
  const pdf = createPdf({
    title: 'Hello PDF',
    pages: [
      {
        width: 210,
        height: 297,
        elements: [
          {
            type: 'text',
            text: 'Hello from pdf-crab-js',
            x: 20,
            y: 260,
          },
        ],
      },
    ],
  })

  assertPdfBuffer(pdf)
  match(pdf.toString('latin1'), /Hello from pdf-crab-js/)
})

test('createPdf maps text, line, and rectangle elements', () => {
  const pdf = createPdf({
    unit: 'pt',
    pages: [
      {
        width: 300,
        height: 300,
        elements: [
          {
            type: 'rect',
            x: 32,
            y: 80,
            width: 180,
            height: 96,
            fill: '#f3f4f6',
            stroke: '#111827',
            strokeWidth: 2,
          },
          {
            type: 'line',
            x1: 32,
            y1: 208,
            x2: 212,
            y2: 208,
            stroke: '#2563eb',
            strokeWidth: 1.5,
          },
          {
            type: 'text',
            text: 'Mixed elements',
            x: 40,
            y: 132,
            font: 'HelveticaBold',
            fontSize: 16,
            fill: '#111827',
          },
        ],
      },
    ],
  })

  assertPdfBuffer(pdf)
  const body = pdf.toString('latin1')
  match(body, / re\n/)
  match(body, / l\n/)
  match(body, /Mixed elements/)
})

test('createPdf maps textBox, polygon, path, metadata, and link annotations', () => {
  const pdf = createPdf({
    title: 'Rich PDF',
    metadata: {
      author: 'pdf-crab-js',
      subject: 'feature coverage',
      keywords: ['pdf', 'pdf-writer'],
      trapped: false,
    },
    pages: [
      {
        width: 210,
        height: 297,
        annotations: [
          {
            type: 'link',
            x: 20,
            y: 20,
            width: 50,
            height: 12,
            url: 'https://example.com',
          },
        ],
        elements: [
          {
            type: 'textBox',
            text: 'This wrapped text box proves line layout for a longer paragraph.',
            x: 20,
            y: 210,
            width: 70,
            height: 30,
            align: 'left',
            hyphenate: true,
          },
          {
            type: 'polygon',
            points: [
              { x: 120, y: 220 },
              { x: 160, y: 220 },
              { x: 140, y: 250 },
            ],
            fill: '#22c55e',
            stroke: '#14532d',
          },
          {
            type: 'path',
            points: [
              { x: 120, y: 200 },
              { x: 160, y: 205 },
              { x: 140, y: 190 },
            ],
            closed: false,
            stroke: '#2563eb',
          },
        ],
      },
    ],
  })

  assertPdfBuffer(pdf)
  const body = pdf.toString('latin1')
  match(body, /Rich PDF/)
  match(body, /pdf-writer/)
  match(body, /https:\/\/example\.com/)
  match(body, /This wrapped text/)
})

test('createPdfAsync returns a PDF buffer', async () => {
  const pdf = await createPdfAsync({
    pages: [
      {
        width: 210,
        height: 297,
        elements: [{ type: 'text', text: 'Async PDF', x: 20, y: 260 }],
      },
    ],
  })

  assertPdfBuffer(pdf)
  match(pdf.toString('latin1'), /Async PDF/)
})

test('PdfDocumentBuilder builds a PDF with chunked page elements', () => {
  const builder = new PdfDocumentBuilder({
    title: 'Builder PDF',
    metadata: {
      creator: 'builder test',
    },
  })

  builder.startPage({ width: 210, height: 297 })
  builder.appendElements([
    {
      type: 'text',
      text: 'Builder page',
      x: 20,
      y: 260,
      font: 'HelveticaBold',
    },
  ])
  builder.appendElements([
    {
      type: 'line',
      x1: 20,
      y1: 250,
      x2: 120,
      y2: 250,
      stroke: '#2563eb',
    },
  ])
  builder.appendAnnotations([
    {
      type: 'link',
      x: 20,
      y: 235,
      width: 40,
      height: 10,
      url: 'https://example.com/builder',
    },
  ])
  builder.endPage()

  const pdf = builder.finish()

  assertPdfBuffer(pdf)
  const body = pdf.toString('latin1')
  match(body, /Builder page/)
  match(body, /https:\/\/example\.com\/builder/)
})

test('PdfDocumentBuilder addPage, addPages, and finishAsync work', async () => {
  const builder = new PdfDocumentBuilder()

  builder.addPage({
    width: 210,
    height: 297,
    elements: [{ type: 'text', text: 'First builder page', x: 20, y: 260 }],
  })
  builder.addPages([
    {
      width: 210,
      height: 297,
      elements: [{ type: 'text', text: 'Second builder page', x: 20, y: 260 }],
    },
  ])

  const pdf = await builder.finishAsync()

  assertPdfBuffer(pdf)
  const body = pdf.toString('latin1')
  match(body, /First builder page/)
  match(body, /Second builder page/)
})

test('PdfDocumentBuilder validates state transitions', () => {
  const builder = new PdfDocumentBuilder()

  throws(
    () => builder.appendElements([]),
    (error) => {
      match((error as Error).message, /appendElements requires an open page/)
      return true
    },
  )
  throws(
    () => builder.finish(),
    (error) => {
      match((error as Error).message, /pages must contain at least one page/)
      return true
    },
  )
  throws(
    () => builder.startPage({ width: 210, height: 297 }),
    (error) => {
      match((error as Error).message, /already finished/)
      return true
    },
  )
})

test('PdfDocumentBuilder rejects nested pages and open-page finish', () => {
  const nestedBuilder = new PdfDocumentBuilder()
  nestedBuilder.startPage({ width: 210, height: 297 })

  throws(
    () => nestedBuilder.startPage({ width: 210, height: 297 }),
    (error) => {
      match((error as Error).message, /cannot start a new page/)
      return true
    },
  )
  throws(
    () => nestedBuilder.finish(),
    (error) => {
      match((error as Error).message, /cannot finish while a page is open/)
      return true
    },
  )
})

test('createPdf validates missing pages', () => {
  throws(
    () => createPdfUnchecked({}),
    (error) => {
      match((error as Error).message, /pages is required/)
      return true
    },
  )
})

test('createPdf validates page dimensions', () => {
  throws(
    () => createPdf({ pages: [{ width: 0, height: 297 }] }),
    (error) => {
      match((error as Error).message, /pages\[0\]\.width must be greater than 0/)
      return true
    },
  )
})

test('createPdf validates unknown element types', () => {
  throws(
    () =>
      createPdfUnchecked({
        pages: [
          {
            width: 210,
            height: 297,
            elements: [{ type: 'circle' }],
          },
        ],
      }),
    (error) => {
      match((error as Error).message, /pages\[0\]\.elements\[0\]\.type must be one of/)
      return true
    },
  )
})

test('createPdf validates missing required element fields', () => {
  throws(
    () =>
      createPdfUnchecked({
        pages: [
          {
            width: 210,
            height: 297,
            elements: [{ type: 'text', x: 20, y: 260 }],
          },
        ],
      }),
    (error) => {
      match((error as Error).message, /pages\[0\]\.elements\[0\]\.text is required/)
      return true
    },
  )
})

test('createPdf rejects bezier path points in the pdf-writer phase', () => {
  throws(
    () =>
      createPdfUnchecked({
        pages: [
          {
            width: 210,
            height: 297,
            elements: [
              {
                type: 'path',
                points: [
                  { x: 10, y: 10 },
                  { x: 20, y: 20, bezier: true },
                ],
              },
            ],
          },
        ],
      }),
    (error) => {
      match((error as Error).message, /bezier is not supported/)
      return true
    },
  )
})
