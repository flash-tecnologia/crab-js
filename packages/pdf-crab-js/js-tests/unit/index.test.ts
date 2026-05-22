import { equal, match, ok, throws } from 'node:assert/strict'
import { test } from 'vite-plus/test'

import * as pdfCrab from '../../index.js'
import {
  createPdf,
  createPdfAsync,
  createPdfFromHtml,
  createPdfFromHtmlAsync,
  parsePdf,
  renderPdfPageToSvg,
  renderPdfPageToSvgAsync,
} from '../../index.js'

const PNG_1X1 = Buffer.from(
  'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4nGP4z8DwHwAFAAH/iZk9HQAAAABJRU5ErkJggg==',
  'base64',
)
const JPEG_1X1 = Buffer.from(
  '/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDAAMCAgICAgMCAgIDAwMDBAYEBAQEBAgGBgUGCQgKCgkICQkKDA8MCgsOCwkJDRENDg8QEBEQCgwSExIQEw8QEBD/2wBDAQMDAwQDBAgEBAgQCwkLEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBD/wAARCAABAAEDAREAAhEBAxEB/8QAFAABAAAAAAAAAAAAAAAAAAAACP/EABQQAQAAAAAAAAAAAAAAAAAAAAD/xAAVAQEBAAAAAAAAAAAAAAAAAAAHCf/EABQRAQAAAAAAAAAAAAAAAAAAAAD/2gAMAwEAAhEDEQA/ADoDFU3/2Q==',
  'base64',
)

function assertPdfBuffer(pdf: Buffer): void {
  ok(Buffer.isBuffer(pdf))
  equal(pdf.subarray(0, 5).toString('utf8'), '%PDF-')
  equal(pdf.toString('utf8').trimEnd().endsWith('%%EOF'), true)
}

function createPdfUnchecked(input: unknown): Buffer {
  return createPdf(input as never)
}

function createPdfFromHtmlUnchecked(input: unknown): Buffer {
  return createPdfFromHtml(input as never)
}

test('public API exports remain available from the unified NAPI boundary', () => {
  const publicApi = pdfCrab as Record<string, unknown>

  equal(typeof createPdf, 'function')
  equal(typeof createPdfAsync, 'function')
  equal(typeof createPdfFromHtml, 'function')
  equal(typeof createPdfFromHtmlAsync, 'function')
  equal(typeof parsePdf, 'function')
  equal(typeof renderPdfPageToSvg, 'function')
  equal(typeof renderPdfPageToSvgAsync, 'function')
  equal(publicApi.htmlToDocument, undefined)
  equal(publicApi.pdfToDocument, undefined)
  equal(publicApi.documentToPdf, undefined)
  equal(publicApi.resourcesForPage, undefined)
  equal(publicApi.pageToSvg, undefined)
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
})

test('createPdf maps PNG and JPEG image elements', () => {
  const pdf = createPdf({
    pages: [
      {
        width: 120,
        height: 120,
        elements: [
          { type: 'image', source: PNG_1X1, format: 'png', x: 10, y: 70, width: 30, height: 30 },
          { type: 'image', source: JPEG_1X1, format: 'jpeg', x: 50, y: 70, width: 30, height: 30 },
        ],
      },
    ],
  })

  assertPdfBuffer(pdf)
})

test('createPdf maps SVG, textBox, polygon, path, metadata, bookmarks, layers, and annotations', () => {
  const pdf = createPdf({
    title: 'Rich PDF',
    metadata: {
      author: 'pdf-crab-js',
      subject: 'feature coverage',
      keywords: ['pdf', 'printpdf'],
    },
    saveOptions: {
      optimize: true,
      subsetFonts: true,
      secure: true,
      imageOptimization: {
        quality: 0.8,
        format: 'auto',
      },
    },
    conformance: 'pdf1_3',
    bookmarks: [{ name: 'First page', pageIndex: 0 }],
    layers: [{ id: 'content', name: 'Content' }],
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
            type: 'svg',
            layer: 'content',
            svg: '<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20"><rect width="20" height="20" fill="red"/></svg>',
            x: 20,
            y: 230,
            width: 30,
            height: 30,
          },
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
  const parsed = parsePdf({ pdf })
  equal(parsed.pageCount, 1)
  equal(parsed.metadata.title, 'Rich PDF')
})

test('createPdfFromHtml returns a PDF buffer for printpdf HTML input', () => {
  const pdf = createPdfFromHtml({
    title: 'My PDF',
    html: `
        <html>
          <body style="padding:10mm">
            <p style="color: red; font-family: sans-serif;" data-chapter="1" data-subsection="First subsection">Hello!</p>
            <div style="width:200px;height:200px;background:red;" data-chapter="1" data-subsection="Second subsection">
              <p>World!</p>
            </div>
          </body>
        </html>
      `,
    pageWidth: 210,
    pageHeight: 297,
  })

  assertPdfBuffer(pdf)
}, 60_000)

test('parsePdf and renderPdfPageToSvg inspect a generated PDF', () => {
  const pdf = createPdf({
    pages: [
      {
        width: 210,
        height: 297,
        elements: [{ type: 'text', text: 'Render me', x: 20, y: 260 }],
      },
    ],
  })
  const parsed = parsePdf({ pdf })
  const svg = renderPdfPageToSvg({ pdf, pageIndex: 0, imageFormats: ['png', 'jpeg'] })

  equal(parsed.pageCount, 1)
  match(svg, /<svg/)
})

test('async APIs return PDF and SVG results', async () => {
  const pdf = await createPdfAsync({
    pages: [
      {
        width: 210,
        height: 297,
        elements: [{ type: 'text', text: 'Async PDF', x: 20, y: 260 }],
      },
    ],
  })
  const htmlPdf = await createPdfFromHtmlAsync({
    html: '<html><body><p>Async HTML PDF</p></body></html>',
    pageWidth: 120,
    pageHeight: 120,
  })
  const svg = await renderPdfPageToSvgAsync({ pdf, pageIndex: 0 })
  const parsed = parsePdf({ pdf })

  assertPdfBuffer(pdf)
  assertPdfBuffer(htmlPdf)
  match(svg, /<svg/)
  equal(parsed.pageCount, 1)
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

test('createPdfFromHtml validates missing html', () => {
  throws(
    () => createPdfFromHtmlUnchecked({}),
    (error) => {
      match((error as Error).message, /html is required/)
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
