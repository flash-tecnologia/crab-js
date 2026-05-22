import { mkdirSync, writeFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

import { createPdf, createPdfFromHtml, type CreatePdfInput, type PdfElementInput } from 'pdf-crab-js'

const currentDirectory = dirname(fileURLToPath(import.meta.url))
const outputDirectory = join(currentDirectory, 'output')

const pageWidth = 210
const pageHeight = 297

const png1x1 = Buffer.from(
  'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4nGP4z8DwHwAFAAH/iZk9HQAAAABJRU5ErkJggg==',
  'base64',
)
const jpeg1x1 = Buffer.from(
  '/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDAAMCAgICAgMCAgIDAwMDBAYEBAQEBAgGBgUGCQgKCgkICQkKDA8MCgsOCwkJDRENDg8QEBEQCgwSExIQEw8QEBD/2wBDAQMDAwQDBAgEBAgQCwkLEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBD/wAARCAABAAEDAREAAhEBAxEB/8QAFAABAAAAAAAAAAAAAAAAAAAACP/EABQQAQAAAAAAAAAAAAAAAAAAAAD/xAAVAQEBAAAAAAAAAAAAAAAAAAAHCf/EABQRAQAAAAAAAAAAAAAAAAAAAAD/2gAMAwEAAhEDEQA/ADoDFU3/2Q==',
  'base64',
)

function writeExample(fileName: string, pdf: Buffer): void {
  mkdirSync(outputDirectory, { recursive: true })

  const outputPath = join(outputDirectory, fileName)
  writeFileSync(outputPath, pdf)

  console.log(`Generated ${outputPath}`)
  console.log(`Size: ${pdf.length.toLocaleString()} bytes`)
}

function writePdfExample(fileName: string, input: CreatePdfInput): void {
  writeExample(fileName, createPdf(input))
}

function writeHtmlExample(fileName: string, title: string, html: string): void {
  writeExample(
    fileName,
    createPdfFromHtml({
      title,
      html,
      pageWidth,
      pageHeight,
      marginTop: 20,
      marginRight: 15,
      marginBottom: 20,
      marginLeft: 15,
      showPageNumbers: true,
      skipFirstPage: false,
    }),
  )
}

function text(textValue: string, x: number, y: number, options: Partial<PdfElementInput> = {}): PdfElementInput {
  return {
    type: 'text',
    text: textValue,
    x,
    y,
    ...options,
  }
}

function textBox(
  textValue: string,
  x: number,
  y: number,
  width: number,
  options: Partial<PdfElementInput> = {},
): PdfElementInput {
  return {
    type: 'textBox',
    text: textValue,
    x,
    y,
    width,
    ...options,
  }
}

function rect(
  x: number,
  y: number,
  width: number,
  height: number,
  options: Partial<PdfElementInput> = {},
): PdfElementInput {
  return {
    type: 'rect',
    x,
    y,
    width,
    height,
    ...options,
  }
}

function line(x1: number, y1: number, x2: number, y2: number, options: Partial<PdfElementInput> = {}): PdfElementInput {
  return {
    type: 'line',
    x1,
    y1,
    x2,
    y2,
    ...options,
  }
}

function polygon(points: { x: number; y: number }[], options: Partial<PdfElementInput> = {}): PdfElementInput {
  return {
    type: 'polygon',
    points,
    ...options,
  }
}

function path(points: { x: number; y: number }[], options: Partial<PdfElementInput> = {}): PdfElementInput {
  return {
    type: 'path',
    points,
    ...options,
  }
}

function writeTextExample(): void {
  writePdfExample('printpdf-text-example.pdf', {
    title: 'Text Example',
    pages: [
      {
        width: pageWidth,
        height: pageHeight,
        elements: [
          text('Hello from Helvetica!', 20, 270, {
            font: 'Helvetica',
            fontSize: 24,
            fill: '#0000cc',
          }),
          text('This is Times Roman font', 20, 254, {
            font: 'TimesRoman',
            fontSize: 18,
            fill: '#cc0000',
          }),
          text('This line uses Courier as the built-in-font equivalent for the upstream custom-font step.', 20, 240, {
            font: 'Courier',
            fontSize: 11,
            fill: '#009900',
          }),
          textBox(
            'The upstream Rust example loads Roboto through ParsedFont. The JS declarative createPdf API intentionally uses built-in fonts, so this sample keeps the same text-style coverage without external font loading.',
            20,
            220,
            165,
            {
              fontSize: 10,
              lineHeight: 13,
              fill: '#334155',
              hyphenate: true,
            },
          ),
        ],
      },
    ],
  })
}

function writeShapesExample(): void {
  writePdfExample('printpdf-shapes-example.pdf', {
    title: 'Shapes and Graphics Example',
    pages: [
      {
        width: pageWidth,
        height: pageHeight,
        elements: [
          text('PDF Shapes and Graphics', 20, 280, { fontSize: 24, font: 'HelveticaBold' }),
          text('1. Filled Rectangle', 20, 260),
          rect(35, 240, 45, 16, { fill: '#cc3333' }),
          text('2. Outlined Rectangle', 20, 225),
          rect(35, 205, 45, 16, { stroke: '#3333cc', strokeWidth: 3 }),
          text('3. Filled and Outlined Rectangle', 20, 190),
          rect(35, 170, 45, 16, { fill: '#ffcc33', stroke: '#cc6600', strokeWidth: 2 }),
          text('4. Triangle', 20, 155),
          polygon(
            [
              { x: 35, y: 132 },
              { x: 80, y: 132 },
              { x: 57.5, y: 158 },
            ],
            { fill: '#33cc33', stroke: '#006600', strokeWidth: 2 },
          ),
          text('5. Complex Star-Like Shape', 105, 260),
          polygon(
            [
              { x: 145, y: 245 },
              { x: 153, y: 230 },
              { x: 170, y: 234 },
              { x: 160, y: 220 },
              { x: 170, y: 206 },
              { x: 145, y: 211 },
              { x: 120, y: 206 },
              { x: 130, y: 220 },
              { x: 120, y: 234 },
              { x: 137, y: 230 },
            ],
            { fill: '#9966cc', stroke: '#4c3380', strokeWidth: 2 },
          ),
          text('6. Solid lines', 105, 190),
          line(120, 174, 185, 174, { stroke: '#111827', strokeWidth: 2 }),
          line(120, 164, 185, 154, { stroke: '#2563eb', strokeWidth: 4 }),
          text('7. Path and even-odd style star', 20, 112),
          path(
            [
              { x: 35, y: 88 },
              { x: 55, y: 103 },
              { x: 75, y: 88 },
              { x: 95, y: 103 },
            ],
            { stroke: '#0f766e', strokeWidth: 3 },
          ),
          polygon(
            [
              { x: 150, y: 118 },
              { x: 162, y: 92 },
              { x: 190, y: 92 },
              { x: 168, y: 76 },
              { x: 178, y: 50 },
              { x: 150, y: 66 },
              { x: 122, y: 50 },
              { x: 132, y: 76 },
              { x: 110, y: 92 },
              { x: 138, y: 92 },
            ],
            { fill: '#cccc33', stroke: '#999900', strokeWidth: 1, winding: 'evenOdd' },
          ),
        ],
      },
    ],
  })
}

function writeImageExample(): void {
  writePdfExample('printpdf-image-example.pdf', {
    title: 'Image Example',
    pages: [
      {
        width: pageWidth,
        height: pageHeight,
        elements: [
          text('Image Example', 20, 275, { fontSize: 24, font: 'HelveticaBold' }),
          textBox(
            'The upstream Rust example places the same image twice, including transform options. The JS API exposes raster placement with explicit position and size.',
            20,
            258,
            170,
            { fontSize: 10, lineHeight: 13, fill: '#334155' },
          ),
          rect(18, 166, 76, 72, { fill: '#f8fafc', stroke: '#94a3b8' }),
          {
            type: 'image',
            source: png1x1,
            format: 'png',
            x: 30,
            y: 178,
            width: 52,
            height: 48,
          },
          text('PNG image source', 26, 154, { fontSize: 10 }),
          rect(112, 166, 76, 72, { fill: '#f8fafc', stroke: '#94a3b8' }),
          {
            type: 'image',
            source: jpeg1x1,
            format: 'jpeg',
            x: 124,
            y: 178,
            width: 52,
            height: 48,
          },
          text('JPEG image source', 120, 154, { fontSize: 10 }),
        ],
      },
    ],
  })
}

function writeSvgExample(): void {
  const tigerLikeSvg = `
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 180 120">
  <rect width="180" height="120" fill="#fff7ed"/>
  <circle cx="90" cy="58" r="38" fill="#f97316" stroke="#111827" stroke-width="4"/>
  <path d="M52 42 L75 50 L54 61 M128 42 L105 50 L126 61" stroke="#111827" stroke-width="8" fill="none"/>
  <circle cx="76" cy="57" r="5" fill="#111827"/>
  <circle cx="104" cy="57" r="5" fill="#111827"/>
  <path d="M78 82 Q90 92 102 82" stroke="#111827" stroke-width="4" fill="none"/>
</svg>`
  const cameraLikeSvg = `
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 180 120">
  <rect x="20" y="35" width="140" height="70" rx="8" fill="#334155"/>
  <rect x="45" y="20" width="48" height="25" rx="5" fill="#475569"/>
  <circle cx="92" cy="70" r="25" fill="#0f172a" stroke="#e2e8f0" stroke-width="8"/>
  <circle cx="92" cy="70" r="9" fill="#38bdf8"/>
  <rect x="126" y="48" width="18" height="9" rx="2" fill="#f8fafc"/>
</svg>`

  writePdfExample('printpdf-svg-example.pdf', {
    title: 'SVG Example',
    pages: [
      {
        width: pageWidth,
        height: pageHeight,
        elements: [
          text('SVG Example', 20, 275, { fontSize: 24, font: 'HelveticaBold' }),
          {
            type: 'svg',
            svg: tigerLikeSvg,
            x: 24,
            y: 170,
            width: 74,
            height: 58,
          },
          {
            type: 'svg',
            svg: cameraLikeSvg,
            x: 112,
            y: 170,
            width: 74,
            height: 58,
          },
          text('This PDF demonstrates embedding SVGs as vector graphics', 20, 145, {
            fontSize: 12,
            fill: '#334155',
          }),
        ],
      },
    ],
  })
}

function writeLayersExample(): void {
  writePdfExample('printpdf-layers-example.pdf', {
    title: 'Layers Example',
    layers: [
      { id: 'background', name: 'Background' },
      { id: 'text', name: 'Text Content' },
      { id: 'graphics', name: 'Graphics' },
    ],
    pages: [
      {
        width: pageWidth,
        height: pageHeight,
        elements: [
          rect(0, 0, pageWidth, pageHeight, { fill: '#f2f2f2', layer: 'background' }),
          text('PDF Layers Example', 20, 270, {
            fontSize: 24,
            fill: '#000099',
            font: 'HelveticaBold',
            layer: 'text',
          }),
          textBox(
            'This PDF demonstrates layers, also called optional content groups. The content is organized in background, text, and graphics layers.',
            20,
            250,
            165,
            { fontSize: 11, lineHeight: 14, layer: 'text' },
          ),
          rect(35, 65, 45, 22, { fill: '#ffcc00', stroke: '#cc6600', strokeWidth: 2, layer: 'graphics' }),
          polygon(
            [
              { x: 122, y: 65 },
              { x: 168, y: 65 },
              { x: 145, y: 100 },
            ],
            { fill: '#00b3b3', stroke: '#006666', strokeWidth: 2, layer: 'graphics' },
          ),
          line(180, 65, 195, 100, { stroke: '#990099', strokeWidth: 3, layer: 'graphics' }),
        ],
      },
    ],
  })
}

function writeBookmarksExample(): void {
  writePdfExample('printpdf-bookmarks-example.pdf', {
    title: 'Bookmarks and Annotations Example',
    bookmarks: [
      { name: 'Introduction', pageIndex: 0 },
      { name: 'Section 1: Documentation', pageIndex: 1 },
      { name: 'Section 2: Advanced Usage', pageIndex: 2 },
    ],
    pages: [
      {
        width: pageWidth,
        height: pageHeight,
        annotations: [
          { type: 'link', x: 35, y: 158, width: 110, height: 16, pageIndex: 1, top: 297 },
          { type: 'link', x: 35, y: 110, width: 110, height: 16, url: 'https://github.com/fschutt/printpdf' },
        ],
        elements: [
          text('Bookmarks and Annotations Example', 20, 278, {
            font: 'HelveticaBold',
            fontSize: 22,
            fill: '#0000b3',
          }),
          textBox(
            'Use the bookmarks panel in your PDF viewer to navigate through sections. This page also contains internal and external link annotations.',
            20,
            255,
            165,
            { fontSize: 11, lineHeight: 14 },
          ),
          rect(35, 158, 110, 16, { fill: '#e6e6ff', stroke: '#0000cc' }),
          text('Go to Section 1: Documentation', 40, 163, { fontSize: 11, fill: '#0000cc' }),
          rect(35, 110, 110, 16, { fill: '#e6ffe6', stroke: '#009900' }),
          text('Visit printpdf GitHub', 40, 115, { fontSize: 11, fill: '#007a00' }),
        ],
      },
      {
        width: pageWidth,
        height: pageHeight,
        annotations: [
          { type: 'link', x: 35, y: 158, width: 120, height: 16, pageIndex: 2, top: 297 },
          { type: 'link', x: 35, y: 110, width: 90, height: 16, pageIndex: 0, top: 297 },
        ],
        elements: [
          text('Section 1: Documentation', 20, 278, {
            font: 'HelveticaBold',
            fontSize: 22,
            fill: '#008000',
          }),
          textBox(
            'This page demonstrates internal document navigation. You arrived here by clicking a link on the previous page.',
            20,
            255,
            165,
            { fontSize: 11, lineHeight: 14 },
          ),
          rect(35, 158, 120, 16, { fill: '#ffe6ff', stroke: '#800080' }),
          text('Go to Section 2: Advanced Usage', 40, 163, { fontSize: 11, fill: '#800080' }),
          rect(35, 110, 90, 16, { fill: '#e6e6ff', stroke: '#0000cc' }),
          text('Back to Introduction', 40, 115, { fontSize: 11, fill: '#0000cc' }),
        ],
      },
      {
        width: pageWidth,
        height: pageHeight,
        annotations: [
          { type: 'link', x: 35, y: 158, width: 90, height: 16, pageIndex: 1, top: 297 },
          { type: 'link', x: 35, y: 110, width: 90, height: 16, pageIndex: 0, top: 297 },
        ],
        elements: [
          text('Section 2: Advanced Usage', 20, 278, {
            font: 'HelveticaBold',
            fontSize: 22,
            fill: '#b30000',
          }),
          textBox('The use of bookmarks and link annotations improves PDF navigation and usability.', 20, 255, 165, {
            fontSize: 11,
            lineHeight: 14,
          }),
          rect(35, 158, 90, 16, { fill: '#e6ffe6', stroke: '#008000' }),
          text('Back to Section 1', 40, 163, { fontSize: 11, fill: '#008000' }),
          rect(35, 110, 90, 16, { fill: '#e6e6ff', stroke: '#0000cc' }),
          text('Back to Introduction', 40, 115, { fontSize: 11, fill: '#0000cc' }),
        ],
      },
    ],
  })
}

function writeMultipageExample(): void {
  writePdfExample('printpdf-multipage-example.pdf', {
    title: 'Multi-page Example',
    bookmarks: [
      { name: 'Cover Page', pageIndex: 0 },
      { name: 'Content Page', pageIndex: 1 },
      { name: 'Graphics Page', pageIndex: 2 },
    ],
    pages: [
      {
        width: pageWidth,
        height: pageHeight,
        elements: [
          text('Multi-page PDF Example', 34, 162, {
            font: 'TimesBold',
            fontSize: 30,
            fill: '#334db3',
          }),
          text('Page 1: Cover Page', 74, 146, {
            font: 'TimesRoman',
            fontSize: 16,
            fill: '#4d4d4d',
          }),
        ],
      },
      {
        width: pageWidth,
        height: pageHeight,
        elements: [
          text('Page 2: Content Page', 20, 278, {
            font: 'HelveticaBold',
            fontSize: 24,
            fill: '#008000',
          }),
          text('This is the second page of our multi-page PDF document.', 20, 255, { fontSize: 12 }),
          text('It demonstrates how to create multiple pages with different content.', 20, 242, { fontSize: 12 }),
        ],
      },
      {
        width: pageWidth,
        height: pageHeight,
        elements: [
          text('Page 3: Graphics Page', 20, 278, {
            font: 'HelveticaBold',
            fontSize: 24,
            fill: '#b30000',
          }),
          rect(35, 165, 45, 22, { fill: '#cc4d4d' }),
          line(120, 185, 170, 160, { stroke: '#0000cc', strokeWidth: 3 }),
        ],
      },
    ],
  })
}

function writeBasicHtmlExample(): void {
  writeHtmlExample(
    'printpdf-html-example.pdf',
    'HTML Example',
    `
<html>
  <head>
    <style>
      .title { font-size: 24px; color: #333333; margin-bottom: 10px; }
      .content { font-size: 14px; color: #666666; padding: 20px; }
      .box { width: 200px; height: 100px; background-color: #e0e0e0; border: 1px solid #999999; }
    </style>
  </head>
  <body>
    <div class="title">Hello from Azul layout through printpdf</div>
    <div class="content">
      This is a TypeScript reproduction of the upstream HTML example using createPdfFromHtml.
    </div>
    <div class="box"></div>
  </body>
</html>`,
  )
}

function writeCssShapesExample(): void {
  writeHtmlExample(
    'printpdf-css-shapes-example.pdf',
    'CSS Shapes Example',
    `
<html>
  <head>
    <style>
      body { padding: 50px; font-family: Helvetica, sans-serif; }
      .title { font-size: 24px; color: #333333; margin-bottom: 20px; }
      .circle-box {
        width: 200px;
        height: 200px;
        border: 2px solid #999999;
        shape-inside: circle(100px at 100px 100px);
        font-size: 12px;
        color: #000000;
      }
      .ellipse-box {
        width: 300px;
        height: 200px;
        border: 2px solid #666666;
        shape-inside: ellipse(150px 100px at 150px 100px);
        font-size: 12px;
        color: #000000;
        margin-top: 20px;
      }
    </style>
  </head>
  <body>
    <div class="title">CSS Shapes Test: shape-inside Property</div>
    <div class="circle-box">
      Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore
      et dolore magna aliqua. Text should flow within a circular boundary.
    </div>
    <div class="ellipse-box">
      Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore
      et dolore magna aliqua. This text should flow within an elliptical boundary.
    </div>
  </body>
</html>`,
  )
}

function writeHtmlFullExample(): void {
  writeHtmlExample(
    'printpdf-html-full-example.pdf',
    'HTML Full Example',
    `
<html>
  <head>
    <style>
      body { font-family: Helvetica, sans-serif; color: #1f2937; }
      h1 { color: #1a5276; border-bottom: 3px solid #1a5276; padding-bottom: 8px; }
      h2 { color: #2c3e50; margin-top: 24px; }
      p { line-height: 1.5; }
      .highlight { background: #fff3cd; padding: 2px 5px; }
      table { width: 100%; border-collapse: collapse; margin: 15px 0; }
      th { background-color: #1a5276; color: white; text-align: left; padding: 8px; border: 1px solid #ddd; }
      td { padding: 8px; border: 1px solid #ddd; }
      .footer { margin-top: 40px; font-size: 10px; color: #64748b; }
    </style>
  </head>
  <body>
    <h1>Quarterly Report</h1>
    <p>This example mirrors the upstream full HTML report case with headings, paragraphs, lists, tables, borders, backgrounds, and page margins.</p>
    <h2>Highlights</h2>
    <ul>
      <li>Revenue growth remained strong across core business units.</li>
      <li><span class="highlight">Operating margin improved</span> in technology and manufacturing.</li>
      <li>Services remained stable while investment continued.</li>
    </ul>
    <h2>Business Unit Performance</h2>
    <table>
      <tr><th>Business Unit</th><th>Revenue</th><th>YoY Growth</th><th>Operating Margin</th></tr>
      <tr><td>Technology</td><td>143.2</td><td>+18.7%</td><td>24.3%</td></tr>
      <tr><td>Manufacturing</td><td>82.5</td><td>+8.4%</td><td>15.8%</td></tr>
      <tr><td>Consumer Products</td><td>45.3</td><td>+5.2%</td><td>12.1%</td></tr>
      <tr><td>Services</td><td>16.5</td><td>+2.1%</td><td>14.5%</td></tr>
    </table>
    <ol>
      <li>Continue operating discipline.</li>
      <li>Increase automation in reporting workflows.</li>
      <li>Review pricing and package strategy.</li>
    </ol>
    <p class="footer">Generated by pdf-crab-js through printpdf HTML rendering.</p>
  </body>
</html>`,
  )
}

function writeTableDebugExample(): void {
  writeHtmlExample(
    'printpdf-table-debug-example.pdf',
    'Table Debug Example',
    `
<!DOCTYPE html>
<html>
  <head>
    <style>
      body { font-family: Helvetica, sans-serif; margin: 20px; }
      table { width: 100%; border-collapse: collapse; margin: 15px 0; }
      th { background-color: #1a5276; color: white; font-weight: bold; text-align: left; padding: 8px; border: 1px solid #ddd; }
      td { padding: 8px; border: 1px solid #ddd; }
    </style>
  </head>
  <body>
    <h2>Simple Table Test</h2>
    <table>
      <tr><th>Business Unit</th><th>Revenue</th><th>YoY Growth</th><th>Operating Margin</th><th>YoY Change</th></tr>
      <tr><td>Technology</td><td>143.2</td><td>+18.7%</td><td>24.3%</td><td>+3.2pts</td></tr>
      <tr><td>Manufacturing</td><td>82.5</td><td>+8.4%</td><td>15.8%</td><td>+1.5pts</td></tr>
      <tr><td>Consumer Products</td><td>45.3</td><td>+5.2%</td><td>12.1%</td><td>+0.8pts</td></tr>
      <tr><td>Services</td><td>16.5</td><td>+2.1%</td><td>14.5%</td><td>-0.5pts</td></tr>
    </table>
  </body>
</html>`,
  )
}

function writePaginationExample(): void {
  const sections = Array.from({ length: 12 }, (_, index) => {
    const section = index + 1
    return `
      <section>
        <h2>Paginated Section ${section}</h2>
        <p>
          This paragraph intentionally repeats enough content to trigger printpdf HTML pagination.
          The upstream examples focus on page breaking, margin handling, and table layout behavior.
          This TypeScript version sends the same kind of long-form HTML through createPdfFromHtml.
        </p>
        <p>
          Section ${section} includes normal flow content, headings, and spacing so each page break
          has realistic layout pressure.
        </p>
      </section>`
  }).join('')

  writeHtmlExample(
    'printpdf-pagination-example.pdf',
    'Pagination Example',
    `
<html>
  <head>
    <style>
      body { font-family: Helvetica, sans-serif; margin: 18px; }
      h1 { color: #0f172a; }
      h2 { color: #1d4ed8; margin-top: 28px; }
      p { font-size: 13px; line-height: 1.45; }
    </style>
  </head>
  <body>
    <h1>Pagination Test</h1>
    ${sections}
  </body>
</html>`,
  )
}

function writeMarginCollapseExample(): void {
  writeHtmlExample(
    'printpdf-margin-collapse-example.pdf',
    'Margin Collapse Example',
    `
<html>
  <head>
    <style>
      body { font-family: Helvetica, sans-serif; padding: 30px; }
      .block { margin: 24px 0; padding: 12px; border: 2px solid #64748b; background: #f8fafc; }
      .nested { margin: 20px 0; padding: 8px; border: 1px solid #94a3b8; background: #eef2ff; }
      h1 { margin-bottom: 30px; }
      p { margin: 18px 0; }
    </style>
  </head>
  <body>
    <h1>Margin Collapse Test</h1>
    <div class="block">
      <p>First block with vertical margins.</p>
      <div class="nested">Nested block with its own margins.</div>
      <p>Second paragraph after a nested element.</p>
    </div>
    <div class="block">
      <p>Another block to exercise adjacent vertical margins.</p>
      <p>The upstream examples include several margin-collapse variants; this gallery keeps one representative HTML case.</p>
    </div>
  </body>
</html>`,
  )
}

function main(): void {
  writeTextExample()
  writeShapesExample()
  writeImageExample()
  writeSvgExample()
  writeLayersExample()
  writeBookmarksExample()
  writeMultipageExample()
  writeBasicHtmlExample()
  writeCssShapesExample()
  writeHtmlFullExample()
  writeTableDebugExample()
  writePaginationExample()
  writeMarginCollapseExample()
}

main()
