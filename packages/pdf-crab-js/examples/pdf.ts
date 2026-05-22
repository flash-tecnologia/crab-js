import { mkdirSync, writeFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

import { createPdf } from 'pdf-crab-js'

const currentDirectory = dirname(fileURLToPath(import.meta.url))
const outputDirectory = join(currentDirectory, 'output')
const outputPath = join(outputDirectory, 'pdf-crab-js-example.pdf')

const pdf = createPdf({
  title: 'pdf-crab-js example',
  pages: [
    {
      width: 210,
      height: 297,
      elements: [
        {
          type: 'rect',
          x: 18,
          y: 240,
          width: 174,
          height: 34,
          fill: '#f8fafc',
          stroke: '#0f172a',
          strokeWidth: 1,
        },
        {
          type: 'text',
          text: 'pdf-crab-js',
          x: 26,
          y: 260,
          font: 'HelveticaBold',
          fontSize: 18,
          fill: '#0f172a',
        },
        {
          type: 'text',
          text: 'Generated from the printpdf-backed NAPI API',
          x: 26,
          y: 250,
          fontSize: 10,
          fill: '#334155',
        },
        {
          type: 'line',
          x1: 18,
          y1: 226,
          x2: 192,
          y2: 226,
          stroke: '#2563eb',
          strokeWidth: 1.5,
        },
        {
          type: 'rect',
          x: 24,
          y: 170,
          width: 72,
          height: 36,
          stroke: '#16a34a',
          strokeWidth: 1,
        },
        {
          type: 'text',
          text: 'Text, lines, and rectangles',
          x: 104,
          y: 192,
          fontSize: 12,
          fill: '#111827',
        },
        {
          type: 'text',
          text: 'Coordinates use the PDF bottom-left origin.',
          x: 104,
          y: 182,
          fontSize: 9,
          fill: '#475569',
        },
      ],
    },
  ],
})

mkdirSync(outputDirectory, { recursive: true })
writeFileSync(outputPath, pdf)

console.log(`Generated ${outputPath}`)
console.log(`Size: ${pdf.length.toLocaleString()} bytes`)
