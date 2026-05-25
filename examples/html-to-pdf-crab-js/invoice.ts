import { mkdirSync, readFileSync, writeFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

import { createPdfFromHtml } from 'html-to-pdf-crab-js'

const currentDirectory = dirname(fileURLToPath(import.meta.url))
const outputDirectory = join(currentDirectory, 'output')
const outputPath = join(outputDirectory, 'html-to-pdf-invoice-example.pdf')

const html = readFileSync(join(currentDirectory, 'invoice.html'), 'utf8')
const css = readFileSync(join(currentDirectory, 'invoice.css'), 'utf8')
const font = readFileSync(join(currentDirectory, 'assets/Tuffy.ttf'))

const pdf = await createPdfFromHtml({
  basePath: currentDirectory,
  css,
  fonts: [font],
  html,
  page: {
    margin: { top: 14, right: 14, bottom: 16, left: 14, unit: 'mm' },
    size: 'A4',
  },
  systemFonts: false,
  title: 'Invoice INV-2026-042',
})

mkdirSync(outputDirectory, { recursive: true })
writeFileSync(outputPath, pdf)

console.log(`Generated ${outputPath}`)
console.log(`Size: ${pdf.length.toLocaleString()} bytes`)
