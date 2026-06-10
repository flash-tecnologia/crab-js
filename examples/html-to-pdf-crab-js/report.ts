import { mkdirSync, readFileSync, writeFileSync } from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

import { createPdfFromHtml } from 'html-to-pdf-crab-js'

const currentDirectory = path.dirname(fileURLToPath(import.meta.url))
const outputDirectory = path.join(currentDirectory, 'output')
const outputPath = path.join(outputDirectory, 'html-to-pdf-report-example.pdf')

const html = readFileSync(path.join(currentDirectory, 'report.html'), 'utf8')
const css = readFileSync(path.join(currentDirectory, 'report.css'), 'utf8')
const font = readFileSync(path.join(currentDirectory, 'assets/Tuffy.ttf'))

const pdf = await createPdfFromHtml({
  basePath: currentDirectory,
  css,
  fonts: [font],
  html,
  page: {
    landscape: true,
    margin: 10,
    size: 'A4',
  },
  systemFonts: false,
  tagged: true,
  title: 'HTML-to-PDF Operational Report',
})

mkdirSync(outputDirectory, { recursive: true })
writeFileSync(outputPath, pdf)

console.log(`Generated ${outputPath}`)
console.log(`Size: ${pdf.length.toLocaleString()} bytes`)
