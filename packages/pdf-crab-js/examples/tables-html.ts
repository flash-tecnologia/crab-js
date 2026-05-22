import { mkdirSync, writeFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

import { createPdfFromHtml } from 'pdf-crab-js'

const currentDirectory = dirname(fileURLToPath(import.meta.url))
const outputDirectory = join(currentDirectory, 'output')
const outputPath = join(outputDirectory, 'pdf-crab-js-tables-html.pdf')

const html = `
<html>
  <!-- printpdf automatically breaks content into pages -->
  <body style="padding:10mm">
    <p style="color: red; font-family: sans-serif;" data-chapter="1" data-subsection="First subsection">Hello!</p>
    <div style="width:200px;height:200px;background:red;" data-chapter="1" data-subsection="Second subsection">
      <p>World!</p>
    </div>
  </body>

  <!-- configure header and footer for each page -->
  <head>
    <header>
      <h4 style="color: #2e2e2e;min-height: 8mm;">Chapter {attr:chapter} * {attr:subsection}</h4>
      <p style="position: absolute;top:5mm;left:5mm;">{builtin:pagenum}</p>
    </header>

    <footer>
      <hr/>
    </footer>
  </head>
</html>
`

const pdf = createPdfFromHtml({
  title: 'My PDF',
  html,
  pageWidth: 210,
  pageHeight: 297,
})

mkdirSync(outputDirectory, { recursive: true })
writeFileSync(outputPath, pdf)

console.log(`Generated ${outputPath}`)
console.log(`Size: ${pdf.length.toLocaleString()} bytes`)
