import { equal, ok } from 'node:assert/strict'

import { createPdfAsync } from '../index.js'

const pdf = await createPdfAsync({
  title: 'WASM PDF smoke',
  pages: [
    {
      width: 120,
      height: 120,
      elements: [{ type: 'text', text: 'WASM PDF smoke', x: 12, y: 90 }],
    },
  ],
})

ok(pdf instanceof Uint8Array)
equal(Buffer.from(pdf.subarray(0, 5)).toString('utf8'), '%PDF-')
equal(Buffer.from(pdf).toString('utf8').trimEnd().endsWith('%%EOF'), true)
ok(pdf.byteLength > 800)
ok(Buffer.from(pdf).toString('latin1').includes('WASM PDF smoke'))
