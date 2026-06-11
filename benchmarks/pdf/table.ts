import { Buffer } from 'node:buffer'
import { spawn, spawnSync } from 'node:child_process'
import { mkdirSync, writeFileSync } from 'node:fs'
import path from 'node:path'
import { performance } from 'node:perf_hooks'
import { fileURLToPath } from 'node:url'

import { createPdfFromHtml } from 'html-to-pdf-crab-js'
import { createPdf, PdfDocumentBuilder, type CreatePdfInput, type PdfElementInput } from 'pdf-crab-js'

const resultMarker = '__PDF_BENCHMARK_RESULT__'
const columns = ['id', 'customer', 'product', 'region', 'amount', 'status', 'date', 'owner'] as const
const rowsPerPage = 10
const customers = ['Acme North', 'Beacon Labs', 'Crimson Works', 'Delta One', 'Evergreen Co'] as const
const products = ['Pro Plan', 'Scale Plan', 'Core Plan', 'Insight Pack', 'Velocity Add-on'] as const
const regions = ['LATAM', 'NA', 'EMEA', 'APAC'] as const
const statuses = ['paid', 'pending', 'overdue', 'refunded'] as const
const owners = ['team-a', 'team-b', 'team-c', 'team-d'] as const

type Column = (typeof columns)[number]
type PdfBuffer = ReturnType<typeof createPdf>
type ScenarioId = 'pdf-crab' | 'pdf-crab-builder' | 'html-to-pdf-crab-js' | 'gotenberg-node'
type ScenarioStatus = 'completed' | 'failed' | 'timeout'
type TableRow = Record<Column, string>

interface Dataset {
  columns: typeof columns
  pages: number
  rows: TableRow[]
  rowsPerPage: number
}

interface BenchmarkConfig {
  colors: boolean
  gotenbergUrl: string
  memorySampleMs: number
  pages: number
  requestTimeoutMs: number
  runs: number
  scenarioTimeoutMs: number
  selectedScenarios: ScenarioId[]
  warmupRuns: number
  writeOutput: boolean
}

interface ScenarioDefinition {
  id: ScenarioId
  language: string
  mode: string
}

interface ScenarioResult {
  durationsMs: number[]
  error?: string
  id: ScenarioId
  language: string
  meanMs: number
  mode: string
  pages: number
  pdfSizeBytes: number
  peakRssBytes: number
  status: ScenarioStatus
  throughputPagesPerSecond: number
}

const scenarioDefinitions: Record<ScenarioId, ScenarioDefinition> = {
  'gotenberg-node': {
    id: 'gotenberg-node',
    language: 'Node + Gotenberg',
    mode: 'gotenberg',
  },
  'html-to-pdf-crab-js': {
    id: 'html-to-pdf-crab-js',
    language: 'Node + html-to-pdf-crab',
    mode: 'local-html',
  },
  'pdf-crab': {
    id: 'pdf-crab',
    language: 'Node + pdf-crab',
    mode: 'local',
  },
  'pdf-crab-builder': {
    id: 'pdf-crab-builder',
    language: 'Node + pdf-crab',
    mode: 'builder',
  },
}

const pageWidth = 210
const pageHeight = 297
const tableX = 10
const tableTopY = 256
const tableWidth = 190
const headerHeight = 10
const rowHeight = 12
const cellPaddingX = 2.2

const columnWidths: Record<Column, number> = {
  amount: 22,
  customer: 32,
  date: 26,
  id: 18,
  owner: 22,
  product: 31,
  region: 17,
  status: 22,
}

const columnTextLimits: Record<Column, number> = {
  amount: 9,
  customer: 15,
  date: 10,
  id: 8,
  owner: 7,
  product: 14,
  region: 6,
  status: 9,
}

const styles = {
  bold: '\u001B[1m',
  cyan: '\u001B[36m',
  green: '\u001B[32m',
  gray: '\u001B[90m',
  red: '\u001B[31m',
  reset: '\u001B[0m',
}

function pad(value: number, width: number): string {
  return String(value).padStart(width, '0')
}

function pick(values: readonly string[], index: number): string {
  const value = values[index % values.length]

  if (value === undefined) {
    throw new Error('Cannot pick from an empty values list')
  }

  return value
}

function generateDataset(pages: number): Dataset {
  const totalRows = pages * rowsPerPage
  const rows: TableRow[] = []

  for (let index = 0; index < totalRows; index += 1) {
    const customer = pick(customers, pages + index)
    const product = pick(products, pages * 2 + index * 3)
    const region = pick(regions, pages + index * 2)
    const status = pick(statuses, pages + index * 5)
    const owner = pick(owners, pages + index * 7)
    const amountValue = 10000 + ((pages * 17 + index * 13) % 900000)
    const month = ((pages + index) % 12) + 1
    const day = ((pages * 3 + index * 2) % 28) + 1

    rows.push({
      amount: (amountValue / 100).toFixed(2),
      customer,
      date: `2026-${pad(month, 2)}-${pad(day, 2)}`,
      id: pad(index + 1, 6),
      owner,
      product,
      region,
      status,
    })
  }

  return { columns, pages, rows, rowsPerPage }
}

function createBenchmarkInput(dataset: Dataset): CreatePdfInput {
  return {
    metadata: {
      creator: 'pdf-benchmark',
      producer: 'pdf-crab-js',
      title: `table benchmark (${dataset.pages} pages)`,
    },
    pages: Array.from({ length: dataset.pages }, (_unused, pageIndex) => ({
      elements: createPageElements(dataset, pageIndex),
      height: pageHeight,
      width: pageWidth,
    })),
    title: `table benchmark (${dataset.pages} pages)`,
    unit: 'mm',
  }
}

function createPageElements(dataset: Dataset, pageIndex: number): PdfElementInput[] {
  return createPageElementChunks(dataset, pageIndex).flat()
}

function createPageElementChunks(dataset: Dataset, pageIndex: number): PdfElementInput[][] {
  const startRow = pageIndex * dataset.rowsPerPage
  const pageRows = dataset.rows.slice(startRow, startRow + dataset.rowsPerPage)
  const bodyHeight = dataset.rowsPerPage * rowHeight
  const tableBottomY = tableTopY - headerHeight - bodyHeight
  const headerBottomY = tableTopY - headerHeight
  const headerElements: PdfElementInput[] = []
  const backgroundElements: PdfElementInput[] = []
  const gridElements: PdfElementInput[] = []
  const headerTextElements: PdfElementInput[] = []
  const rowTextElements: PdfElementInput[] = []

  headerElements.push(
    {
      fill: '#111827',
      font: 'HelveticaBold',
      fontSize: 15,
      text: 'Simple Revenue Table',
      type: 'text',
      x: tableX,
      y: 277,
    },
    {
      fill: '#64748b',
      fontSize: 8,
      text: `Page ${pageIndex + 1} of ${dataset.pages}`,
      type: 'text',
      x: tableX,
      y: 267,
    },
    {
      fill: '#e2e8f0',
      height: headerHeight,
      type: 'rect',
      width: tableWidth,
      x: tableX,
      y: headerBottomY,
    },
  )

  for (let rowIndex = 0; rowIndex < dataset.rowsPerPage; rowIndex += 1) {
    backgroundElements.push({
      fill: rowIndex % 2 === 0 ? '#ffffff' : '#f8fafc',
      height: rowHeight,
      type: 'rect',
      width: tableWidth,
      x: tableX,
      y: headerBottomY - (rowIndex + 1) * rowHeight,
    })
  }

  appendGrid(gridElements, tableBottomY)
  appendHeaderText(headerTextElements, headerBottomY)

  for (const [rowIndex, row] of pageRows.entries()) {
    appendRowText(rowTextElements, row, headerBottomY - (rowIndex + 1) * rowHeight)
  }

  return [
    headerElements,
    backgroundElements,
    gridElements,
    headerTextElements,
    rowTextElements,
    [
      {
        fill: '#64748b',
        fontSize: 7,
        text: `Rows ${startRow + 1}-${startRow + pageRows.length}`,
        type: 'text',
        x: tableX,
        y: 22,
      },
    ],
  ]
}

function appendGrid(elements: PdfElementInput[], tableBottomY: number): void {
  for (let lineIndex = 0; lineIndex <= rowsPerPage + 1; lineIndex += 1) {
    const lineY = tableTopY - (lineIndex === 0 ? 0 : headerHeight + (lineIndex - 1) * rowHeight)

    elements.push({
      stroke: '#cbd5e1',
      strokeWidth: lineIndex === 0 || lineIndex === rowsPerPage + 1 ? 0.8 : 0.4,
      type: 'line',
      x1: tableX,
      x2: tableX + tableWidth,
      y1: lineY,
      y2: lineY,
    })
  }

  let cursorX = tableX

  for (const column of columns) {
    elements.push({
      stroke: '#cbd5e1',
      strokeWidth: 0.4,
      type: 'line',
      x1: cursorX,
      x2: cursorX,
      y1: tableTopY,
      y2: tableBottomY,
    })
    cursorX += columnWidths[column]
  }

  elements.push({
    stroke: '#cbd5e1',
    strokeWidth: 0.4,
    type: 'line',
    x1: tableX + tableWidth,
    x2: tableX + tableWidth,
    y1: tableTopY,
    y2: tableBottomY,
  })
}

function appendHeaderText(elements: PdfElementInput[], headerBottomY: number): void {
  let cursorX = tableX

  for (const column of columns) {
    elements.push({
      fill: '#0f172a',
      font: 'HelveticaBold',
      fontSize: 6.8,
      text: column.toUpperCase(),
      type: 'text',
      x: cursorX + cellPaddingX,
      y: headerBottomY + 3.5,
    })
    cursorX += columnWidths[column]
  }
}

function appendRowText(elements: PdfElementInput[], row: TableRow, rowY: number): void {
  let cursorX = tableX

  for (const column of columns) {
    elements.push({
      fill: '#111827',
      fontSize: 6.6,
      text: truncate(row[column], columnTextLimits[column]),
      type: 'text',
      x: cursorX + cellPaddingX,
      y: rowY + 4.5,
    })
    cursorX += columnWidths[column]
  }
}

function truncate(value: string, maxLength: number): string {
  if (value.length <= maxLength) {
    return value
  }

  return `${value.slice(0, maxLength - 3)}...`
}

function createBenchmarkHtml(dataset: Dataset): string {
  const pages = Array.from({ length: dataset.pages }, (_unused, pageIndex) => createHtmlPage(dataset, pageIndex)).join(
    '\n',
  )

  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>table benchmark (${dataset.pages} pages)</title>
  <style>
    @page { size: A4; margin: 0; }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: #ffffff;
      color: #111827;
      font-family: Helvetica, Arial, sans-serif;
    }
    .page {
      break-after: page;
      height: 297mm;
      padding: 20mm 10mm;
      width: 210mm;
    }
    .page:last-child { break-after: auto; }
    h1 {
      font-size: 15pt;
      line-height: 1;
      margin: 0 0 7mm;
    }
    .subhead {
      color: #64748b;
      font-size: 8pt;
      margin-bottom: 8mm;
    }
    table {
      border-collapse: collapse;
      table-layout: fixed;
      width: 190mm;
    }
    thead tr { background: #e2e8f0; }
    tbody tr:nth-child(even) { background: #f8fafc; }
    th,
    td {
      border: 0.4pt solid #cbd5e1;
      height: 12mm;
      overflow: hidden;
      padding: 0 2.2mm;
      text-align: left;
      white-space: nowrap;
    }
    th {
      color: #0f172a;
      font-size: 6.8pt;
      font-weight: 700;
      height: 10mm;
      text-transform: uppercase;
    }
    td {
      color: #111827;
      font-size: 6.6pt;
    }
    .footer {
      color: #64748b;
      font-size: 7pt;
      margin-top: 12mm;
    }
    ${createColumnCss()}
  </style>
</head>
<body>
${pages}
</body>
</html>`
}

function createColumnCss(): string {
  return columns
    .map(
      (column, index) => `th:nth-child(${index + 1}), td:nth-child(${index + 1}) { width: ${columnWidths[column]}mm; }`,
    )
    .join('\n    ')
}

function createHtmlPage(dataset: Dataset, pageIndex: number): string {
  const startRow = pageIndex * dataset.rowsPerPage
  const pageRows = dataset.rows.slice(startRow, startRow + dataset.rowsPerPage)
  const bodyRows = pageRows.map((row) => createHtmlRow(row)).join('\n')

  return `<section class="page">
  <h1>Simple Revenue Table</h1>
  <div class="subhead">Page ${pageIndex + 1} of ${dataset.pages}</div>
  <table>
    <thead>
      <tr>${columns.map((column) => `<th>${escapeHtml(column)}</th>`).join('')}</tr>
    </thead>
    <tbody>
${bodyRows}
    </tbody>
  </table>
  <div class="footer">Rows ${startRow + 1}-${startRow + pageRows.length}</div>
</section>`
}

function createHtmlRow(row: TableRow): string {
  return `      <tr>${columns
    .map((column) => `<td>${escapeHtml(truncate(row[column], columnTextLimits[column]))}</td>`)
    .join('')}</tr>`
}

function escapeHtml(value: string): string {
  return value.replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;')
}

function readConfig(): BenchmarkConfig {
  return {
    colors: process.env.PDF_BENCHMARK_COLORS !== '0',
    gotenbergUrl: process.env.PDF_BENCHMARK_GOTENBERG_URL?.trim() || 'http://localhost:3000',
    memorySampleMs: readInteger('PDF_BENCHMARK_MEMORY_SAMPLE_MS', 50, 1),
    pages: readInteger('PDF_BENCHMARK_PAGES', 10, 1),
    requestTimeoutMs: readInteger('PDF_BENCHMARK_REQUEST_TIMEOUT_MS', 120000, 1),
    runs: readInteger('PDF_BENCHMARK_RUNS', 10, 1),
    scenarioTimeoutMs: readInteger('PDF_BENCHMARK_SCENARIO_TIMEOUT_MS', 180000, 1),
    selectedScenarios: readSelectedScenarios(),
    warmupRuns: readInteger('PDF_BENCHMARK_WARMUP', 3, 0),
    writeOutput: process.env.PDF_BENCHMARK_WRITE === '1',
  }
}

function readInteger(name: string, fallback: number, minimum: number): number {
  const raw = process.env[name]?.trim()

  if (!raw) {
    return fallback
  }

  const parsed = Number.parseInt(raw, 10)

  if (!Number.isInteger(parsed) || parsed < minimum) {
    throw new Error(`${name} must be an integer greater than or equal to ${minimum}`)
  }

  return parsed
}

function readSelectedScenarios(): ScenarioId[] {
  const raw = process.env.PDF_BENCHMARK_ONLY?.trim()

  if (!raw) {
    return ['pdf-crab', 'pdf-crab-builder', 'html-to-pdf-crab-js', 'gotenberg-node']
  }

  const selected = raw
    .split(',')
    .map((value) => value.trim())
    .filter((value) => value.length > 0)

  if (selected.length === 0) {
    throw new Error('PDF_BENCHMARK_ONLY did not include any scenario ids')
  }

  for (const scenario of selected) {
    if (!(scenario in scenarioDefinitions)) {
      throw new Error(`Unknown PDF_BENCHMARK_ONLY scenario: ${scenario}`)
    }
  }

  return selected as ScenarioId[]
}

function collectGarbage(): void {
  const runtimeGlobal = globalThis as typeof globalThis & { gc?: () => void }

  runtimeGlobal.gc?.()
}

function average(values: readonly number[]): number {
  return values.reduce((total, value) => total + value, 0) / values.length
}

function formatDuration(value: number): string {
  if (!Number.isFinite(value)) {
    return '-'
  }

  if (value < 1000) {
    return `${value.toFixed(3)} ms`
  }

  return `${(value / 1000).toFixed(3)} s`
}

function formatBytes(value: number): string {
  if (value <= 0 || !Number.isFinite(value)) {
    return '-'
  }

  if (value < 1024) {
    return `${value} B`
  }

  if (value < 1024 * 1024) {
    return `${(value / 1024).toFixed(1)} KB`
  }

  return `${(value / 1024 / 1024).toFixed(3)} MB`
}

function formatThroughput(value: number): string {
  return Number.isFinite(value) && value > 0 ? `${value.toFixed(3)} pages/s` : '-'
}

function assertPdf(pdf: PdfBuffer): void {
  if (!pdf.subarray(0, 5).equals(Buffer.from('%PDF-'))) {
    throw new Error('Benchmark output does not start with %PDF-')
  }

  if (!pdf.subarray(-16).toString('latin1').includes('%%EOF')) {
    throw new Error('Benchmark output does not end with %%EOF')
  }
}

function maybeWriteOutput(config: BenchmarkConfig, scenario: ScenarioDefinition, pdf: PdfBuffer): void {
  if (!config.writeOutput) {
    return
  }

  const outputDirectory = path.join(path.dirname(fileURLToPath(import.meta.url)), 'output')
  const outputPath = path.join(outputDirectory, `table-${scenario.id}-${config.pages}-pages.pdf`)

  mkdirSync(outputDirectory, { recursive: true })
  writeFileSync(outputPath, pdf)
}

async function createGotenbergPdf(config: BenchmarkConfig, html: string): Promise<Buffer> {
  const endpoint = new URL('/forms/chromium/convert/html', config.gotenbergUrl)
  const formData = new FormData()
  const controller = new AbortController()
  const timeout = setTimeout(() => controller.abort(), config.requestTimeoutMs)

  formData.append('files', new Blob([html], { type: 'text/html' }), 'index.html')
  formData.append('paperWidth', '8.27')
  formData.append('paperHeight', '11.7')
  formData.append('marginTop', '0')
  formData.append('marginBottom', '0')
  formData.append('marginLeft', '0')
  formData.append('marginRight', '0')
  formData.append('preferCssPageSize', 'true')
  formData.append('printBackground', 'true')

  try {
    let response: Response

    try {
      response = await fetch(endpoint, {
        body: formData,
        method: 'POST',
        signal: controller.signal,
      })
    } catch (error) {
      throw new Error(`Gotenberg unreachable at ${endpoint.origin}: ${formatErrorWithCause(error)}`, { cause: error })
    }

    if (!response.ok) {
      const body = await response.text()
      throw new Error(`Gotenberg returned ${response.status}: ${body.slice(0, 400)}`)
    }

    return Buffer.from(await response.arrayBuffer())
  } finally {
    clearTimeout(timeout)
  }
}

async function runMeasuredScenario(config: BenchmarkConfig, scenario: ScenarioDefinition): Promise<ScenarioResult> {
  const dataset = generateDataset(config.pages)
  const runner = createScenarioRunner(config, scenario, dataset)
  const durationsMs: number[] = []
  let pdf: PdfBuffer | undefined

  for (let index = 0; index < config.warmupRuns; index += 1) {
    pdf = await runner()
  }

  for (let index = 0; index < config.runs; index += 1) {
    collectGarbage()
    const start = performance.now()
    pdf = await runner()
    durationsMs.push(performance.now() - start)
  }

  if (pdf === undefined) {
    throw new Error('Benchmark produced no PDF output')
  }

  assertPdf(pdf)
  maybeWriteOutput(config, scenario, pdf)

  const meanMs = average(durationsMs)

  return {
    durationsMs,
    id: scenario.id,
    language: scenario.language,
    meanMs,
    mode: scenario.mode,
    pages: config.pages,
    pdfSizeBytes: pdf.length,
    peakRssBytes: process.memoryUsage().rss,
    status: 'completed',
    throughputPagesPerSecond: config.pages / (meanMs / 1000),
  }
}

function createScenarioRunner(
  config: BenchmarkConfig,
  scenario: ScenarioDefinition,
  dataset: Dataset,
): () => Promise<PdfBuffer> {
  if (scenario.id === 'pdf-crab') {
    const input = createBenchmarkInput(dataset)

    return async () => createPdf(input)
  }

  if (scenario.id === 'pdf-crab-builder') {
    const pageChunks = Array.from({ length: dataset.pages }, (_unused, pageIndex) =>
      createPageElementChunks(dataset, pageIndex),
    )

    return async () => createPdfWithBuilder(dataset, pageChunks)
  }

  const html = createBenchmarkHtml(dataset)

  if (scenario.id === 'html-to-pdf-crab-js') {
    return async () =>
      createPdfFromHtml({
        html,
        page: {
          size: 'A4',
        },
        title: `table benchmark (${dataset.pages} pages)`,
      })
  }

  return async () => createGotenbergPdf(config, html)
}

function createPdfWithBuilder(dataset: Dataset, pageChunks: PdfElementInput[][][]): PdfBuffer {
  const builder = new PdfDocumentBuilder({
    metadata: {
      creator: 'pdf-benchmark',
      producer: 'pdf-crab-js',
      title: `table benchmark (${dataset.pages} pages)`,
    },
    title: `table benchmark (${dataset.pages} pages)`,
    unit: 'mm',
  })

  for (const chunks of pageChunks) {
    builder.startPage({ height: pageHeight, width: pageWidth })
    for (const chunk of chunks) {
      builder.appendElements(chunk)
    }
    builder.endPage()
  }

  return builder.finish()
}

function createFailedResult(
  config: BenchmarkConfig,
  scenario: ScenarioDefinition,
  status: ScenarioStatus,
  error: string,
): ScenarioResult {
  return {
    durationsMs: [],
    error,
    id: scenario.id,
    language: scenario.language,
    meanMs: Number.POSITIVE_INFINITY,
    mode: scenario.mode,
    pages: config.pages,
    pdfSizeBytes: 0,
    peakRssBytes: 0,
    status,
    throughputPagesPerSecond: 0,
  }
}

async function runChild(): Promise<void> {
  const scenarioId = process.env.PDF_BENCHMARK_CHILD_SCENARIO as ScenarioId | undefined

  if (scenarioId === undefined) {
    throw new Error('Missing PDF_BENCHMARK_CHILD_SCENARIO')
  }

  const config = readConfig()
  const scenario = scenarioDefinitions[scenarioId]

  if (scenario === undefined) {
    throw new Error(`Unknown child scenario: ${scenarioId}`)
  }

  try {
    const result = await runMeasuredScenario(config, scenario)
    console.log(`${resultMarker}${JSON.stringify(result)}`)
  } catch (error) {
    const result = createFailedResult(config, scenario, 'failed', formatUnknownError(error))
    console.log(`${resultMarker}${JSON.stringify(result)}`)
  }
}

async function runParent(): Promise<void> {
  const config = readConfig()
  const results: ScenarioResult[] = []

  for (const scenarioId of config.selectedScenarios) {
    const scenario = scenarioDefinitions[scenarioId]
    results.push(await runScenarioProcess(config, scenario))
  }

  printResults(config, results)
}

async function runScenarioProcess(config: BenchmarkConfig, scenario: ScenarioDefinition): Promise<ScenarioResult> {
  const child = spawn(process.execPath, ['--expose-gc', '--import', 'tsx', fileURLToPath(import.meta.url)], {
    cwd: path.dirname(fileURLToPath(import.meta.url)),
    env: {
      ...process.env,
      PDF_BENCHMARK_CHILD_SCENARIO: scenario.id,
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  let stdout = ''
  let stderr = ''
  let peakRssBytes = 0
  let timedOut = false

  child.stdout.setEncoding('utf8')
  child.stderr.setEncoding('utf8')
  child.stdout.on('data', (chunk: string) => {
    stdout += chunk
  })
  child.stderr.on('data', (chunk: string) => {
    stderr += chunk
  })

  const sampler = setInterval(() => {
    if (child.pid !== undefined) {
      peakRssBytes = Math.max(peakRssBytes, readProcessRssBytes(child.pid))
    }
  }, config.memorySampleMs)
  const timeout = setTimeout(() => {
    timedOut = true
    child.kill('SIGTERM')
  }, config.scenarioTimeoutMs)
  const exit = await waitForExit(child)

  clearInterval(sampler)
  clearTimeout(timeout)

  if (timedOut) {
    return createFailedResult(config, scenario, 'timeout', `timeout after ${config.scenarioTimeoutMs} ms`)
  }

  const parsed = parseChildResult(stdout)

  if (parsed === undefined) {
    const exitMessage = exit.signal ? `signal ${exit.signal}` : `exit code ${exit.code ?? 'unknown'}`
    return createFailedResult(config, scenario, 'failed', compactError(`${exitMessage}\n${stderr || stdout}`))
  }

  return {
    ...parsed,
    peakRssBytes: Math.max(peakRssBytes, parsed.peakRssBytes),
  }
}

function waitForExit(child: ReturnType<typeof spawn>): Promise<{ code: number | null; signal: NodeJS.Signals | null }> {
  return new Promise((resolve) => {
    child.on('exit', (code, signal) => {
      resolve({ code, signal })
    })
  })
}

function readProcessRssBytes(pid: number): number {
  const result = spawnSync('ps', ['-o', 'rss=', '-p', String(pid)], {
    encoding: 'utf8',
  })
  const rssKilobytes = Number.parseInt(result.stdout.trim(), 10)

  return Number.isFinite(rssKilobytes) ? rssKilobytes * 1024 : 0
}

function parseChildResult(output: string): ScenarioResult | undefined {
  const line = output
    .split(/\r?\n/)
    .find((value) => value.startsWith(resultMarker))
    ?.slice(resultMarker.length)

  if (line === undefined) {
    return undefined
  }

  return JSON.parse(line) as ScenarioResult
}

function printResults(config: BenchmarkConfig, results: readonly ScenarioResult[]): void {
  const rankedResults = results.toSorted(compareScenarioResults)

  console.log(`\nFor the ${config.pages}-page scenario, fastest to slowest by execution time is:`)

  printTable(
    {
      headers: ['Order', 'Language', 'Mode', 'Execution time', 'Throughput', 'Peak RAM', 'PDF size', 'Status'],
      rightAlignedColumns: new Set([0, 3, 4, 5, 6]),
      rows: rankedResults.map((result, index) => formatResultRow(result, index, config.colors)),
    },
    config.colors,
  )
}

function compareScenarioResults(left: ScenarioResult, right: ScenarioResult): number {
  if (left.status === 'completed' && right.status !== 'completed') {
    return -1
  }

  if (left.status !== 'completed' && right.status === 'completed') {
    return 1
  }

  return left.meanMs - right.meanMs
}

function formatResultRow(result: ScenarioResult, index: number, useColors: boolean): string[] {
  const color = result.status === 'completed' ? (index === 0 ? styles.green : styles.cyan) : styles.red
  const values = [
    String(index + 1),
    result.language,
    result.mode,
    formatDuration(result.meanMs),
    formatThroughput(result.throughputPagesPerSecond),
    formatBytes(result.peakRssBytes),
    formatBytes(result.pdfSizeBytes),
    formatStatus(result),
  ]

  return values.map((value) => colorize(value, useColors, color))
}

function formatStatus(result: ScenarioResult): string {
  if (result.status === 'completed') {
    return 'completed'
  }

  return `${result.status}: ${result.error ?? 'unknown error'}`
}

function printTable(
  table: {
    headers: readonly string[]
    rightAlignedColumns: ReadonlySet<number>
    rows: readonly string[][]
  },
  useColors: boolean,
): void {
  const widths = table.headers.map((header, columnIndex) =>
    Math.max(visibleLength(header), ...table.rows.map((row) => visibleLength(row[columnIndex] ?? ''))),
  )
  const separator = widths.map((width) => '-'.repeat(width + 2)).join('+')
  const formatRow = (row: readonly string[]) =>
    row
      .map((cell, columnIndex) => {
        const align = table.rightAlignedColumns.has(columnIndex) ? 'right' : 'left'
        return ` ${padCell(cell, widths[columnIndex] ?? visibleLength(cell), align)} `
      })
      .join('|')

  console.log(formatRow(table.headers.map((header) => colorize(header, useColors, styles.bold))))
  console.log(colorize(separator, useColors, styles.gray))

  for (const row of table.rows) {
    console.log(formatRow(row))
  }
}

function colorize(value: string, useColors: boolean, ...codes: string[]): string {
  if (!useColors || codes.length === 0 || value.length === 0) {
    return value
  }

  return `${codes.join('')}${value}${styles.reset}`
}

function stripAnsi(value: string): string {
  let output = ''

  for (let index = 0; index < value.length; index += 1) {
    if (value.charCodeAt(index) !== 27 || value[index + 1] !== '[') {
      output += value[index]
      continue
    }

    index += 2
    while (index < value.length && value[index] !== 'm') {
      index += 1
    }
  }

  return output
}

function visibleLength(value: string): number {
  return stripAnsi(value).length
}

function padCell(value: string, width: number, align: 'left' | 'right' = 'left'): string {
  const padding = Math.max(0, width - visibleLength(value))

  return align === 'right' ? `${' '.repeat(padding)}${value}` : `${value}${' '.repeat(padding)}`
}

function formatUnknownError(error: unknown): string {
  if (error instanceof Error) {
    return compactError(error.message)
  }

  return compactError(String(error))
}

function formatErrorWithCause(error: unknown): string {
  if (!(error instanceof Error)) {
    return String(error)
  }

  const cause = formatCause(error.cause)

  return cause === undefined ? error.message : `${error.message} (${cause})`
}

function formatCause(cause: unknown): string | undefined {
  const aggregateCause = formatAggregateCause(cause)

  if (aggregateCause !== undefined) {
    return aggregateCause
  }

  if (cause instanceof Error) {
    return cause.message || undefined
  }

  if (typeof cause !== 'object' || cause === null) {
    return undefined
  }

  const fields = cause as Record<string, unknown>
  const code = typeof fields.code === 'string' ? fields.code : undefined
  const address = typeof fields.address === 'string' ? fields.address : undefined
  const port = typeof fields.port === 'number' ? fields.port : undefined

  if (code !== undefined && address !== undefined && port !== undefined) {
    return `${code} ${address}:${port}`
  }

  if (code !== undefined) {
    return code
  }

  return undefined
}

function formatAggregateCause(cause: unknown): string | undefined {
  if (typeof cause !== 'object' || cause === null || !('errors' in cause)) {
    return undefined
  }

  const errors = (cause as { errors?: unknown }).errors

  if (!Array.isArray(errors)) {
    return undefined
  }

  return (
    errors
      .map(formatErrorWithCause)
      .filter((message) => message.length > 0)
      .join('; ') || undefined
  )
}

function compactError(value: string): string {
  return value.replaceAll(/\s+/g, ' ').trim().slice(0, 180)
}

if (process.env.PDF_BENCHMARK_CHILD_SCENARIO === undefined) {
  await runParent()
} else {
  await runChild()
}
