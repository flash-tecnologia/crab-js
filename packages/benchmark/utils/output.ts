import type { MemoryUsageSnapshot } from './memory.js'
import {
  createBenchmarkResult,
  formatDifference,
  formatThroughput,
  formatTolerance,
  throughputValue,
  type BenchmarkResult,
  type RunMeasurement,
} from './results.js'

interface MemoryBenchmarkResult {
  scenario: {
    label: string
  }
  measurements: readonly RunMeasurement[]
  memory: {
    peak: MemoryUsageSnapshot
    peakDelta: MemoryUsageSnapshot
    retainedDelta: MemoryUsageSnapshot
  }
}

const styles = {
  reset: '\u001B[0m',
  bold: '\u001B[1m',
  red: '\u001B[31m',
  green: '\u001B[32m',
  yellow: '\u001B[33m',
  blue: '\u001B[34m',
  cyan: '\u001B[36m',
  gray: '\u001B[90m',
}

export function printBenchmarkResults(
  results: Record<string, BenchmarkResult>,
  options: { title?: string; useColors: boolean },
) {
  const entries = Object.entries(results).toSorted(
    ([, left], [, right]) => throughputValue(left) - throughputValue(right),
  )
  const rows = entries.map(([label, result], index) => {
    const colors = resultColor(index, entries.length)
    const previous = entries[index - 1]?.[1]
    return [
      colorize(String(index + 1), options.useColors, ...colors),
      colorize(label, options.useColors, ...colors),
      colorize(String(result.size), options.useColors, ...colors),
      colorize(formatThroughput(result), options.useColors, ...colors),
      colorize(formatTolerance(result), options.useColors, styles.gray),
      colorize(formatDifference(result, previous), options.useColors, styles.green),
    ]
  })

  printTable(
    {
      title: options.title ?? 'Consumer throughput',
      headers: ['#', 'Scenario', 'Runs', 'Result', 'Tolerance', 'Vs previous'],
      rows,
      rightAlignedColumns: new Set([0, 2, 3, 4, 5]),
    },
    options.useColors,
  )
}

export function printMemoryResults(results: readonly MemoryBenchmarkResult[], options: { useColors: boolean }) {
  const rankedResults = results
    .map((result) => ({
      result,
      benchmarkResult: createBenchmarkResult(result.measurements),
    }))
    .toSorted((left, right) => throughputValue(left.benchmarkResult) - throughputValue(right.benchmarkResult))

  const peakRssValues = rankedResults.map(({ result }) => result.memory.peak.rss)
  const peakRssDeltaValues = rankedResults.map(({ result }) => result.memory.peakDelta.rss)
  const retainedRssValues = rankedResults.map(({ result }) => result.memory.retainedDelta.rss)
  const heapValues = rankedResults.map(({ result }) => result.memory.peak.heapUsed)
  const externalValues = rankedResults.map(({ result }) => result.memory.peak.external)
  const arrayBufferValues = rankedResults.map(({ result }) => result.memory.peak.arrayBuffers)

  const rows = rankedResults.map(({ result, benchmarkResult }, index) => {
    const colors = resultColor(index, rankedResults.length)
    const previous = rankedResults[index - 1]?.benchmarkResult

    return [
      colorize(String(index + 1), options.useColors, ...colors),
      colorize(result.scenario.label, options.useColors, ...colors),
      colorize(String(result.measurements.length), options.useColors, ...colors),
      colorize(formatThroughput(benchmarkResult), options.useColors, ...colors),
      colorize(formatTolerance(benchmarkResult), options.useColors, styles.gray),
      colorize(formatDifference(benchmarkResult, previous), options.useColors, styles.green),
      colorize(
        formatBytes(result.memory.peak.rss),
        options.useColors,
        ...memoryColor(result.memory.peak.rss, peakRssValues),
      ),
      colorize(
        formatBytes(result.memory.peakDelta.rss),
        options.useColors,
        ...memoryColor(result.memory.peakDelta.rss, peakRssDeltaValues),
      ),
      colorize(
        formatBytes(result.memory.retainedDelta.rss),
        options.useColors,
        ...memoryColor(result.memory.retainedDelta.rss, retainedRssValues),
      ),
      colorize(
        formatBytes(result.memory.peak.heapUsed),
        options.useColors,
        ...memoryColor(result.memory.peak.heapUsed, heapValues),
      ),
      colorize(
        formatBytes(result.memory.peak.external),
        options.useColors,
        ...memoryColor(result.memory.peak.external, externalValues),
      ),
      colorize(
        formatBytes(result.memory.peak.arrayBuffers),
        options.useColors,
        ...memoryColor(result.memory.peak.arrayBuffers, arrayBufferValues),
      ),
    ]
  })

  printTable(
    {
      title: 'Consumer benchmark (isolated process + memory)',
      headers: [
        '#',
        'Scenario',
        'Runs',
        'Result',
        'Tolerance',
        'Vs previous',
        'Peak RSS',
        'RSS delta',
        'Retained RSS',
        'Peak heap',
        'External',
        'ArrayBuffer',
      ],
      rows,
      rightAlignedColumns: new Set([0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]),
    },
    options.useColors,
  )
}

function formatBytes(bytes: number): string {
  const sign = bytes < 0 ? '-' : ''
  const absoluteBytes = Math.abs(bytes)
  const units = ['B', 'KiB', 'MiB', 'GiB']
  let value = absoluteBytes
  let unitIndex = 0

  while (value >= 1024 && unitIndex < units.length - 1) {
    value /= 1024
    unitIndex += 1
  }

  return `${sign}${value.toFixed(unitIndex === 0 ? 0 : 1)} ${units[unitIndex]}`
}

function colorize(value: string, useColors: boolean, ...codes: string[]): string {
  if (!useColors || codes.length === 0 || value.length === 0) {
    return value
  }

  return `${codes.join('')}${value}${styles.reset}`
}

function stripAnsi(value: string): string {
  let output = ''
  for (let index = 0; index < value.length; index++) {
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

function resultColor(index: number, total: number): string[] {
  if (total <= 1) {
    return [styles.green]
  }

  if (index === 0) {
    return [styles.red]
  }

  if (index === total - 1) {
    return [styles.green, styles.bold]
  }

  return [styles.cyan]
}

function memoryColor(bytes: number, values: readonly number[], preferLower = true): string[] {
  if (values.length <= 1) {
    return []
  }

  const best = preferLower ? Math.min(...values) : Math.max(...values)
  const worst = preferLower ? Math.max(...values) : Math.min(...values)

  if (bytes === best) {
    return [styles.green]
  }

  if (bytes === worst) {
    return [styles.red]
  }

  return [styles.yellow]
}

function printTable(
  table: {
    title: string
    headers: readonly string[]
    rows: readonly string[][]
    rightAlignedColumns: ReadonlySet<number>
  },
  useColors: boolean,
) {
  console.log()
  console.log(colorize(table.title, useColors, styles.bold, styles.blue))

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
