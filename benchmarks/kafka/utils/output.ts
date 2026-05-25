import type { GcSummary } from './gc.js'
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
  gc: GcSummary
}

interface OutputOptions {
  title?: string
  useColors: boolean
  showCharts?: boolean
}

interface ThroughputChartEntry {
  label: string
  result: BenchmarkResult
}

interface MemoryEfficiencyEntry extends ThroughputChartEntry {
  rssDelta: number
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

export function printBenchmarkResults(results: Record<string, BenchmarkResult>, options: OutputOptions) {
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

  if (options.showCharts ?? true) {
    printThroughputChart(
      entries.map(([label, result]) => ({ label, result })),
      options.useColors,
    )
  }
}

export function printMemoryResults(results: readonly MemoryBenchmarkResult[], options: OutputOptions) {
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
      title: 'Consumer benchmark (isolated process + lifecycle memory)',
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

  printGcResults(
    rankedResults.map(({ result }) => ({ label: result.scenario.label, gc: result.gc })),
    options.useColors,
  )

  if (options.showCharts ?? true) {
    printThroughputChart(
      rankedResults.map(({ result, benchmarkResult }) => ({ label: result.scenario.label, result: benchmarkResult })),
      options.useColors,
    )
    printMemoryEfficiencyChart(
      rankedResults.map(({ result, benchmarkResult }) => ({
        label: result.scenario.label,
        result: benchmarkResult,
        rssDelta: result.memory.peakDelta.rss,
      })),
      options.useColors,
    )
  }
}

function printGcResults(results: readonly { label: string; gc: GcSummary }[], useColors: boolean) {
  const rankedResults = results.toSorted((left, right) => left.gc.totalDurationMs - right.gc.totalDurationMs)
  const totalDurations = rankedResults.map(({ gc }) => gc.totalDurationMs)
  const activeShares = rankedResults.map(({ gc }) => gcShare(gc))
  const totalCounts = rankedResults.map(({ gc }) => gc.totalCount)
  const maxDurations = rankedResults.map(({ gc }) => gc.maxDurationMs)
  const forcedCounts = rankedResults.map(({ gc }) => gc.forcedCount)

  const rows = rankedResults.map(({ label, gc }, index) => {
    const colors = resultColor(rankedResults.length - index - 1, rankedResults.length)

    return [
      colorize(String(index + 1), useColors, ...colors),
      colorize(label, useColors, ...colors),
      colorize(formatDurationMs(gc.totalDurationMs), useColors, ...memoryColor(gc.totalDurationMs, totalDurations)),
      colorize(formatPercent(gcShare(gc)), useColors, ...memoryColor(gcShare(gc), activeShares)),
      colorize(String(gc.totalCount), useColors, ...memoryColor(gc.totalCount, totalCounts)),
      colorize(formatDurationMs(averageGcDurationMs(gc)), useColors, styles.gray),
      colorize(formatDurationMs(gc.maxDurationMs), useColors, ...memoryColor(gc.maxDurationMs, maxDurations)),
      colorize(String(gc.minorCount), useColors),
      colorize(String(gc.majorCount), useColors),
      colorize(String(gc.incrementalCount), useColors),
      colorize(String(gc.forcedCount), useColors, ...memoryColor(gc.forcedCount, forcedCounts)),
    ]
  })

  printTable(
    {
      title: 'GC comparison (measured message window, lower is better)',
      headers: [
        '#',
        'Scenario',
        'GC time',
        'GC share',
        'Events',
        'Avg pause',
        'Max pause',
        'Minor',
        'Major',
        'Incr',
        'Forced',
      ],
      rows,
      rightAlignedColumns: new Set([0, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
    },
    useColors,
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

function formatDurationMs(durationMs: number): string {
  if (durationMs < 1000) {
    return `${durationMs.toFixed(2)} ms`
  }

  return `${(durationMs / 1000).toFixed(2)} s`
}

function formatPercent(value: number): string {
  return `${value.toFixed(2)} %`
}

function averageGcDurationMs(gc: GcSummary): number {
  return gc.totalCount > 0 ? gc.totalDurationMs / gc.totalCount : 0
}

function gcShare(gc: GcSummary): number {
  return gc.activeDurationMs > 0 ? (gc.totalDurationMs / gc.activeDurationMs) * 100 : 0
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

function printThroughputChart(entries: readonly ThroughputChartEntry[], useColors: boolean) {
  const rankedEntries = entries
    .filter((entry) => entry.result.success)
    .toSorted((left, right) => throughputValue(right.result) - throughputValue(left.result))

  const fastestThroughput = throughputValue(rankedEntries[0]?.result ?? emptyBenchmarkResult)
  const rows = rankedEntries.map((entry, index) => {
    const throughput = throughputValue(entry.result)
    const relative = fastestThroughput > 0 ? throughput / fastestThroughput : 0
    const colors = resultColor(rankedEntries.length - index - 1, rankedEntries.length)

    return [
      colorize(String(index + 1), useColors, ...colors),
      colorize(entry.label, useColors, ...colors),
      colorize(formatThroughput(entry.result), useColors, ...colors),
      colorize(formatBar(relative), useColors, ...colors),
    ]
  })

  printTable(
    {
      title: 'Throughput comparison (fastest = 100%)',
      headers: ['#', 'Scenario', 'Result', 'Relative'],
      rows,
      rightAlignedColumns: new Set([0, 2]),
    },
    useColors,
  )
}

function printMemoryEfficiencyChart(entries: readonly MemoryEfficiencyEntry[], useColors: boolean) {
  const rankedEntries = entries
    .filter((entry) => entry.result.success && entry.rssDelta > 0)
    .map((entry) => ({
      label: entry.label,
      result: entry.result,
      rssDelta: entry.rssDelta,
      opsPerMiB: throughputValue(entry.result) / (entry.rssDelta / 1024 / 1024),
    }))
    .toSorted((left, right) => right.opsPerMiB - left.opsPerMiB)

  const bestEfficiency = rankedEntries[0]?.opsPerMiB ?? 0
  const rows = rankedEntries.map((entry, index) => {
    const relative = bestEfficiency > 0 ? entry.opsPerMiB / bestEfficiency : 0
    const colors = resultColor(rankedEntries.length - index - 1, rankedEntries.length)

    return [
      colorize(String(index + 1), useColors, ...colors),
      colorize(entry.label, useColors, ...colors),
      colorize(`${entry.opsPerMiB.toFixed(0)} op/sec/MiB`, useColors, ...colors),
      colorize(
        formatBytes(entry.rssDelta),
        useColors,
        ...memoryColor(
          entry.rssDelta,
          rankedEntries.map((item) => item.rssDelta),
        ),
      ),
      colorize(formatBar(relative), useColors, ...colors),
    ]
  })

  printTable(
    {
      title: 'Memory efficiency comparison (throughput per RSS delta MiB)',
      headers: ['#', 'Scenario', 'Efficiency', 'RSS delta', 'Relative'],
      rows,
      rightAlignedColumns: new Set([0, 2, 3]),
    },
    useColors,
  )
}

function formatBar(ratio: number): string {
  const width = 24
  const clampedRatio = Math.min(1, Math.max(0, ratio))
  const filledWidth = Math.round(clampedRatio * width)
  const emptyWidth = width - filledWidth

  return `[${'#'.repeat(filledWidth)}${'.'.repeat(emptyWidth)}] ${(clampedRatio * 100).toFixed(1)} %`
}

const emptyBenchmarkResult: BenchmarkResult = {
  success: false,
  size: 0,
  min: 0,
  max: 0,
  mean: 0,
  stddev: 0,
  standardError: 0,
  percentiles: {},
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
