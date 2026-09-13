import type { GcSummary } from './gc.js'
import type { MemoryUsageSnapshot } from './memory.js'
import {
  createBenchmarkResult,
  formatOps,
  formatRelativeToBaseline,
  formatSignedPercent,
  formatThroughput,
  throughputPercentile,
  throughputValue,
  type BenchmarkResult,
  type RunMeasurement,
} from './results.js'

interface MemoryBenchmarkResult {
  scenario: {
    id?: string
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
  memoryTitle?: string
  useColors: boolean
  showCharts?: boolean
}

interface LabeledBenchmarkResult {
  id?: string
  label: string
  result: BenchmarkResult
  measurements?: readonly RunMeasurement[]
}

interface ThroughputChartEntry {
  label: string
  result: BenchmarkResult
}

const CRAB_BASELINE_PAIRS = [
  { baselineId: 'previous-serial', currentId: 'crab-serial', name: 'serial' },
  { baselineId: 'previous-batch', currentId: 'crab-batch', name: 'batch' },
  { baselineId: 'previous-producer', currentId: 'crab-producer', name: 'producer autoFlush' },
  {
    baselineId: 'previous-producer-manual',
    currentId: 'crab-producer-manual',
    name: 'producer manual flush',
  },
] as const

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

export function printBenchmarkResults(
  results: Record<string, BenchmarkResult> | readonly LabeledBenchmarkResult[],
  options: OutputOptions,
) {
  const entries = normalizeLabeledResults(results).toSorted(
    (left, right) => throughputValue(left.result) - throughputValue(right.result),
  )
  const rows = entries.map((entry, index) => {
    const colors = resultColor(index, entries.length)
    return [
      colorize(String(index + 1), options.useColors, ...colors),
      colorize(entry.label, options.useColors, ...colors),
      colorize(String(entry.result.size), options.useColors, ...colors),
      ...throughputStatCells(entry, options.useColors, colors),
    ]
  })

  printTable(
    {
      title: options.title ?? 'Consumer throughput',
      headers: ['#', 'Scenario', 'Runs', 'Mean', 'Median', 'p05', 'p95'],
      rows,
      rightAlignedColumns: new Set([0, 2, 3, 4, 5, 6]),
    },
    options.useColors,
  )
  printSpreadNotes(entries, options.useColors)
  printCrabBaselineComparison(entries, options.useColors)

  if (options.showCharts ?? true) {
    printThroughputChart(
      entries.map((entry) => ({ label: entry.label, result: entry.result })),
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

  const labeledResults: LabeledBenchmarkResult[] = rankedResults.map(({ result, benchmarkResult }) => ({
    id: result.scenario.id,
    label: result.scenario.label,
    result: benchmarkResult,
    measurements: result.measurements,
  }))

  const rows = rankedResults.map(({ result, benchmarkResult }, index) => {
    const colors = resultColor(index, rankedResults.length)
    const entry = labeledResults[index]

    return [
      colorize(String(index + 1), options.useColors, ...colors),
      colorize(result.scenario.label, options.useColors, ...colors),
      colorize(String(result.measurements.length), options.useColors, ...colors),
      ...throughputStatCells(
        entry ?? { label: result.scenario.label, result: benchmarkResult },
        options.useColors,
        colors,
      ),
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
      title: options.memoryTitle ?? 'Consumer benchmark (isolated process + lifecycle memory)',
      headers: [
        '#',
        'Scenario',
        'Runs',
        'Mean',
        'Median',
        'p05',
        'p95',
        'Peak RSS',
        'RSS delta',
        'Retained RSS',
        'Peak heap',
        'External',
        'ArrayBuffer',
      ],
      rows,
      rightAlignedColumns: new Set([0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]),
    },
    options.useColors,
  )
  printSpreadNotes(labeledResults, options.useColors)
  printCrabBaselineComparison(labeledResults, options.useColors)

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

function normalizeLabeledResults(
  results: Record<string, BenchmarkResult> | readonly LabeledBenchmarkResult[],
): LabeledBenchmarkResult[] {
  if (Array.isArray(results)) {
    return [...results]
  }

  return Object.entries(results).map(([label, result]) => ({ label, result }))
}

function throughputStatCells(entry: LabeledBenchmarkResult, useColors: boolean, colors: string[]): string[] {
  const measurements = entry.measurements
  const mean = formatThroughput(entry.result)
  if (!measurements || measurements.length === 0) {
    return [
      colorize(mean, useColors, ...colors),
      colorize(mean, useColors, styles.gray),
      colorize(mean, useColors, styles.gray),
      colorize(mean, useColors, styles.gray),
    ]
  }

  return [
    colorize(mean, useColors, ...colors),
    colorize(formatOps(throughputPercentile(measurements, 50)), useColors, ...colors),
    colorize(formatOps(throughputPercentile(measurements, 5)), useColors, styles.gray),
    colorize(formatOps(throughputPercentile(measurements, 95)), useColors, styles.gray),
  ]
}

function printSpreadNotes(entries: readonly LabeledBenchmarkResult[], useColors: boolean) {
  for (const entry of entries) {
    const measurements = entry.measurements
    if (!measurements || measurements.length < 2) {
      continue
    }

    const p05 = throughputPercentile(measurements, 5)
    const p95 = throughputPercentile(measurements, 95)
    if (p05 <= 0 || p95 / p05 < 1.5) {
      continue
    }

    console.log(
      colorize(
        `Note: ${entry.label} p95/p05 = ${(p95 / p05).toFixed(2)}; mean is a poor summary of this scenario.`,
        useColors,
        styles.yellow,
      ),
    )
  }
}

function printCrabBaselineComparison(entries: readonly LabeledBenchmarkResult[], useColors: boolean) {
  const byId = new Map(entries.filter((entry) => entry.id).map((entry) => [entry.id, entry]))
  const rows = CRAB_BASELINE_PAIRS.flatMap((pair) => {
    const baseline = byId.get(pair.baselineId)
    const current = byId.get(pair.currentId)
    if (!baseline || !current) {
      return []
    }

    const baselineMean = throughputValue(baseline.result)
    const currentMean = throughputValue(current.result)
    const delta = baselineMean > 0 ? ((currentMean - baselineMean) / baselineMean) * 100 : 0
    const deltaColor = delta >= 0 ? styles.green : styles.red
    const baselineMedian = baseline.measurements ? throughputPercentile(baseline.measurements, 50) : baselineMean
    const currentMedian = current.measurements ? throughputPercentile(current.measurements, 50) : currentMean
    const medianDelta = baselineMedian > 0 ? ((currentMedian - baselineMedian) / baselineMedian) * 100 : 0

    return [
      [
        colorize(pair.name, useColors),
        colorize(formatThroughput(baseline.result), useColors),
        colorize(formatThroughput(current.result), useColors),
        colorize(formatRelativeToBaseline(currentMean, baselineMean), useColors, deltaColor),
        colorize(formatSignedPercent(delta), useColors, deltaColor),
        colorize(formatSignedPercent(medianDelta), useColors, medianDelta >= 0 ? styles.green : styles.red),
      ],
    ]
  })

  if (rows.length === 0) {
    return
  }

  printTable(
    {
      title: 'vs previous kafka-crab-js@4.1.3',
      headers: ['Mode', 'previous mean', 'kafka-crab-js mean', 'this / previous', 'mean delta', 'median delta'],
      rows,
      rightAlignedColumns: new Set([1, 2, 3, 4, 5]),
    },
    useColors,
  )
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
