export interface RunMeasurement {
  messages: number
  elapsedNs: number
}

export interface BenchmarkResult {
  success: boolean
  error?: Error
  size: number
  min: number
  max: number
  mean: number
  stddev: number
  standardError: number
  percentiles: Record<string, number>
}

export function formatOpsPerSecond(measurement: RunMeasurement): string {
  return ((measurement.messages * 1e9) / measurement.elapsedNs).toFixed(2)
}

export function formatThroughput(result: BenchmarkResult): string {
  return result.success ? `${throughputValue(result).toFixed(2)} op/sec` : 'Errored'
}

export function throughputValue(result: BenchmarkResult): number {
  return result.success ? 1e9 / result.mean : 0
}

export function formatTolerance(result: BenchmarkResult): string {
  return result.success ? `+/- ${((result.standardError / result.mean) * 100).toFixed(2)} %` : 'N/A'
}

export function formatDifference(current: BenchmarkResult, previous?: BenchmarkResult): string {
  if (!current.success || !previous?.success) {
    return ''
  }

  const previousThroughput = throughputValue(previous)
  if (previousThroughput <= 0) {
    return ''
  }

  return `+ ${(((throughputValue(current) - previousThroughput) / previousThroughput) * 100).toFixed(2)} %`
}

export function createBenchmarkResult(measurements: readonly RunMeasurement[]): BenchmarkResult {
  if (measurements.length === 0) {
    return {
      success: false,
      error: new Error('No benchmark measurements were collected'),
      size: 0,
      min: 0,
      max: 0,
      mean: 0,
      stddev: 0,
      standardError: 0,
      percentiles: {},
    }
  }

  const values = measurements.map((measurement) => measurement.elapsedNs / measurement.messages)
  const mean = values.reduce((sum, value) => sum + value, 0) / values.length
  const variance = values.reduce((sum, value) => sum + (value - mean) ** 2, 0) / values.length
  const stddev = Math.sqrt(variance)
  const sortedValues = values.toSorted((a, b) => a - b)

  return {
    success: true,
    size: values.length,
    min: sortedValues[0] ?? 0,
    max: sortedValues.at(-1) ?? 0,
    mean,
    stddev,
    standardError: stddev / Math.sqrt(values.length),
    percentiles: createPercentiles(sortedValues),
  }
}

function createPercentiles(sortedValues: readonly number[]): Record<string, number> {
  const percentiles = [0.001, 0.01, 0.1, 1, 2.5, 10, 25, 50, 75, 90, 97.5, 99, 99.9, 99.99, 99.999]

  return Object.fromEntries(
    percentiles.map((percentile) => [String(percentile), percentileValue(sortedValues, percentile)]),
  )
}

function percentileValue(sortedValues: readonly number[], percentile: number): number {
  if (sortedValues.length === 0) {
    return 0
  }

  const index = Math.min(sortedValues.length - 1, Math.max(0, Math.ceil((percentile / 100) * sortedValues.length) - 1))
  return sortedValues[index] ?? 0
}
