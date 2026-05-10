import { Consumer as PlatformaticKafkaConsumer, MessagesStreamModes } from '@platformatic/kafka'
import { printResults, type Result } from 'cronometro'
import { KafkaClient, type Message } from 'kafka-crab-js'
import { KafkaClient as KafkaClientV3 } from 'kafka-crab-js-v3'
import { Kafka as KafkaJS, logLevel } from 'kafkajs'
import { spawn } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import { setTimeout as sleep } from 'node:timers/promises'
import { fileURLToPath } from 'node:url'
import { brokers, topic } from './utils/definitions.js'

type BenchmarkLibrary = 'crab' | 'kafkajs' | 'platformatic-kafka'
type BenchmarkScenarioId =
  | 'v3-serial'
  | 'v4-serial'
  | 'kafkajs-serial'
  | 'platformatic-kafka'
  | 'v3-batch'
  | 'v4-batch'
  | 'kafkajs-batch'

interface BenchmarkScenario {
  id: BenchmarkScenarioId
  label: string
  library: BenchmarkLibrary
  run(): Promise<RunMeasurement>
}

interface RunState {
  seen: number
  measured: number
  startedAt?: bigint
  finishedAt?: bigint
}

interface RunMeasurement {
  messages: number
  elapsedNs: number
}

interface MemoryUsageSnapshot {
  rss: number
  heapUsed: number
  external: number
  arrayBuffers: number
}

interface MemoryChildResult {
  scenario: {
    id: BenchmarkScenarioId
    label: string
    library: BenchmarkLibrary
  }
  measurements: RunMeasurement[]
  memory: {
    peak: MemoryUsageSnapshot
    peakDelta: MemoryUsageSnapshot
    retainedDelta: MemoryUsageSnapshot
  }
}

const iterations = readPositiveInteger('BENCHMARK_ITERATIONS', 100_000)
const runs = readPositiveInteger('BENCHMARK_RUNS', 5)
const warmupRuns = readNonNegativeInteger('BENCHMARK_WARMUP_RUNS', 1)
const warmupMessages = readNonNegativeInteger('BENCHMARK_WARMUP_MESSAGES', 0)
const maxBytes = readPositiveInteger('BENCHMARK_MAX_BYTES', 2048)
const batchSize = readPositiveInteger('BENCHMARK_BATCH_SIZE', 4096)
const scenarioTimeoutMs = readPositiveInteger('BENCHMARK_SCENARIO_TIMEOUT_MS', 120_000)
const forceGcBeforeRun = readBoolean('BENCHMARK_FORCE_GC', true)
const selectedLibraries = readSelectedLibraries()
const selectedScenarios = readSelectedScenarios()
const memoryMode = readBoolean('BENCHMARK_MEMORY', false)
const memoryChildMode = readBoolean('BENCHMARK_MEMORY_CHILD', false)
const memorySampleIntervalMs = readPositiveInteger('BENCHMARK_MEMORY_SAMPLE_MS', 100)
const memorySettleMs = readNonNegativeInteger('BENCHMARK_MEMORY_SETTLE_MS', 100)
const memoryResultPrefix = 'BENCHMARK_MEMORY_RESULT '

class BenchmarkStopError extends Error {
  public name = 'BenchmarkStopError'
}

function readPositiveInteger(name: string, fallback: number): number {
  const raw = process.env[name]
  if (!raw) {
    return fallback
  }

  const parsed = Number(raw)
  return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : fallback
}

function readNonNegativeInteger(name: string, fallback: number): number {
  const raw = process.env[name]
  if (!raw) {
    return fallback
  }

  const parsed = Number(raw)
  return Number.isFinite(parsed) && parsed >= 0 ? Math.floor(parsed) : fallback
}

function readBoolean(name: string, fallback: boolean): boolean {
  const raw = process.env[name]
  if (!raw) {
    return fallback
  }

  switch (raw.trim().toLowerCase()) {
    case '1':
    case 'true':
    case 'yes':
    case 'on': {
      return true
    }
    case '0':
    case 'false':
    case 'no':
    case 'off': {
      return false
    }
    default: {
      return fallback
    }
  }
}

function readCsvValues(name: string): string[] {
  return Array.from(
    new Set(
      (process.env[name] ?? '')
        .split(',')
        .map((value) => value.trim().toLowerCase())
        .filter(Boolean),
    ),
  )
}

function readSelectedLibraries(): Set<BenchmarkLibrary> {
  const validLibraries = new Set<BenchmarkLibrary>(['crab', 'kafkajs', 'platformatic-kafka'])
  const values = readCsvValues('BENCHMARK_LIBS')
  const invalidValues = values.filter((value) => !validLibraries.has(value as BenchmarkLibrary))
  if (invalidValues.length > 0) {
    throw new Error(`Invalid BENCHMARK_LIBS value(s): ${invalidValues.join(', ')}`)
  }

  return new Set(values as BenchmarkLibrary[])
}

function readSelectedScenarios(): Set<BenchmarkScenarioId> {
  const values = readCsvValues('BENCHMARK_ONLY')
  return new Set(values as BenchmarkScenarioId[])
}

function createRunState(): RunState {
  return {
    seen: 0,
    measured: 0,
  }
}

function observeMessage(state: RunState): boolean {
  state.seen += 1
  if (state.seen <= warmupMessages) {
    return false
  }

  if (state.measured === 0) {
    state.startedAt = process.hrtime.bigint()
  }

  state.measured += 1
  if (state.measured < iterations) {
    return false
  }

  state.finishedAt = process.hrtime.bigint()
  return true
}

function finishRun(state: RunState): RunMeasurement {
  if (state.startedAt === undefined || state.finishedAt === undefined || state.measured < iterations) {
    throw new Error(`Benchmark run finished before ${iterations} measured messages were consumed`)
  }

  return {
    messages: state.measured,
    elapsedNs: Math.max(1, Number(state.finishedAt - state.startedAt)),
  }
}

function forceGc() {
  if (!forceGcBeforeRun) {
    return
  }

  const gc = (globalThis as typeof globalThis & { gc?: () => void }).gc
  if (gc) {
    gc()
  }
}

function formatOpsPerSecond(measurement: RunMeasurement): string {
  return ((measurement.messages * 1e9) / measurement.elapsedNs).toFixed(2)
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

function formatThroughput(result: Result): string {
  return result.success ? `${(1e9 / result.mean).toFixed(2)} op/sec` : 'Errored'
}

function formatTolerance(result: Result): string {
  return result.success ? `+/- ${((result.standardError / result.mean) * 100).toFixed(2)} %` : 'N/A'
}

function createBenchmarkResult(measurements: readonly RunMeasurement[]): Result {
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

function readMemoryUsage(): MemoryUsageSnapshot {
  const usage = process.memoryUsage()

  return {
    rss: usage.rss,
    heapUsed: usage.heapUsed,
    external: usage.external,
    arrayBuffers: usage.arrayBuffers,
  }
}

function maxMemoryUsage(left: MemoryUsageSnapshot, right: MemoryUsageSnapshot): MemoryUsageSnapshot {
  return {
    rss: Math.max(left.rss, right.rss),
    heapUsed: Math.max(left.heapUsed, right.heapUsed),
    external: Math.max(left.external, right.external),
    arrayBuffers: Math.max(left.arrayBuffers, right.arrayBuffers),
  }
}

function diffMemoryUsage(left: MemoryUsageSnapshot, right: MemoryUsageSnapshot): MemoryUsageSnapshot {
  return {
    rss: left.rss - right.rss,
    heapUsed: left.heapUsed - right.heapUsed,
    external: left.external - right.external,
    arrayBuffers: left.arrayBuffers - right.arrayBuffers,
  }
}

function startMemorySampler() {
  let peak = readMemoryUsage()
  const timer = setInterval(() => {
    peak = maxMemoryUsage(peak, readMemoryUsage())
  }, memorySampleIntervalMs)

  timer.unref()

  return {
    stop() {
      clearInterval(timer)
      peak = maxMemoryUsage(peak, readMemoryUsage())
      return peak
    },
  }
}

async function runScenario(scenario: BenchmarkScenario): Promise<RunMeasurement> {
  let timeout: ReturnType<typeof setTimeout> | undefined

  forceGc()

  try {
    return await Promise.race([
      scenario.run(),
      new Promise<RunMeasurement>((_, reject) => {
        timeout = setTimeout(() => {
          reject(
            new Error(
              `Scenario "${scenario.id}" timed out after ${scenarioTimeoutMs}ms. ` +
                `Check that topic "${topic}" has at least ${warmupMessages + iterations} readable messages. ` +
                'Run setup with BENCHMARK_SETUP_MESSAGES >= BENCHMARK_WARMUP_MESSAGES + BENCHMARK_ITERATIONS if needed.',
            ),
          )
        }, scenarioTimeoutMs)
      }),
    ])
  } finally {
    if (timeout) {
      clearTimeout(timeout)
    }
  }
}

async function kafkajsSerial(): Promise<RunMeasurement> {
  const { promise, resolve, reject } = Promise.withResolvers<RunMeasurement>()
  const state = createRunState()
  let completed = false

  const client = new KafkaJS({ clientId: 'benchmarks', brokers, logLevel: logLevel.ERROR })
  const consumer = client.consumer({ groupId: randomUUID(), maxWaitTimeInMs: 10, maxBytes })

  await consumer.connect()
  await consumer.subscribe({ topic, fromBeginning: true })

  consumer.on('consumer.crash', reject)

  consumer
    .run({
      autoCommit: false,
      partitionsConsumedConcurrently: 1,
      async eachMessage({ pause }) {
        if (completed) {
          return
        }

        if (!observeMessage(state)) {
          return
        }

        completed = true
        pause()
        resolve(finishRun(state))
        setImmediate(() => {
          consumer.disconnect().catch(() => {})
        })
      },
    })
    .catch(reject)

  return promise
}

async function kafkajsBatch(): Promise<RunMeasurement> {
  const { promise, resolve, reject } = Promise.withResolvers<RunMeasurement>()
  const state = createRunState()
  let completed = false

  const client = new KafkaJS({ clientId: 'benchmarks', brokers, logLevel: logLevel.ERROR })
  const consumer = client.consumer({ groupId: randomUUID(), maxWaitTimeInMs: 10, maxBytes })

  await consumer.connect()
  await consumer.subscribe({ topic, fromBeginning: true })

  consumer.on('consumer.crash', reject)

  consumer
    .run({
      autoCommit: false,
      eachBatchAutoResolve: false,
      partitionsConsumedConcurrently: 3,
      async eachBatch({ batch, pause, resolveOffset }) {
        for (const message of batch.messages) {
          if (completed) {
            return
          }

          resolveOffset(message.offset)

          if (!observeMessage(state)) {
            continue
          }

          pause()
          completed = true
          resolve(finishRun(state))
          setImmediate(() => {
            consumer.disconnect().catch(() => {})
          })
          return
        }
      },
    })
    .catch(reject)

  return promise
}

async function platformaticKafka(): Promise<RunMeasurement> {
  const { promise, resolve, reject } = Promise.withResolvers<RunMeasurement>()
  const state = createRunState()

  const consumer = new PlatformaticKafkaConsumer<Buffer, Buffer>({
    clientId: 'benchmarks',
    groupId: randomUUID(),
    bootstrapBrokers: brokers,
    minBytes: 1,
    maxBytes,
    maxWaitTime: 10,
    autocommit: false,
  })

  const stream = await consumer.consume({
    topics: [topic],
    mode: MessagesStreamModes.EARLIEST,
  })

  stream.on('data', () => {
    if (!observeMessage(state)) {
      return
    }

    stream.removeAllListeners('data')
    stream.pause()
    resolve(finishRun(state))

    setImmediate(() => {
      consumer.close(true, () => {
        // Noop
      })
    })
  })

  stream.on('error', reject)

  return promise
}

async function kafkaCrabJsV3(useBatchMode = false): Promise<RunMeasurement> {
  const { promise, resolve, reject } = Promise.withResolvers<RunMeasurement>()
  const state = createRunState()

  const client = new KafkaClientV3({
    brokers: brokers.join(','),
    clientId: 'benchmarks',
    securityProtocol: 'Plaintext',
    logLevel: 'warn',
    brokerAddressFamily: 'v4',
  })

  const consumer = client.createStreamConsumer({
    groupId: randomUUID(),
    enableAutoCommit: false,
    batchSize: useBatchMode ? batchSize : 1,
    configuration: {
      'auto.offset.reset': 'earliest',
      'enable.auto.commit': false,
      'fetch.min.bytes': 1,
      'fetch.message.max.bytes': maxBytes,
      'fetch.wait.max.ms': 10,
    },
  })

  await consumer.subscribe([{ topic, allOffsets: { position: 'Beginning' } }])

  consumer.on('data', () => {
    if (!observeMessage(state)) {
      return
    }

    consumer.removeAllListeners('data')
    consumer.pause()
    const measurement = finishRun(state)

    disconnectV3Consumer(consumer).then(() => {
      resolve(measurement)
    }, reject)
  })

  consumer.on('error', reject)

  return promise
}

async function kafkaCrabJsV4(useBatchMode = false): Promise<RunMeasurement> {
  const state = createRunState()
  const stopError = new BenchmarkStopError()

  const client = new KafkaClient({
    brokers: brokers.join(','),
    clientId: 'benchmarks',
    securityProtocol: 'Plaintext',
    logLevel: 'warn',
    brokerAddressFamily: 'v4',
    diagnostics: false,
  })

  const webConsumer = client.createWebStreamConsumer({
    groupId: randomUUID(),
    enableAutoCommit: false,
    batchSize: useBatchMode ? batchSize : 1,
    batchTimeout: 2,
    serialPrefetchSize: 64,
    serialPrefetchTimeout: 5,
    configuration: {
      'auto.offset.reset': 'earliest',
      'enable.auto.commit': false,
      'fetch.min.bytes': 1,
      'fetch.message.max.bytes': maxBytes,
      'fetch.wait.max.ms': 10,
    },
  })

  await webConsumer.consumer.subscribe([{ topic, allOffsets: { position: 'Beginning' } }])

  try {
    try {
      if (useBatchMode) {
        const stream = webConsumer.stream as ReadableStream<Message[]>
        await stream.pipeTo(
          new WritableStream<Message[]>({
            write(messages) {
              for (const message of messages) {
                void message
                if (observeMessage(state)) {
                  throw stopError
                }
              }
            },
          }),
        )
      } else {
        const stream = webConsumer.stream as ReadableStream<Message>
        await stream.pipeTo(
          new WritableStream<Message>({
            write(message) {
              void message
              if (observeMessage(state)) {
                throw stopError
              }
            },
          }),
        )
      }
    } catch (error) {
      if (!(error instanceof BenchmarkStopError)) {
        throw error
      }
    }

    return finishRun(state)
  } finally {
    try {
      webConsumer.consumer.unsubscribe()
    } catch {
      // Noop
    }

    try {
      await webConsumer.consumer.disconnect()
    } catch {
      // Noop
    }
  }
}

async function disconnectV3Consumer(consumer: { unsubscribe(): void; disconnect(): Promise<void> }) {
  try {
    consumer.unsubscribe()
  } catch {
    // Noop
  }

  await consumer.disconnect()
}

const scenarios: BenchmarkScenario[] = [
  {
    id: 'v3-serial',
    label: 'v3 kafka-crab-js (serial)',
    library: 'crab',
    run: () => kafkaCrabJsV3(false),
  },
  {
    id: 'v4-serial',
    label: 'kafka-crab-js v4 (stream, serial)',
    library: 'crab',
    run: () => kafkaCrabJsV4(false),
  },
  {
    id: 'kafkajs-serial',
    label: 'KafkaJS (eachMessage)',
    library: 'kafkajs',
    run: kafkajsSerial,
  },
  {
    id: 'platformatic-kafka',
    label: '@platformatic/kafka',
    library: 'platformatic-kafka',
    run: platformaticKafka,
  },
  {
    id: 'v3-batch',
    label: 'v3 kafka-crab-js (batch)',
    library: 'crab',
    run: () => kafkaCrabJsV3(true),
  },
  {
    id: 'v4-batch',
    label: 'kafka-crab-js v4 (stream, batch)',
    library: 'crab',
    run: () => kafkaCrabJsV4(true),
  },
  {
    id: 'kafkajs-batch',
    label: 'KafkaJS (eachBatch)',
    library: 'kafkajs',
    run: kafkajsBatch,
  },
]

function isV3Scenario(scenario: BenchmarkScenario): boolean {
  return scenario.id === 'v3-serial' || scenario.id === 'v3-batch'
}

function selectScenarios(options: { includeV3: boolean }): BenchmarkScenario[] {
  return scenarios.filter((scenario) => {
    if (!options.includeV3 && isV3Scenario(scenario)) {
      return false
    }

    if (selectedLibraries.size > 0 && !selectedLibraries.has(scenario.library)) {
      return false
    }

    return selectedScenarios.size === 0 || selectedScenarios.has(scenario.id)
  })
}

async function main() {
  console.log('Starting consumer benchmark...')
  console.log(`Benchmark brokers: ${brokers.join(',')}`)
  console.log(`Benchmark topic: ${topic}`)
  console.log(`Benchmark iterations: ${iterations}`)
  console.log(`Benchmark warmup messages: ${warmupMessages}`)
  console.log(`Benchmark warmup runs: ${warmupRuns}`)
  console.log(`Benchmark runs: ${runs}`)
  console.log(`Benchmark force GC before run: ${forceGcBeforeRun}`)
  console.log(`Benchmark scenario timeout: ${scenarioTimeoutMs}ms`)
  console.log(`Benchmark max bytes: ${maxBytes}`)
  console.log(`Benchmark batch size: ${batchSize}`)

  const scenariosToRun = selectScenarios({ includeV3: true })

  if (scenariosToRun.length === 0) {
    throw new Error('No benchmark scenarios selected')
  }

  console.log(`Benchmark scenarios: ${scenariosToRun.map((scenario) => scenario.id).join(', ')}`)

  const measurements = new Map<BenchmarkScenarioId, RunMeasurement[]>()
  for (const scenario of scenariosToRun) {
    measurements.set(scenario.id, [])
  }

  for (let warmupRunIndex = 1; warmupRunIndex <= warmupRuns; warmupRunIndex++) {
    console.log(`Starting benchmark warmup run ${warmupRunIndex}/${warmupRuns}`)

    for (const scenario of scenariosToRun) {
      console.log(`Warming scenario: ${scenario.id} (${warmupRunIndex}/${warmupRuns})`)
      await runScenario(scenario)
    }
  }

  for (let runIndex = 1; runIndex <= runs; runIndex++) {
    console.log(`Starting benchmark run ${runIndex}/${runs}`)

    for (const scenario of scenariosToRun) {
      console.log(`Running scenario: ${scenario.id} (${runIndex}/${runs})`)
      const measurement = await runScenario(scenario)
      measurements.get(scenario.id)?.push(measurement)
      console.log(
        `Completed scenario: ${scenario.id} (${runIndex}/${runs}) ` +
          `${formatOpsPerSecond(measurement)} op/sec over ${measurement.messages} messages`,
      )
    }
  }

  const results: Record<string, Result> = {}
  for (const scenario of scenariosToRun) {
    results[scenario.label] = createBenchmarkResult(measurements.get(scenario.id) ?? [])
  }

  printResults(results, true, true, 'previous')
}

async function runMemoryChild() {
  const scenarioIds = readCsvValues('BENCHMARK_ONLY')
  if (scenarioIds.length !== 1) {
    throw new Error('BENCHMARK_MEMORY_CHILD requires exactly one BENCHMARK_ONLY scenario id')
  }

  const scenario = scenarios.find((item) => item.id === scenarioIds[0])
  if (!scenario) {
    throw new Error(`Unknown benchmark scenario: ${scenarioIds[0]}`)
  }

  if (isV3Scenario(scenario)) {
    throw new Error(`Memory benchmark does not run v3 scenario: ${scenario.id}`)
  }

  forceGc()
  await sleep(0)
  const baseline = readMemoryUsage()
  const sampler = startMemorySampler()
  const measurements: RunMeasurement[] = []

  try {
    for (let warmupRunIndex = 1; warmupRunIndex <= warmupRuns; warmupRunIndex++) {
      await runScenario(scenario)
      await sleep(memorySettleMs)
    }

    for (let runIndex = 1; runIndex <= runs; runIndex++) {
      measurements.push(await runScenario(scenario))
      await sleep(memorySettleMs)
    }
  } finally {
    await sleep(memorySettleMs)
  }

  const peak = sampler.stop()
  forceGc()
  await sleep(0)
  forceGc()
  const after = readMemoryUsage()
  const result: MemoryChildResult = {
    scenario: {
      id: scenario.id,
      label: scenario.label,
      library: scenario.library,
    },
    measurements,
    memory: {
      peak,
      peakDelta: diffMemoryUsage(peak, baseline),
      retainedDelta: diffMemoryUsage(after, baseline),
    },
  }

  console.log(`${memoryResultPrefix}${JSON.stringify(result)}`)
}

function childNodeArgs(): string[] {
  return process.execArgv.includes('--expose-gc') ? process.execArgv : ['--expose-gc', ...process.execArgv]
}

function parseMemoryChildResult(stdout: string): MemoryChildResult {
  const resultLine = stdout.split(/\r?\n/).findLast((line) => line.startsWith(memoryResultPrefix))

  if (!resultLine) {
    throw new Error(`Memory child did not print ${memoryResultPrefix.trim()} output`)
  }

  return JSON.parse(resultLine.slice(memoryResultPrefix.length)) as MemoryChildResult
}

async function runScenarioInIsolatedProcess(scenario: BenchmarkScenario): Promise<MemoryChildResult> {
  const scriptPath = process.argv[1] ?? fileURLToPath(import.meta.url)
  const child = spawn(process.execPath, [...childNodeArgs(), scriptPath], {
    cwd: process.cwd(),
    env: {
      ...process.env,
      BENCHMARK_MEMORY: '0',
      BENCHMARK_MEMORY_CHILD: '1',
      BENCHMARK_ONLY: scenario.id,
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  })

  let stdout = ''
  let stderr = ''
  child.stdout.setEncoding('utf8')
  child.stderr.setEncoding('utf8')
  child.stdout.on('data', (chunk) => {
    stdout += chunk
  })
  child.stderr.on('data', (chunk) => {
    stderr += chunk
  })

  const exitCode = await new Promise<number | null>((resolve, reject) => {
    child.on('error', reject)
    child.on('close', resolve)
  })

  if (exitCode !== 0) {
    throw new Error(
      `Memory child for "${scenario.id}" exited with code ${exitCode ?? 'unknown'}\n` +
        `stdout:\n${stdout}\n` +
        `stderr:\n${stderr}`,
    )
  }

  return parseMemoryChildResult(stdout)
}

function printMemoryResults(results: readonly MemoryChildResult[]) {
  const rows = [
    [
      'Scenario',
      'Runs',
      'Result',
      'Tolerance',
      'Peak RSS',
      'Peak RSS delta',
      'Peak heap',
      'Peak external',
      'Peak ArrayBuffer',
      'Retained RSS',
    ],
  ]

  for (const result of results) {
    const benchmarkResult = createBenchmarkResult(result.measurements)
    rows.push([
      result.scenario.label,
      String(result.measurements.length),
      formatThroughput(benchmarkResult),
      formatTolerance(benchmarkResult),
      formatBytes(result.memory.peak.rss),
      formatBytes(result.memory.peakDelta.rss),
      formatBytes(result.memory.peak.heapUsed),
      formatBytes(result.memory.peak.external),
      formatBytes(result.memory.peak.arrayBuffers),
      formatBytes(result.memory.retainedDelta.rss),
    ])
  }

  const header = rows[0] ?? []
  const widths = header.map((_, columnIndex) => Math.max(...rows.map((row) => row[columnIndex]?.length ?? 0)))

  for (const [rowIndex, row] of rows.entries()) {
    const line = row
      .map((cell, columnIndex) => {
        const width = widths[columnIndex] ?? cell.length
        const padded = columnIndex === 0 ? cell.padEnd(width) : cell.padStart(width)
        return ` ${padded} `
      })
      .join('|')
    console.log(line)

    if (rowIndex === 0) {
      console.log(widths.map((width) => '-'.repeat(width + 2)).join('+'))
    }
  }
}

async function runIsolatedMemoryBenchmark() {
  console.log('Starting isolated consumer memory benchmark...')
  console.log(`Benchmark brokers: ${brokers.join(',')}`)
  console.log(`Benchmark topic: ${topic}`)
  console.log(`Benchmark iterations: ${iterations}`)
  console.log(`Benchmark warmup runs: ${warmupRuns}`)
  console.log(`Benchmark runs: ${runs}`)
  console.log(`Benchmark batch size: ${batchSize}`)
  console.log(`Benchmark memory sample interval: ${memorySampleIntervalMs}ms`)
  console.log(`Benchmark memory settle time: ${memorySettleMs}ms`)
  console.log('Benchmark memory mode skips v3 kafka-crab-js scenarios')

  const scenariosToRun = selectScenarios({ includeV3: false })
  if (scenariosToRun.length === 0) {
    throw new Error('No memory benchmark scenarios selected')
  }

  console.log(`Benchmark scenarios: ${scenariosToRun.map((scenario) => scenario.id).join(', ')}`)

  const results: MemoryChildResult[] = []
  for (const scenario of scenariosToRun) {
    console.log(`Running isolated memory scenario: ${scenario.id}`)
    results.push(await runScenarioInIsolatedProcess(scenario))
  }

  printMemoryResults(results)
}

let entrypoint = main
if (memoryChildMode) {
  entrypoint = runMemoryChild
} else if (memoryMode) {
  entrypoint = runIsolatedMemoryBenchmark
}

entrypoint().catch((error) => {
  console.error(error instanceof Error ? error : new Error(String(error)))
  process.exit(1)
})
