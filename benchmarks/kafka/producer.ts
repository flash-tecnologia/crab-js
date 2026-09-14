import type { RecordMetadata } from 'kafka-crab-js'
import { spawn } from 'node:child_process'
import { setTimeout as sleep } from 'node:timers/promises'
import { fileURLToPath } from 'node:url'
import { brokers, partitionCount } from './utils/definitions.js'
import { readBoolean, readCsvValues, readNonNegativeInteger, readPositiveInteger } from './utils/env.js'
import { startGcObserver, type GcSummary } from './utils/gc.js'
import { diffMemoryUsage, readMemoryUsage, startMemorySampler, type MemoryUsageSnapshot } from './utils/memory.js'
import { printBenchmarkResults, printMemoryResults } from './utils/output.js'
import { maybeShuffle } from './utils/shuffle.js'
import { createBenchmarkResult, formatOpsPerSecond, type RunMeasurement } from './utils/results.js'
import { createBenchmarkMessage, createBenchmarkPartitionKeys } from './utils/messages.js'

type BenchmarkLibrary = 'crab'
type BenchmarkScenarioId = 'previous-producer' | 'crab-producer' | 'previous-producer-manual' | 'crab-producer-manual'

interface ProducerLike {
  send: (record: { topic: string; messages: unknown[] }) => Promise<RecordMetadata[]>
  flush: () => Promise<RecordMetadata[]>
}

interface BenchmarkScenario {
  id: BenchmarkScenarioId
  label: string
  library: BenchmarkLibrary
  run: (hooks?: RunMeasurementHooks) => Promise<RunMeasurement>
}

interface RunMeasurementHooks {
  onMeasureStart?: () => void
  onMeasureFinish?: () => void
}

interface RunState {
  seen: number
  measured: number
  hooks: RunMeasurementHooks
  startedAt?: bigint
  finishedAt?: bigint
  finished: boolean
}

interface BatchLatencySummary {
  batches: number
  mean: number
  p50: number
  p95: number
  p99: number
  max: number
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
  gc: GcSummary
  latency?: BatchLatencySummary
}

const producerTopic = process.env.BENCHMARK_PRODUCER_TOPIC?.trim() || 'benchmarks-producer'
const iterations = readPositiveInteger('BENCHMARK_PRODUCER_ITERATIONS', 20_000)
const batchSize = readPositiveInteger('BENCHMARK_PRODUCER_BATCH_SIZE', 100)
const runs = readPositiveInteger('BENCHMARK_RUNS', 3)
const scenarioTimeoutMs = readPositiveInteger('BENCHMARK_SCENARIO_TIMEOUT_MS', 120_000)
const forceGcBeforeRun = readBoolean('BENCHMARK_FORCE_GC', true)
const selectedLibraries = readSelectedLibraries()
const selectedScenarios = readSelectedScenarios()
const showPreviousScenarios = readBoolean('BENCHMARK_SHOW_PREVIOUS', false)
const shuffleScenarios = readBoolean('BENCHMARK_SHUFFLE_SCENARIOS', false)
const isolatedMode = readBoolean('BENCHMARK_ISOLATED', false)
const memoryMode = readBoolean('BENCHMARK_MEMORY', true)
const memoryChildMode = readBoolean('BENCHMARK_MEMORY_CHILD', false)
const memorySampleIntervalMs = readPositiveInteger('BENCHMARK_MEMORY_SAMPLE_MS', 100)
const memorySettleMs = readNonNegativeInteger('BENCHMARK_MEMORY_SETTLE_MS', 100)
const memoryResultPrefix = 'BENCHMARK_MEMORY_RESULT '
const useColors = readBoolean('BENCHMARK_COLORS', true)
const showCharts = readBoolean('BENCHMARK_CHARTS', true)

const batchLatenciesMs = new Map<BenchmarkScenarioId, number[]>()

function readSelectedLibraries(): Set<BenchmarkLibrary> {
  const validLibraries = new Set<BenchmarkLibrary>(['crab'])
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

function createRunState(hooks: RunMeasurementHooks = {}): RunState {
  return {
    seen: 0,
    measured: 0,
    hooks,
    finished: false,
  }
}

function startMeasurement(state: RunState) {
  if (state.startedAt !== undefined) {
    return
  }

  state.startedAt = process.hrtime.bigint()
  state.hooks.onMeasureStart?.()
}

function finishMeasurement(state: RunState) {
  if (state.finished) {
    return
  }

  state.finishedAt = process.hrtime.bigint()
  state.finished = true
  state.hooks.onMeasureFinish?.()
}

function finishRun(state: RunState): RunMeasurement {
  if (state.startedAt === undefined || state.finishedAt === undefined || state.measured < iterations) {
    throw new Error(`Benchmark run finished before ${iterations} measured messages were produced`)
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

async function runScenario(scenario: BenchmarkScenario, hooks?: RunMeasurementHooks): Promise<RunMeasurement> {
  let timeout: ReturnType<typeof setTimeout> | undefined

  forceGc()

  try {
    return await new Promise<RunMeasurement>((resolve, reject) => {
      timeout = setTimeout(() => {
        reject(new Error(`Scenario ${scenario.id} timed out after ${scenarioTimeoutMs}ms`))
      }, scenarioTimeoutMs)
      timeout.unref?.()

      scenario
        .run(hooks)
        .then(resolve, reject)
        .finally(() => {
          if (timeout) {
            clearTimeout(timeout)
          }
        })
    })
  } finally {
    if (timeout) {
      clearTimeout(timeout)
    }
  }
}

function createKafkaCrabJsClientConfiguration() {
  return {
    brokers: brokers.join(','),
    clientId: 'benchmarks-producer',
    securityProtocol: 'Plaintext',
    logLevel: 'warn',
    brokerAddressFamily: 'v4',
    diagnostics: false,
  } as const
}

async function ensureProducerTopic() {
  const { KafkaClient } = await import('kafka-crab-js')
  const client = new KafkaClient(createKafkaCrabJsClientConfiguration())
  const consumer = client.createConsumer({
    groupId: `benchmark-producer-setup-${process.pid}`,
    enableAutoCommit: false,
    fetchMetadataTimeout: 30_000,
    configuration: { 'auto.offset.reset': 'earliest' },
  })

  try {
    await consumer.subscribe([
      {
        topic: producerTopic,
        createTopic: true,
        numPartitions: partitionCount,
        replicas: 1,
        allOffsets: { position: 'Beginning' },
      },
    ])
  } finally {
    try {
      await consumer.disconnect()
    } catch {
      // Ignore cleanup failures so the original setup error is preserved.
    }
  }
}

async function runProducer(
  scenarioId: BenchmarkScenarioId,
  createProducer: () => Promise<ProducerLike>,
  autoFlush: boolean,
  hooks: RunMeasurementHooks = {},
): Promise<RunMeasurement> {
  const state = createRunState(hooks)
  const producer = await createProducer()
  const partitionKeys = createBenchmarkPartitionKeys(partitionCount)
  const latencies = batchLatenciesMs.get(scenarioId) ?? []
  batchLatenciesMs.set(scenarioId, latencies)

  try {
    while (state.measured < iterations) {
      const count = Math.min(batchSize, iterations - state.measured)
      const base = state.measured
      const messages = Array.from({ length: count }, (_, index) => createBenchmarkMessage(base + index, partitionKeys))
      const batchStart = process.hrtime.bigint()
      if (autoFlush) {
        const result = await producer.send({ topic: producerTopic, messages })
        if (!Array.isArray(result) || result.length !== count) {
          throw new Error(`Expected ${count} delivery confirmations, got ${result?.length ?? 'none'}`)
        }
      } else {
        await producer.send({ topic: producerTopic, messages })
        const result = await producer.flush()
        if (!Array.isArray(result) || result.length !== count) {
          throw new Error(`Expected ${count} delivery confirmations, got ${result?.length ?? 'none'}`)
        }
      }
      latencies.push(Number(process.hrtime.bigint() - batchStart) / 1e6)

      state.seen += count
      if (state.measured === 0) {
        startMeasurement(state)
      }
      state.measured += count
      if (state.measured >= iterations) {
        finishMeasurement(state)
        return finishRun(state)
      }
    }
  } finally {
    try {
      await producer.flush()
    } catch {
      // Ignore cleanup failures so the measurement error is preserved.
    }
  }

  return finishRun(state)
}

async function createWorkspaceProducer(autoFlush: boolean): Promise<ProducerLike> {
  // Dynamic import (mirroring consumer.ts) keeps the workspace native binding out of
  // Previous-version children and vice versa, preserving clean per-scenario process state.
  const { KafkaClient } = await import('kafka-crab-js')
  const client = new KafkaClient(createKafkaCrabJsClientConfiguration())
  return client.createProducer(autoFlush ? {} : { autoFlush: false }) as unknown as ProducerLike
}

async function createPreviousProducer(autoFlush: boolean): Promise<ProducerLike> {
  const { KafkaClient: KafkaClientPrevious } = (await import('kafka-crab-js-previous')) as unknown as {
    KafkaClient: new (config: Record<string, unknown>) => {
      createProducer: (config: Record<string, unknown>) => ProducerLike
    }
  }
  const client = new KafkaClientPrevious({ ...createKafkaCrabJsClientConfiguration() })
  return client.createProducer(autoFlush ? {} : { autoFlush: false })
}

const scenarios: BenchmarkScenario[] = [
  {
    id: 'previous-producer',
    label: 'previous kafka-crab-js (producer, autoFlush)',
    library: 'crab',
    run: (hooks) => runProducer('previous-producer', () => createPreviousProducer(true), true, hooks),
  },
  {
    id: 'crab-producer',
    label: 'kafka-crab-js (producer, autoFlush)',
    library: 'crab',
    run: (hooks) => runProducer('crab-producer', () => createWorkspaceProducer(true), true, hooks),
  },
  {
    id: 'previous-producer-manual',
    label: 'previous kafka-crab-js (producer, manual flush)',
    library: 'crab',
    run: (hooks) => runProducer('previous-producer-manual', () => createPreviousProducer(false), false, hooks),
  },
  {
    id: 'crab-producer-manual',
    label: 'kafka-crab-js (producer, manual flush)',
    library: 'crab',
    run: (hooks) => runProducer('crab-producer-manual', () => createWorkspaceProducer(false), false, hooks),
  },
]

function isPreviousScenario(scenario: BenchmarkScenario): boolean {
  return scenario.id === 'previous-producer' || scenario.id === 'previous-producer-manual'
}

function isPreviousScenarioId(scenarioId: BenchmarkScenarioId): boolean {
  return scenarioId === 'previous-producer' || scenarioId === 'previous-producer-manual'
}

function selectedPreviousScenarioIds(): BenchmarkScenarioId[] {
  return [...selectedScenarios].filter(isPreviousScenarioId)
}

function shouldShowPreviousScenarios(): boolean {
  return (
    selectedPreviousScenarioIds().length > 0 ||
    (showPreviousScenarios && (selectedLibraries.size === 0 || selectedLibraries.has('crab')))
  )
}

function selectScenarios(): BenchmarkScenario[] {
  const includePrevious = shouldShowPreviousScenarios()

  return scenarios.filter((scenario) => {
    if (isPreviousScenario(scenario) && !includePrevious) {
      return false
    }

    if (selectedLibraries.size > 0 && !selectedLibraries.has(scenario.library)) {
      return false
    }

    return selectedScenarios.size === 0 || selectedScenarios.has(scenario.id)
  })
}

function scenariosInRunOrder(selected: readonly BenchmarkScenario[]): BenchmarkScenario[] {
  return maybeShuffle(selected, shuffleScenarios)
}

function percentile(sortedValues: number[], pct: number): number {
  if (sortedValues.length === 0) {
    return 0
  }

  const rank = Math.min(sortedValues.length - 1, Math.floor((pct / 100) * sortedValues.length))
  return sortedValues[rank] ?? 0
}

function summarizeLatencies(latencies: number[]): BatchLatencySummary | undefined {
  if (latencies.length === 0) {
    return undefined
  }

  const sorted = latencies.toSorted((a, b) => a - b)
  return {
    batches: sorted.length,
    mean: sorted.reduce((sum, value) => sum + value, 0) / sorted.length,
    p50: percentile(sorted, 50),
    p95: percentile(sorted, 95),
    p99: percentile(sorted, 99),
    max: sorted.at(-1) ?? 0,
  }
}

function printBatchLatencyResults() {
  console.log('\nProducer batch latency (ms per send, lower is better)')
  console.log(
    ' # | Scenario                                    | Batches |     Mean |      p50 |      p95 |      p99 |      Max',
  )
  console.log(
    '---+---------------------------------------------+---------+----------+----------+----------+----------+----------',
  )

  let index = 0
  for (const scenario of selectScenarios()) {
    const latencies = (batchLatenciesMs.get(scenario.id) ?? []).toSorted((a, b) => a - b)
    if (latencies.length === 0) {
      continue
    }

    const mean = latencies.reduce((sum, value) => sum + value, 0) / latencies.length
    index += 1
    console.log(
      ` ${String(index).padStart(1)} | ${scenario.label.padEnd(43)} | ${String(latencies.length).padStart(7)} | ` +
        `${mean.toFixed(2).padStart(8)} | ${percentile(latencies, 50).toFixed(2).padStart(8)} | ` +
        `${percentile(latencies, 95).toFixed(2).padStart(8)} | ${percentile(latencies, 99).toFixed(2).padStart(8)} | ` +
        (latencies.at(-1) ?? 0).toFixed(2).padStart(8),
    )
  }
}

async function main() {
  console.log('Starting producer benchmark...')
  console.log(`Benchmark brokers: ${brokers.join(',')}`)
  console.log(`Benchmark topic: ${producerTopic}`)
  console.log(`Benchmark iterations: ${iterations}`)
  console.log(`Benchmark batch size: ${batchSize}`)
  console.log(`Benchmark runs: ${runs}`)
  console.log(`Benchmark force GC before run: ${forceGcBeforeRun}`)
  console.log(`Benchmark scenario timeout: ${scenarioTimeoutMs}ms`)

  await ensureProducerTopic()

  const scenariosToRun = selectScenarios()

  if (scenariosToRun.length === 0) {
    throw new Error('No benchmark scenarios selected')
  }

  console.log(`Benchmark scenarios: ${scenariosToRun.map((scenario) => scenario.id).join(', ')}`)
  console.log(`Benchmark shuffle scenarios: ${shuffleScenarios}`)

  const measurements = new Map<BenchmarkScenarioId, RunMeasurement[]>()
  for (const scenario of scenariosToRun) {
    measurements.set(scenario.id, [])
  }

  for (let runIndex = 1; runIndex <= runs; runIndex++) {
    console.log(`Starting benchmark run ${runIndex}/${runs}`)
    const runOrder = scenariosInRunOrder(scenariosToRun)
    if (shuffleScenarios) {
      console.log(`Run ${runIndex} scenario order: ${runOrder.map((scenario) => scenario.id).join(', ')}`)
    }

    for (const scenario of runOrder) {
      console.log(`Running scenario: ${scenario.id} (${runIndex}/${runs})`)
      const measurement = await runScenario(scenario)
      measurements.get(scenario.id)?.push(measurement)
      console.log(
        `Completed scenario: ${scenario.id} (${runIndex}/${runs}) ` +
          `${formatOpsPerSecond(measurement)} op/sec over ${measurement.messages} messages`,
      )
    }
  }

  printBenchmarkResults(
    scenariosToRun.map((scenario) => {
      const scenarioMeasurements = measurements.get(scenario.id) ?? []
      return {
        id: scenario.id,
        label: scenario.label,
        result: createBenchmarkResult(scenarioMeasurements),
        measurements: scenarioMeasurements,
      }
    }),
    { title: 'Producer benchmark (same process)', useColors, showCharts },
  )
  printBatchLatencyResults()
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

  await ensureProducerTopic()

  forceGc()
  await sleep(0)
  const baseline = readMemoryUsage()
  const sampler = startMemorySampler(memorySampleIntervalMs)
  const gcObserver = startGcObserver()
  const measurements: RunMeasurement[] = []

  try {
    for (let runIndex = 1; runIndex <= runs; runIndex++) {
      try {
        measurements.push(
          await runScenario(scenario, {
            onMeasureStart: gcObserver.resume,
            onMeasureFinish: gcObserver.pause,
          }),
        )
      } finally {
        gcObserver.pause()
      }
      await sleep(memorySettleMs)
    }
  } finally {
    gcObserver.pause()
    await sleep(memorySettleMs)
  }

  const peak = sampler.stop()
  const gc = gcObserver.stop()
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
    latency: summarizeLatencies(batchLatenciesMs.get(scenario.id) ?? []),
    memory: {
      peak,
      peakDelta: diffMemoryUsage(peak, baseline),
      retainedDelta: diffMemoryUsage(after, baseline),
    },
    gc,
  }

  console.log(`${memoryResultPrefix}${JSON.stringify(result)}`)
  printBatchLatencyResults()
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
      BENCHMARK_ISOLATED: '0',
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

async function runIsolatedMemoryBenchmark() {
  console.log('Starting isolated producer memory benchmark...')
  console.log(`Benchmark brokers: ${brokers.join(',')}`)
  console.log(`Benchmark topic: ${producerTopic}`)
  console.log(`Benchmark iterations: ${iterations}`)
  console.log(`Benchmark batch size: ${batchSize}`)
  console.log(`Benchmark runs: ${runs}`)
  console.log(`Benchmark memory sample interval: ${memorySampleIntervalMs}ms`)
  console.log(`Benchmark memory settle time: ${memorySettleMs}ms`)
  console.log(`Benchmark colors: ${useColors}`)
  console.log(`Benchmark charts: ${showCharts}`)

  await ensureProducerTopic()

  const scenariosToRun = selectScenarios()
  if (scenariosToRun.length === 0) {
    throw new Error('No memory benchmark scenarios selected')
  }

  console.log(`Benchmark scenarios: ${scenariosToRun.map((scenario) => scenario.id).join(', ')}`)
  console.log(`Benchmark shuffle scenarios: ${shuffleScenarios}`)

  const results: MemoryChildResult[] = []
  const runOrder = scenariosInRunOrder(scenariosToRun)
  if (shuffleScenarios) {
    console.log(`Benchmark scenario order: ${runOrder.map((scenario) => scenario.id).join(', ')}`)
  }
  for (const scenario of runOrder) {
    console.log(`Running isolated memory scenario: ${scenario.id}`)
    results.push(await runScenarioInIsolatedProcess(scenario))
  }

  printMemoryResults(results, {
    memoryTitle: 'Producer benchmark (isolated process + lifecycle memory)',
    useColors,
    showCharts,
  })
  printChildLatencyResults(results)
}

async function runIsolatedThroughputBenchmark() {
  console.log('Starting isolated producer throughput benchmark...')
  console.log(`Benchmark brokers: ${brokers.join(',')}`)
  console.log(`Benchmark topic: ${producerTopic}`)
  console.log(`Benchmark iterations: ${iterations}`)
  console.log(`Benchmark batch size: ${batchSize}`)
  console.log(`Benchmark runs: ${runs}`)
  console.log(`Benchmark force GC before run: ${forceGcBeforeRun}`)
  console.log(`Benchmark scenario timeout: ${scenarioTimeoutMs}ms`)
  console.log('Benchmark isolated mode starts one child Node.js process per scenario')

  await ensureProducerTopic()

  const scenariosToRun = selectScenarios()
  if (scenariosToRun.length === 0) {
    throw new Error('No isolated benchmark scenarios selected')
  }

  console.log(`Benchmark scenarios: ${scenariosToRun.map((scenario) => scenario.id).join(', ')}`)
  console.log(`Benchmark shuffle scenarios: ${shuffleScenarios}`)

  const isolatedResults: MemoryChildResult[] = []
  const runOrder = scenariosInRunOrder(scenariosToRun)
  if (shuffleScenarios) {
    console.log(`Benchmark scenario order: ${runOrder.map((scenario) => scenario.id).join(', ')}`)
  }
  for (const scenario of runOrder) {
    console.log(`Running isolated throughput scenario: ${scenario.id}`)
    isolatedResults.push(await runScenarioInIsolatedProcess(scenario))
  }

  printBenchmarkResults(
    isolatedResults.map((result) => ({
      id: result.scenario.id,
      label: result.scenario.label,
      result: createBenchmarkResult(result.measurements),
      measurements: result.measurements,
    })),
    { title: 'Producer benchmark (isolated process)', useColors, showCharts },
  )
}

let entrypoint = main
if (memoryChildMode) {
  entrypoint = runMemoryChild
} else if (memoryMode) {
  entrypoint = runIsolatedMemoryBenchmark
} else if (isolatedMode) {
  entrypoint = runIsolatedThroughputBenchmark
}

entrypoint().catch((error) => {
  console.error(error instanceof Error ? error : new Error(String(error)))
  process.exit(1)
})

function printChildLatencyResults(results: readonly MemoryChildResult[]) {
  console.log('\nProducer batch latency (ms per send, lower is better)')
  console.log(
    ' # | Scenario                                    | Batches |     Mean |      p50 |      p95 |      p99 |      Max',
  )
  console.log(
    '---+---------------------------------------------+---------+----------+----------+----------+----------+----------',
  )

  let index = 0
  for (const result of results) {
    const latency = result.latency
    if (!latency) {
      continue
    }

    index += 1
    console.log(
      ` ${String(index).padStart(1)} | ${result.scenario.label.padEnd(43)} | ${String(latency.batches).padStart(7)} | ` +
        `${latency.mean.toFixed(2).padStart(8)} | ${latency.p50.toFixed(2).padStart(8)} | ` +
        `${latency.p95.toFixed(2).padStart(8)} | ${latency.p99.toFixed(2).padStart(8)} | ` +
        latency.max.toFixed(2).padStart(8),
    )
  }
}
