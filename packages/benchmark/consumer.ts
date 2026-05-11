import type { KafkaClient, Message } from 'kafka-crab-js'
import { spawn } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import { setTimeout as sleep } from 'node:timers/promises'
import { fileURLToPath } from 'node:url'
import { brokers, topic } from './utils/definitions.js'
import { readBoolean, readCsvValues, readNonNegativeInteger, readPositiveInteger } from './utils/env.js'
import { startGcObserver, type GcSummary } from './utils/gc.js'
import { diffMemoryUsage, readMemoryUsage, startMemorySampler, type MemoryUsageSnapshot } from './utils/memory.js'
import { printBenchmarkResults, printMemoryResults } from './utils/output.js'
import {
  createBenchmarkResult,
  formatOpsPerSecond,
  type BenchmarkResult,
  type RunMeasurement,
} from './utils/results.js'

type BenchmarkLibrary = 'crab' | 'kafkajs' | 'platformatic-kafka'
type BenchmarkScenarioId =
  | 'v3-serial'
  | 'v4-serial'
  | 'kafkajs-serial'
  | 'kafkajs-serial-concurrent'
  | 'platformatic-kafka'
  | 'v3-batch'
  | 'v4-batch'
  | 'v4-direct-batch'
  | 'v4-native-batch-stream'
  | 'v4-compact-batch'
  | 'kafkajs-batch'

interface BenchmarkScenario {
  id: BenchmarkScenarioId
  label: string
  library: BenchmarkLibrary
  diagnostic?: boolean
  run(hooks?: RunMeasurementHooks): Promise<RunMeasurement>
}

interface RunMeasurementHooks {
  onMeasureStart?(): void
  onMeasureFinish?(): void
}

interface RunState {
  seen: number
  measured: number
  hooks: RunMeasurementHooks
  startedAt?: bigint
  finishedAt?: bigint
  finished: boolean
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
}

interface CompactMessageBatch {
  payloads: unknown[]
}

type CompactBatchStreamConsumer = ReturnType<KafkaClient['createConsumer']> & {
  recvBatchStream(size: number, timeoutMs: number): ReadableStream<Message[]>
  recvBatchStreamCompact(size: number, timeoutMs: number): ReadableStream<CompactMessageBatch>
}

const iterations = readPositiveInteger('BENCHMARK_ITERATIONS', 100_000)
const runs = readPositiveInteger('BENCHMARK_RUNS', 5)
const maxBytes = readPositiveInteger('BENCHMARK_MAX_BYTES', 2048)
const fetchMinBytes = readPositiveInteger('BENCHMARK_FETCH_MIN_BYTES', 1)
const fetchWaitMs = readPositiveInteger('BENCHMARK_FETCH_WAIT_MS', 10)
const fetchMaxBytes = maxBytes
const partitionMaxBytes = maxBytes
const kafkaJsEachMessageConcurrency = readPositiveInteger('BENCHMARK_KAFKAJS_EACH_MESSAGE_CONCURRENCY', 3)
const requestedBatchSize = readPositiveInteger('BENCHMARK_BATCH_SIZE', 4096)
const maxComparableBatchSize = 16_384
const batchSize = Math.min(requestedBatchSize, maxComparableBatchSize)
const batchTimeoutMs = readPositiveInteger('BENCHMARK_BATCH_TIMEOUT_MS', 2)
const scenarioTimeoutMs = readPositiveInteger('BENCHMARK_SCENARIO_TIMEOUT_MS', 120_000)
const forceGcBeforeRun = readBoolean('BENCHMARK_FORCE_GC', true)
const selectedLibraries = readSelectedLibraries()
const selectedScenarios = readSelectedScenarios()
const showV3Scenarios = readBoolean('BENCHMARK_SHOW_V3', false)
const isolatedMode = readBoolean('BENCHMARK_ISOLATED', false)
const memoryMode = readBoolean('BENCHMARK_MEMORY', true)
const memoryChildMode = readBoolean('BENCHMARK_MEMORY_CHILD', false)
const memorySampleIntervalMs = readPositiveInteger('BENCHMARK_MEMORY_SAMPLE_MS', 100)
const memorySettleMs = readNonNegativeInteger('BENCHMARK_MEMORY_SETTLE_MS', 100)
const memoryResultPrefix = 'BENCHMARK_MEMORY_RESULT '
const useColors = readBoolean('BENCHMARK_COLORS', true)
const showCharts = readBoolean('BENCHMARK_CHARTS', true)

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

function observeMessage(state: RunState): boolean {
  state.seen += 1

  if (state.measured === 0) {
    startMeasurement(state)
  }

  state.measured += 1
  if (state.measured < iterations) {
    return false
  }

  finishMeasurement(state)
  return true
}

function observeMessageCount(state: RunState, messageCount: number): boolean {
  if (messageCount <= 0) {
    return false
  }

  state.seen += messageCount

  if (state.measured === 0) {
    startMeasurement(state)
  }

  const remainingMessages = iterations - state.measured
  if (messageCount < remainingMessages) {
    state.measured += messageCount
    return false
  }

  state.measured = iterations
  finishMeasurement(state)
  return true
}

function observeBatchPayload(state: RunState, batch: Message[] | CompactMessageBatch): boolean {
  if (!Array.isArray(batch)) {
    return observeMessageCount(state, batch.payloads.length)
  }

  for (const message of batch) {
    void message
    if (observeMessage(state)) {
      return true
    }
  }

  return false
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

async function measureReadableStream<Payload>(
  state: RunState,
  stream: ReadableStream<Payload>,
  observePayload: (payload: Payload) => boolean,
): Promise<RunMeasurement> {
  const reader = stream.getReader()
  let shouldCancel = true

  try {
    while (true) {
      const { value, done } = await reader.read()
      if (done) {
        shouldCancel = false
        break
      }

      if (value !== undefined && observePayload(value)) {
        return finishRun(state)
      }
    }
  } finally {
    if (shouldCancel) {
      try {
        await reader.cancel()
      } catch {
        // Noop
      }
    }
  }

  return finishRun(state)
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
    return await Promise.race([
      scenario.run(hooks),
      new Promise<RunMeasurement>((_, reject) => {
        timeout = setTimeout(() => {
          reject(
            new Error(
              `Scenario "${scenario.id}" timed out after ${scenarioTimeoutMs}ms. ` +
                `Check that topic "${topic}" has at least ${iterations} readable messages. ` +
                'Run setup with BENCHMARK_SETUP_MESSAGES >= BENCHMARK_ITERATIONS if needed.',
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

async function kafkajsSerial(hooks?: RunMeasurementHooks, partitionsConsumedConcurrently = 1): Promise<RunMeasurement> {
  const { Kafka: KafkaJS, logLevel } = await import('kafkajs')
  const { promise, resolve, reject } = Promise.withResolvers<RunMeasurement>()
  const state = createRunState(hooks)
  let completed = false

  const client = new KafkaJS({ clientId: 'benchmarks', brokers, logLevel: logLevel.ERROR })
  const consumer = client.consumer({
    groupId: randomUUID(),
    minBytes: fetchMinBytes,
    maxBytes: fetchMaxBytes,
    maxBytesPerPartition: partitionMaxBytes,
    maxWaitTimeInMs: fetchWaitMs,
  })

  await consumer.connect()
  await consumer.subscribe({ topic, fromBeginning: true })

  consumer.on('consumer.crash', reject)

  consumer
    .run({
      autoCommit: false,
      partitionsConsumedConcurrently,
      async eachMessage({ pause }) {
        if (completed) {
          return
        }

        if (!observeMessage(state)) {
          return
        }

        completed = true
        pause()
        resolveAfterKafkaJsDisconnect(consumer, finishRun(state), resolve, reject)
      },
    })
    .catch(reject)

  return promise
}

async function kafkajsBatch(hooks?: RunMeasurementHooks): Promise<RunMeasurement> {
  const { Kafka: KafkaJS, logLevel } = await import('kafkajs')
  const { promise, resolve, reject } = Promise.withResolvers<RunMeasurement>()
  const state = createRunState(hooks)
  let completed = false

  const client = new KafkaJS({ clientId: 'benchmarks', brokers, logLevel: logLevel.ERROR })
  const consumer = client.consumer({
    groupId: randomUUID(),
    minBytes: fetchMinBytes,
    maxBytes: fetchMaxBytes,
    maxBytesPerPartition: partitionMaxBytes,
    maxWaitTimeInMs: fetchWaitMs,
  })

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
          resolveAfterKafkaJsDisconnect(consumer, finishRun(state), resolve, reject)
          return
        }
      },
    })
    .catch(reject)

  return promise
}

async function platformaticKafka(hooks?: RunMeasurementHooks): Promise<RunMeasurement> {
  const { Consumer: PlatformaticKafkaConsumer, MessagesStreamModes } = await import('@platformatic/kafka')
  const { promise, resolve, reject } = Promise.withResolvers<RunMeasurement>()
  const state = createRunState(hooks)

  const consumer = new PlatformaticKafkaConsumer<Buffer, Buffer>({
    clientId: 'benchmarks',
    groupId: randomUUID(),
    bootstrapBrokers: brokers,
    minBytes: fetchMinBytes,
    maxBytes: fetchMaxBytes,
    maxWaitTime: fetchWaitMs,
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
    const measurement = finishRun(state)

    setImmediate(() => {
      consumer.close(true, () => {
        resolve(measurement)
      })
    })
  })

  stream.on('error', reject)

  return promise
}

function resolveAfterKafkaJsDisconnect(
  consumer: { disconnect(): Promise<void> },
  measurement: RunMeasurement,
  resolve: (measurement: RunMeasurement) => void,
  reject: (error: unknown) => void,
) {
  setImmediate(() => {
    consumer.disconnect().then(() => resolve(measurement), reject)
  })
}

async function kafkaCrabJsV3(useBatchMode = false, hooks?: RunMeasurementHooks): Promise<RunMeasurement> {
  const { KafkaClient: KafkaClientV3 } = await import('kafka-crab-js-v3')
  const { promise, resolve, reject } = Promise.withResolvers<RunMeasurement>()
  const state = createRunState(hooks)

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
      'fetch.min.bytes': fetchMinBytes,
      'fetch.max.bytes': fetchMaxBytes,
      'message.max.bytes': partitionMaxBytes,
      'fetch.message.max.bytes': partitionMaxBytes,
      'fetch.wait.max.ms': fetchWaitMs,
      'max.partition.fetch.bytes': partitionMaxBytes,
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

async function createKafkaCrabJsV4Client(): Promise<KafkaClient> {
  const { KafkaClient } = await import('kafka-crab-js')

  return new KafkaClient({
    brokers: brokers.join(','),
    clientId: 'benchmarks',
    securityProtocol: 'Plaintext',
    logLevel: 'warn',
    brokerAddressFamily: 'v4',
    diagnostics: false,
  })
}

function createKafkaCrabJsV4ConsumerConfiguration() {
  return {
    groupId: randomUUID(),
    enableAutoCommit: false,
    configuration: {
      'auto.offset.reset': 'earliest',
      'enable.auto.commit': false,
      'fetch.min.bytes': fetchMinBytes,
      'fetch.max.bytes': fetchMaxBytes,
      'message.max.bytes': partitionMaxBytes,
      'fetch.message.max.bytes': partitionMaxBytes,
      'fetch.wait.max.ms': fetchWaitMs,
      'max.partition.fetch.bytes': partitionMaxBytes,
    },
  }
}

async function kafkaCrabJsV4(useBatchMode = false, hooks?: RunMeasurementHooks): Promise<RunMeasurement> {
  const state = createRunState(hooks)

  const client = await createKafkaCrabJsV4Client()
  const webConsumer = client.createWebStreamConsumer({
    ...createKafkaCrabJsV4ConsumerConfiguration(),
    batchSize: useBatchMode ? batchSize : 1,
    batchTimeout: batchTimeoutMs,
    serialPrefetchSize: 64,
    serialPrefetchTimeout: 5,
  })

  await webConsumer.consumer.subscribe([{ topic, allOffsets: { position: 'Beginning' } }])

  try {
    if (webConsumer.mode === 'batch') {
      return await measureReadableStream(state, webConsumer.stream, (batch) => observeBatchPayload(state, batch))
    }

    return await measureReadableStream(state, webConsumer.stream, (message) => {
      void message
      return observeMessage(state)
    })
  } finally {
    await disconnectV4Consumer(webConsumer.consumer)
  }
}

async function kafkaCrabJsV4DirectBatchCount(hooks?: RunMeasurementHooks): Promise<RunMeasurement> {
  const state = createRunState(hooks)
  const client = await createKafkaCrabJsV4Client()
  const consumer = client.createConsumer(createKafkaCrabJsV4ConsumerConfiguration())

  await consumer.subscribe([{ topic, allOffsets: { position: 'Beginning' } }])

  try {
    while (true) {
      const messages = await consumer.recvBatch(batchSize, batchTimeoutMs)
      if (observeMessageCount(state, messages.length)) {
        return finishRun(state)
      }
    }
  } finally {
    await disconnectV4Consumer(consumer)
  }
}

async function kafkaCrabJsV4NativeBatchStreamCount(hooks?: RunMeasurementHooks): Promise<RunMeasurement> {
  const state = createRunState(hooks)
  const client = await createKafkaCrabJsV4Client()
  const consumer = client.createConsumer(createKafkaCrabJsV4ConsumerConfiguration()) as CompactBatchStreamConsumer

  await consumer.subscribe([{ topic, allOffsets: { position: 'Beginning' } }])

  try {
    const stream = consumer.recvBatchStream(batchSize, batchTimeoutMs)
    return await measureReadableStream(state, stream, (batch) => observeMessageCount(state, batch.length))
  } finally {
    await disconnectV4Consumer(consumer)
  }
}

async function kafkaCrabJsV4CompactBatchCount(hooks?: RunMeasurementHooks): Promise<RunMeasurement> {
  const state = createRunState(hooks)

  const client = await createKafkaCrabJsV4Client()
  const consumer = client.createConsumer(createKafkaCrabJsV4ConsumerConfiguration()) as CompactBatchStreamConsumer

  await consumer.subscribe([{ topic, allOffsets: { position: 'Beginning' } }])

  try {
    const stream = consumer.recvBatchStreamCompact(batchSize, batchTimeoutMs)
    return await measureReadableStream(state, stream, (batch) => observeMessageCount(state, batch.payloads.length))
  } finally {
    await disconnectV4Consumer(consumer)
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

async function disconnectV4Consumer(consumer: { unsubscribe(): void; disconnect(): Promise<void> }) {
  try {
    consumer.unsubscribe()
  } catch {
    // Noop
  }

  try {
    await consumer.disconnect()
  } catch {
    // Noop
  }
}

const scenarios: BenchmarkScenario[] = [
  {
    id: 'v3-serial',
    label: 'v3 kafka-crab-js (serial)',
    library: 'crab',
    run: (hooks) => kafkaCrabJsV3(false, hooks),
  },
  {
    id: 'v4-serial',
    label: 'kafka-crab-js v4 (stream, serial)',
    library: 'crab',
    run: (hooks) => kafkaCrabJsV4(false, hooks),
  },
  {
    id: 'kafkajs-serial',
    label: 'KafkaJS (eachMessage)',
    library: 'kafkajs',
    run: kafkajsSerial,
  },
  {
    id: 'kafkajs-serial-concurrent',
    label: 'KafkaJS (eachMessage, concurrent)',
    library: 'kafkajs',
    run: (hooks) => kafkajsSerial(hooks, kafkaJsEachMessageConcurrency),
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
    run: (hooks) => kafkaCrabJsV3(true, hooks),
  },
  {
    id: 'v4-batch',
    label: 'kafka-crab-js v4 (stream, batch)',
    library: 'crab',
    run: (hooks) => kafkaCrabJsV4(true, hooks),
  },
  {
    id: 'v4-direct-batch',
    label: 'kafka-crab-js v4 (recvBatch, diagnostic count)',
    library: 'crab',
    diagnostic: true,
    run: kafkaCrabJsV4DirectBatchCount,
  },
  {
    id: 'v4-native-batch-stream',
    label: 'kafka-crab-js v4 (recvBatchStream, diagnostic count)',
    library: 'crab',
    diagnostic: true,
    run: kafkaCrabJsV4NativeBatchStreamCount,
  },
  {
    id: 'v4-compact-batch',
    label: 'kafka-crab-js v4 (compact stream, diagnostic count)',
    library: 'crab',
    diagnostic: true,
    run: kafkaCrabJsV4CompactBatchCount,
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

function isV3ScenarioId(scenarioId: BenchmarkScenarioId): boolean {
  return scenarioId === 'v3-serial' || scenarioId === 'v3-batch'
}

function selectedV3ScenarioIds(): BenchmarkScenarioId[] {
  return [...selectedScenarios].filter(isV3ScenarioId)
}

function shouldShowV3Scenarios(): boolean {
  return (
    selectedV3ScenarioIds().length > 0 ||
    (showV3Scenarios && (selectedLibraries.size === 0 || selectedLibraries.has('crab')))
  )
}

function selectScenarios(): BenchmarkScenario[] {
  const includeV3 = shouldShowV3Scenarios()

  return scenarios.filter((scenario) => {
    if (isV3Scenario(scenario) && !includeV3) {
      return false
    }

    if (selectedLibraries.size > 0 && !selectedLibraries.has(scenario.library)) {
      return false
    }

    if (scenario.diagnostic && selectedScenarios.size === 0) {
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
  console.log(`Benchmark runs: ${runs}`)
  console.log(`Benchmark force GC before run: ${forceGcBeforeRun}`)
  console.log(`Benchmark scenario timeout: ${scenarioTimeoutMs}ms`)
  console.log(`Benchmark fetch min bytes: ${fetchMinBytes}`)
  console.log(`Benchmark fetch wait: ${fetchWaitMs}ms`)
  console.log(`Benchmark fetch max bytes: ${fetchMaxBytes}`)
  console.log(`Benchmark partition max bytes: ${partitionMaxBytes}`)
  console.log(`Benchmark KafkaJS eachMessage concurrency: ${kafkaJsEachMessageConcurrency}`)
  console.log(`Benchmark batch size: ${batchSize}`)
  console.log(`Benchmark batch timeout: ${batchTimeoutMs}ms`)
  if (batchSize !== requestedBatchSize) {
    console.log(`Benchmark requested batch size: ${requestedBatchSize} (normalized for comparable batch scenarios)`)
  }

  const scenariosToRun = selectScenarios()

  if (scenariosToRun.length === 0) {
    throw new Error('No benchmark scenarios selected')
  }

  console.log(`Benchmark scenarios: ${scenariosToRun.map((scenario) => scenario.id).join(', ')}`)

  const measurements = new Map<BenchmarkScenarioId, RunMeasurement[]>()
  for (const scenario of scenariosToRun) {
    measurements.set(scenario.id, [])
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

  const results: Record<string, BenchmarkResult> = {}
  for (const scenario of scenariosToRun) {
    results[scenario.label] = createBenchmarkResult(measurements.get(scenario.id) ?? [])
  }

  printBenchmarkResults(results, { title: 'Consumer benchmark (same process)', useColors, showCharts })
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
    memory: {
      peak,
      peakDelta: diffMemoryUsage(peak, baseline),
      retainedDelta: diffMemoryUsage(after, baseline),
    },
    gc,
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
  console.log('Starting isolated consumer memory benchmark...')
  console.log(`Benchmark brokers: ${brokers.join(',')}`)
  console.log(`Benchmark topic: ${topic}`)
  console.log(`Benchmark iterations: ${iterations}`)
  console.log(`Benchmark runs: ${runs}`)
  console.log(`Benchmark fetch min bytes: ${fetchMinBytes}`)
  console.log(`Benchmark fetch wait: ${fetchWaitMs}ms`)
  console.log(`Benchmark fetch max bytes: ${fetchMaxBytes}`)
  console.log(`Benchmark partition max bytes: ${partitionMaxBytes}`)
  console.log(`Benchmark KafkaJS eachMessage concurrency: ${kafkaJsEachMessageConcurrency}`)
  console.log(`Benchmark batch size: ${batchSize}`)
  console.log(`Benchmark batch timeout: ${batchTimeoutMs}ms`)
  if (batchSize !== requestedBatchSize) {
    console.log(`Benchmark requested batch size: ${requestedBatchSize} (normalized for comparable batch scenarios)`)
  }
  console.log(`Benchmark memory sample interval: ${memorySampleIntervalMs}ms`)
  console.log(`Benchmark memory settle time: ${memorySettleMs}ms`)
  console.log(`Benchmark colors: ${useColors}`)
  console.log(`Benchmark charts: ${showCharts}`)

  const scenariosToRun = selectScenarios()
  if (scenariosToRun.length === 0) {
    throw new Error('No memory benchmark scenarios selected')
  }

  console.log(`Benchmark scenarios: ${scenariosToRun.map((scenario) => scenario.id).join(', ')}`)

  const results: MemoryChildResult[] = []
  for (const scenario of scenariosToRun) {
    console.log(`Running isolated memory scenario: ${scenario.id}`)
    results.push(await runScenarioInIsolatedProcess(scenario))
  }

  printMemoryResults(results, { useColors, showCharts })
}

async function runIsolatedThroughputBenchmark() {
  console.log('Starting isolated consumer throughput benchmark...')
  console.log(`Benchmark brokers: ${brokers.join(',')}`)
  console.log(`Benchmark topic: ${topic}`)
  console.log(`Benchmark iterations: ${iterations}`)
  console.log(`Benchmark runs: ${runs}`)
  console.log(`Benchmark force GC before run: ${forceGcBeforeRun}`)
  console.log(`Benchmark scenario timeout: ${scenarioTimeoutMs}ms`)
  console.log(`Benchmark fetch min bytes: ${fetchMinBytes}`)
  console.log(`Benchmark fetch wait: ${fetchWaitMs}ms`)
  console.log(`Benchmark fetch max bytes: ${fetchMaxBytes}`)
  console.log(`Benchmark partition max bytes: ${partitionMaxBytes}`)
  console.log(`Benchmark KafkaJS eachMessage concurrency: ${kafkaJsEachMessageConcurrency}`)
  console.log(`Benchmark batch size: ${batchSize}`)
  console.log(`Benchmark batch timeout: ${batchTimeoutMs}ms`)
  if (batchSize !== requestedBatchSize) {
    console.log(`Benchmark requested batch size: ${requestedBatchSize} (normalized for comparable batch scenarios)`)
  }
  console.log('Benchmark isolated mode starts one child Node.js process per scenario')

  const scenariosToRun = selectScenarios()
  if (scenariosToRun.length === 0) {
    throw new Error('No isolated benchmark scenarios selected')
  }

  console.log(`Benchmark scenarios: ${scenariosToRun.map((scenario) => scenario.id).join(', ')}`)

  const isolatedResults: MemoryChildResult[] = []
  for (const scenario of scenariosToRun) {
    console.log(`Running isolated throughput scenario: ${scenario.id}`)
    isolatedResults.push(await runScenarioInIsolatedProcess(scenario))
  }

  const results: Record<string, BenchmarkResult> = {}
  for (const result of isolatedResults) {
    results[result.scenario.label] = createBenchmarkResult(result.measurements)
  }

  printBenchmarkResults(results, { title: 'Consumer benchmark (isolated process)', useColors, showCharts })
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
