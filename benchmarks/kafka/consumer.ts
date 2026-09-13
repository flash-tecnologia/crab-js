import type { KafkaClient, Message } from 'kafka-crab-js'
import { spawn, spawnSync } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import { writeFileSync } from 'node:fs'
import { freemem, loadavg, tmpdir } from 'node:os'
import path from 'node:path'
import { captureBenchmarkEnvironment } from './utils/runtime-fingerprint.js'
import {
  beginDelivery,
  createConsumerRunState,
  finishRun,
  observeMessage,
  observeMessageCount,
  observeMeasuredMessage,
  type RunState,
  type RunMeasurementHooks,
} from './utils/consumer-measurement.js'
import { setTimeout as sleep } from 'node:timers/promises'
import { fileURLToPath } from 'node:url'
import { brokers, topic } from './utils/definitions.js'
import { readBoolean, readCsvValues, readNonNegativeInteger, readPositiveInteger } from './utils/env.js'
import { startGcObserver, type GcSummary } from './utils/gc.js'
import {
  diffMemoryUsage,
  maxMemoryUsage,
  readMemoryUsage,
  startMemorySampler,
  type MemoryUsageSnapshot,
} from './utils/memory.js'
import { printBenchmarkResults, printMemoryResults } from './utils/output.js'
import { maybeShuffle } from './utils/shuffle.js'
import { createSerialTiming, type SerialTimingResult } from './utils/serial-timing.js'
import { createBenchmarkResult, formatOpsPerSecond, type RunMeasurement } from './utils/results.js'

type BenchmarkLibrary = 'crab' | 'kafkajs' | 'platformatic-kafka'
type BenchmarkScenarioId =
  | 'previous-serial'
  | 'crab-serial'
  | 'kafkajs-serial'
  | 'kafkajs-serial-concurrent'
  | 'platformatic-kafka'
  | 'previous-batch'
  | 'crab-batch'
  | 'crab-direct-batch'
  | 'crab-native-batch-stream'
  | 'crab-compact-batch'
  | 'kafkajs-batch'

interface BenchmarkScenario {
  id: BenchmarkScenarioId
  label: string
  library: BenchmarkLibrary
  diagnostic?: boolean
  run: (hooks?: RunMeasurementHooks) => Promise<RunMeasurement>
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
    afterGc: MemoryUsageSnapshot
    sampledProcessingPeak: MemoryUsageSnapshot | null
    processingSamples: number
    osPeakRssBytes: number
  }
  gc: GcSummary
  serialTiming?: SerialTimingResult[]
  runtime?: unknown
  execution?: {
    startedAt: string
    finishedAt: string
    pid: number
    hostBefore: { freeMemoryBytes: number; loadAverage: number[] }
    hostAfter: { freeMemoryBytes: number; loadAverage: number[] }
    environment: ReturnType<typeof captureBenchmarkEnvironment>
  }
  lifecycle?: { beforeRun: MemoryUsageSnapshot; afterRun: MemoryUsageSnapshot; afterSettle: MemoryUsageSnapshot }[]
}

interface CompactMessageBatch {
  payloads: unknown[]
}

type CompactBatchStreamConsumer = ReturnType<KafkaClient['createConsumer']> & {
  recvBatchStream: (size: number, timeoutMs: number) => ReadableStream<Message[]>
  recvBatchStreamCompact: (size: number, timeoutMs: number) => ReadableStream<CompactMessageBatch>
}

const iterations = readPositiveInteger('BENCHMARK_ITERATIONS', 100_000)
const runs = readPositiveInteger('BENCHMARK_RUNS', 5)
const measurementWindow = process.env.BENCHMARK_MEASUREMENT_WINDOW ?? 'steady'
if (measurementWindow !== 'steady' && measurementWindow !== 'first-message') {
  throw new Error('BENCHMARK_MEASUREMENT_WINDOW must be steady or first-message')
}
const warmupMessages =
  measurementWindow === 'steady' ? readNonNegativeInteger('BENCHMARK_WARMUP_MESSAGES', iterations) : 0
const pairOrder = process.env.BENCHMARK_PAIR_ORDER ?? 'previous-first'
if (!['previous-first', 'current-first'].includes(pairOrder)) throw new Error('Invalid BENCHMARK_PAIR_ORDER')
const captureKafka = readBoolean('BENCHMARK_KAFKA_SNAPSHOT', true)
const isolatedBlocks = readPositiveInteger('BENCHMARK_BLOCKS', 2)
const maxBytes = readPositiveInteger('BENCHMARK_MAX_BYTES', 2048)
const fetchMinBytes = readPositiveInteger('BENCHMARK_FETCH_MIN_BYTES', 1)
const fetchWaitMs = readPositiveInteger('BENCHMARK_FETCH_WAIT_MS', 10)
const rawFetchQueueBackoff = process.env.BENCHMARK_FETCH_QUEUE_BACKOFF_MS?.trim()
const fetchQueueBackoffMs = rawFetchQueueBackoff ? Number(rawFetchQueueBackoff) : null
if (
  fetchQueueBackoffMs !== null &&
  (!Number.isSafeInteger(fetchQueueBackoffMs) || fetchQueueBackoffMs < 0 || fetchQueueBackoffMs > 300_000)
) {
  throw new Error('BENCHMARK_FETCH_QUEUE_BACKOFF_MS must be an integer between 0 and 300000')
}
const fetchMaxBytes = maxBytes
const partitionMaxBytes = maxBytes
const kafkaJsEachMessageConcurrency = readPositiveInteger('BENCHMARK_KAFKAJS_EACH_MESSAGE_CONCURRENCY', 3)
const requestedBatchSize = readPositiveInteger('BENCHMARK_BATCH_SIZE', 4096)
const maxComparableBatchSize = 16_384
const batchSize = Math.min(requestedBatchSize, maxComparableBatchSize)
const batchTimeoutMs = readPositiveInteger('BENCHMARK_BATCH_TIMEOUT_MS', 2)
const serialPrefetchSize = readPositiveInteger('BENCHMARK_SERIAL_PREFETCH_SIZE', 64)
const serialPrefetchTimeoutMs = readPositiveInteger('BENCHMARK_SERIAL_PREFETCH_TIMEOUT_MS', 5)
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
const serialTimingMode = process.env.BENCHMARK_SERIAL_TIMING ?? 'off'
if (!['off', 'cpu', 'gaps'].includes(serialTimingMode)) {
  throw new Error('BENCHMARK_SERIAL_TIMING must be off, cpu, or gaps')
}
if (serialTimingMode !== 'off' && !memoryMode && !memoryChildMode && !isolatedMode) {
  throw new Error('BENCHMARK_SERIAL_TIMING requires isolated or memory mode')
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

function createRunState(hooks: RunMeasurementHooks = {}): RunState {
  return createConsumerRunState(
    { iterations, window: measurementWindow as 'steady' | 'first-message', warmupMessages },
    hooks,
  )
}

function observeBatchPayload(state: RunState, batch: Message[] | CompactMessageBatch): boolean {
  if (!Array.isArray(batch)) return observeMessageCount(state, batch.payloads.length)
  if (!beginDelivery(state, batch.length)) return false
  for (const message of batch) {
    void message
    if (observeMeasuredMessage(state)) return true
  }
  return false
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
  const state = createRunState(hooks)
  const { Kafka: KafkaJS, logLevel } = await import('kafkajs')
  const { promise, resolve, reject } = Promise.withResolvers<RunMeasurement>()
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
  const state = createRunState(hooks)
  const { Kafka: KafkaJS, logLevel } = await import('kafkajs')
  const { promise, resolve, reject } = Promise.withResolvers<RunMeasurement>()
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
        if (completed) return
        if (!beginDelivery(state, batch.messages.length)) {
          const last = batch.messages.at(-1)
          if (last) resolveOffset(last.offset)
          return
        }
        for (const message of batch.messages) {
          if (completed) {
            return
          }

          resolveOffset(message.offset)

          if (!observeMeasuredMessage(state)) {
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
  const state = createRunState(hooks)
  const { Consumer: PlatformaticKafkaConsumer, MessagesStreamModes } = await import('@platformatic/kafka')
  const { promise, resolve, reject } = Promise.withResolvers<RunMeasurement>()

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
  consumer: { disconnect: () => Promise<void> },
  measurement: RunMeasurement,
  resolve: (measurement: RunMeasurement) => void,
  reject: (error: unknown) => void,
) {
  setImmediate(() => {
    consumer.disconnect().then(() => resolve(measurement), reject)
  })
}

async function kafkaCrabJsPrevious(useBatchMode = false, hooks?: RunMeasurementHooks): Promise<RunMeasurement> {
  const state = createRunState(hooks)
  const { KafkaClient: KafkaClientPrevious } = await import('kafka-crab-js-previous')

  const client = new KafkaClientPrevious(createKafkaCrabJsClientConfiguration())
  const webConsumer = client.createWebStreamConsumer({
    ...createKafkaCrabJsConsumerConfiguration(),
    batchSize: useBatchMode ? batchSize : 1,
    batchTimeout: batchTimeoutMs,
    serialPrefetchSize,
    serialPrefetchTimeout: serialPrefetchTimeoutMs,
    enableAutoCommit: false,
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
    await disconnectKafkaCrabJsConsumer(webConsumer.consumer)
  }
}

function createKafkaCrabJsClientConfiguration() {
  return {
    brokers: brokers.join(','),
    clientId: 'benchmarks',
    securityProtocol: 'Plaintext',
    logLevel: 'warn',
    brokerAddressFamily: 'v4',
    diagnostics: false,
  } as const
}

async function createKafkaCrabJsV4Client(): Promise<KafkaClient> {
  const { KafkaClient } = await import('kafka-crab-js')

  return new KafkaClient(createKafkaCrabJsClientConfiguration())
}

function createKafkaCrabJsConsumerConfiguration() {
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
      ...(fetchQueueBackoffMs === null ? {} : { 'fetch.queue.backoff.ms': fetchQueueBackoffMs }),
    },
  }
}

async function kafkaCrabJsV4(useBatchMode = false, hooks?: RunMeasurementHooks): Promise<RunMeasurement> {
  const state = createRunState(hooks)

  const client = await createKafkaCrabJsV4Client()
  const webConsumer = client.createWebStreamConsumer({
    ...createKafkaCrabJsConsumerConfiguration(),
    batchSize: useBatchMode ? batchSize : 1,
    batchTimeout: batchTimeoutMs,
    serialPrefetchSize,
    serialPrefetchTimeout: serialPrefetchTimeoutMs,
    enableAutoCommit: false,
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
    await disconnectKafkaCrabJsConsumer(webConsumer.consumer)
  }
}

async function kafkaCrabJsV4DirectBatchCount(hooks?: RunMeasurementHooks): Promise<RunMeasurement> {
  const state = createRunState(hooks)
  const client = await createKafkaCrabJsV4Client()
  const consumer = client.createConsumer(createKafkaCrabJsConsumerConfiguration())

  await consumer.subscribe([{ topic, allOffsets: { position: 'Beginning' } }])

  try {
    while (true) {
      const messages = await consumer.recvBatch(batchSize, batchTimeoutMs)
      if (observeMessageCount(state, messages.length)) {
        return finishRun(state)
      }
    }
  } finally {
    await disconnectKafkaCrabJsConsumer(consumer)
  }
}

async function kafkaCrabJsV4NativeBatchStreamCount(hooks?: RunMeasurementHooks): Promise<RunMeasurement> {
  const state = createRunState(hooks)
  const client = await createKafkaCrabJsV4Client()
  const consumer = client.createConsumer(createKafkaCrabJsConsumerConfiguration()) as CompactBatchStreamConsumer

  await consumer.subscribe([{ topic, allOffsets: { position: 'Beginning' } }])

  try {
    const stream = consumer.recvBatchStream(batchSize, batchTimeoutMs)
    return await measureReadableStream(state, stream, (batch) => observeMessageCount(state, batch.length))
  } finally {
    await disconnectKafkaCrabJsConsumer(consumer)
  }
}

async function kafkaCrabJsV4CompactBatchCount(hooks?: RunMeasurementHooks): Promise<RunMeasurement> {
  const state = createRunState(hooks)

  const client = await createKafkaCrabJsV4Client()
  const consumer = client.createConsumer(createKafkaCrabJsConsumerConfiguration()) as CompactBatchStreamConsumer

  await consumer.subscribe([{ topic, allOffsets: { position: 'Beginning' } }])

  try {
    const stream = consumer.recvBatchStreamCompact(batchSize, batchTimeoutMs)
    return await measureReadableStream(state, stream, (batch) => observeMessageCount(state, batch.payloads.length))
  } finally {
    await disconnectKafkaCrabJsConsumer(consumer)
  }
}

async function disconnectKafkaCrabJsConsumer(consumer: { unsubscribe: () => void; disconnect: () => Promise<void> }) {
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
    id: 'previous-serial',
    label: 'previous kafka-crab-js (Web Stream, serial)',
    library: 'crab',
    run: (hooks) => kafkaCrabJsPrevious(false, hooks),
  },
  {
    id: 'crab-serial',
    label: 'kafka-crab-js (stream, serial)',
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
    id: 'previous-batch',
    label: 'previous kafka-crab-js (Web Stream, batch)',
    library: 'crab',
    run: (hooks) => kafkaCrabJsPrevious(true, hooks),
  },
  {
    id: 'crab-batch',
    label: 'kafka-crab-js (stream, batch)',
    library: 'crab',
    run: (hooks) => kafkaCrabJsV4(true, hooks),
  },
  {
    id: 'crab-direct-batch',
    label: 'kafka-crab-js (recvBatch, diagnostic count)',
    library: 'crab',
    diagnostic: true,
    run: kafkaCrabJsV4DirectBatchCount,
  },
  {
    id: 'crab-native-batch-stream',
    label: 'kafka-crab-js (recvBatchStream, diagnostic count)',
    library: 'crab',
    diagnostic: true,
    run: kafkaCrabJsV4NativeBatchStreamCount,
  },
  {
    id: 'crab-compact-batch',
    label: 'kafka-crab-js (compact stream, diagnostic count)',
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

function isPreviousScenario(scenario: BenchmarkScenario): boolean {
  return scenario.id === 'previous-serial' || scenario.id === 'previous-batch'
}

function isPreviousScenarioId(scenarioId: BenchmarkScenarioId): boolean {
  return scenarioId === 'previous-serial' || scenarioId === 'previous-batch'
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

    if (scenario.diagnostic && selectedScenarios.size === 0) {
      return false
    }

    return selectedScenarios.size === 0 || selectedScenarios.has(scenario.id)
  })
}

function scenariosInRunOrder(selected: readonly BenchmarkScenario[]): BenchmarkScenario[] {
  return maybeShuffle(selected, shuffleScenarios)
}

async function main() {
  console.log('Starting consumer benchmark...')
  console.log(`Benchmark brokers: ${brokers.join(',')}`)
  console.log(`Benchmark topic: ${topic}`)
  console.log(`Benchmark iterations: ${iterations}`)
  console.log(`Benchmark runs: ${runs}`)
  console.log(
    `Benchmark measurement window: ${measurementWindow}; warmup target: ${warmupMessages} messages (whole deliveries)`,
  )
  console.log(`Benchmark force GC before run: ${forceGcBeforeRun}`)
  console.log(`Benchmark scenario timeout: ${scenarioTimeoutMs}ms`)
  console.log(`Benchmark fetch min bytes: ${fetchMinBytes}`)
  console.log(`Benchmark fetch wait: ${fetchWaitMs}ms`)
  console.log(
    `Benchmark crab fetch queue backoff: ${fetchQueueBackoffMs === null ? 'version default' : `${fetchQueueBackoffMs}ms`}`,
  )
  console.log(`Benchmark fetch max bytes: ${fetchMaxBytes}`)
  console.log(`Benchmark partition max bytes: ${partitionMaxBytes}`)
  console.log(`Benchmark KafkaJS eachMessage concurrency: ${kafkaJsEachMessageConcurrency}`)
  console.log(`Benchmark batch size: ${batchSize}`)
  console.log(`Benchmark batch timeout: ${batchTimeoutMs}ms`)
  console.log(`Benchmark serial prefetch size: ${serialPrefetchSize}`)
  console.log(`Benchmark serial prefetch timeout: ${serialPrefetchTimeoutMs}ms`)
  if (batchSize !== requestedBatchSize) {
    console.log(`Benchmark requested batch size: ${requestedBatchSize} (normalized for comparable batch scenarios)`)
  }

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
    { title: 'Consumer benchmark (same process)', useColors, showCharts },
  )
}

async function runMemoryChild() {
  const startedAt = new Date().toISOString()
  const environment = captureBenchmarkEnvironment()
  const hostBefore = { freeMemoryBytes: freemem(), loadAverage: loadavg() }
  const lifecycle: NonNullable<MemoryChildResult['lifecycle']> = []
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
  const serialTiming: SerialTimingResult[] = []

  try {
    for (let runIndex = 1; runIndex <= runs; runIndex++) {
      const beforeRun = readMemoryUsage()
      const timing =
        serialTimingMode !== 'off' && (scenario.id === 'previous-serial' || scenario.id === 'crab-serial')
          ? createSerialTiming(iterations, serialTimingMode as 'cpu' | 'gaps')
          : undefined
      try {
        measurements.push(
          await runScenario(scenario, {
            onMeasureStart: () => {
              sampler.resumeWindow()
              gcObserver.resume()
              timing?.start()
            },
            onMeasureFinish: () => {
              timing?.finish()
              gcObserver.pause()
              sampler.pauseWindow()
            },
            onMessage: timing?.delivery,
          }),
        )
        if (timing) serialTiming.push(timing.result())
      } finally {
        gcObserver.pause()
      }
      const afterRun = readMemoryUsage()
      await sleep(memorySettleMs)
      lifecycle.push({ beforeRun, afterRun, afterSettle: readMemoryUsage() })
    }
  } finally {
    gcObserver.pause()
    await sleep(memorySettleMs)
  }

  const gc = gcObserver.stop()
  forceGc()
  await sleep(0)
  forceGc()
  const after = readMemoryUsage()
  const peak = maxMemoryUsage(sampler.stop(), after)
  const processing = sampler.processingResult()
  const osPeakRssBytes = process.resourceUsage().maxRSS * 1024
  if (JSON.stringify(environment.harness) !== JSON.stringify(captureBenchmarkEnvironment().harness)) {
    throw new Error('Benchmark source changed during the child process')
  }
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
      afterGc: after,
      sampledProcessingPeak: processing.peak,
      processingSamples: processing.samples,
      osPeakRssBytes,
    },
    gc,
    lifecycle,
    execution: {
      startedAt,
      finishedAt: new Date().toISOString(),
      pid: process.pid,
      hostBefore,
      hostAfter: { freeMemoryBytes: freemem(), loadAverage: loadavg() },
      environment,
    },
    serialTiming: serialTiming.length > 0 ? serialTiming : undefined,
  }

  if (
    readBoolean('BENCHMARK_CAPTURE_RUNTIME', true) &&
    ['previous-serial', 'crab-serial', 'previous-batch', 'crab-batch'].includes(scenario.id)
  ) {
    const { captureRuntime } = await import('./utils/runtime-fingerprint.js')
    result.runtime = {
      ...captureRuntime(scenario.id.startsWith('previous-') ? 'kafka-crab-js-previous' : 'kafka-crab-js'),
      configuration: {
        brokers,
        topic,
        iterations,
        runs,
        serialTimingMode,
        measurementWindow,
        warmupMessages,
        serialPrefetchSize,
        serialPrefetchTimeoutMs,
        fetchMinBytes,
        fetchWaitMs,
        fetchQueueBackoffMs,
        fetchMaxBytes,
        partitionMaxBytes,
        batchSize: scenario.id.endsWith('-batch') ? batchSize : 1,
        batchTimeoutMs,
        forceGcBeforeRun,
        memorySampleIntervalMs,
        memorySettleMs,
      },
    }
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

// Keep each current/previous pair adjacent and reverse it in the next block,
// This matches the ABBA diagnostic. Unpaired scenarios also get fresh processes.
function isolatedScenarioOrder(selected: BenchmarkScenario[]): BenchmarkScenario[] {
  const remaining = new Set(selected)
  const order: BenchmarkScenario[] = []
  for (const scenario of scenariosInRunOrder(selected)) {
    if (!remaining.delete(scenario)) continue
    const group = [scenario]
    const counterpartId = scenario.id.startsWith('previous-')
      ? scenario.id.replace('previous-', 'crab-')
      : scenario.id.replace('crab-', 'previous-')
    const counterpart = selected.find((item) => item.id !== scenario.id && item.id === counterpartId)
    if (counterpart && remaining.delete(counterpart)) group.push(counterpart)
    if (group.length === 2 && group[0]?.id.startsWith('previous-') !== (pairOrder === 'previous-first')) group.reverse()
    for (let block = 0; block < isolatedBlocks; block++) {
      order.push(...(block % 2 === 0 ? group : group.toReversed()))
    }
  }
  return order
}

function mergeIsolatedBlocks(blocks: MemoryChildResult[]): MemoryChildResult {
  const first = blocks[0]
  if (!first || blocks.some((block) => block.scenario.id !== first.scenario.id)) {
    throw new Error('Cannot merge empty or mismatched benchmark blocks')
  }
  if (blocks.some((block) => JSON.stringify(block.runtime) !== JSON.stringify(first.runtime))) {
    throw new Error(`Loaded artifacts or configuration changed between blocks for ${first.scenario.id}`)
  }
  if (
    blocks.some(
      (block) => JSON.stringify(block.execution?.environment) !== JSON.stringify(first.execution?.environment),
    )
  ) {
    throw new Error(`Benchmark source or environment changed between blocks for ${first.scenario.id}`)
  }
  const gc = { ...first.gc }
  const memory = { ...first.memory }
  for (const block of blocks.slice(1)) {
    for (const key of Object.keys(gc) as (keyof GcSummary)[]) {
      gc[key] = key === 'maxDurationMs' ? Math.max(gc[key], block.gc[key]) : gc[key] + block.gc[key]
    }
    memory.peak = maxMemoryUsage(memory.peak, block.memory.peak)
    memory.peakDelta = maxMemoryUsage(memory.peakDelta, block.memory.peakDelta)
    memory.retainedDelta = maxMemoryUsage(memory.retainedDelta, block.memory.retainedDelta)
    memory.afterGc = maxMemoryUsage(memory.afterGc, block.memory.afterGc)
    memory.processingSamples += block.memory.processingSamples
    memory.osPeakRssBytes = Math.max(memory.osPeakRssBytes, block.memory.osPeakRssBytes)
    if (block.memory.sampledProcessingPeak) {
      memory.sampledProcessingPeak = memory.sampledProcessingPeak
        ? maxMemoryUsage(memory.sampledProcessingPeak, block.memory.sampledProcessingPeak)
        : block.memory.sampledProcessingPeak
    }
  }
  const timing = blocks.flatMap((block) => block.serialTiming ?? [])
  return {
    scenario: first.scenario,
    measurements: blocks.flatMap((block) => block.measurements),
    memory,
    gc,
    serialTiming: timing.length > 0 ? timing : undefined,
  }
}

function kafkaSnapshot(sample: boolean): { offsets?: unknown } | null {
  if (!captureKafka) return null
  const probe = fileURLToPath(new URL('diagnostics/consumer-abba.mjs', import.meta.url))
  const child = spawnSync(
    process.execPath,
    ['--expose-gc', '--import', 'tsx', probe, '--probe', ...(sample ? ['--sample'] : [])],
    {
      cwd: process.cwd(),
      env: process.env,
      encoding: 'utf8',
      timeout: 120_000,
      maxBuffer: 16 * 1024 * 1024,
    },
  )
  if (child.status !== 0) throw new Error(`Kafka snapshot failed: ${child.error?.message ?? child.stderr}`)
  const line = child.stdout.split(/\r?\n/).findLast((candidate) => candidate.startsWith('ABBA_PROBE '))
  if (!line) throw new Error('Kafka snapshot did not return a result')
  return JSON.parse(line.slice('ABBA_PROBE '.length)) as { offsets?: unknown }
}

function benchmarkConfiguration() {
  return {
    brokers,
    fetchMinBytes,
    fetchWaitMs,
    fetchQueueBackoffMs,
    fetchMaxBytes,
    partitionMaxBytes,
    batchSize,
    batchTimeoutMs,
    serialPrefetchSize,
    serialPrefetchTimeoutMs,
    forceGcBeforeRun,
    memorySampleIntervalMs,
    memorySettleMs,
    shuffleScenarios,
    measurementWindow,
    warmupMessages,
    pairOrder,
    captureKafka,
  }
}

async function collectIsolatedBlocks(selected: BenchmarkScenario[]) {
  const resultPath =
    process.env.BENCHMARK_RESULT_PATH ?? path.join(tmpdir(), `consumer-${Date.now()}-${randomUUID()}.json`)
  const order = isolatedScenarioOrder(selected)
  const blocks: MemoryChildResult[] = []
  const report = {
    schemaVersion: 2,
    status: 'running',
    date: new Date().toISOString(),
    finishedAt: undefined as string | undefined,
    node: process.version,
    execPath: process.execPath,
    platform: process.platform,
    arch: process.arch,
    topic,
    iterations,
    runs,
    isolatedBlocks,
    serialTimingMode,
    configuration: benchmarkConfiguration(),
    environment: captureBenchmarkEnvironment(),
    order: order.map((scenario) => scenario.id),
    blocks,
    results: [] as MemoryChildResult[],
    kafkaBefore: null as ReturnType<typeof kafkaSnapshot>,
    kafkaAfter: null as ReturnType<typeof kafkaSnapshot>,
    topicOffsetsUnchanged: null as boolean | null,
    error: undefined as string | undefined,
  }
  // Reserve before Kafka work; never replace another invocation's evidence.
  if (resultPath) writeFileSync(resultPath, JSON.stringify(report, null, 2) + '\n', { flag: 'wx' })
  console.log(`Benchmark result: ${resultPath}`)
  const save = () => {
    if (resultPath) writeFileSync(resultPath, JSON.stringify(report, null, 2) + '\n')
  }
  try {
    if (selected.some((scenario) => scenario.id.startsWith('crab-')))
      console.log(`Benchmark current module: ${import.meta.resolve('kafka-crab-js')}`)
    if (selected.some((scenario) => scenario.id.startsWith('previous-')))
      console.log(`Benchmark previous module: ${import.meta.resolve('kafka-crab-js-previous')}`)
    console.log(
      `Benchmark blocks per scenario: ${isolatedBlocks}; ${runs} runs/block; ${runs * isolatedBlocks} measured runs/scenario`,
    )
    console.log(`Benchmark block order: ${report.order.join(', ')}`)
    report.kafkaBefore = kafkaSnapshot(true)
    save()
    for (const [index, scenario] of order.entries()) {
      console.log(`Running isolated block ${index + 1}/${order.length}: ${scenario.id}`)
      blocks.push(await runScenarioInIsolatedProcess(scenario))
      save()
    }
    report.kafkaAfter = kafkaSnapshot(false)
    if (report.kafkaBefore?.offsets && report.kafkaAfter?.offsets) {
      report.topicOffsetsUnchanged =
        JSON.stringify(report.kafkaBefore.offsets) === JSON.stringify(report.kafkaAfter.offsets)
    }
    if (JSON.stringify(report.environment.harness) !== JSON.stringify(captureBenchmarkEnvironment().harness)) {
      throw new Error('Benchmark source changed during the run')
    }
    report.results = selected.map((scenario) =>
      mergeIsolatedBlocks(blocks.filter((block) => block.scenario.id === scenario.id)),
    )
    report.status = 'complete'
    report.finishedAt = new Date().toISOString()
    save()
    return { blocks, results: report.results }
  } catch (error) {
    report.status = 'failed'
    report.error = error instanceof Error ? error.message : String(error)
    report.finishedAt = new Date().toISOString()
    save()
    throw error
  }
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
  console.log(
    `Benchmark measurement window: ${measurementWindow}; warmup target: ${warmupMessages} messages (whole deliveries)`,
  )
  console.log(`Benchmark fetch min bytes: ${fetchMinBytes}`)
  console.log(`Benchmark fetch wait: ${fetchWaitMs}ms`)
  console.log(
    `Benchmark crab fetch queue backoff: ${fetchQueueBackoffMs === null ? 'version default' : `${fetchQueueBackoffMs}ms`}`,
  )
  console.log(`Benchmark fetch max bytes: ${fetchMaxBytes}`)
  console.log(`Benchmark partition max bytes: ${partitionMaxBytes}`)
  console.log(`Benchmark KafkaJS eachMessage concurrency: ${kafkaJsEachMessageConcurrency}`)
  console.log(`Benchmark batch size: ${batchSize}`)
  console.log(`Benchmark batch timeout: ${batchTimeoutMs}ms`)
  console.log(`Benchmark serial prefetch size: ${serialPrefetchSize}`)
  console.log(`Benchmark serial prefetch timeout: ${serialPrefetchTimeoutMs}ms`)
  if (batchSize !== requestedBatchSize) {
    console.log(`Benchmark requested batch size: ${requestedBatchSize} (normalized for comparable batch scenarios)`)
  }
  console.log(`Benchmark memory sample interval: ${memorySampleIntervalMs}ms`)
  console.log(`Benchmark memory settle time: ${memorySettleMs}ms`)
  console.log(`Benchmark colors: ${useColors}`)
  console.log(`Benchmark charts: ${showCharts}`)
  console.log(`Benchmark serial timing: ${serialTimingMode}`)

  const scenariosToRun = selectScenarios()
  if (scenariosToRun.length === 0) {
    throw new Error('No memory benchmark scenarios selected')
  }

  console.log(`Benchmark scenarios: ${scenariosToRun.map((scenario) => scenario.id).join(', ')}`)
  console.log(`Benchmark shuffle scenarios: ${shuffleScenarios}`)

  const { results } = await collectIsolatedBlocks(scenariosToRun)

  console.log(
    'Memory columns are lifecycle sampled maxima (including final GC); per-process OS peak and processing samples are in JSON.',
  )
  printMemoryResults(results, { useColors, showCharts })
  const timingRows = results.flatMap((result) => {
    const timing = result.serialTiming
    if (!timing) return []
    const sum = (key: 'wallMs' | 'processCpuMs' | 'mainThreadCpuMs') =>
      timing.reduce((total, row) => total + row[key], 0).toFixed(2)
    return [
      {
        scenario: result.scenario.id,
        'wall ms': sum('wallMs'),
        'process CPU ms': sum('processCpuMs'),
        'main thread CPU ms': sum('mainThreadCpuMs'),
      },
    ]
  })
  if (timingRows.length > 0) {
    console.log('Serial timing totals (diagnostic; process CPU includes all threads)')
    console.table(timingRows)
  }
}

async function runIsolatedThroughputBenchmark() {
  console.log('Starting isolated consumer throughput benchmark...')
  console.log(`Benchmark brokers: ${brokers.join(',')}`)
  console.log(`Benchmark topic: ${topic}`)
  console.log(`Benchmark iterations: ${iterations}`)
  console.log(`Benchmark runs: ${runs}`)
  console.log(
    `Benchmark measurement window: ${measurementWindow}; warmup target: ${warmupMessages} messages (whole deliveries)`,
  )
  console.log(`Benchmark force GC before run: ${forceGcBeforeRun}`)
  console.log(`Benchmark scenario timeout: ${scenarioTimeoutMs}ms`)
  console.log(`Benchmark fetch min bytes: ${fetchMinBytes}`)
  console.log(`Benchmark fetch wait: ${fetchWaitMs}ms`)
  console.log(
    `Benchmark crab fetch queue backoff: ${fetchQueueBackoffMs === null ? 'version default' : `${fetchQueueBackoffMs}ms`}`,
  )
  console.log(`Benchmark fetch max bytes: ${fetchMaxBytes}`)
  console.log(`Benchmark partition max bytes: ${partitionMaxBytes}`)
  console.log(`Benchmark KafkaJS eachMessage concurrency: ${kafkaJsEachMessageConcurrency}`)
  console.log(`Benchmark batch size: ${batchSize}`)
  console.log(`Benchmark batch timeout: ${batchTimeoutMs}ms`)
  console.log(`Benchmark serial prefetch size: ${serialPrefetchSize}`)
  console.log(`Benchmark serial prefetch timeout: ${serialPrefetchTimeoutMs}ms`)
  if (batchSize !== requestedBatchSize) {
    console.log(`Benchmark requested batch size: ${requestedBatchSize} (normalized for comparable batch scenarios)`)
  }
  console.log('Benchmark isolated mode starts a fresh child Node.js process for each scenario block')

  const scenariosToRun = selectScenarios()
  if (scenariosToRun.length === 0) {
    throw new Error('No isolated benchmark scenarios selected')
  }

  console.log(`Benchmark scenarios: ${scenariosToRun.map((scenario) => scenario.id).join(', ')}`)
  console.log(`Benchmark shuffle scenarios: ${shuffleScenarios}`)

  const { results: isolatedResults } = await collectIsolatedBlocks(scenariosToRun)

  printBenchmarkResults(
    isolatedResults.map((result) => ({
      id: result.scenario.id,
      label: result.scenario.label,
      result: createBenchmarkResult(result.measurements),
      measurements: result.measurements,
    })),
    { title: 'Consumer benchmark (isolated process)', useColors, showCharts },
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
