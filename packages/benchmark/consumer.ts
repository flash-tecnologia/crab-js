import { printResults, type Result, Tracker } from 'cronometro'
import { KafkaClient, type Message, type MessageProducer } from 'kafka-crab-js'
import { KafkaClient as KafkaClientV3, type Message as V3Message } from 'kafka-crab-js-v3'
import { Kafka as KafkaJS, logLevel } from 'kafkajs'
import RDKafka from 'node-rdkafka'
import assert from 'node:assert'
import { randomUUID } from 'node:crypto'
import { once } from 'node:events'
import { setTimeout as sleep } from 'node:timers/promises'
import { brokers, topic } from './utils/definitions.ts'

type AssertYieldMode = 'none' | 'microtask' | 'sleep'

function readPositiveInteger(name: string, fallback: number): number {
  const raw = process.env[name]
  if (!raw) {
    return fallback
  }

  const parsed = Number(raw)
  if (!Number.isFinite(parsed) || parsed < 1) {
    return fallback
  }

  return Math.floor(parsed)
}

function readNonNegativeInteger(name: string, fallback: number): number {
  const raw = process.env[name]
  if (!raw) {
    return fallback
  }

  const parsed = Number(raw)
  if (!Number.isFinite(parsed) || parsed < 0) {
    return fallback
  }

  return Math.floor(parsed)
}

function readAssertYieldMode(): AssertYieldMode {
  const raw = (process.env.BENCHMARK_ASSERT_YIELD_MODE ?? 'none').trim().toLowerCase()
  if (raw === 'none' || raw === 'microtask' || raw === 'sleep') {
    return raw
  }

  return 'none'
}

const iterations = readPositiveInteger('BENCHMARK_ITERATIONS', 100_000)
const warmupMessages = readNonNegativeInteger('BENCHMARK_WARMUP_MESSAGES', 10_000)
const benchmarkRuns = readPositiveInteger('BENCHMARK_RUNS', 7)
const scenarioCooldownMs = readNonNegativeInteger('BENCHMARK_SCENARIO_COOLDOWN_MS', 25)
const maxBytes = readPositiveInteger('BENCHMARK_MAX_BYTES', 200)
const v4SerialPrefetchSize = readPositiveInteger('BENCHMARK_V4_SERIAL_PREFETCH_SIZE', 64)
const v4SerialPrefetchTimeoutMs = readPositiveInteger('BENCHMARK_V4_SERIAL_PREFETCH_TIMEOUT_MS', 1)
const v4BatchSize = readPositiveInteger('BENCHMARK_V4_BATCH_SIZE', 8192)
const v4BatchTimeoutMs = readPositiveInteger('BENCHMARK_V4_BATCH_TIMEOUT_MS', 2)
const assertYieldMode = readAssertYieldMode()
const assertSleepMs = readNonNegativeInteger('BENCHMARK_ASSERT_SLEEP_MS', 0)
const isolatedTopics = process.env.BENCHMARK_ISOLATED_TOPICS !== '0'
const scenarioMessageCount = readPositiveInteger(
  'BENCHMARK_SCENARIO_MESSAGES',
  warmupMessages + iterations + 2048,
)
const scenarioSetupBatchSize = readPositiveInteger('BENCHMARK_SCENARIO_SETUP_BATCH_SIZE', 10_000)
const scenarioPartitions = readPositiveInteger('BENCHMARK_SCENARIO_PARTITIONS', 3)
const scenarioReplicas = readPositiveInteger('BENCHMARK_SCENARIO_REPLICAS', 1)
const benchmarkDebugSetup = process.env.BENCHMARK_DEBUG_SETUP === '1'
const benchmarkDebugCollector = process.env.BENCHMARK_DEBUG_COLLECTOR === '1'
const benchmarkForceGcBeforeRun = process.env.BENCHMARK_FORCE_GC_BEFORE_RUN !== '0'
const setupClient = isolatedTopics
  ? new KafkaClient({
    brokers: brokers.join(','),
    clientId: 'benchmark-scenario-setup',
    securityProtocol: 'Plaintext',
    logLevel: 'warn',
    brokerAddressFamily: 'v4',
    diagnostics: false,
  })
  : undefined
const setupProducer = setupClient?.createProducer()
const scenarioPartitionKeys = Array.from(
  { length: Math.max(1, scenarioPartitions) },
  (_, partition) => Buffer.from(`partition-${partition}`),
)

interface MeasurementState {
  seen: number
  measured: number
  last?: bigint
}

interface BatchCollectorStats {
  chunks: number
  messages: number
  minChunk: number
  maxChunk: number
}

class BenchmarkStopError extends Error {
  constructor() {
    super('Benchmark reached requested iteration count')
    this.name = 'BenchmarkStopError'
  }
}

function toError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error))
}

function assertPayloadSync(payload: Buffer | undefined | null) {
  if (!payload) {
    throw new Error('Payload is undefined')
  }

  const result = JSON.parse(payload.toString())
  const index = result.index as number
  assert(Number.isInteger(index))
  assert(new Date(result.date) instanceof Date)
  assert(result.message === `message index ${index}`)
}

function assertPayload(payload: Buffer | undefined | null): Promise<void> | void {
  assertPayloadSync(payload)

  if (assertYieldMode === 'microtask') {
    return Promise.resolve()
  }

  if (assertYieldMode === 'sleep' && assertSleepMs > 0) {
    return sleep(assertSleepMs).then(() => undefined)
  }
}

function createMeasurementState(): MeasurementState {
  return {
    seen: 0,
    measured: 0,
  }
}

function recordMeasurementSample(state: MeasurementState, tracker: Tracker, now: bigint): boolean {
  state.seen += 1

  if (state.seen <= warmupMessages) {
    state.last = now
    return false
  }

  if (state.last === undefined) {
    state.last = now
    return false
  }

  tracker.track(state.last)
  state.last = now
  state.measured += 1

  return state.measured >= iterations
}

function measurementStatus(state: MeasurementState): string {
  return `${state.measured}/${iterations} measured (seen=${state.seen}, warmup=${warmupMessages})`
}

function measurementDone(state: MeasurementState): boolean {
  return state.measured >= iterations
}

function throughput(result: Result): number {
  return result.mean > 0 ? 1e9 / result.mean : 0
}

function maybeForceGc() {
  if (!benchmarkForceGcBeforeRun) {
    return
  }

  const gc = (globalThis as { gc?: () => void }).gc
  if (typeof gc === 'function') {
    gc()
  }
}

function selectMedianResult(results: Result[]): Result {
  if (results.length === 0) {
    throw new Error('No benchmark results available for median selection')
  }

  const sorted = results.toSorted((left, right) => left.mean - right.mean)
  return sorted[Math.floor(sorted.length / 2)] as Result
}

function createScenarioTopicName(name: string, run: number): string {
  const normalized = name
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 32)
  const suffix = randomUUID().replaceAll('-', '').slice(0, 8)
  return `${topic}-${normalized}-${run}-${suffix}`.slice(0, 249)
}

const BENCHMARK_SEED_PAYLOAD = Buffer.from(
  '{"message":"message index 0","index":0,"date":"2024-01-01T00:00:00.000Z"}',
)

function buildScenarioMessagePayload(): Buffer {
  return BENCHMARK_SEED_PAYLOAD
}

async function prepareScenarioTopic(name: string, run: number): Promise<string> {
  if (!isolatedTopics) {
    return topic
  }

  if (!setupClient || !setupProducer) {
    throw new Error('Benchmark scenario setup client was not initialized')
  }

  const scenarioTopic = createScenarioTopicName(name, run)
  const consumer = setupClient.createConsumer({
    groupId: randomUUID(),
    configuration: {
      'auto.offset.reset': 'earliest',
    },
  })

  try {
    await consumer.subscribe([
      {
        topic: scenarioTopic,
        createTopic: true,
        numPartitions: scenarioPartitions,
        replicas: scenarioReplicas,
        allOffsets: { position: 'Beginning' },
      },
    ])

    for (let start = 0; start < scenarioMessageCount; start += scenarioSetupBatchSize) {
      const end = Math.min(start + scenarioSetupBatchSize, scenarioMessageCount)
      const messages: MessageProducer[] = Array.from({ length: end - start }, (_, offset) => {
        const index = start + offset
        return {
          payload: buildScenarioMessagePayload(),
          key: scenarioPartitionKeys[index % scenarioPartitionKeys.length],
        }
      })

      await setupProducer.send({
        topic: scenarioTopic,
        messages,
      })
    }

    if (benchmarkDebugSetup) {
      console.log(`[setup:${name}#${run}] topic=${scenarioTopic} messages=${scenarioMessageCount}`)
    }

    return scenarioTopic
  } finally {
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
}

async function resolveScenarioTopics(name: string): Promise<string[]> {
  if (!isolatedTopics) {
    return Array.from({ length: benchmarkRuns }, () => topic)
  }

  const scenarioTopics: string[] = []
  for (let run = 1; run <= benchmarkRuns; run += 1) {
    const scenarioTopic = await prepareScenarioTopic(name, run)
    scenarioTopics.push(scenarioTopic)
  }

  return scenarioTopics
}

async function runScenario(
  name: string,
  scenarioTopics: string[],
  scenario: (scenarioTopic: string) => Promise<Result>,
): Promise<Result> {
  if (benchmarkRuns <= 1) {
    const scenarioTopic = scenarioTopics[0] ?? topic
    return scenario(scenarioTopic)
  }

  const runResults: Result[] = []

  for (let run = 1; run <= benchmarkRuns; run += 1) {
    const scenarioTopic = scenarioTopics[run - 1] ?? topic
    maybeForceGc()
    const result = await scenario(scenarioTopic)
    runResults.push(result)

    if (scenarioCooldownMs > 0 && run < benchmarkRuns) {
      await sleep(scenarioCooldownMs)
    }
  }

  const median = selectMedianResult(runResults)

  if (process.env.BENCHMARK_DEBUG_MEDIAN === '1') {
    const throughputs = runResults.map(result => throughput(result).toFixed(2)).join(', ')
    console.log(`[${name}] runs op/sec: ${throughputs}; median=${throughput(median).toFixed(2)}`)
  }

  return median
}

async function destroyReadableStream(stream: {
  destroyed?: boolean
  destroy(error?: Error): void
  pause?(): void
  removeAllListeners(event?: string): void
  once(event: string, listener: (...args: unknown[]) => void): void
}) {
  stream.removeAllListeners('data')
  stream.removeAllListeners('error')

  if (stream.pause) {
    try {
      stream.pause()
    } catch {
      // Noop
    }
  }

  if (stream.destroyed) {
    return
  }

  const closePromise = once(stream as never, 'close').then(() => undefined).catch(() => undefined)
  stream.destroy()
  await Promise.race([
    closePromise,
    new Promise<void>(resolve => {
      setTimeout(resolve, 250)
    }),
  ])
}

async function disconnectV3StreamConsumer(streamConsumer: {
  unsubscribe(): void
  disconnect(): Promise<void>
}) {
  try {
    streamConsumer.unsubscribe()
  } catch {
    // Noop
  }

  try {
    await streamConsumer.disconnect()
  } catch {
    // Noop
  }
}

async function kafkaCrabJsV3(scenarioTopic: string, useBatchMode = false): Promise<Result> {
  const { promise, resolve, reject } = Promise.withResolvers<Result>()
  const tracker = new Tracker()
  const measurement = createMeasurementState()

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
    batchSize: useBatchMode ? 1024 : 1,
    configuration: {
      'auto.offset.reset': 'earliest',
      'enable.auto.commit': false,
      'fetch.min.bytes': 1,
      'fetch.message.max.bytes': maxBytes,
      'fetch.wait.max.ms': 10,
    },
  })

  await consumer.subscribe([{ topic: scenarioTopic, allOffsets: { position: 'Beginning' } }])

  let completed = false

  const finish = async (error?: Error) => {
    if (completed) {
      return
    }

    completed = true

    try {
      await destroyReadableStream(consumer)
    } finally {
      await disconnectV3StreamConsumer(consumer)
    }

    if (error) {
      reject(error)
      return
    }

    if (!measurementDone(measurement)) {
      reject(new Error(`Stream ended before reaching iterations. ${measurementStatus(measurement)}`))
      return
    }

    resolve(tracker.results)
  }

  const scheduleFinish = (error?: Error) => {
    finish(error).catch(() => {})
  }

  consumer.on('data', async ({ payload }: V3Message) => {
    if (completed) {
      return
    }

    try {
      const assertion = assertPayload(payload)
      if (assertion) {
        await assertion
      }

      const shouldStop = recordMeasurementSample(measurement, tracker, process.hrtime.bigint())
      if (shouldStop) {
        await finish()
      }
    } catch (error) {
      await finish(toError(error))
    }
  })

  consumer.on('end', () => {
    scheduleFinish()
  })

  consumer.on('close', () => {
    if (!completed) {
      scheduleFinish()
    }
  })

  consumer.on('error', (error: Error) => {
    scheduleFinish(error)
  })

  return promise
}

async function disconnectV4Consumer(consumer: {
  unsubscribe(): void
  disconnect(): Promise<void>
}) {
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

async function kafkaCrabJsV4(scenarioTopic: string, useBatchMode = false): Promise<Result> {
  const tracker = new Tracker()
  const measurement = createMeasurementState()
  const collectorStats: BatchCollectorStats | undefined = useBatchMode
    ? {
      chunks: 0,
      messages: 0,
      minChunk: Number.POSITIVE_INFINITY,
      maxChunk: 0,
    }
    : undefined

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
    batchSize: useBatchMode ? v4BatchSize : 1,
    batchTimeout: useBatchMode ? v4BatchTimeoutMs : v4SerialPrefetchTimeoutMs,
    serialPrefetchSize: v4SerialPrefetchSize,
    serialPrefetchTimeout: v4SerialPrefetchTimeoutMs,
    configuration: {
      'auto.offset.reset': 'earliest',
      'enable.auto.commit': false,
      'fetch.min.bytes': 1,
      'fetch.message.max.bytes': maxBytes,
      'fetch.wait.max.ms': 10,
    },
  })

  await webConsumer.consumer.subscribe([{ topic: scenarioTopic, allOffsets: { position: 'Beginning' } }])

  const stopError = new BenchmarkStopError()

  try {
    try {
      if (assertYieldMode === 'none') {
        await webConsumer.stream.pipeTo(
          new WritableStream<Message | Message[]>({
            write(value) {
              if (Array.isArray(value)) {
                if (collectorStats) {
                  const batchLength = value.length
                  collectorStats.chunks += 1
                  collectorStats.messages += batchLength
                  if (batchLength < collectorStats.minChunk) {
                    collectorStats.minChunk = batchLength
                  }
                  if (batchLength > collectorStats.maxChunk) {
                    collectorStats.maxChunk = batchLength
                  }
                }

                for (const message of value) {
                  assertPayloadSync(message.payload)

                  if (recordMeasurementSample(measurement, tracker, process.hrtime.bigint())) {
                    throw stopError
                  }
                }
                return
              }

              assertPayloadSync(value.payload)
              if (recordMeasurementSample(measurement, tracker, process.hrtime.bigint())) {
                throw stopError
              }
            },
          }),
        )
      } else {
        await webConsumer.stream.pipeTo(
          new WritableStream<Message | Message[]>({
            async write(value) {
              if (Array.isArray(value)) {
                if (collectorStats) {
                  const batchLength = value.length
                  collectorStats.chunks += 1
                  collectorStats.messages += batchLength
                  if (batchLength < collectorStats.minChunk) {
                    collectorStats.minChunk = batchLength
                  }
                  if (batchLength > collectorStats.maxChunk) {
                    collectorStats.maxChunk = batchLength
                  }
                }

                for (const message of value) {
                  const assertion = assertPayload(message.payload)
                  if (assertion) {
                    await assertion
                  }

                  if (recordMeasurementSample(measurement, tracker, process.hrtime.bigint())) {
                    throw stopError
                  }
                }
                return
              }

              const assertion = assertPayload(value.payload)
              if (assertion) {
                await assertion
              }

              if (recordMeasurementSample(measurement, tracker, process.hrtime.bigint())) {
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

    if (collectorStats && benchmarkDebugCollector) {
      const minChunk = Number.isFinite(collectorStats.minChunk) ? collectorStats.minChunk : 0
      const avgChunk = collectorStats.chunks > 0 ? collectorStats.messages / collectorStats.chunks : 0
      console.log(
        `[v4-batch-collector] chunks=${collectorStats.chunks} messages=${collectorStats.messages} min=${minChunk} avg=${
          avgChunk.toFixed(2)
        } max=${collectorStats.maxChunk}`,
      )
    }

    if (!measurementDone(measurement)) {
      throw new Error(`Stream ended before reaching iterations. ${measurementStatus(measurement)}`)
    }

    return tracker.results
  } finally {
    await disconnectV4Consumer(webConsumer.consumer)
  }
}

function rdkafkaEvented(scenarioTopic: string): Promise<Result> {
  const { promise, resolve, reject } = Promise.withResolvers<Result>()
  const tracker = new Tracker()
  const measurement = createMeasurementState()

  const consumer = new RDKafka.KafkaConsumer(
    {
      'client.id': 'benchmarks',
      'group.id': randomUUID(),
      'metadata.broker.list': brokers.join(','),
      'enable.auto.commit': false,
      'fetch.min.bytes': 1,
      'fetch.message.max.bytes': maxBytes,
      'fetch.wait.max.ms': 10,
    },
    { 'auto.offset.reset': 'earliest' },
  )

  let completed = false

  const finish = async (error?: Error) => {
    if (completed) {
      return
    }

    completed = true

    consumer.removeAllListeners('data')
    consumer.removeAllListeners('event.error')

    try {
      consumer.pause([
        { topic: scenarioTopic, partition: 0 },
        { topic: scenarioTopic, partition: 1 },
        { topic: scenarioTopic, partition: 2 },
      ])
    } catch {
      // Noop
    }

    await new Promise<void>(resolveDisconnect => {
      setTimeout(() => {
        try {
          consumer.disconnect()
        } catch {
          // Noop
        }
        resolveDisconnect()
      }, 20)
    })

    if (error) {
      reject(error)
      return
    }

    if (!measurementDone(measurement)) {
      reject(new Error(`Stream ended before reaching iterations. ${measurementStatus(measurement)}`))
      return
    }

    resolve(tracker.results)
  }

  const scheduleFinish = (error?: Error) => {
    finish(error).catch(() => {})
  }

  consumer.on('data', async (message: RDKafka.Message) => {
    if (completed) {
      return
    }

    try {
      const assertion = assertPayload(message.value)
      if (assertion) {
        await assertion
      }

      if (recordMeasurementSample(measurement, tracker, process.hrtime.bigint())) {
        await finish()
      }
    } catch (error) {
      await finish(toError(error))
    }
  })

  consumer.on('ready', () => {
    consumer.subscribe([scenarioTopic])
    consumer.consume()
  })

  consumer.on('event.error', (error: unknown) => {
    scheduleFinish(toError(error))
  })

  consumer.connect()

  return promise
}

function rdkafkaStream(scenarioTopic: string): Promise<Result> {
  const { promise, resolve, reject } = Promise.withResolvers<Result>()
  const tracker = new Tracker()
  const measurement = createMeasurementState()

  const stream = RDKafka.KafkaConsumer.createReadStream(
    {
      'client.id': 'benchmarks',
      'group.id': randomUUID(),
      'metadata.broker.list': brokers.join(','),
      'enable.auto.commit': false,
      'fetch.min.bytes': 1,
      'fetch.message.max.bytes': maxBytes,
      'fetch.wait.max.ms': 10,
    },
    { 'auto.offset.reset': 'earliest' },
    { topics: [scenarioTopic], waitInterval: 0, highWaterMark: 1024, objectMode: true },
  )

  let completed = false

  const finish = async (error?: Error) => {
    if (completed) {
      return
    }

    completed = true

    try {
      await destroyReadableStream(stream)
    } catch {
      // Noop
    }

    if (error) {
      reject(error)
      return
    }

    if (!measurementDone(measurement)) {
      reject(new Error(`Stream ended before reaching iterations. ${measurementStatus(measurement)}`))
      return
    }

    resolve(tracker.results)
  }

  const scheduleFinish = (error?: Error) => {
    finish(error).catch(() => {})
  }

  stream.on('data', async (message: RDKafka.Message) => {
    if (completed) {
      return
    }

    try {
      const assertion = assertPayload(message.value)
      if (assertion) {
        await assertion
      }

      if (recordMeasurementSample(measurement, tracker, process.hrtime.bigint())) {
        await finish()
      }
    } catch (error) {
      await finish(toError(error))
    }
  })

  stream.on('end', () => {
    scheduleFinish()
  })

  stream.on('close', () => {
    if (!completed) {
      scheduleFinish()
    }
  })

  stream.on('error', (error: unknown) => {
    scheduleFinish(toError(error))
  })

  return promise
}

async function kafkajs(scenarioTopic: string): Promise<Result> {
  const { promise, resolve, reject } = Promise.withResolvers<Result>()
  const tracker = new Tracker()
  const measurement = createMeasurementState()

  const client = new KafkaJS({ clientId: 'benchmarks', brokers, logLevel: logLevel.ERROR })
  const consumer = client.consumer({ groupId: randomUUID(), maxBytes, maxWaitTimeInMs: 10 })

  await consumer.connect()
  await consumer.subscribe({ topics: [scenarioTopic], fromBeginning: true })

  let completed = false

  const finish = async (error?: Error) => {
    if (completed) {
      return
    }

    completed = true

    try {
      await consumer.stop()
    } catch {
      // Noop
    }

    try {
      await consumer.disconnect()
    } catch {
      // Noop
    }

    if (error) {
      reject(error)
      return
    }

    if (!measurementDone(measurement)) {
      reject(new Error(`Stream ended before reaching iterations. ${measurementStatus(measurement)}`))
      return
    }

    resolve(tracker.results)
  }

  const scheduleFinish = (error?: Error) => {
    finish(error).catch(() => {})
  }

  consumer.on('consumer.crash', (event: unknown) => {
    if (completed) {
      return
    }

    const crashError = toError(
      event && typeof event === 'object' && 'payload' in event
        ? (event as { payload?: { error?: unknown } }).payload?.error ?? event
        : event,
    )
    scheduleFinish(crashError)
  })

  try {
    await consumer.run({
      autoCommit: false,
      partitionsConsumedConcurrently: 3,
      async eachMessage({ pause, message }) {
        if (completed) {
          return
        }

        try {
          const assertion = assertPayload(message.value)
          if (assertion) {
            await assertion
          }

          if (recordMeasurementSample(measurement, tracker, process.hrtime.bigint())) {
            pause()
            scheduleFinish()
          }
        } catch (error) {
          scheduleFinish(toError(error))
        }
      },
    })
  } catch (error) {
    await finish(toError(error))
  }

  return promise
}

console.log('Starting consumer benchmark...')
console.log(`Benchmark brokers: ${brokers.join(',')}`)
console.log(`Benchmark iterations: ${iterations}`)
console.log(`Benchmark warmup messages: ${warmupMessages}`)
console.log(`Benchmark runs per scenario: ${benchmarkRuns}`)
console.log(
  `Benchmark assertion yield mode: ${assertYieldMode}${assertYieldMode === 'sleep' ? `(${assertSleepMs}ms)` : ''}`,
)
console.log(`Benchmark force GC before run: ${benchmarkForceGcBeforeRun}`)
console.log(
  `v4 stream tuning => serial prefetch ${v4SerialPrefetchSize}/${v4SerialPrefetchTimeoutMs}ms, batch ${v4BatchSize}/${v4BatchTimeoutMs}ms`,
)

const benchmarkOnly = new Set(
  (process.env.BENCHMARK_ONLY ?? '')
    .split(',')
    .map(value => value.trim())
    .filter(Boolean),
)

const shouldRun = (id: string) => benchmarkOnly.size === 0 || benchmarkOnly.has(id)
const scenariosToRun = [
  'v3-serial',
  'v4-serial',
  'rdkafka-stream',
  'rdkafka-evented',
  'v3-batch',
  'v4-batch',
  'kafkajs',
].filter(shouldRun)

function scenarioDatasetKey(scenario: string): string {
  if (scenario === 'v3-serial' || scenario === 'v4-serial') {
    return 'serial-shared'
  }

  if (scenario === 'v3-batch' || scenario === 'v4-batch') {
    return 'batch-shared'
  }

  return scenario
}

const datasetKeys = Array.from(new Set(scenariosToRun.map(scenarioDatasetKey)))
const scenarioTopicsByDatasetKey = new Map<string, string[]>()
for (const datasetKey of datasetKeys) {
  scenarioTopicsByDatasetKey.set(datasetKey, await resolveScenarioTopics(datasetKey))
}

const scenarioTopicsByName = new Map<string, string[]>()
for (const scenario of scenariosToRun) {
  scenarioTopicsByName.set(
    scenario,
    scenarioTopicsByDatasetKey.get(scenarioDatasetKey(scenario)) ?? [topic],
  )
}

const results: Record<string, Result> = {}

if (shouldRun('v3-serial')) {
  results['v3 kafka-crab-js (serial)'] = await runScenario(
    'v3-serial',
    scenarioTopicsByName.get('v3-serial') ?? [topic],
    scenarioTopic => kafkaCrabJsV3(scenarioTopic, false),
  )
}
if (shouldRun('v4-serial')) {
  results['kafka-crab-js v4 (stream, serial)'] = await runScenario(
    'v4-serial',
    scenarioTopicsByName.get('v4-serial') ?? [topic],
    scenarioTopic => kafkaCrabJsV4(scenarioTopic, false),
  )
}
if (shouldRun('rdkafka-stream')) {
  results['node-rdkafka (stream)'] = await runScenario(
    'rdkafka-stream',
    scenarioTopicsByName.get('rdkafka-stream') ?? [topic],
    scenarioTopic => rdkafkaStream(scenarioTopic),
  )
}
if (shouldRun('rdkafka-evented')) {
  results['node-rdkafka (evented)'] = await runScenario(
    'rdkafka-evented',
    scenarioTopicsByName.get('rdkafka-evented') ?? [topic],
    scenarioTopic => rdkafkaEvented(scenarioTopic),
  )
}
if (shouldRun('v3-batch')) {
  results['v3 kafka-crab-js (batch)'] = await runScenario(
    'v3-batch',
    scenarioTopicsByName.get('v3-batch') ?? [topic],
    scenarioTopic => kafkaCrabJsV3(scenarioTopic, true),
  )
}
if (shouldRun('v4-batch')) {
  results['kafka-crab-js v4 (stream, batch)'] = await runScenario(
    'v4-batch',
    scenarioTopicsByName.get('v4-batch') ?? [topic],
    scenarioTopic => kafkaCrabJsV4(scenarioTopic, true),
  )
}
if (shouldRun('kafkajs')) {
  results.kafkajs = await runScenario(
    'kafkajs',
    scenarioTopicsByName.get('kafkajs') ?? [topic],
    scenarioTopic => kafkajs(scenarioTopic),
  )
}

printResults(results, true, true, 'previous')

if (process.env.BENCHMARK_NO_FORCE_EXIT !== '1') {
  process.exit(0)
}
