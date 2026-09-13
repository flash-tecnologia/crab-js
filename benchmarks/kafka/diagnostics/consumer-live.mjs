import { deepEqual, equal, ok } from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { createHash, randomUUID } from 'node:crypto'
import { mkdtemp, open, readFile, writeFile } from 'node:fs/promises'
import { arch, cpus, freemem, loadavg, platform, release, tmpdir, totalmem } from 'node:os'
import { join } from 'node:path'
import { createHistogram, monitorEventLoopDelay } from 'node:perf_hooks'
import { setTimeout as sleep } from 'node:timers/promises'
import { fileURLToPath } from 'node:url'
import { crc32 } from 'node:zlib'
import { KafkaClient } from 'kafka-crab-js'
import { startGcObserver } from '../utils/gc.ts'

// Live traffic, not a drain benchmark: one producer and one consumer in separate
// processes, natural GC, public Web Stream API, and confirmed processing commits.
const script = fileURLToPath(import.meta.url)
const worker = process.argv[2]
if (worker === 'producer' || worker === 'consumer') {
  try {
    const config = JSON.parse(await readFile(process.argv[3], 'utf8'))
    const result = await (worker === 'producer' ? produce(config) : consume(config))
    await save(join(config.directory, `${worker}.json`), result)
    process.exit(0)
  } catch (error) {
    console.error(error)
    process.exit(1)
  }
} else {
  equal(worker, undefined, 'Use LIVE_MODES and LIVE_ORDER to select cases')
  await coordinate()
}

function integer(name, fallback, minimum, maximum) {
  const value = Number(process.env[name] ?? fallback)
  ok(Number.isSafeInteger(value) && value >= minimum && value <= maximum, `Invalid ${name}`)
  return value
}

function save(path, value) {
  return writeFile(path, JSON.stringify(value, null, 2), { flag: 'wx' })
}

function keyForPartition(partition) {
  for (let i = 0; ; i++) {
    const key = Buffer.from(`live-key-${i}`)
    if (crc32(key) % 3 === partition) return key
  }
}

function profile(id) {
  const kind = id % 20
  if (kind < 14) return { payload: 2048, header: 128 }
  if (kind < 18) return { payload: 32768, header: 512 }
  return { payload: kind === 18 ? 1024 : 0, header: 131072 }
}

function tickSize(tick, config) {
  const fraction = (tick * config.tickMs) / config.durationMs
  const burst = (fraction >= 1 / 6 && fraction < 1 / 3) || (fraction >= 13 / 24 && fraction < 17 / 24)
  return ((burst ? config.burstRate : config.rate) * config.tickMs) / 1000
}

function stats(histogram, scale = 1000) {
  return {
    count: histogram.count,
    p50: histogram.count ? histogram.percentile(50) / scale : null,
    p95: histogram.count ? histogram.percentile(95) / scale : null,
    p99: histogram.count ? histogram.percentile(99) / scale : null,
    max: histogram.count ? histogram.max / scale : null,
  }
}

function record(histogram, ms) {
  ok(ms >= 0 && Number.isFinite(ms), 'Clock moved backwards or latency is invalid')
  histogram.record(Math.max(1, Math.ceil(ms * 1000)))
}

function telemetry() {
  ok(!global.gc, 'Do not run this workload with --expose-gc')
  const gc = startGcObserver()
  gc.resume()
  const eventLoop = monitorEventLoopDelay({ resolution: 20 })
  eventLoop.enable()
  const baseline = process.memoryUsage()
  const cpu = process.cpuUsage()
  const started = performance.now()
  const samples = []
  let progress = {}
  const sample = () =>
    samples.push({
      timestamp: Date.now(),
      elapsedMs: performance.now() - started,
      ...progress,
      ...process.memoryUsage(),
    })
  sample()
  const timer = setInterval(sample, 100)
  return {
    update(value) {
      progress = value
    },
    finish() {
      clearInterval(timer)
      sample()
      eventLoop.disable()
      const gcResult = gc.stop()
      equal(gcResult.forcedCount, 0, 'GC must remain natural')
      return {
        pid: process.pid,
        baseline,
        final: process.memoryUsage(),
        samples,
        elapsedMs: performance.now() - started,
        cpu: process.cpuUsage(cpu),
        peakRss: Math.max(...samples.map((sample) => sample.rss)),
        osPeakRss: process.resourceUsage().maxRSS * 1024,
        gc: gcResult,
        eventLoopDelayMs: stats(eventLoop, 1e6),
      }
    },
  }
}

async function startSignal() {
  process.send({ type: 'ready' })
  return new Promise((resolve) => process.once('message', ({ startAt }) => resolve(startAt)))
}

function client(config, role) {
  return new KafkaClient({ brokers: config.brokers, clientId: `${config.id}-${role}`, logLevel: 'error' })
}

async function produce(config) {
  const producer = client(config, 'producer').createProducer({
    queueTimeout: 30000,
    configuration: {
      'enable.idempotence': true,
      acks: 'all',
      partitioner: 'consistent',
      'compression.type': 'none',
      'linger.ms': 5,
      'message.timeout.ms': 30000,
    },
  })
  const keys = [0, 1, 2].map(keyForPartition)
  const offsets = [-1, -1, -1]
  const ack = createHistogram()
  const scheduling = createHistogram()
  const steadyScheduling = createHistogram()
  const monitor = telemetry()
  const startAt = await startSignal()
  let produced = 0
  let bytes = 0
  const progress = []
  for (let tick = 0; tick < config.durationMs / config.tickMs; tick++) {
    const scheduled = startAt + tick * config.tickMs
    while (scheduled > Date.now()) await sleep(scheduled - Date.now())
    record(scheduling, Date.now() - scheduled)
    if (tick * config.tickMs >= config.durationMs / 8) record(steadyScheduling, Date.now() - scheduled)
    const messages = Array.from({ length: tickSize(tick, config) }, (_, n) => {
      const id = produced + n
      const size = profile(id)
      const meta = Buffer.alloc(20)
      meta.writeUInt32LE(id)
      meta.writeDoubleLE(scheduled, 4)
      meta.writeDoubleLE(Date.now(), 12)
      bytes += size.payload + size.header + meta.length + keys[id % 3].length
      return {
        key: keys[id % 3],
        ...(size.payload ? { payload: Buffer.alloc(size.payload, id % 251) } : { isTombstone: true }),
        headers: { meta, data: Buffer.alloc(size.header, id % 251) },
      }
    })
    const before = performance.now()
    const metadata = await producer.send({ topic: config.topic, messages })
    record(ack, performance.now() - before)
    equal(metadata.length, messages.length)
    for (let n = 0; n < metadata.length; n++) {
      const partition = (produced + n) % 3
      const item = metadata[n]
      ok(!item.error, JSON.stringify(item.error))
      equal(item.topic, config.topic)
      equal(item.partition, partition)
      equal(item.offset, ++offsets[partition])
    }
    produced += messages.length
    monitor.update({ produced, inFlight: producer.inFlightCount() })
    if (tick % (1000 / config.tickMs) === 0) {
      const sample = { elapsedMs: Date.now() - startAt, produced, ...process.memoryUsage() }
      progress.push(sample)
      process.send({ type: 'progress', ...sample })
    }
  }
  await producer.flush()
  equal(producer.inFlightCount(), 0)
  equal(produced, config.expected)
  return {
    produced,
    bytes,
    offsets,
    startAt,
    elapsedMs: Date.now() - startAt,
    inFlight: producer.inFlightCount(),
    acknowledgementMs: stats(ack),
    schedulingDelayMs: stats(scheduling),
    steadySchedulingDelayMs: stats(steadyScheduling),
    progress,
    telemetry: monitor.finish(),
  }
}

async function consume(config) {
  const configuration = {
    'auto.offset.reset': 'earliest',
    'enable.auto.offset.store': false,
    'queued.max.messages.kbytes': 65536,
    'queued.min.messages': config.queueMessages,
    'fetch.max.bytes': 8388608,
    'max.partition.fetch.bytes': 1048576,
    'fetch.queue.backoff.ms': 20,
    'fetch.wait.max.ms': 10,
  }
  const { consumer, stream } = client(config, 'consumer').createWebStreamConsumer({
    groupId: config.groupId,
    enableAutoCommit: false,
    configuration,
    batchSize: config.mode === 'batch' ? 64 : 1,
    batchTimeout: 5,
    serialPrefetchSize: 64,
    serialPrefetchTimeout: 5,
  })
  const keys = [0, 1, 2].map(keyForPartition)
  const scratch = Buffer.alloc(131072, 0xa5)
  const checkBuffer = (buffer, size, byte) => {
    equal(buffer.length, size)
    scratch.fill(byte, 0, size)
    ok(buffer.equals(scratch.subarray(0, size)), 'Payload or header bytes differ')
  }
  const lastIds = [-3, -2, -1]
  const lastScheduled = [-1, -1, -1]
  const offsets = [-1, -1, -1]
  const committed = [-1, -1, -1]
  const e2e = createHistogram()
  const scheduledLatency = createHistogram()
  const normalLatency = createHistogram()
  const commitLatency = createHistogram()
  const progress = []
  const stalls = []
  const monitor = telemetry()
  let reader
  let received = 0
  let commits = 0
  let nextCommit = 0
  let nextProgress = 0
  let stallIndex = 0
  const commitProcessed = async () => {
    for (let partition = 0; partition < 3; partition++) {
      if (offsets[partition] === committed[partition]) continue
      const before = performance.now()
      await consumer.commit(config.topic, partition, offsets[partition] + 1, 'Sync')
      record(commitLatency, performance.now() - before)
      committed[partition] = offsets[partition]
      commits++
    }
  }
  try {
    await consumer.subscribe([{ topic: config.topic, allOffsets: { position: 'Beginning' } }])
    reader = stream.getReader()
    const startAt = await startSignal()
    const isNormal = (time) =>
      time >= config.durationMs / 8 &&
      config.stalls.every((stall) => time < stall.startMs || time >= stall.endMs + config.durationMs / 12)
    while (received < config.expected) {
      const elapsed = Date.now() - startAt
      const stall = config.stalls[stallIndex]
      if (stall && elapsed >= stall.startMs) {
        ok(elapsed < stall.endMs, 'Consumer fell behind before reaching the injected stall')
        stalls.push({ ...stall, actualStartMs: elapsed, received })
        console.log(`LIVE_STALL ${JSON.stringify(stalls.at(-1))}`)
        // Model a downstream outage: stop draining, keep producer and native
        // prefetch running. No consumer.pause(), forced GC, or trace logging.
        await sleep(stall.endMs - elapsed)
        stalls.at(-1).actualEndMs = Date.now() - startAt
        stallIndex++
      }
      const result = await reader.read()
      equal(result.done, false)
      const messages = config.mode === 'batch' ? result.value : [result.value]
      for (const message of messages) {
        const id = message.headers.meta.readUInt32LE(0)
        ok(id < config.expected)
        const partition = id % 3
        const size = profile(id)
        equal(message.topic, config.topic)
        equal(message.partition, partition)
        equal(id, lastIds[partition] + 3, 'Lost, duplicate, or reordered record')
        equal(message.offset, offsets[partition] + 1)
        equal(message.key.compare(keys[partition]), 0)
        deepEqual(Object.keys(message.headers).sort(), ['data', 'meta'])
        equal(message.headers.meta.length, 20)
        checkBuffer(message.payload, size.payload, id % 251)
        checkBuffer(message.headers.data, size.header, id % 251)
        equal(message.isTombstone === true, size.payload === 0)
        const scheduled = message.headers.meta.readDoubleLE(4)
        const sent = message.headers.meta.readDoubleLE(12)
        const now = Date.now()
        record(e2e, now - sent)
        record(scheduledLatency, now - scheduled)
        if (isNormal(scheduled - startAt) && isNormal(now - startAt)) record(normalLatency, now - sent)
        lastIds[partition] = id
        lastScheduled[partition] = scheduled - startAt
        offsets[partition] = message.offset
        received++
      }
      monitor.update({ received })
      const now = Date.now() - startAt
      for (const stopped of stalls) {
        if (stopped.caughtUpMs === undefined && lastScheduled.every((time) => time >= stopped.endMs)) {
          stopped.caughtUpMs = now
          stopped.drainDelayMs = now - stopped.actualEndMs
        }
      }
      if (now >= nextCommit) {
        await commitProcessed()
        nextCommit = Date.now() - startAt + 1000
      }
      if (now >= nextProgress) {
        const sample = { elapsedMs: now, received, ...process.memoryUsage() }
        progress.push(sample)
        process.send({ type: 'progress', ...sample })
        nextProgress = now + 1000
      }
    }
    await commitProcessed()
    const drainCompleteMs = Date.now() - startAt
    equal(stallIndex, config.stalls.length)
    ok(
      stalls.every((stall) => stall.caughtUpMs !== undefined),
      'Every injected backlog must drain',
    )
    equal(received, config.expected)
    await reader.cancel()
    reader.releaseLock()
    reader = undefined
    await consumer.disconnect()
    // Natural idle observation, not a retained-heap/leak test.
    await sleep(2000)
    return {
      received,
      offsets,
      committedNextOffsets: committed.map((offset) => offset + 1),
      configuration,
      startAt,
      drainCompleteMs,
      normalE2eMs: stats(normalLatency),
      e2eMs: stats(e2e),
      scheduledE2eMs: stats(scheduledLatency),
      commits,
      commitMs: stats(commitLatency),
      stalls,
      progress,
      telemetry: monitor.finish(),
    }
  } finally {
    await reader?.cancel().catch(() => undefined)
    reader?.releaseLock()
    await consumer.disconnect()
  }
}

async function coordinate() {
  const durationMs = integer('LIVE_SECONDS', 120, 12, 3600) * 1000
  const rate = integer('LIVE_RATE', 500, 20, 100000)
  const burstRate = integer('LIVE_BURST_RATE', 1000, 20, 100000)
  ok(rate % 20 === 0 && burstRate % 20 === 0, 'Rates must be multiples of 20')
  const modes = (process.env.LIVE_MODES ?? 'serial,batch').split(',')
  const order = (process.env.LIVE_ORDER ?? 'default,bounded,bounded,default').split(',')
  ok(modes.length && modes.every((mode) => ['serial', 'batch'].includes(mode)))
  ok(order.length && order.every((value) => ['default', 'bounded'].includes(value)))
  const brokers = process.env.KAFKA_BROKERS || 'localhost:9092'
  const directory = await mkdtemp(join(tmpdir(), 'crab-consumer-live-'))
  console.log(`Live workload evidence: ${directory}`)
  const { Kafka, logLevel } = (await import('kafkajs')).default
  const admin = new Kafka({ brokers: brokers.split(','), logLevel: logLevel.ERROR }).admin()
  const paths = [
    script,
    fileURLToPath(import.meta.resolve('kafka-crab-js')),
    fileURLToPath(new URL('kafka-client.js', import.meta.resolve('kafka-crab-js'))),
    ...process.report.getReport().sharedObjects.filter((path) => path.endsWith('.node')),
  ]
  const fingerprints = await Promise.all(
    paths.map(async (path) => ({
      path,
      sha256: createHash('sha256')
        .update(await readFile(path))
        .digest('hex'),
    })),
  )
  const manifest = {
    startedAt: new Date().toISOString(),
    directory,
    durationMs,
    tickMs: 200,
    rate,
    burstRate,
    modes,
    order,
    brokers,
    fingerprints,
    runtime: {
      node: process.version,
      platform: platform(),
      release: release(),
      arch: arch(),
      cpu: cpus()[0].model,
      totalMemory: totalmem(),
    },
    methodology: 'Concurrent producer/consumer, fresh processes per case, natural GC, no compression, 3 partitions/RF1',
  }
  const results = []
  try {
    await admin.connect()
    manifest.cluster = await admin.describeCluster()
    await save(join(directory, 'run.json'), manifest)
    for (const mode of modes)
      for (const prefetch of order) {
        const id = `${String(results.length + 1).padStart(2, '0')}-${mode}-${prefetch}`
        const caseDirectory = await mkdtemp(join(directory, `${id}-`))
        const config = {
          id,
          directory: caseDirectory,
          topic: `crab-live-${randomUUID()}`,
          groupId: `crab-live-${randomUUID()}`,
          mode,
          prefetch,
          brokers,
          durationMs,
          tickMs: 200,
          rate,
          burstRate,
          queueMessages: prefetch === 'default' ? 100000 : 256,
          stalls: [1 / 4, 5 / 8].map((fraction) => ({
            startMs: durationMs * fraction,
            endMs: durationMs * (fraction + 1 / 12),
          })),
        }
        config.expected = Array.from({ length: durationMs / config.tickMs }, (_, tick) =>
          tickSize(tick, config),
        ).reduce((a, b) => a + b)
        config.hostBefore = { freeMemory: freemem(), loadAverage: loadavg() }
        ok(
          await admin.createTopics({
            waitForLeaders: true,
            topics: [
              {
                topic: config.topic,
                numPartitions: 3,
                replicationFactor: 1,
                configEntries: [
                  { name: 'retention.ms', value: '3600000' },
                  { name: 'segment.bytes', value: '67108864' },
                ],
              },
            ],
          }),
          'Every case needs a fresh topic',
        )
        config.metadata = await admin.fetchTopicMetadata({ topics: [config.topic] })
        const configPath = join(caseDirectory, 'config.json')
        await save(configPath, config)
        console.log(`LIVE_START ${id}: ${config.expected} records, ${durationMs / 1000}s`)
        await runPair(config, configPath)
        const producer = JSON.parse(await readFile(join(caseDirectory, 'producer.json'), 'utf8'))
        const consumer = JSON.parse(await readFile(join(caseDirectory, 'consumer.json'), 'utf8'))
        equal(producer.startAt, consumer.startAt)
        equal(producer.produced, consumer.received)
        deepEqual(producer.offsets, consumer.offsets)
        ok(
          producer.steadySchedulingDelayMs.p99 <= config.tickMs && producer.steadySchedulingDelayMs.max <= 1000,
          'Producer did not sustain the offered rate; inspect producer.json',
        )
        ok(producer.elapsedMs <= durationMs + 1000, 'Production exceeded the planned duration')
        const brokerOffsets = await admin.fetchTopicOffsets(config.topic)
        const brokerCommits = await admin.fetchOffsets({ groupId: config.groupId, topics: [config.topic] })
        for (let p = 0; p < 3; p++) {
          equal(Number(brokerOffsets.find((entry) => entry.partition === p).high), consumer.offsets[p] + 1)
          equal(
            Number(brokerCommits[0].partitions.find((entry) => entry.partition === p).offset),
            consumer.offsets[p] + 1,
          )
        }
        const result = {
          config,
          producer,
          consumer,
          brokerOffsets,
          brokerCommits,
          hostAfter: { freeMemory: freemem(), loadAverage: loadavg() },
        }
        await save(join(caseDirectory, 'result.json'), result)
        results.push(result)
        // Unique run directory; checkpoint survives failure in a later case.
        await writeFile(join(directory, 'results.json'), JSON.stringify(results, null, 2))
        console.log(
          `LIVE_DONE ${JSON.stringify({
            id,
            received: consumer.received,
            consumerPeakRssMiB: consumer.telemetry.peakRss / 1048576,
            normalP95Ms: consumer.normalE2eMs.p95,
            finalBrokerLag: 0,
            forcedGc: consumer.telemetry.gc.forcedCount,
          })}`,
        )
      }
  } catch (error) {
    await save(join(directory, 'failure.json'), {
      error: String(error),
      stack: error.stack,
      completedCases: results.length,
    })
    throw error
  } finally {
    await admin.disconnect()
  }
  console.log(`LIVE_COMPLETE ${join(directory, 'results.json')}`)
}

async function runPair(config, configPath) {
  const children = []
  const files = []
  const completed = []
  const ready = []
  const state = {}
  let previousPrint = 0
  const deadline = setTimeout(() => {
    for (const child of children) child.kill('SIGKILL')
  }, config.durationMs + 90000)
  try {
    for (const role of ['consumer', 'producer']) {
      const log = await open(join(config.directory, `${role}.log`), 'wx')
      files.push(log)
      const child = spawn(process.execPath, ['--import', 'tsx', script, role, configPath], {
        stdio: ['ignore', log.fd, log.fd, 'ipc'],
        env: process.env,
      })
      children.push(child)
      const result = new Promise((resolve, reject) => {
        child.once('error', reject)
        child.once('exit', (code, signal) =>
          code === 0 ? resolve() : reject(new Error(`${role} exited ${code}/${signal}; ${config.directory}`)),
        )
      })
      // Observe failure immediately, even while awaiting the other worker's ready signal.
      result.catch(() => {
        for (const other of children) if (other !== child) other.kill('SIGKILL')
      })
      completed.push(result)
      ready.push(
        new Promise((resolve, reject) => {
          child.on('message', (message) => {
            if (message.type === 'ready') resolve()
            if (message.type === 'progress') {
              state[role] = message
              if (Date.now() - previousPrint >= 15000) {
                console.log(
                  `LIVE_PROGRESS ${JSON.stringify({ id: config.id, producer: state.producer, consumer: state.consumer })}`,
                )
                previousPrint = Date.now()
              }
            }
          })
          result.then(() => reject(new Error(`${role} exited before ready`)), reject)
        }),
      )
    }
    await Promise.all(ready)
    const startAt = Date.now() + 500
    for (const child of children) child.send({ startAt })
    await Promise.all(completed)
  } finally {
    clearTimeout(deadline)
    for (const child of children) if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL')
    await Promise.allSettled(completed)
    await Promise.all(files.map((file) => file.close()))
  }
}
