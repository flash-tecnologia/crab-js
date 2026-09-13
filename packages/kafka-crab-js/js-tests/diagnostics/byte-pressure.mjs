import { equal, ok } from 'node:assert/strict'
import { createHash, randomUUID } from 'node:crypto'
import { execFile, spawn } from 'node:child_process'
import { createReadStream } from 'node:fs'
import { mkdtemp, open, readFile, writeFile } from 'node:fs/promises'
import { arch, platform, release, tmpdir } from 'node:os'
import { join } from 'node:path'
import { createInterface } from 'node:readline'
import { fileURLToPath } from 'node:url'
import { setTimeout as sleep } from 'node:timers/promises'
import { promisify } from 'node:util'
import { KafkaClient } from '../../dist/index.js'
import { expandCompactBatch } from '../../dist/kafka-client.js'

const mode = process.argv[2] ?? 'batch'
const scenario = process.argv[3] ?? 'oversized'
if (scenario === 'sustained') {
  await runSustained(mode)
} else if (scenario === 'pressure-producer') {
  await seedPressure(process.argv[4])
} else if (scenario === 'pressure-consumer') {
  await consumePressure(mode, process.argv[4])
} else {
  equal(scenario, 'oversized')
  await runOversized(mode)
}

async function runOversized(mode) {
  ok(['batch', 'compact'].includes(mode))
  const topic = `rfc-bytes-${randomUUID()}`
  const client = new KafkaClient({ brokers: process.env.KAFKA_BROKERS || 'localhost:9092', logLevel: 'error' })
  const producer = client.createProducer({ queueTimeout: 30_000 })
  const count = 80
  const payloadBytes = 512 * 1024
  const consumer = client.createConsumer({ groupId: topic, enableAutoCommit: false })
  let reader
  let peakRss = process.memoryUsage().rss
  const sampler = setInterval(() => {
    peakRss = Math.max(peakRss, process.memoryUsage().rss)
  }, 20)
  try {
    const offsets = []
    for (let start = 0; start < count; start += 8) {
      offsets.push(
        ...(await producer.send({
          topic,
          messages: Array.from({ length: 8 }, (_, i) => ({
            key: Buffer.from('one-partition'),
            payload: Buffer.alloc(payloadBytes, start + i),
            headers: { index: Buffer.from(String(start + i)) },
          })),
        })),
      )
    }
    equal(offsets.length, count)
    ok(offsets.every((item) => !item.error && item.partition === offsets[0].partition))
    await consumer.subscribe([
      { topic, partitionOffset: [{ partition: offsets[0].partition, offset: { offset: offsets[0].offset } }] },
    ])
    reader = (
      mode === 'batch' ? consumer.recvBatchStream(count, 10_000) : consumer.recvBatchStreamCompact(count, 10_000)
    ).getReader()
    const result = await reader.read()
    equal(result.done, false)
    const messages = mode === 'batch' ? result.value : expandCompactBatch(result.value)
    equal(messages.length, count, 'One oversized batch must be delivered intact')
    for (let i = 0; i < count; i++) {
      equal(messages[i].offset, offsets[i].offset)
      equal(messages[i].payload.length, payloadBytes)
      equal(messages[i].payload[0], i)
      equal(messages[i].headers.index.toString(), String(i))
    }
    await consumer.disconnect()
    equal((await reader.read()).done, true)
    peakRss = Math.max(peakRss, process.memoryUsage().rss)
    console.log(
      `BYTE_PRESSURE_RESULT ${JSON.stringify({ mode, count, batchPayloadBytes: count * payloadBytes, peakRss, external: process.memoryUsage().external })}`,
    )
  } finally {
    clearInterval(sampler)
    await reader?.cancel().catch(() => undefined)
    await consumer.disconnect()
  }
}

function pressureProfile(index, kind = 'mixed') {
  const mixed = [
    { payload: 192 * 1024, header: 1024 },
    { payload: 1024, header: 192 * 1024 },
    { payload: 64 * 1024, header: 64 * 1024 },
    { payload: 0, header: 128 * 1024 },
  ][index % 4]
  if (kind === 'mixed') return mixed
  const total = mixed.payload + mixed.header
  if (kind === 'payload') return { payload: total - 1024, header: 1024 }
  equal(kind, 'headers')
  return { payload: 1024, header: total - 1024 }
}

function integerSetting(name, fallback, minimum = 0) {
  const value = Number(process.env[name] ?? fallback)
  ok(Number.isSafeInteger(value) && value >= minimum, `${name} must be an integer >= ${minimum}`)
  return value
}

function pressureSettings() {
  return {
    cycles: integerSetting('PRESSURE_CYCLES', 1, 1),
    minDurationMs: integerSetting('PRESSURE_MIN_SECONDS', 0) * 1000,
    queueKiB: integerSetting('PRESSURE_QUEUE_KIB', 65536, 1),
    queueMessages: integerSetting('PRESSURE_QUEUE_MESSAGES', 100000, 1),
    fetchBytes: integerSetting('PRESSURE_FETCH_BYTES', 8 * 1024 * 1024, 1),
    partitionFetchBytes: integerSetting('PRESSURE_PARTITION_FETCH_BYTES', 1024 * 1024, 1),
    initialPauseMs: integerSetting('PRESSURE_INITIAL_PAUSE_MS', 1500),
    readDelayMs: integerSetting('PRESSURE_READ_DELAY_MS', 250),
    pauseMs: integerSetting('PRESSURE_PAUSE_MS', 1000),
    settleMs: integerSetting('PRESSURE_SETTLE_MS', 2000),
    requireBlocking: process.env.PRESSURE_REQUIRE_BLOCKING !== '0',
  }
}

async function seedPressure(manifestPath) {
  const profileKind = process.env.PRESSURE_PROFILE ?? 'mixed'
  ok(['mixed', 'payload', 'headers'].includes(profileKind))
  const topic = `rfc-sustained-${randomUUID()}`
  const client = new KafkaClient({ brokers: process.env.KAFKA_BROKERS || 'localhost:9092', logLevel: 'error' })
  const producer = client.createProducer({ queueTimeout: 30_000 })
  const count = 3072
  let first
  let dataBytes = 0
  for (let start = 0; start < count; start += 32) {
    const messages = Array.from({ length: 32 }, (_, n) => {
      const index = start + n
      const profile = pressureProfile(index, profileKind)
      dataBytes += profile.payload + profile.header
      return {
        key: Buffer.from('single-partition'),
        ...(profile.payload ? { payload: Buffer.alloc(profile.payload, index % 251) } : { isTombstone: true }),
        headers: { index: Buffer.from(String(index)), heavy: Buffer.alloc(profile.header, index % 251) },
      }
    })
    const metadata = await producer.send({ topic, messages })
    first ??= metadata[0]
    equal(metadata.length, messages.length)
    metadata.forEach((message, n) => {
      ok(!message.error)
      equal(message.partition, first.partition)
      equal(message.offset, first.offset + start + n)
    })
  }
  await writeFile(manifestPath, JSON.stringify({ topic, count, first, dataBytes, profileKind }), { flag: 'wx' })
}

async function consumePressure(mode, manifestPath) {
  ok(['batch', 'compact'].includes(mode))
  const manifest = JSON.parse(await readFile(manifestPath, 'utf8'))
  const { count, dataBytes } = manifest
  const settings = pressureSettings()
  ok(global.gc, 'Run with --expose-gc for settled memory samples')
  // One touched scratch buffer, included in the baseline. Validation checks every
  // byte without allocating another payload/header-sized buffer per record.
  const expected = Buffer.alloc(192 * 1024, 0xa5)
  const validateBuffer = (actual, length, byte) => {
    equal(actual.length, length)
    ok(length <= expected.length)
    expected.fill(byte, 0, length)
    ok(actual.equals(expected.subarray(0, length)))
  }
  const collect = async () => {
    global.gc()
    await sleep(0)
    global.gc()
    await sleep(settings.settleMs)
    global.gc()
    await sleep(100)
  }
  const nativeSnapshots = []
  const snapshot = async (phase) => {
    if (process.env.PRESSURE_NATIVE_MEMORY !== '1' || platform() !== 'darwin') return
    const directory = process.env.PRESSURE_EVIDENCE_DIR
    ok(directory, 'PRESSURE_NATIVE_MEMORY requires PRESSURE_EVIDENCE_DIR')
    for (const [tool, args] of [
      ['vmmap', ['-summary']],
      ['heap', ['-s']],
    ]) {
      const path = join(directory, `${mode}-${phase}-${tool}.txt`)
      try {
        const { stdout, stderr } = await promisify(execFile)('rtk', ['proxy', tool, ...args, String(process.pid)], {
          timeout: 30_000,
          maxBuffer: 16 * 1024 * 1024,
        })
        await writeFile(path, stdout + stderr, { flag: 'wx' })
        nativeSnapshots.push({ phase, tool, path })
      } catch (error) {
        nativeSnapshots.push({ phase, tool, error: String(error) })
      }
    }
  }
  await collect()
  const baseline = process.memoryUsage()
  await snapshot('baseline')
  const cycles = []
  const refs = []
  const started = performance.now()
  do {
    const cycle = cycles.length + 1
    const deadline = setTimeout(() => {
      console.error(`Pressure cycle ${cycle} exceeded 120 seconds`)
      process.exit(1)
    }, 120_000)
    let result
    try {
      // This function must return before collection: no reader, consumer or
      // last batch is retained in this outer scope or the returned measurements.
      result = await pressureCycle(mode, manifest, settings, cycle, validateBuffer, snapshot, refs)
    } finally {
      clearTimeout(deadline)
    }
    await collect()
    result.collected = process.memoryUsage()
    result.consumersStillReachable = refs.filter((ref) => ref.deref() !== undefined).length
    result.elapsedSinceStartMs = performance.now() - started
    cycles.push(result)
    if (cycle === 1) await snapshot('cycle-1-collected')
    console.log(
      `PRESSURE_CYCLE ${JSON.stringify({ mode, cycle, delivered: result.delivered, peakRss: result.peakRss, collected: result.collected, consumersStillReachable: result.consumersStillReachable, elapsedMs: result.elapsedSinceStartMs })}`,
    )
    process.send?.({
      type: 'cycle',
      mode,
      cycle,
      delivered: result.delivered,
      peakRss: result.peakRss,
      collected: result.collected,
      consumersStillReachable: result.consumersStillReachable,
      elapsedMs: result.elapsedSinceStartMs,
    })
  } while (cycles.length < settings.cycles || performance.now() - started < settings.minDurationMs)
  // Let temporary WeakRef dereferences expire before final collection.
  await collect()
  const settled = process.memoryUsage()
  await snapshot('final-collected')
  console.log(
    `SUSTAINED_RESULT ${JSON.stringify({
      mode,
      count,
      dataBytes,
      profileKind: manifest.profileKind ?? 'mixed',
      settings,
      runtime: { node: process.version, platform: platform(), arch: arch(), release: release() },
      configuration: cycles[0].configuration,
      delivered: cycles.reduce((sum, cycle) => sum + cycle.delivered, 0),
      baseline,
      settled,
      elapsedMs: performance.now() - started,
      peakRss: Math.max(baseline.rss, settled.rss, ...cycles.flatMap((cycle) => [cycle.peakRss, cycle.collected.rss])),
      osPeakRss: process.resourceUsage().maxRSS * 1024,
      consumersStillReachable: refs.filter((ref) => ref.deref() !== undefined).length,
      cycles,
      nativeSnapshots,
    })}`,
  )
}

async function pressureCycle(mode, manifest, settings, cycle, validateBuffer, snapshot, refs) {
  const { topic, count, first, profileKind = 'mixed' } = manifest
  const client = new KafkaClient({ brokers: process.env.KAFKA_BROKERS || 'localhost:9092', logLevel: 'trace' })
  const configuration = {
    'queued.max.messages.kbytes': settings.queueKiB,
    'queued.min.messages': settings.queueMessages,
    'fetch.queue.backoff.ms': 20,
    'fetch.max.bytes': settings.fetchBytes,
    'max.partition.fetch.bytes': settings.partitionFetchBytes,
  }
  const consumer = client.createConsumer({
    groupId: `${topic}-${mode}-${cycle}`,
    enableAutoCommit: false,
    configuration,
  })
  refs.push(new WeakRef(consumer))
  let reader
  let delivered = 0
  let nextPauseAt = 512
  const started = performance.now()
  const samples = []
  const stalled = []
  const readWaitMs = []
  const sample = () => samples.push({ elapsedMs: performance.now() - started, delivered, ...process.memoryUsage() })
  const sampler = setInterval(sample, 100)
  try {
    await consumer.subscribe([
      { topic, partitionOffset: [{ partition: first.partition, offset: { offset: first.offset } }] },
    ])
    reader = (
      mode === 'batch' ? consumer.recvBatchStream(32, 100) : consumer.recvBatchStreamCompact(32, 100)
    ).getReader()
    sample()
    if (settings.initialPauseMs) await sleep(settings.initialPauseMs)
    const consumeStarted = performance.now()
    while (delivered < count) {
      const readStarted = performance.now()
      let result = await reader.read()
      readWaitMs.push(performance.now() - readStarted)
      equal(result.done, false)
      let messages = mode === 'batch' ? result.value : expandCompactBatch(result.value)
      for (const message of messages) {
        const profile = pressureProfile(delivered, profileKind)
        equal(message.topic, topic)
        equal(message.partition, first.partition)
        equal(message.offset, first.offset + delivered)
        equal(message.headers.index.toString(), String(delivered))
        validateBuffer(message.payload, profile.payload, delivered % 251)
        validateBuffer(message.headers.heavy, profile.header, delivered % 251)
        equal(message.isTombstone === true, profile.payload === 0)
        delivered += 1
      }
      result = null
      messages = null
      if (settings.readDelayMs) await sleep(settings.readDelayMs)
      // Timeout can produce a partial batch. Pause when crossing a checkpoint,
      // rather than requiring a read to end at an exact multiple of 512.
      if (settings.pauseMs && delivered >= nextPauseAt && delivered < count) {
        const checkpoint = nextPauseAt
        nextPauseAt += 512
        global.gc()
        await sleep(settings.pauseMs)
        stalled.push({ delivered, ...process.memoryUsage() })
        if (cycle === 1 && checkpoint === 1536) await snapshot('cycle-1-paused')
      }
    }
    const consumeElapsedMs = performance.now() - consumeStarted
    equal(delivered, count)
    await consumer.disconnect()
    equal((await reader.read()).done, true)
    sample()
    if (settings.pauseMs) ok(stalled.length >= 3, 'collect multiple full-queue memory windows')
    const lateRss = stalled.slice(-3).map((sample) => sample.rss)
    readWaitMs.sort((a, b) => a - b)
    return {
      cycle,
      delivered,
      configuration,
      elapsedMs: performance.now() - started,
      // This includes validation and configured pauses; it is not the steady
      // throughput benchmark. Raw read latency is recorded separately.
      consumeElapsedMs,
      messagesPerSecond: (count * 1000) / consumeElapsedMs,
      readWaitMs: {
        p50: readWaitMs[Math.floor(readWaitMs.length * 0.5)],
        p95: readWaitMs[Math.floor(readWaitMs.length * 0.95)],
        max: readWaitMs.at(-1),
      },
      peakRss: Math.max(...samples.map((s) => s.rss)),
      // Descriptive only: three windows cannot prove a general RSS plateau.
      lateRssSpread: lateRss.length ? Math.max(...lateRss) - Math.min(...lateRss) : null,
      stalled,
      samples,
    }
  } finally {
    clearInterval(sampler)
    await reader?.cancel().catch(() => undefined)
    reader?.releaseLock()
    await consumer.disconnect()
  }
}

async function runSustained(mode) {
  ok(['batch', 'compact', 'all'].includes(mode))
  const directory = await mkdtemp(join(tmpdir(), 'crab-byte-pressure-'))
  console.log(`Sustained byte-pressure evidence: ${directory}`)
  const manifestPath = join(directory, 'input.json')
  const execute = promisify(execFile)
  const script = fileURLToPath(import.meta.url)
  const settings = pressureSettings()
  const fingerprintPaths = [
    script,
    fileURLToPath(new URL('../../dist/index.js', import.meta.url)),
    fileURLToPath(new URL('../../dist/kafka-client.js', import.meta.url)),
    ...process.report.getReport().sharedObjects.filter((path) => path.endsWith('.node')),
  ]
  const fingerprints = await Promise.all(
    fingerprintPaths.map(async (path) => ({
      path,
      sha256: createHash('sha256')
        .update(await readFile(path))
        .digest('hex'),
    })),
  )
  await writeFile(
    join(directory, 'run.json'),
    JSON.stringify(
      {
        startedAt: new Date().toISOString(),
        mode,
        settings,
        fingerprints,
        runtime: { node: process.version, platform: platform(), arch: arch(), release: release() },
        nativeMemory: process.env.PRESSURE_NATIVE_MEMORY === '1',
        manifestSource: process.env.PRESSURE_MANIFEST ?? null,
      },
      null,
      2,
    ),
    { flag: 'wx' },
  )
  const options = {
    timeout: Math.max(180_000, settings.cycles * 120_000, settings.minDurationMs + 180_000),
    killSignal: 'SIGKILL',
    maxBuffer: 64 * 1024 * 1024,
    env: { ...process.env, PRESSURE_EVIDENCE_DIR: directory },
  }
  if (process.env.PRESSURE_MANIFEST) {
    await writeFile(manifestPath, await readFile(process.env.PRESSURE_MANIFEST), { flag: 'wx' })
  } else {
    const production = await execute(process.execPath, [script, 'batch', 'pressure-producer', manifestPath], options)
    await writeFile(join(directory, 'producer.log'), production.stdout + production.stderr, { flag: 'wx' })
  }
  const results = []
  const resultsPath = join(directory, 'results.json')
  await writeFile(resultsPath, '[]', { flag: 'wx' })
  for (const selected of mode === 'all' ? ['batch', 'compact'] : [mode]) {
    console.log(`Checking sustained ${selected} pressure...`)
    const logPath = join(directory, `${selected}.log`)
    const log = await open(logPath, 'wx')
    try {
      // Trace logs can exceed 100 MiB during a soak. Write directly to disk;
      // keep neither stdout nor stderr in the coordinator's memory.
      await new Promise((resolve, reject) => {
        const child = spawn(process.execPath, ['--expose-gc', script, selected, 'pressure-consumer', manifestPath], {
          env: options.env,
          timeout: options.timeout,
          killSignal: options.killSignal,
          stdio: ['ignore', log.fd, log.fd, 'ipc'],
        })
        child.on('message', (message) => {
          if (message.type === 'cycle') console.log(`PRESSURE_CYCLE ${JSON.stringify(message)}`)
        })
        child.once('error', reject)
        child.once('close', (code, signal) => {
          if (code === 0) resolve()
          else reject(new Error(`Pressure consumer exited ${code ?? signal}; see ${logPath}`))
        })
      })
    } catch (error) {
      await writeFile(
        join(directory, 'failure.json'),
        JSON.stringify({ mode: selected, error: String(error), logPath }),
        { flag: 'wx' },
      )
      throw error
    } finally {
      await log.close()
    }
    let result
    const budget = []
    for await (const line of createInterface({ input: createReadStream(logPath), crlfDelay: Infinity })) {
      if (line.startsWith('SUSTAINED_RESULT ')) result = JSON.parse(line.slice('SUSTAINED_RESULT '.length))
      else if (line.includes('byte_budget_action')) budget.push(JSON.parse(line).fields)
    }
    ok(result, 'consumer must report completion')
    const reserves = budget.filter((entry) => entry.byte_budget_action === 'reserve')
    ok(reserves.length > 0, 'native byte-budget instrumentation must be present')
    const blocked = budget.filter((entry) => entry.byte_budget_action === 'blocked')
    const resumed = reserves.filter((entry) => entry.waited)
    if (settings.requireBlocking) {
      ok(blocked.length >= 3 * result.cycles.length, 'native byte budget must actually block repeatedly')
      ok(resumed.length >= 3 * result.cycles.length, 'reader progress must unblock the native producer repeatedly')
    }
    ok(blocked.every((entry) => entry.queued_bytes + entry.incoming_bytes > entry.limit_bytes))
    ok(
      reserves.every((entry) => entry.queued_bytes <= entry.limit_bytes),
      'all ordinary batches must respect the byte limit',
    )
    result.budget = {
      peakQueuedBytes: Math.max(...reserves.map((entry) => entry.queued_bytes)),
      limitBytes: 32 * 1024 * 1024,
      blocked: blocked.length,
      resumed: resumed.length,
      observations: budget,
    }
    results.push(result)
    await writeFile(resultsPath, JSON.stringify(results, null, 2))
    console.log(
      JSON.stringify({
        mode: selected,
        delivered: result.delivered,
        peakRss: result.peakRss,
        cycles: result.cycles.length,
        collectedRss: result.settled.rss,
        blocked: blocked.length,
        resumed: resumed.length,
      }),
    )
  }
}
