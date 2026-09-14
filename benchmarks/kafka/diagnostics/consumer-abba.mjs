import assert from 'node:assert/strict'
import { spawnSync } from 'node:child_process'
import { createHash, randomUUID } from 'node:crypto'
import { readFileSync, writeFileSync, realpathSync } from 'node:fs'
import { cpus, release, totalmem, freemem, loadavg } from 'node:os'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { brokers, topic } from '../utils/definitions.ts'
import { readBoolean, readPositiveInteger } from '../utils/env.ts'
import { createBenchmarkResult, throughputValue } from '../utils/results.ts'

const cwd = resolve(dirname(fileURLToPath(import.meta.url)), '..')
const iterations = readPositiveInteger('BENCHMARK_ITERATIONS', 20000)
const runs = readPositiveInteger('BENCHMARK_RUNS', 30)
const scenarioFamily = process.argv.includes('--batch') ? 'batch' : 'serial'
const previousScenario = `previous-${scenarioFamily}`
const currentScenario = `crab-${scenarioFamily}`
const pairOrder = process.env.BENCHMARK_PAIR_ORDER ?? 'previous-first'
assert(['previous-first', 'current-first'].includes(pairOrder), 'Invalid BENCHMARK_PAIR_ORDER')
const order =
  pairOrder === 'previous-first'
    ? [previousScenario, currentScenario, currentScenario, previousScenario]
    : [currentScenario, previousScenario, previousScenario, currentScenario]
const captureKafka = readBoolean('BENCHMARK_KAFKA_SNAPSHOT', true)
const modes = (process.env.BENCHMARK_ABBA_MODES ?? (scenarioFamily === 'batch' ? 'off' : 'off,cpu')).split(',')
assert(
  modes.length > 0 && new Set(modes).size === modes.length && modes.every((m) => ['off', 'cpu', 'gaps'].includes(m)),
)
assert(scenarioFamily !== 'batch' || modes.every((mode) => mode === 'off'), 'Batch ABBA supports timing off only')
const hash = (bytes) => createHash('sha256').update(bytes).digest('hex')

function git(args, encoding = 'utf8') {
  const child = spawnSync('rtk', ['proxy', 'git', ...args], {
    cwd,
    encoding,
    timeout: 15000,
    maxBuffer: 16 * 1024 * 1024,
  })
  assert.equal(child.status, 0, child.error?.message ?? String(child.stderr))
  return child.stdout
}

function sourceSnapshot() {
  const root = git(['rev-parse', '--show-toplevel']).trim()
  const scopes = [
    'benchmarks/kafka',
    'packages/kafka-crab-js/src',
    'packages/kafka-crab-js/js-src',
    'packages/kafka-crab-js/Cargo.toml',
    'packages/kafka-crab-js/Cargo.lock',
    'packages/kafka-crab-js/package.json',
    'Cargo.lock',
    'pnpm-lock.yaml',
  ]
  const files = git(['-C', root, 'ls-files', '--cached', '--others', '--exclude-standard', '-z', '--', ...scopes])
    .split('\0')
    .filter(
      (file) =>
        /\.(?:ts|mjs|cjs|js|rs|toml|lock|yaml)$/.test(file) ||
        file.endsWith('package.json') ||
        /tsconfig[^/]*\.json$/.test(file),
    )
  const fingerprints = [...new Set(files)].sort().map((path) => {
    try {
      return { path, sha256: hash(readFileSync(resolve(root, path))) }
    } catch (error) {
      return { path, error: error.code }
    }
  })
  return {
    root,
    commit: git(['rev-parse', 'HEAD']).trim(),
    branch: git(['branch', '--show-current']).trim(),
    status: git(['status', '--porcelain=v1']),
    fingerprints,
    sha256: hash(JSON.stringify(fingerprints)),
  }
}

function runChild(script, args, env, prefix, timeout) {
  const child = spawnSync(process.execPath, ['--expose-gc', '--import', 'tsx', script, ...args], {
    cwd,
    env: { ...process.env, ...env },
    encoding: 'utf8',
    timeout,
    killSignal: 'SIGKILL',
    maxBuffer: 16 * 1024 * 1024,
  })
  assert.equal(child.status, 0, child.error?.message ?? child.stderr)
  const line = child.stdout.split(/\r?\n/).findLast((value) => value.startsWith(prefix))
  assert(line, `Missing ${prefix}: ${child.stdout}`)
  return JSON.parse(line.slice(prefix.length))
}

async function kafkaSnapshot(sampleMessages) {
  const { Kafka, ConfigResourceTypes, logLevel } = (await import('kafkajs')).default
  const admin = new Kafka({
    brokers,
    clientId: 'consumer-abba-metadata',
    logLevel: logLevel.ERROR,
    requestTimeout: 10000,
    retry: { retries: 0 },
  }).admin()
  const snapshot = { topic, brokers }
  try {
    await admin.connect()
    snapshot.cluster = await admin.describeCluster()
    snapshot.metadata = await admin.fetchTopicMetadata({ topics: [topic] })
    snapshot.offsets = await admin.fetchTopicOffsets(topic)
    snapshot.configs = []
    const resources = [
      {
        type: ConfigResourceTypes.TOPIC,
        name: topic,
        configNames: [
          'cleanup.policy',
          'compression.type',
          'max.message.bytes',
          'retention.ms',
          'retention.bytes',
          'segment.bytes',
          'min.insync.replicas',
        ],
      },
      ...snapshot.cluster.brokers.map((broker) => ({
        type: ConfigResourceTypes.BROKER,
        name: String(broker.nodeId),
        configNames: [
          'num.network.threads',
          'num.io.threads',
          'socket.send.buffer.bytes',
          'socket.receive.buffer.bytes',
          'message.max.bytes',
          'compression.type',
          'log.flush.interval.messages',
        ],
      })),
    ]
    for (const resource of resources) {
      try {
        const response = await admin.describeConfigs({ resources: [resource], includeSynonyms: false })
        snapshot.configs.push(
          ...response.resources.map((item) => ({
            ...item,
            configEntries: item.configEntries.map((entry) => ({
              configName: entry.configName,
              configValue: entry.isSensitive ? '[redacted]' : entry.configValue,
              isDefault: entry.isDefault,
              configSource: entry.configSource,
            })),
          })),
        )
      } catch (error) {
        snapshot.configs.push({ resource, error: String(error) })
      }
    }
  } finally {
    await admin.disconnect()
  }
  if (sampleMessages) {
    const { KafkaClient } = await import('kafka-crab-js')
    const client = new KafkaClient({ brokers: brokers.join(','), clientId: 'consumer-abba-sample', diagnostics: false })
    snapshot.samples = []
    // Sample each partition independently from its beginning. Store sizes and
    // digests only, never message contents. Empty/short partitions remain visible.
    for (const partition of snapshot.offsets) {
      const consumer = client.createConsumer({
        groupId: randomUUID(),
        enableAutoCommit: false,
        configuration: { 'enable.auto.commit': false },
      })
      const messages = []
      try {
        await consumer.subscribe([
          { topic, partitionOffset: [{ partition: partition.partition, offset: { position: 'Beginning' } }] },
        ])
        const deadline = Date.now() + 5000
        while (messages.length < 32 && Date.now() < deadline) {
          const batch = await consumer.recvBatch(32 - messages.length, 100)
          for (const message of batch) {
            const headers = Object.entries(message.headers ?? {}).sort(([a], [b]) => a.localeCompare(b))
            const digest = hash(
              JSON.stringify({
                partition: message.partition,
                offset: message.offset,
                payload: hash(message.payload),
                key: message.key ? hash(message.key) : null,
                headers: headers.map(([key, value]) => [key, hash(value)]),
                tombstone: !!message.isTombstone,
              }),
            )
            messages.push({
              offset: message.offset,
              payloadBytes: message.payload.length,
              keyBytes: message.key?.length ?? 0,
              headerCount: headers.length,
              headerBytes: headers.reduce((total, [key, value]) => total + Buffer.byteLength(key) + value.length, 0),
              tombstone: !!message.isTombstone,
              sha256: digest,
            })
          }
          if (BigInt(partition.high) === BigInt(partition.low)) break
        }
      } finally {
        await consumer.disconnect()
      }
      snapshot.samples.push({ partition: partition.partition, requested: 32, messages })
    }
  }
  return snapshot
}

if (process.argv.includes('--probe')) {
  console.log(`ABBA_PROBE ${JSON.stringify(await kafkaSnapshot(process.argv.includes('--sample')))}`)
} else {
  assert(process.env.BENCHMARK_ABBA_RESULT_PATH, 'Set BENCHMARK_ABBA_RESULT_PATH')
  const output = resolve(process.env.BENCHMARK_ABBA_RESULT_PATH)
  const before = sourceSnapshot()
  const result = {
    schemaVersion: 1,
    startedAt: new Date().toISOString(),
    status: 'running',
    environment: {
      node: process.version,
      execPath: realpathSync(process.execPath),
      platform: process.platform,
      arch: process.arch,
      osRelease: release(),
      cpus: cpus().map(({ model, speed }) => ({ model, speed })),
      totalMemoryBytes: totalmem(),
      nodeOptions: process.env.NODE_OPTIONS ?? null,
      uvThreadpoolSize: process.env.UV_THREADPOOL_SIZE ?? null,
    },
    sourceBefore: before,
    iterations,
    runsPerBlock: runs,
    scenarioFamily,
    modes,
    order,
    blocks: [],
  }
  // Check the destination before Kafka work and retain a partial report on failure.
  writeFileSync(output, JSON.stringify(result, null, 2) + '\n', { flag: 'wx' })
  const save = () => writeFileSync(output, JSON.stringify(result, null, 2) + '\n')
  try {
    console.log('Capturing Kafka metadata and message sample in a separate process')
    result.kafkaBefore = captureKafka
      ? runChild(fileURLToPath(import.meta.url), ['--probe', '--sample'], {}, 'ABBA_PROBE ', 120000)
      : null
    save()
    for (const mode of modes) {
      for (const [index, scenario] of result.order.entries()) {
        console.log(`ABBA ${mode} ${index + 1}/4: ${scenario}, ${runs} runs × ${iterations}`)
        const block = {
          mode,
          index,
          scenario,
          startedAt: new Date().toISOString(),
          hostBefore: { freeMemoryBytes: freemem(), loadAverage: loadavg() },
        }
        block.result = runChild(
          resolve(cwd, 'consumer.ts'),
          [],
          {
            BENCHMARK_ITERATIONS: String(iterations),
            BENCHMARK_RUNS: String(runs),
            BENCHMARK_MEMORY_CHILD: '1',
            BENCHMARK_MEMORY: '0',
            BENCHMARK_ISOLATED: '0',
            BENCHMARK_ONLY: scenario,
            BENCHMARK_SERIAL_TIMING: mode,
            BENCHMARK_CAPTURE_RUNTIME: '1',
          },
          'BENCHMARK_MEMORY_RESULT ',
          readPositiveInteger('BENCHMARK_ABBA_TIMEOUT_MS', 600000),
        )
        assert.equal(block.result.scenario.id, scenario)
        assert.equal(block.result.measurements.length, runs)
        assert(block.result.measurements.every((row) => row.messages === iterations && row.elapsedNs > 0))
        assert(block.result.runtime.nativeBindings.length > 0)
        block.finishedAt = new Date().toISOString()
        block.throughput = throughputValue(createBenchmarkResult(block.result.measurements))
        result.blocks.push(block)
        save()
        console.log(`Completed: ${block.throughput.toFixed(2)} msg/s`)
      }
    }
    result.kafkaAfter = captureKafka
      ? runChild(fileURLToPath(import.meta.url), ['--probe'], {}, 'ABBA_PROBE ', 120000)
      : null
    result.sourceAfter = sourceSnapshot()
    result.sourceUnchanged = before.sha256 === result.sourceAfter.sha256 && before.commit === result.sourceAfter.commit
    result.topicOffsetsUnchanged =
      result.kafkaBefore?.offsets && result.kafkaAfter?.offsets
        ? JSON.stringify(result.kafkaBefore.offsets) === JSON.stringify(result.kafkaAfter.offsets)
        : null
    result.artifactConsistency = Object.fromEntries(
      [previousScenario, currentScenario].map((scenario) => {
        const signatures = result.blocks
          .filter((block) => block.scenario === scenario)
          .map((block) => {
            const { node, execPath, package: loadedPackage, javascript, nativeBindings } = block.result.runtime
            return hash(JSON.stringify({ node, execPath, loadedPackage, javascript, nativeBindings }))
          })
        return [scenario, new Set(signatures).size === 1]
      }),
    )
    result.comparisons = modes.map((mode) => {
      const blocks = result.blocks.filter((block) => block.mode === mode)
      const rate = (scenario) => {
        const measurements = blocks
          .filter((block) => block.scenario === scenario)
          .flatMap((block) => block.result.measurements)
        return throughputValue(createBenchmarkResult(measurements))
      }
      const previous = rate(previousScenario)
      const current = rate(currentScenario)
      const pairDelta = (pair) =>
        (pair.find((block) => block.scenario === currentScenario).throughput /
          pair.find((block) => block.scenario === previousScenario).throughput -
          1) *
        100
      return {
        mode,
        previous,
        current,
        aggregateDeltaPct: (current / previous - 1) * 100,
        firstPairDeltaPct: pairDelta(blocks.slice(0, 2)),
        reversedPairDeltaPct: pairDelta(blocks.slice(2, 4)),
      }
    })
    result.status = 'complete'
    result.finishedAt = new Date().toISOString()
    save()
    console.log(`Saved ${result.blocks.length} blocks to ${output}`)
    console.table(result.comparisons)
  } catch (error) {
    result.status = 'failed'
    result.error = String(error)
    save()
    throw error
  }
}
