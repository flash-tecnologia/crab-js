import { randomUUID } from 'node:crypto'
import { execFileSync } from 'node:child_process'
import { writeFileSync } from 'node:fs'
import { setTimeout as sleep } from 'node:timers/promises'
import assert from 'node:assert/strict'

// Run each implementation in a fresh process. Samples intentionally include
// teardown outside the throughput window; WeakRefs never own the consumers.
const implementation = process.argv[2] ?? 'current'
assert(['current', 'previous'].includes(implementation))
assert(globalThis.gc, 'Run node with --expose-gc')
const count = Number(process.env.BENCHMARK_ITERATIONS ?? 200000)
const runs = Number(process.env.BENCHMARK_RUNS ?? 30)
assert(Number.isSafeInteger(count) && count > 0)
assert(Number.isSafeInteger(runs) && runs > 0)
const topic = process.env.BENCHMARK_TOPIC ?? 'benchmarks'
const samples = []
const refs = []
const nativeSnapshots = []
function nativeSnapshot(phase) {
  if (process.env.NATIVE_MEMORY !== '1' || process.platform !== 'darwin') return
  try {
    nativeSnapshots.push({
      phase,
      vmmap: execFileSync('rtk', ['proxy', 'vmmap', '-summary', String(process.pid)], {
        encoding: 'utf8',
        timeout: 15000,
        maxBuffer: 4 * 1024 * 1024,
      }),
    })
  } catch (error) {
    nativeSnapshots.push({ phase, error: String(error) })
  }
}
function sample(run, phase) {
  const row = { run, phase, ...process.memoryUsage() }
  samples.push(row)
  return row
}
async function collect() {
  globalThis.gc()
  await sleep(0)
  globalThis.gc()
  await sleep(100)
}
await collect()
sample(0, 'processBaseline')
const { KafkaClient } = await import(implementation === 'current' ? 'kafka-crab-js' : 'kafka-crab-js-previous')
await collect()
sample(0, 'importBaseline')
nativeSnapshot('importBaseline')

async function cycle(run) {
  const client = new KafkaClient({
    brokers: process.env.KAFKA_BROKERS || 'localhost:9092',
    clientId: 'serial-memory',
    diagnostics: false,
    logLevel: 'error',
  })
  const web = client.createWebStreamConsumer({
    groupId: randomUUID(),
    enableAutoCommit: false,
    batchSize: 1,
    serialPrefetchSize: 64,
    serialPrefetchTimeout: 5,
    configuration: {
      'auto.offset.reset': 'earliest',
      'enable.auto.commit': false,
      'fetch.min.bytes': 1,
      'fetch.max.bytes': 2048,
      'message.max.bytes': 2048,
      'fetch.message.max.bytes': 2048,
      'fetch.wait.max.ms': 10,
      'max.partition.fetch.bytes': 2048,
    },
  })
  refs.push(new WeakRef(web.consumer))
  sample(run, 'created')
  let reader
  try {
    await web.consumer.subscribe([{ topic, allOffsets: { position: 'Beginning' } }])
    sample(run, 'subscribed')
    reader = web.stream.getReader()
    for (let i = 0; i < count; i++) {
      const result = await reader.read()
      assert(!result.done, `Stream ended at ${i}/${count}`)
    }
    sample(run, 'consumed')
  } finally {
    await reader?.cancel()
    reader?.releaseLock()
    sample(run, 'cancelled')
    web.consumer.unsubscribe()
    await web.consumer.disconnect()
    sample(run, 'disconnected')
  }
}

// The timer catches idle reads on a topic with insufficient records. A native
// stall that blocks JavaScript requires an external process deadline as well.
for (let run = 1; run <= runs; run++) {
  const deadline = setTimeout(() => {
    console.error(`Cycle ${run} exceeded 120s; check that ${topic} contains ${count} messages`)
    process.exit(1)
  }, 120000)
  try {
    await cycle(run)
  } finally {
    clearTimeout(deadline)
  }
  await collect()
  const row = sample(run, 'collected')
  console.log(
    `CYCLE ${JSON.stringify({ implementation, run, rssMiB: row.rss / 1048576, heapMiB: row.heapUsed / 1048576, externalMiB: row.external / 1048576 })}`,
  )
}
await sleep(2000)
await collect()
sample(runs, 'settled')
nativeSnapshot('settled')
const result = {
  implementation,
  node: process.version,
  platform: process.platform,
  arch: process.arch,
  topic,
  count,
  runs,
  consumersStillReachable: refs.filter((ref) => ref.deref() !== undefined).length,
  activeResources: process.getActiveResourcesInfo(),
  samples,
  nativeSnapshots,
}
if (process.env.MEMORY_RESULT_PATH)
  writeFileSync(process.env.MEMORY_RESULT_PATH, JSON.stringify(result, null, 2) + '\n')
console.log(
  `MEMORY_SUMMARY ${JSON.stringify({ ...result, nativeSnapshots: nativeSnapshots.map(({ phase, error }) => ({ phase, error })), samples: [samples[0], samples[1], samples.at(-1)] })}`,
)
