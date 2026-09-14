import { randomUUID } from 'node:crypto'
import { setTimeout as sleep } from 'node:timers/promises'
import { readNonNegativeInteger, readPositiveInteger } from './utils/env.js'

interface ProducerLike {
  send: (record: { topic: string; messages: unknown[] }) => Promise<unknown[]>
  flush: () => Promise<unknown[]>
  inFlightCount: () => number
}

// Timeout storm against a blackhole broker: every flush times out, so every
// Confirmation is late or never arrives. It measures retained RSS after forced
// GC: unbounded per-message retention would show up here, while dropping
// Receivers (oneshot) or clearing entries on flush error retains nothing.
// No broker needed.
const sends = readPositiveInteger('BENCHMARK_STORM_SENDS', 300)
const messageBytes = readPositiveInteger('BENCHMARK_STORM_MESSAGE_BYTES', 64 * 1024)
const queueTimeout = readPositiveInteger('BENCHMARK_STORM_QUEUE_TIMEOUT_MS', 200)
const settleMs = readNonNegativeInteger('BENCHMARK_STORM_SETTLE_MS', 500)

function forceGc() {
  const gc = (globalThis as typeof globalThis & { gc?: () => void }).gc
  gc?.()
}

function rssMiB() {
  const { rss, external } = process.memoryUsage()
  return { rss: rss / 1048576, external: external / 1048576 }
}

async function storm(label: string, createProducer: () => Promise<ProducerLike>): Promise<void> {
  const producer = await createProducer()
  const payload = Buffer.alloc(messageBytes, 7)
  const topic = `storm-${randomUUID()}`

  forceGc()
  await sleep(settleMs)
  const before = rssMiB()

  let rejected = 0
  for (let index = 0; index < sends; index += 1) {
    try {
      await producer.send({ topic, messages: [{ payload }] })
    } catch {
      rejected += 1
    }
  }

  const inFlight = producer.inFlightCount()
  forceGc()
  await sleep(settleMs)
  const after = rssMiB()

  console.log(
    `${label}: sends=${sends} rejected=${rejected} inFlight=${inFlight} ` +
      `rssΔ=${(after.rss - before.rss).toFixed(1)}MiB externalΔ=${(after.external - before.external).toFixed(1)}MiB`,
  )
}

async function createWorkspaceProducer(): Promise<ProducerLike> {
  const { KafkaClient } = await import('kafka-crab-js')
  const client = new KafkaClient({ brokers: '127.0.0.1:1', clientId: 'storm-v4', logLevel: 'error' })
  return client.createProducer({ queueTimeout }) as unknown as ProducerLike
}

async function createPreviousProducer(): Promise<ProducerLike> {
  const { KafkaClient: KafkaClientPrevious } = (await import('kafka-crab-js-previous')) as unknown as {
    KafkaClient: new (config: Record<string, unknown>) => {
      createProducer: (config: Record<string, unknown>) => ProducerLike
    }
  }
  const client = new KafkaClientPrevious({ brokers: '127.0.0.1:1', clientId: 'storm-previous', logLevel: 'error' })
  return client.createProducer({ queueTimeout })
}

console.log(
  `Storm: ${sends} sends × ${(messageBytes / 1024).toFixed(0)}KiB, queueTimeout=${queueTimeout}ms, blackhole broker`,
)

let failures = 0
for (const [label, create] of [
  ['previous kafka-crab-js', createPreviousProducer],
  ['kafka-crab-js        ', createWorkspaceProducer],
] as const) {
  try {
    await storm(label, create)
  } catch (error) {
    failures += 1
    console.error(
      `STORM_ERROR ${label.trim()}: ${error instanceof Error ? (error.stack ?? error.message) : String(error)}`,
    )
  }
}

if (failures > 0) {
  process.exitCode = 1
}

// Producer handles keep the event loop alive (no disconnect API surfaces them).
// Bench scripts exit explicitly instead of hanging on native handles.
process.exit(process.exitCode ?? 0)
