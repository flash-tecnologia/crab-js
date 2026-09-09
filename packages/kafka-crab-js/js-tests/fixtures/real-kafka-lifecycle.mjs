import { deepEqual, equal, ok } from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { setTimeout as sleep } from 'node:timers/promises'

import { KafkaClient } from '../../dist/index.js'
import { expandCompactBatch } from '../../dist/kafka-client.js'

const [mode, scenario] = process.argv.slice(2)
const batchSize = scenario === 'prefetch' ? 16 : 32
const topic = `lifecycle-${randomUUID()}`
const client = new KafkaClient({
  brokers: process.env.KAFKA_BROKERS || 'localhost:9092',
  clientId: `lifecycle-${randomUUID()}`,
  logLevel: 'error',
})
const producer = client.createProducer({
  queueTimeout: 10_000,
  configuration: { 'message.timeout.ms': 10_000 },
})
let consumer
let reader
const collected = []
let initialMetadata

async function send(start, count) {
  const metadata = await producer.send({
    topic,
    messages: Array.from({ length: count }, (_, index) => ({
      key: Buffer.from('one-partition'),
      payload: Buffer.from(String(start + index)),
    })),
  })
  equal(metadata.length, count)
  ok(
    metadata.every((item) => !item.error),
    'Every produced message must be acknowledged successfully',
  )
  return metadata.toSorted((left, right) => left.offset - right.offset)
}

async function read() {
  const result = await reader.read()
  if (!result.done) {
    const messages =
      mode === 'serial' ? [result.value] : mode === 'batch' ? result.value : expandCompactBatch(result.value)
    collected.push(...messages)
  }
  return result.done
}

function verifyPrefix() {
  deepEqual(
    collected.map((message) => Number(message.payload.toString())),
    Array.from({ length: collected.length }, (_, index) => index),
    'No gaps, duplicates or reordered IDs',
  )
  deepEqual(
    collected.map((message) => message.offset),
    initialMetadata.slice(0, collected.length).map((item) => item.offset),
    'Offsets must match acknowledged records',
  )
  ok(collected.every((message) => message.topic === topic && message.partition === initialMetadata[0].partition))
}

try {
  const count = scenario === 'partial' ? 2 : scenario === 'prefetch' ? 16 : scenario === 'cancel' ? batchSize : 1024
  initialMetadata = await send(0, count)
  ok(
    initialMetadata.every((item) => item.partition === initialMetadata[0].partition),
    'Single partition required',
  )
  consumer = client.createConsumer({
    groupId: `lifecycle-${randomUUID()}`,
    enableAutoCommit: false,
    configuration: { 'enable.auto.offset.store': false },
  })
  await consumer.subscribe([
    {
      topic,
      partitionOffset: [{ partition: initialMetadata[0].partition, offset: { offset: initialMetadata[0].offset } }],
    },
  ])
  const batchTimeout = scenario === 'partial' ? 5000 : 200
  const stream =
    mode === 'serial'
      ? consumer.recvStream(batchSize, batchTimeout)
      : mode === 'batch'
        ? consumer.recvBatchStream(batchSize, batchTimeout)
        : consumer.recvBatchStreamCompact(batchSize, batchTimeout)
  reader = stream.getReader()
  let disconnectMs = null
  let endAfterDisconnectMs = null

  if (scenario === 'slow') {
    while (collected.length < count) {
      equal(await read(), false, 'Stream must remain open until all messages arrive')
      await sleep(1)
    }
    const started = performance.now()
    await consumer.disconnect()
    disconnectMs = performance.now() - started
    equal(await read(), true)
    equal(collected.length, count)
    verifyPrefix()
  } else if (scenario === 'partial') {
    const pending = read()
    // Load scenario only: no public hook acknowledges native collection start.
    await sleep(500)
    const started = performance.now()
    await consumer.disconnect()
    disconnectMs = performance.now() - started
    equal(await pending, false)
    while (!(await read())) {
      /* Drain the partial batch and wait for completion. */
    }
    endAfterDisconnectMs = performance.now() - started
    equal(collected.length, count)
    verifyPrefix()
  } else if (scenario === 'prefetch') {
    equal(mode, 'serial')
    equal(await read(), false)
    equal(collected.length, 1)
    // Receiving the first item does not expose native batch size. The full batch
    // is expected here because size equals the already-acknowledged input count.
    await consumer.disconnect()
    while (!(await read())) {
      /* Drain the still-live reader. */
    }
    equal(collected.length, count, 'Disconnect should drain the serial prefetch')
    verifyPrefix()
  } else if (scenario === 'backlog') {
    equal(await read(), false)
    // Deliberate slow-reader pressure; not a deterministic native queue-full fence.
    await sleep(500)
    const started = performance.now()
    await consumer.disconnect()
    disconnectMs = performance.now() - started
    while (!(await read())) {
      /* Drain batches available after disconnect. */
    }
    endAfterDisconnectMs = performance.now() - started
    ok(collected.length > 0 && collected.length <= count)
    verifyPrefix()
    // Resume at the next application-visible offset. No commit was made for data
    // prefetched by the cancelled/disconnected reader.
    const resumed = client.createConsumer({ groupId: `resume-${randomUUID()}`, enableAutoCommit: false })
    try {
      await resumed.subscribe([
        {
          topic,
          partitionOffset: [
            {
              partition: initialMetadata[0].partition,
              offset: { offset: initialMetadata[0].offset + collected.length },
            },
          ],
        },
      ])
      const remainder = []
      const deadline = Date.now() + 10_000
      while (remainder.length < count - collected.length && Date.now() < deadline) {
        remainder.push(...(await resumed.recvBatch(128, 250)))
      }
      deepEqual(
        remainder.map((message) => Number(message.payload.toString())),
        Array.from({ length: count - collected.length }, (_, index) => collected.length + index),
      )
      deepEqual(
        remainder.map((message) => message.offset),
        initialMetadata.slice(collected.length).map((item) => item.offset),
      )
    } finally {
      await resumed.disconnect()
    }
  } else if (scenario === 'cancel') {
    while (collected.length < count) equal(await read(), false)
    verifyPrefix()
    const pending = reader.read()
    await reader.cancel()
    equal((await pending).done, true)
    await send(count, 3)
    const later = []
    const deadline = Date.now() + 10_000
    while (later.length < 3 && Date.now() < deadline) later.push(...(await consumer.recvBatch(3, 250)))
    deepEqual(
      later.map((message) => Number(message.payload.toString())),
      [count, count + 1, count + 2],
    )
  } else {
    throw new Error(`Unknown scenario: ${scenario}`)
  }
  console.log(
    `LIFECYCLE_RESULT ${JSON.stringify({
      mode,
      scenario,
      topic,
      produced: count,
      deliveredBeforeEnd: collected.length,
      disconnectMs,
      endAfterDisconnectMs,
      rssBytes: process.memoryUsage().rss,
    })}`,
  )
} finally {
  await reader?.cancel().catch(() => undefined)
  await consumer?.disconnect()
}
