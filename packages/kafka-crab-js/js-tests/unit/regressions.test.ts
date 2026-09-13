import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process'
import { createInterface } from 'node:readline'
import { deepEqual, equal, ok, rejects, throws } from 'node:assert/strict'
import { setTimeout as sleep } from 'node:timers/promises'

import { test } from 'vite-plus/test'

import {
  consumerReceiveEndChannel,
  KafkaClient,
  KafkaClientConfig,
  type KafkaConsumer,
  type Message,
  type RecordMetadata,
  type SendFailureError,
} from '../../js-src/index.js'
import { attachSendFailureDetails, SEND_FAILURE_PAYLOAD_MARKER } from '../../js-src/send-failure.js'

const UNAVAILABLE_BROKER = '127.0.0.1:1'
const FIXTURE_TIMEOUT_MS = 10_000

type MockMessage = {
  key?: string
  payload?: string
  isTombstone?: boolean
  headers?: Record<string, string>
}

type PendingRequest = {
  resolve: (result: RecordMetadata[]) => void
  reject: (error: Error) => void
}

class MockKafkaBroker {
  public readonly brokers: string
  private readonly child: ChildProcessWithoutNullStreams
  private requestId = 0
  private readonly pending = new Map<number, PendingRequest>()

  private constructor(child: ChildProcessWithoutNullStreams, brokers: string) {
    this.child = child
    this.brokers = brokers
  }

  public static async start(): Promise<MockKafkaBroker> {
    const child = spawn(process.execPath, ['--import', 'tsx', 'js-tests/fixtures/mock-kafka-broker.mjs'], {
      cwd: process.cwd(),
      stdio: ['pipe', 'pipe', 'pipe'],
    })
    let broker: MockKafkaBroker | undefined

    const ready = new Promise<string>((resolve, reject) => {
      const deadline = setTimeout(
        () => reject(new Error('Timed out while starting the mock Kafka broker')),
        FIXTURE_TIMEOUT_MS,
      )
      const inspectLine = (line: string) => {
        const bootstrapMatch = /replaced with (?<brokers>127\.0\.0\.1:\d+)/.exec(line)
        if (bootstrapMatch?.groups?.brokers) {
          clearTimeout(deadline)
          resolve(bootstrapMatch.groups.brokers)
        }
      }

      createInterface({ input: child.stdout }).on('line', inspectLine)
      createInterface({ input: child.stderr }).on('line', inspectLine)
      child.once('exit', (code) => {
        clearTimeout(deadline)
        reject(new Error(`Mock Kafka broker exited before startup with code ${String(code)}`))
      })
    })

    try {
      const brokers = await ready
      broker = new MockKafkaBroker(child, brokers)
      broker.listenForResponses()
      return broker
    } catch (error) {
      child.kill()
      throw error
    }
  }

  public async send(topic: string, messages: MockMessage[]): Promise<RecordMetadata[]> {
    this.requestId += 1
    const id = this.requestId
    const response = new Promise<RecordMetadata[]>((resolve, reject) => {
      this.pending.set(id, { resolve, reject })
    })

    this.child.stdin.write(`${JSON.stringify({ id, topic, messages })}\n`)
    return Promise.race([
      response,
      sleep(FIXTURE_TIMEOUT_MS).then(() => {
        throw new Error(`Timed out while producing mock Kafka request ${id}`)
      }),
    ])
  }

  public close(): void {
    for (const request of this.pending.values()) {
      request.reject(new Error('Mock Kafka broker closed before responding'))
    }
    this.pending.clear()
    this.child.stdin.end()
    this.child.kill()
  }

  private listenForResponses(): void {
    createInterface({ input: this.child.stdout }).on('line', (line) => {
      if (!line.startsWith('MOCK_RESPONSE ')) {
        return
      }

      const response = JSON.parse(line.slice('MOCK_RESPONSE '.length)) as {
        id: number
        result?: RecordMetadata[]
        error?: string
      }
      const request = this.pending.get(response.id)
      if (!request) {
        return
      }

      this.pending.delete(response.id)
      if (response.error) {
        request.reject(new Error(response.error))
      } else {
        request.resolve(response.result ?? [])
      }
    })
  }
}

function createClient(brokers = UNAVAILABLE_BROKER, diagnostics = false): KafkaClient {
  return new KafkaClient({
    brokers,
    clientId: `kafka-crab-regression-${crypto.randomUUID()}`,
    diagnostics,
    logLevel: 'error',
  })
}

async function collectChildOutput(child: ChildProcessWithoutNullStreams): Promise<string> {
  let output = ''
  child.stdout.on('data', (chunk: Buffer) => {
    output += chunk.toString()
  })
  child.stderr.on('data', (chunk: Buffer) => {
    output += chunk.toString()
  })

  await new Promise<void>((resolve, reject) => {
    const deadline = setTimeout(() => reject(new Error('Child process timed out')), FIXTURE_TIMEOUT_MS)
    child.once('error', (error) => {
      clearTimeout(deadline)
      reject(error)
    })
    child.once('exit', (code) => {
      clearTimeout(deadline)
      if (code === 0) {
        resolve()
      } else {
        reject(new Error(`Child process exited with code ${String(code)}`))
      }
    })
  })

  return output
}

async function disconnect(consumer: KafkaConsumer | undefined): Promise<void> {
  await consumer?.disconnect().catch(() => undefined)
}

test('M02: advanced auto-commit configuration is effective when the convenience option is omitted', () => {
  const client = createClient()

  throws(
    () =>
      client.createConsumer({
        groupId: 'auto-commit-advanced-configuration',
        configuration: { 'enable.auto.commit': 'invalid-value' },
      }),
    /Expected bool value/,
  )
})

test('M02: explicit auto-commit option overrides the advanced configuration', async () => {
  const client = createClient()
  const consumers: KafkaConsumer[] = []

  try {
    consumers.push(
      client.createConsumer({
        groupId: 'auto-commit-explicit-false',
        enableAutoCommit: false,
        configuration: { 'enable.auto.commit': 'invalid-value' },
      }),
      client.createConsumer({
        groupId: 'auto-commit-explicit-true',
        enableAutoCommit: true,
        configuration: { 'enable.auto.commit': 'invalid-value' },
      }),
    )
    equal(consumers.length, 2)
  } finally {
    await Promise.all(consumers.map(async (consumer) => disconnect(consumer)))
  }
})

test('M02: advanced auto-commit alone is honored without the convenience option', async () => {
  const consumer = createClient().createConsumer({
    groupId: 'auto-commit-advanced-alone',
    configuration: { 'enable.auto.commit': 'false' },
  })

  try {
    ok(consumer)
  } finally {
    await disconnect(consumer)
  }
})

test('M02: convenience auto-commit alone creates a consumer', async () => {
  const consumer = createClient().createConsumer({
    groupId: 'auto-commit-explicit-alone',
    enableAutoCommit: false,
  })

  try {
    ok(consumer)
  } finally {
    await disconnect(consumer)
  }
})

test('M03: client and consumer logs do not expose credentials', async () => {
  const secret = `fake-secret-${crypto.randomUUID()}`
  const child = spawn(process.execPath, ['--import', 'tsx', 'js-tests/fixtures/credential-log-probe.mjs', secret], {
    cwd: process.cwd(),
    stdio: ['pipe', 'pipe', 'pipe'],
  })

  const output = await collectChildOutput(child)
  ok(output.includes('PROBE_DONE'))
  equal(output.includes(secret), false)
})

test('M04: explicit assignments preserve multiple topics and partitions', async () => {
  const consumer = createClient().createConsumer({ groupId: 'multiple-topic-assignments', enableAutoCommit: false })

  try {
    await consumer.subscribe([
      {
        topic: 'regression-topic-a',
        partitionOffset: [
          { partition: 0, offset: { position: 'Beginning' } },
          { partition: 1, offset: { position: 'End' } },
        ],
      },
      {
        topic: 'regression-topic-b',
        partitionOffset: [{ partition: 2, offset: { position: 'Stored' } }],
      },
    ])

    const assignments = consumer
      .assignment()
      .flatMap(({ topic, partitionOffset }) => partitionOffset.map(({ partition }) => `${topic}:${partition}`))
      .toSorted()
    deepEqual(assignments, ['regression-topic-a:0', 'regression-topic-a:1', 'regression-topic-b:2'])
  } finally {
    await disconnect(consumer)
  }
})

test('M04: mixed manual and subscribe modes are rejected', async () => {
  const consumer = createClient().createConsumer({ groupId: 'mixed-modes', enableAutoCommit: false })

  try {
    await rejects(
      consumer.subscribe([
        { topic: 'mixed-plain' },
        { topic: 'mixed-manual', partitionOffset: [{ partition: 0, offset: { position: 'Beginning' } }] },
      ]),
      /does not mix manual partition assignment/,
    )
  } finally {
    await disconnect(consumer)
  }
})

test('M05: native receive paths preserve empty payloads, tombstones and regular payloads', async () => {
  const broker = await MockKafkaBroker.start()
  const topic = `tombstone-regression-${crypto.randomUUID()}`
  const metadata = await broker.send(topic, [{ payload: '' }, {}, { isTombstone: true }, { payload: 'regular' }])
  const start = metadata.toSorted((left, right) => left.offset - right.offset)[0]
  ok(start)
  const client = createClient(broker.brokers)
  const directConsumer = client.createConsumer({ groupId: `direct-${crypto.randomUUID()}`, enableAutoCommit: false })
  const compactConsumer = client.createConsumer({ groupId: `compact-${crypto.randomUUID()}`, enableAutoCommit: false })

  try {
    const assignment = [{ topic, partitionOffset: [{ partition: start.partition, offset: { offset: start.offset } }] }]
    await directConsumer.subscribe(assignment)
    await compactConsumer.subscribe(assignment)

    const direct: Message[] = []
    const directDeadline = Date.now() + 4000
    while (direct.length < 4 && Date.now() < directDeadline) {
      direct.push(...(await directConsumer.recvBatch(4 - direct.length, 500)))
    }
    equal(direct.length, 4)
    equal(direct[0]?.payload.length, 0)
    equal(direct[0]?.isTombstone, undefined)
    equal(direct[1]?.isTombstone, true)
    equal(direct[2]?.isTombstone, true)
    equal(direct[3]?.payload.toString(), 'regular')

    const reader = compactConsumer.recvBatchStreamCompact(4, 2000).getReader()
    try {
      const compactPayloads: Buffer[] = []
      const compactTombstones: boolean[] = []
      const compactDeadline = Date.now() + 4000
      while (compactPayloads.length < 4 && Date.now() < compactDeadline) {
        const compact = await reader.read()
        if (compact.done || !compact.value) {
          break
        }

        compactPayloads.push(...compact.value.payloads)
        compactTombstones.push(...(compact.value.tombstones ?? compact.value.payloads.map(() => false)))
      }

      equal(compactPayloads.length, 4)
      deepEqual(compactTombstones, [false, true, true, false])
      equal(compactPayloads[0]?.length, 0)
      equal(compactPayloads[3]?.toString(), 'regular')
    } finally {
      await reader.cancel()
    }
  } finally {
    await Promise.all([disconnect(directConsumer), disconnect(compactConsumer)])
    broker.close()
  }
})

test('M05: diagnostics preserve the tombstone marker', async () => {
  const broker = await MockKafkaBroker.start()
  const topic = `tombstone-diagnostics-${crypto.randomUUID()}`
  const [metadata] = await broker.send(topic, [{}])
  ok(metadata)
  const consumer = createClient(broker.brokers, true).createConsumer({
    groupId: `diagnostics-${crypto.randomUUID()}`,
    enableAutoCommit: false,
  })
  const observedMessages: (Message | null)[] = []
  const onReceiveEnd = ({ message }: { message: Message | null }) => {
    observedMessages.push(message)
  }
  consumerReceiveEndChannel.subscribe(onReceiveEnd)

  try {
    await consumer.subscribe([
      { topic, partitionOffset: [{ partition: metadata.partition, offset: { offset: metadata.offset } }] },
    ])
    const received = await consumer.recv()
    equal(received?.isTombstone, true)
    equal(observedMessages.at(-1)?.isTombstone, true)
  } finally {
    consumerReceiveEndChannel.unsubscribe(onReceiveEnd)
    await disconnect(consumer)
    broker.close()
  }
})

test('M08: custom highWaterMark preserves object mode', async () => {
  const stream = createClient().createStreamConsumer({
    groupId: 'object-mode-high-water-mark',
    streamOptions: { highWaterMark: 4 },
  })

  try {
    equal(stream.readableObjectMode, true)
    equal(stream.readableHighWaterMark, 4)
  } finally {
    stream.destroy()
    await new Promise<void>((resolve) => stream.once('close', resolve))
  }
})

test('HWM: explicit highWaterMark is respected on the batch path', async () => {
  const stream = createClient().createStreamConsumer({
    groupId: 'batch-high-water-mark',
    batchSize: 100,
    streamOptions: { highWaterMark: 4 },
  })

  try {
    equal(stream.readableHighWaterMark, 4)
  } finally {
    stream.destroy()
    await new Promise<void>((resolve) => stream.once('close', resolve))
  }
})

test('M01: cancelling an idle compact stream stops it from consuming later messages', async () => {
  const broker = await MockKafkaBroker.start()
  const topic = `cancel-regression-${crypto.randomUUID()}`
  const [seed] = await broker.send(topic, [{ payload: 'seed' }])
  ok(seed)
  const consumer = createClient(broker.brokers).createConsumer({
    groupId: `cancel-${crypto.randomUUID()}`,
    enableAutoCommit: false,
  })

  try {
    await consumer.subscribe([
      { topic, partitionOffset: [{ partition: seed.partition, offset: { offset: seed.offset + 1 } }] },
    ])
    const reader = consumer.recvBatchStreamCompact(1, 30).getReader()
    const pendingRead = reader.read()
    await sleep(100)
    await reader.cancel()
    await pendingRead

    await broker.send(topic, [{ payload: 'after-cancel' }])
    await sleep(150)
    const messages = await consumer.recvBatch(1, 500)
    deepEqual(
      messages.map(({ payload }) => payload.toString()),
      ['after-cancel'],
    )
  } finally {
    await disconnect(consumer)
    broker.close()
  }
})

test('M01: cancelling an idle serial stream stops it from consuming later messages', async () => {
  const broker = await MockKafkaBroker.start()
  const topic = `cancel-serial-${crypto.randomUUID()}`
  const [seed] = await broker.send(topic, [{ payload: 'seed' }])
  ok(seed)
  const consumer = createClient(broker.brokers).createConsumer({
    groupId: `cancel-serial-${crypto.randomUUID()}`,
    enableAutoCommit: false,
  })

  try {
    await consumer.subscribe([
      { topic, partitionOffset: [{ partition: seed.partition, offset: { offset: seed.offset + 1 } }] },
    ])
    const reader = consumer.recvStream().getReader()
    const pendingRead = reader.read()
    await sleep(100)
    await reader.cancel()
    const result = await pendingRead
    equal(result.done, true)

    await broker.send(topic, [{ payload: 'after-cancel-serial' }])
    await sleep(150)
    const message = await consumer.recv()
    equal(message?.payload.toString(), 'after-cancel-serial')
  } finally {
    await disconnect(consumer)
    broker.close()
  }
})

test('M01: cancelling during a partial batch read stops stream cleanly', async () => {
  const broker = await MockKafkaBroker.start()
  const topic = `cancel-partial-${crypto.randomUUID()}`
  const [seed] = await broker.send(topic, [{ payload: 'partial-1' }])
  ok(seed)
  const consumer = createClient(broker.brokers).createConsumer({
    groupId: `cancel-partial-${crypto.randomUUID()}`,
    enableAutoCommit: false,
  })

  try {
    await consumer.subscribe([
      { topic, partitionOffset: [{ partition: seed.partition, offset: { offset: seed.offset } }] },
    ])
    const reader = consumer.recvBatchStreamCompact(5, 2000).getReader()
    const firstRead = await reader.read()
    equal(firstRead.done, false)
    equal(firstRead.value?.payloads.length, 1)

    const pendingSecondRead = reader.read()
    await sleep(100)
    await reader.cancel()
    const secondRead = await pendingSecondRead
    equal(secondRead.done, true)

    await broker.send(topic, [{ payload: 'after-partial-cancel' }])
    await sleep(150)
    const directBatch = await consumer.recvBatch(1, 1000)
    equal(directBatch.length, 1)
    equal(directBatch[0]?.payload.toString(), 'after-partial-cancel')
  } finally {
    await disconnect(consumer)
    broker.close()
  }
})

test('M01: cancelling a stream with prefetch queue items does not deadlock', async () => {
  const broker = await MockKafkaBroker.start()
  const topic = `cancel-prefetch-${crypto.randomUUID()}`
  const [seed] = await broker.send(topic, [{ payload: 'pf-1' }, { payload: 'pf-2' }, { payload: 'pf-3' }])
  ok(seed)
  const consumer = createClient(broker.brokers).createConsumer({
    groupId: `cancel-pf-${crypto.randomUUID()}`,
    enableAutoCommit: false,
  })

  try {
    await consumer.subscribe([
      { topic, partitionOffset: [{ partition: seed.partition, offset: { offset: seed.offset } }] },
    ])
    const reader = consumer.recvStream(10, 50).getReader()
    const first = await reader.read()
    equal(first.done, false)
    equal(first.value?.payload.toString(), 'pf-1')

    await reader.cancel()
    const afterCancel = await reader.read()
    equal(afterCancel.done, true)
  } finally {
    await disconnect(consumer)
    broker.close()
  }
})

test('M01: disconnect drains a partial batch to a live reader', async () => {
  const broker = await MockKafkaBroker.start()
  const topic = `drain-disconnect-${crypto.randomUUID()}`
  const [seed] = await broker.send(topic, [{ payload: 'd-1' }, { payload: 'd-2' }])
  ok(seed)
  const consumer = createClient(broker.brokers).createConsumer({
    groupId: `drain-${crypto.randomUUID()}`,
    enableAutoCommit: false,
  })

  try {
    await consumer.subscribe([
      { topic, partitionOffset: [{ partition: seed.partition, offset: { offset: seed.offset } }] },
    ])
    const reader = consumer.recvBatchStreamCompact(5, 5000).getReader()
    const pending = reader.read()
    await sleep(150)
    await consumer.disconnect()
    const first = await pending
    equal(first.done, false)
    equal(first.value?.payloads.length, 2)
    const end = await reader.read()
    equal(end.done, true)
  } finally {
    broker.close()
  }
})

test('M01: disconnect drains serial prefetch to a live reader', async () => {
  const broker = await MockKafkaBroker.start()
  const topic = `drain-serial-${crypto.randomUUID()}`
  const [seed] = await broker.send(topic, [{ payload: 's-1' }, { payload: 's-2' }])
  ok(seed)
  const consumer = createClient(broker.brokers).createConsumer({
    groupId: `drain-serial-${crypto.randomUUID()}`,
    enableAutoCommit: false,
  })

  try {
    await consumer.subscribe([
      { topic, partitionOffset: [{ partition: seed.partition, offset: { offset: seed.offset } }] },
    ])
    const reader = consumer.recvStream(64, 5000).getReader()
    const first = reader.read()
    await sleep(150)
    await consumer.disconnect()
    equal((await first).done, false)
    equal((await first).value?.payload.toString(), 's-1')
    const second = await reader.read()
    equal(second.done, false)
    equal(second.value?.payload.toString(), 's-2')
    const end = await reader.read()
    equal(end.done, true)
  } finally {
    broker.close()
  }
})

test('M16: batch stream delivers large payloads through a slow reader', async () => {
  const broker = await MockKafkaBroker.start()
  const topic = `byte-budget-${crypto.randomUUID()}`
  const payloadSize = 8 * 1024
  const count = 4
  const metadata = await broker.send(
    topic,
    Array.from({ length: count }, (_, index) => ({ payload: String.fromCharCode(index + 1).repeat(payloadSize) })),
  )
  const seed = metadata.toSorted((left, right) => left.offset - right.offset)[0]
  ok(seed)

  const consumer = createClient(broker.brokers).createConsumer({
    groupId: `byte-budget-${crypto.randomUUID()}`,
    enableAutoCommit: false,
  })

  try {
    await consumer.subscribe([
      { topic, partitionOffset: [{ partition: seed.partition, offset: { offset: seed.offset } }] },
    ])
    const reader = consumer.recvBatchStream(1, 2000).getReader()
    const received: Message[] = []
    try {
      for (let index = 0; index < count; index += 1) {
        const { done, value } = await reader.read()
        ok(!done && value, `expected batch ${index + 1}`)
        received.push(...value)
        await sleep(10)
      }
    } finally {
      await reader.cancel().catch(() => undefined)
    }

    equal(received.length, count)
    for (const [index, message] of received.entries()) {
      equal(message.payload.length, payloadSize)
      equal(message.payload[0], index + 1)
    }
  } finally {
    await disconnect(consumer)
    broker.close()
  }
})
test('M16: batch stream delivers header-heavy messages through a slow reader', async () => {
  const broker = await MockKafkaBroker.start()
  const topic = `byte-budget-headers-${crypto.randomUUID()}`
  const headerSize = 4 * 1024
  const count = 8
  const metadata = await broker.send(
    topic,
    Array.from({ length: count }, (_, index) => ({
      payload: `h-${index}`,
      headers: {
        'x-first': 'a'.repeat(headerSize),
        'x-second': 'b'.repeat(headerSize),
      },
    })),
  )
  const seed = metadata.toSorted((left, right) => left.offset - right.offset)[0]
  ok(seed)

  const consumer = createClient(broker.brokers).createConsumer({
    groupId: `byte-budget-headers-${crypto.randomUUID()}`,
    enableAutoCommit: false,
  })

  try {
    await consumer.subscribe([
      { topic, partitionOffset: [{ partition: seed.partition, offset: { offset: seed.offset } }] },
    ])
    const reader = consumer.recvBatchStream(2, 2000).getReader()
    const received: Message[] = []
    try {
      for (let index = 0; index < count / 2; index += 1) {
        const { done, value } = await reader.read()
        ok(!done && value, `expected batch ${index + 1}`)
        received.push(...value)
        await sleep(10)
      }
    } finally {
      await reader.cancel().catch(() => undefined)
    }

    equal(received.length, count)
    for (const [index, message] of received.entries()) {
      equal(message.payload.toString(), `h-${index}`)
      equal(message.headers?.['x-first']?.length, headerSize)
      equal(message.headers?.['x-second']?.length, headerSize)
      equal(message.headers?.['x-first']?.[0], 97)
      equal(message.headers?.['x-second']?.[0], 98)
    }
  } finally {
    await disconnect(consumer)
    broker.close()
  }
})

test('M16: compact stream delivers shared headers through a slow reader', async () => {
  const broker = await MockKafkaBroker.start()
  const topic = `byte-budget-compact-headers-${crypto.randomUUID()}`
  const headerSize = 4 * 1024
  const count = 8
  const metadata = await broker.send(
    topic,
    Array.from({ length: count }, (_, index) => ({
      payload: `c-${index}`,
      headers: { xh: `${index}-`.padEnd(headerSize, 'v') },
    })),
  )
  const seed = metadata.toSorted((left, right) => left.offset - right.offset)[0]
  ok(seed)

  const consumer = createClient(broker.brokers).createConsumer({
    groupId: `byte-budget-compact-headers-${crypto.randomUUID()}`,
    enableAutoCommit: false,
  })

  try {
    await consumer.subscribe([
      { topic, partitionOffset: [{ partition: seed.partition, offset: { offset: seed.offset } }] },
    ])
    const reader = consumer.recvBatchStreamCompact(2, 2000).getReader()
    const payloads: string[] = []
    const headerValues: string[] = []
    try {
      for (let index = 0; index < count / 2; index += 1) {
        const { done, value } = await reader.read()
        ok(!done && value, `expected compact batch ${index + 1}`)
        equal(value.sharedHeaderKey, 'xh')
        ok(value.sharedHeaderValues, 'expected per-message shared header values')
        equal(value.sharedHeaderValues?.length, value.payloads.length)
        for (const [offset, payload] of value.payloads.entries()) {
          payloads.push(payload.toString())
          headerValues.push(value.sharedHeaderValues?.[offset]?.toString() ?? '')
        }
        await sleep(10)
      }
    } finally {
      await reader.cancel().catch(() => undefined)
    }

    equal(payloads.length, count)
    for (const [index, payload] of payloads.entries()) {
      equal(payload, `c-${index}`)
      equal(headerValues[index]?.length, headerSize)
      ok(headerValues[index]?.startsWith(`${index}-`))
    }
  } finally {
    await disconnect(consumer)
    broker.close()
  }
})

test('M07: subscribe propagates a fatal createTopic failure', async () => {
  const consumer = createClient().createConsumer({
    groupId: 'create-topic-failure',
    enableAutoCommit: false,
    fetchMetadataTimeout: 50,
  })

  try {
    await rejects(
      consumer.subscribe([{ topic: 'unreachable-create-topic', createTopic: true }]),
      /Failed to create topic\(s\) before subscription: unreachable-create-topic/,
    )
  } finally {
    await disconnect(consumer)
  }
})

test('M10: negative metadata timeout is rejected during consumer creation', async () => {
  let consumer: KafkaConsumer | undefined

  try {
    await rejects(async () => {
      consumer = createClient().createConsumer({
        groupId: 'negative-metadata-timeout',
        fetchMetadataTimeout: -1,
      })
    }, /fetchMetadataTimeout|timeout/i)
  } finally {
    await disconnect(consumer)
  }
})

test('M10: zero metadata timeout falls back to the default instead of failing fast', async () => {
  const consumer = createClient().createConsumer({
    groupId: 'zero-metadata-timeout',
    fetchMetadataTimeout: 0,
  })

  try {
    ok(consumer)
  } finally {
    await disconnect(consumer)
  }
})

test('M06: producer flush does not block timers on the Tokio runtime', async () => {
  const child = spawn(process.execPath, ['--import', 'tsx', 'js-tests/fixtures/runtime-blocking-probe.mjs'], {
    cwd: process.cwd(),
    env: { ...process.env, TOKIO_WORKER_THREADS: '1' },
    stdio: ['ignore', 'pipe', 'pipe'],
  })

  try {
    const elapsedMs = await new Promise<number>((resolve, reject) => {
      const deadline = setTimeout(() => reject(new Error('Runtime blocking probe timed out')), FIXTURE_TIMEOUT_MS)
      const inspectLine = (line: string) => {
        if (!line.startsWith('PROBE_RESULT ')) {
          return
        }

        clearTimeout(deadline)
        const result = JSON.parse(line.slice('PROBE_RESULT '.length)) as { elapsedMs: number }
        resolve(result.elapsedMs)
      }

      createInterface({ input: child.stdout }).on('line', inspectLine)
      createInterface({ input: child.stderr }).on('line', inspectLine)
      child.once('exit', (code) => {
        if (code !== 0) {
          clearTimeout(deadline)
          reject(new Error(`Runtime blocking probe exited with code ${String(code)}`))
        }
      })
    })

    ok(elapsedMs < 200, `A 50ms receive took ${Math.round(elapsedMs)}ms while producer flush was running`)
  } finally {
    child.kill()
  }
})

test('M14: metadata fetch does not block unrelated receives with one Tokio worker', async () => {
  const child = spawn(process.execPath, ['--import', 'tsx', 'js-tests/fixtures/metadata-offload-probe.mjs'], {
    cwd: process.cwd(),
    env: { ...process.env, TOKIO_WORKER_THREADS: '1' },
    stdio: ['ignore', 'pipe', 'pipe'],
  })

  try {
    const result = await new Promise<{
      receiveElapsedMs: number
      metadataElapsedMs: number
      metadataPendingAfterReceive: boolean
    }>((resolve, reject) => {
      const deadline = setTimeout(() => reject(new Error('Metadata offload probe timed out')), FIXTURE_TIMEOUT_MS)
      const inspectLine = (line: string) => {
        if (!line.startsWith('METADATA_OFFLOAD_RESULT ')) {
          return
        }

        clearTimeout(deadline)
        resolve(JSON.parse(line.slice('METADATA_OFFLOAD_RESULT '.length)))
      }

      createInterface({ input: child.stdout }).on('line', inspectLine)
      createInterface({ input: child.stderr }).on('line', inspectLine)
      child.once('exit', (code) => {
        if (code !== 0) {
          clearTimeout(deadline)
          reject(new Error(`Metadata offload probe exited with code ${String(code)}`))
        }
      })
    })

    equal(result.metadataPendingAfterReceive, true, 'The metadata fetch must still be pending after the receive')
    ok(result.metadataElapsedMs >= 400, `Metadata fetch settled too early: ${Math.round(result.metadataElapsedMs)}ms`)
    ok(result.receiveElapsedMs < 200, `A 50ms receive took ${Math.round(result.receiveElapsedMs)}ms`)
  } finally {
    child.kill()
  }
})

test('M06: late delivery callbacks are discarded or recoverable after send timeout', async () => {
  const client = createClient(UNAVAILABLE_BROKER)
  const producer = client.createProducer({
    queueTimeout: 50,
    configuration: {
      'message.timeout.ms': 200,
    },
  })

  await rejects(
    async () =>
      producer.send({
        topic: 'late-delivery-topic',
        messages: [{ payload: Buffer.from('test-late') }],
      }),
    /Flush completed with error|timed out|Failed to send/i,
  )

  await sleep(600)

  // Wait until in-flight count drops to 0 after message timeout
  const deadline = Date.now() + 3000
  while (producer.inFlightCount() > 0 && Date.now() < deadline) {
    await sleep(50)
  }
  equal(producer.inFlightCount(), 0)

  const flushed = await producer.flush()
  equal(flushed.length, 0)
  equal(producer.getLastDeliveryResults().length, 0)
})

test('M06: manual flush does not discard confirmations of concurrent sends', async () => {
  const broker = await MockKafkaBroker.start()
  const topic = `concurrent-flush-${crypto.randomUUID()}`
  const client = createClient(broker.brokers)
  const producer = client.createProducer({
    autoFlush: false,
  })

  try {
    const p1 = producer.send({
      topic,
      messages: [{ payload: Buffer.from('msg-1') }, { payload: Buffer.from('msg-2') }],
    })
    const p2 = producer.send({
      topic,
      messages: [{ payload: Buffer.from('msg-3') }, { payload: Buffer.from('msg-4') }],
    })
    await Promise.all([p1, p2])

    const flushPromise = producer.flush()
    const p3 = producer.send({
      topic,
      messages: [{ payload: Buffer.from('msg-5') }],
    })

    const [flushed1] = await Promise.all([flushPromise, p3])
    const flushed2 = await producer.flush()

    const totalConfirmed = flushed1.length + flushed2.length
    equal(totalConfirmed, 5)
    equal(producer.inFlightCount(), 0)
  } finally {
    broker.close()
  }
})

test('M06: partial enqueue failures expose accepted and rejected messages without losing delivery results', async () => {
  const client = createClient(UNAVAILABLE_BROKER)
  const producer = client.createProducer({
    autoFlush: false,
    configuration: {
      'queue.buffering.max.messages': 1,
    },
  })

  let caughtError: SendFailureError | undefined
  try {
    await producer.send({
      topic: 'partial-failure-topic',
      messages: [{ payload: Buffer.from('msg-1') }, { payload: Buffer.from('msg-2') }],
    })
  } catch (err) {
    caughtError = err as SendFailureError
  }

  ok(caughtError)
  ok(caughtError.message.startsWith('Failed to send all messages (enqueued 1 of 2, confirmed 0): '))
  equal(caughtError.enqueuedCount, 1)
  equal(caughtError.totalCount, 2)
  equal(caughtError.confirmedCount, 0)
  ok(Array.isArray(caughtError.confirmedMessages))
  equal(caughtError.confirmedMessages.length, 0)
  equal(producer.getLastDeliveryResults().length, 0)
})

test('P1: producer rejects isTombstone combined with a payload', async () => {
  const producer = createClient(UNAVAILABLE_BROKER).createProducer({ queueTimeout: 50 })

  await rejects(
    async () =>
      producer.send({
        topic: 'tombstone-reject',
        messages: [{ payload: Buffer.from('x'), isTombstone: true }],
      }),
    /isTombstone/,
  )
})

test('M06: concurrent native sends isolate delivery results without the wrapper chain', async () => {
  const broker = await MockKafkaBroker.start()
  const topic = `native-concurrent-${crypto.randomUUID()}`
  const native = new KafkaClientConfig({
    clientId: `native-concurrent-${crypto.randomUUID()}`,
    brokers: broker.brokers,
  }).createProducer({})

  try {
    const [first, second] = await Promise.all([
      native.send({ topic, messages: [{ payload: Buffer.from('n-1') }, { payload: Buffer.from('n-2') }] }),
      native.send({ topic, messages: [{ payload: Buffer.from('n-3') }] }),
    ])
    equal(first.length, 2)
    equal(second.length, 1)
    equal(native.inFlightCount(), 0)
  } finally {
    broker.close()
  }
})

test('M06: partial enqueue failure recovers confirmed metadata', async () => {
  const broker = await MockKafkaBroker.start()
  const topic = `partial-recovery-${crypto.randomUUID()}`
  const producer = createClient(broker.brokers).createProducer({
    configuration: { 'queue.buffering.max.messages': 1 },
  })

  let caughtError: SendFailureError | undefined
  try {
    await producer.send({
      topic,
      messages: [{ payload: Buffer.from('r-1') }, { payload: Buffer.from('r-2') }],
    })
  } catch (error) {
    caughtError = error as SendFailureError
  } finally {
    broker.close()
  }

  ok(caughtError)
  equal(caughtError.enqueuedCount, 1)
  equal(caughtError.totalCount, 2)
  equal(caughtError.confirmedCount, 1)
  equal(caughtError.confirmedMessages?.length, 1)
  equal(caughtError.confirmedMessages?.[0]?.topic, topic)
  equal(caughtError.message.includes(SEND_FAILURE_PAYLOAD_MARKER), false)
})

test('M06: send-failure metadata is taken from the native payload, not a shared slot', () => {
  const first = new Error(
    `Failed to send all messages (enqueued 1 of 2, confirmed 1): queue full${SEND_FAILURE_PAYLOAD_MARKER}${JSON.stringify(
      {
        enqueuedCount: 1,
        totalCount: 2,
        confirmedCount: 1,
        confirmedMessages: [{ topic: 'topic-a', partition: 0, offset: 1 }],
      },
    )}`,
  ) as SendFailureError
  const second = new Error(
    `Failed to send all messages (enqueued 1 of 2, confirmed 1): queue full${SEND_FAILURE_PAYLOAD_MARKER}${JSON.stringify(
      {
        enqueuedCount: 1,
        totalCount: 2,
        confirmedCount: 1,
        confirmedMessages: [{ topic: 'topic-b', partition: 1, offset: 9 }],
      },
    )}`,
  ) as SendFailureError

  attachSendFailureDetails(first)
  attachSendFailureDetails(second)

  equal(first.confirmedMessages?.[0]?.topic, 'topic-a')
  equal(second.confirmedMessages?.[0]?.topic, 'topic-b')
  equal(first.enqueuedCount, 1)
  equal(first.message.includes(SEND_FAILURE_PAYLOAD_MARKER), false)
  equal(first.message.startsWith('Failed to send all messages (enqueued 1 of 2, confirmed 1): '), true)
})

test('M06: public send failure does not read getLastDeliveryResults', async () => {
  const broker = await MockKafkaBroker.start()
  const topic = `no-shared-slot-${crypto.randomUUID()}`
  const producer = createClient(broker.brokers).createProducer({
    configuration: { 'queue.buffering.max.messages': 1 },
  })
  const originalGet = producer.getLastDeliveryResults.bind(producer)
  let slotReads = 0
  producer.getLastDeliveryResults = () => {
    slotReads += 1
    return originalGet()
  }

  try {
    await rejects(
      producer.send({
        topic,
        messages: [{ payload: Buffer.from('r-1') }, { payload: Buffer.from('r-2') }],
      }),
    )
    equal(slotReads, 0)
  } finally {
    broker.close()
  }
})

test('M06: concurrent partial failures keep per-send confirmed metadata', async () => {
  const broker = await MockKafkaBroker.start()
  const producer = createClient(broker.brokers).createProducer({
    configuration: { 'queue.buffering.max.messages': 1 },
  })
  const topicA = `partial-a-${crypto.randomUUID()}`
  const topicB = `partial-b-${crypto.randomUUID()}`
  const originalGet = producer.getLastDeliveryResults.bind(producer)
  let slotReads = 0
  producer.getLastDeliveryResults = () => {
    slotReads += 1
    return originalGet()
  }

  const sendAndBind = async (topic: string, payloads: string[]) => {
    try {
      await producer.send({
        topic,
        messages: payloads.map((payload) => ({ payload: Buffer.from(payload) })),
      })
      throw new Error(`${topic} should fail with a partial enqueue`)
    } catch (error) {
      if (error instanceof Error && error.message.endsWith('should fail with a partial enqueue')) {
        throw error
      }
      const err = error as SendFailureError
      equal(err.totalCount, 2)
      for (const metadata of err.confirmedMessages ?? []) {
        equal(metadata.topic, topic)
      }
      return err
    }
  }

  try {
    const [errorA, errorB] = await Promise.all([
      sendAndBind(topicA, ['a-1', 'a-2']),
      sendAndBind(topicB, ['b-1', 'b-2']),
    ])

    ok(errorA.enqueuedCount !== undefined)
    ok(errorB.enqueuedCount !== undefined)
    equal(slotReads, 0)
  } finally {
    broker.close()
  }
})

test('M09: a valid message followed by a consumer error preserves the message and surfaces the error', async () => {
  const broker = await MockKafkaBroker.start()
  const topic = `m09-regression-${crypto.randomUUID()}`
  const [seed] = await broker.send(topic, [{ payload: 'first-valid-message' }])
  ok(seed)

  const consumer = createClient(broker.brokers).createConsumer({
    groupId: `m09-${crypto.randomUUID()}`,
    enableAutoCommit: false,
    configuration: {
      'enable.partition.eof': 'true',
    },
  })

  try {
    await consumer.subscribe([
      { topic, partitionOffset: [{ partition: seed.partition, offset: { offset: seed.offset } }] },
    ])

    // Requesting batch size 2: first message succeeds, second message encounters EOF error in Phase B
    const messages = await consumer.recvBatch(2, 1000)
    equal(messages.length, 1)
    equal(messages[0]?.payload.toString(), 'first-valid-message')

    // Next call should immediately surface the pending error saved from Phase B, even with timeout 0
    const start = Date.now()
    await rejects(consumer.recvBatch(2, 0), /PartitionEOF|_PARTITION_EOF|Reached end of|Failed to receive message/i)
    ok(Date.now() - start < 50, 'Pending EOF error was surfaced immediately without polling')
  } finally {
    await disconnect(consumer)
    broker.close()
  }
})

test('M13: Async commit without onEvents is rejected', async () => {
  const consumer = createClient().createConsumer({
    groupId: 'async-commit-no-listener',
    enableAutoCommit: false,
  })

  try {
    await rejects(consumer.commit('topic', 0, 1, 'Async'), /onEvents/)
  } finally {
    await disconnect(consumer)
  }
})

test('M13: Async commit is allowed after onEvents is registered', async () => {
  const consumer = createClient().createConsumer({
    groupId: 'async-commit-with-listener',
    enableAutoCommit: false,
  })

  try {
    consumer.onEvents(() => undefined)
    try {
      await consumer.commit('topic', 0, 1, 'Async')
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error)
      equal(message.includes('onEvents'), false)
    }
  } finally {
    await disconnect(consumer)
  }
})
test('M13: Async commit is rejected after disconnect stops the event listener', async () => {
  const consumer = createClient().createConsumer({
    groupId: 'async-commit-after-disconnect',
    enableAutoCommit: false,
  })

  try {
    consumer.onEvents(() => undefined)
    await consumer.disconnect()
    await rejects(consumer.commit('topic', 0, 1, 'Async'), /onEvents/)
  } finally {
    await disconnect(consumer)
  }
})

test('M13: Async commitMessage is rejected after disconnect stops the event listener', async () => {
  const consumer = createClient().createConsumer({
    groupId: 'async-commit-message-after-disconnect',
    enableAutoCommit: false,
  })

  try {
    consumer.onEvents(() => undefined)
    await consumer.disconnect()
    await rejects(
      consumer.commitMessage(
        { payload: Buffer.from('commit-message-seed'), topic: 'topic', partition: 0, offset: 0 },
        'Async',
      ),
      /onEvents/,
    )
  } finally {
    await disconnect(consumer)
  }
})

test('M13: Async commit stays rejected after disconnect with multiple listeners', async () => {
  const consumer = createClient().createConsumer({
    groupId: 'async-commit-multiple-listeners',
    enableAutoCommit: false,
  })

  try {
    consumer.onEvents(() => undefined)
    consumer.onEvents(() => undefined)
    await consumer.disconnect()
    await rejects(consumer.commit('topic', 0, 1, 'Async'), /onEvents/)
  } finally {
    await disconnect(consumer)
  }
})

test('M09: compact stream preserves message before surfacing EOF error', async () => {
  const broker = await MockKafkaBroker.start()
  const topic = `m09-compact-${crypto.randomUUID()}`
  const [seed] = await broker.send(topic, [{ payload: 'compact-valid-message' }])
  ok(seed)

  const consumer = createClient(broker.brokers).createConsumer({
    groupId: `m09-compact-${crypto.randomUUID()}`,
    enableAutoCommit: false,
    configuration: {
      'enable.partition.eof': 'true',
    },
  })

  try {
    await consumer.subscribe([
      { topic, partitionOffset: [{ partition: seed.partition, offset: { offset: seed.offset } }] },
    ])

    const reader = consumer.recvBatchStreamCompact(2, 1000).getReader()
    const firstRead = await reader.read()
    equal(firstRead.done, false)
    equal(firstRead.value?.payloads.length, 1)

    await rejects(reader.read(), /PartitionEOF|_PARTITION_EOF|Reached end of|Failed to receive message/i)
  } finally {
    await disconnect(consumer)
    broker.close()
  }
})
