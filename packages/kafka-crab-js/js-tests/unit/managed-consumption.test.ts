import { spawn } from 'node:child_process'
import { deepEqual, equal, ok, rejects, throws } from 'node:assert/strict'
import { createInterface } from 'node:readline'
import { setTimeout as sleep } from 'node:timers/promises'

import { test } from 'vite-plus/test'

import * as api from '../../js-src/index.js'
import {
  consumerProcessEndChannel,
  consumerProcessStartChannel,
  consumerReceiveEndChannel,
  consumerReceiveStartChannel,
  isManagedConsumptionError,
  KafkaClient,
  type KafkaConsumer,
  MANAGED_CONSUMPTION_VERSION,
  MANAGED_ERROR_CODES,
  type ManagedRecord,
} from '../../js-src/index.js'

const UNAVAILABLE_BROKER = '127.0.0.1:1'
const FIXTURE_TIMEOUT_MS = 10_000

const createClient = (brokers = UNAVAILABLE_BROKER, diagnostics = false) =>
  new KafkaClient({ brokers, clientId: `managed-unit-${crypto.randomUUID()}`, diagnostics, logLevel: 'error' })

const managedConsumer = (client = createClient(), overrides: Record<string, unknown> = {}) =>
  client.createConsumer({
    groupId: `managed-unit-${crypto.randomUUID()}`,
    managed: true,
    configuration: { 'auto.offset.reset': 'earliest' },
    ...overrides,
  })

const legacyConsumer = (client = createClient()) =>
  client.createConsumer({ groupId: `legacy-unit-${crypto.randomUUID()}`, enableAutoCommit: false })

const hasCode = (code: string) => (error: unknown) => {
  ok(isManagedConsumptionError(error), `expected a managed error, got ${String(error)}`)
  equal(error.code, code)
  ok(error.message.includes(code))
  return true
}

const modeError = /\[ERR_MANAGED_MODE\]/

/** Mock cluster shared with the regression suite's fixture; only what these tests need. */
async function startMockBroker() {
  const child = spawn(process.execPath, ['--import', 'tsx', 'js-tests/fixtures/mock-kafka-broker.mjs'], {
    cwd: process.cwd(),
    stdio: ['pipe', 'pipe', 'pipe'],
  })
  const responses = new Map<number, (response: { error?: string }) => void>()
  const brokers = await new Promise<string>((resolve, reject) => {
    const deadline = setTimeout(() => reject(new Error('Timed out while starting the mock broker')), FIXTURE_TIMEOUT_MS)
    const inspect = (line: string) => {
      const bootstrap = /replaced with (?<brokers>127\.0\.0\.1:\d+)/.exec(line)?.groups?.brokers
      if (bootstrap) {
        clearTimeout(deadline)
        resolve(bootstrap)
      }
      if (line.startsWith('MOCK_RESPONSE ')) {
        const response = JSON.parse(line.slice('MOCK_RESPONSE '.length)) as { id: number; error?: string }
        responses.get(response.id)?.(response)
      }
    }
    createInterface({ input: child.stdout }).on('line', inspect)
    createInterface({ input: child.stderr }).on('line', inspect)
  })
  let requestId = 0
  return {
    brokers,
    send: async (topic: string, messages: Record<string, unknown>[]) => {
      requestId += 1
      const id = requestId
      const response = new Promise<{ error?: string }>((resolve) => responses.set(id, resolve))
      child.stdin.write(`${JSON.stringify({ id, topic, messages })}\n`)
      const { error } = await response
      equal(error, undefined)
    },
    close: () => {
      child.stdin.end()
      child.kill()
    },
  }
}

async function nextRecord(consumer: KafkaConsumer, kind: ManagedRecord['kind'], timeoutMs = 8000) {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    const record = await consumer.recvManaged(250)
    if (record?.kind === kind) {
      return record
    }
    ok(record?.kind === 'idle', `unexpected ${JSON.stringify(record)} while waiting for ${kind}`)
  }
  throw new Error(`Timed out waiting for a ${kind} record`)
}

test('managed consumption exports a versioned capability and structured error codes', async () => {
  equal(MANAGED_CONSUMPTION_VERSION, 1)
  equal(api.MANAGED_CONSUMPTION_VERSION, 1)
  equal(typeof api.instrumentManagedReceive, 'function')
  equal(MANAGED_ERROR_CODES.STALE_ASSIGNMENT, 'ERR_MANAGED_STALE_ASSIGNMENT')
  equal(new Set(Object.values(MANAGED_ERROR_CODES)).size, Object.keys(MANAGED_ERROR_CODES).length)
  ok(!isManagedConsumptionError(new Error('ERR_MANAGED_CLOSED')))

  const legacy = legacyConsumer()
  const managed = managedConsumer()
  try {
    equal(legacy.managed, false)
    equal(managed.managed, true)
    equal(managed.getConfig().managed, true)
    for (const method of ['recvManaged', 'commitManaged', 'cancelManagedReceive', 'managedState'] as const) {
      equal(typeof managed[method], 'function')
    }
  } finally {
    await Promise.all([legacy.disconnect(), managed.disconnect()])
  }
})

test('managed activation is fixed at creation and rejects automatic offset commit or storage', async () => {
  const client = createClient()
  throws(() => managedConsumer(client, { enableAutoCommit: true }), /ERR_MANAGED_MODE.*enable\.auto\.commit=false/)
  throws(
    () => managedConsumer(client, { configuration: { 'enable.auto.commit': 'true' } }),
    /ERR_MANAGED_MODE.*enable\.auto\.commit=false/,
  )
  throws(
    () => managedConsumer(client, { configuration: { 'enable.auto.offset.store': true } }),
    /ERR_MANAGED_MODE.*enable\.auto\.offset\.store=false/,
  )
  throws(
    () => client.createConsumer({ groupId: 'legacy', managedOptions: { maxControlRecords: 8 } }),
    /managedOptions requires managed: true/,
  )
  for (const managedOptions of [{ maxControlRecords: 0 }, { maxBufferedMessages: 0 }, { maxControlRecords: 1e6 }]) {
    throws(() => managedConsumer(client, { managedOptions }), /Invalid managedOptions\./)
  }

  // Explicitly disabled settings and custom limits are accepted.
  const managed = managedConsumer(client, {
    enableAutoCommit: false,
    configuration: { 'enable.auto.offset.store': 'false' },
    managedOptions: { maxControlRecords: 8, maxBufferedMessages: 4, maxBufferedBytes: 1024 },
  })
  // There is no late activation: the mode of an existing consumer cannot change.
  const legacy = legacyConsumer(client)
  try {
    const config = legacy.getConfig()
    config.managed = true
    equal(legacy.managed, false)
    await rejects(legacy.recvManaged(), hasCode(MANAGED_ERROR_CODES.MODE))
    equal(managed.managedState().status, 'active')
  } finally {
    await Promise.all([legacy.disconnect(), managed.disconnect()])
  }
})

test('a managed consumer rejects legacy receives, unfenced commits and repositioning', async () => {
  const client = createClient()
  const consumer = managedConsumer(client)
  try {
    await rejects(consumer.recv(), modeError)
    await rejects(consumer.recvBatch(1, 10), modeError)
    throws(() => consumer.recvStream(), modeError)
    throws(() => consumer.recvBatchStream(1, 10), modeError)
    throws(() => consumer.recvBatchStreamCompact(1, 10), modeError)
    await rejects(consumer.commit('topic', 0, 1, 'Sync'), modeError)
    await rejects(consumer.commit('topic', 0, 1, 'Async'), modeError)
    await rejects(
      consumer.commitMessage({ topic: 'topic', partition: 0, offset: 1, payload: Buffer.alloc(0) }, 'Sync'),
      modeError,
    )
    throws(() => consumer.seek('topic', 0, { offset: 0 }), modeError)
    throws(() => consumer.unsubscribe(), modeError)
    await rejects(consumer.subscribe([{ topic: 'topic', allOffsets: { position: 'Beginning' } }]), modeError)
    throws(() => client.createStreamConsumer({ groupId: 'streams', managed: true }), modeError)
    throws(() => client.createWebStreamConsumer({ groupId: 'streams', managed: true }), modeError)
    // The observational event listener stays available.
    consumer.onEvents(() => undefined)
  } finally {
    await consumer.disconnect()
  }
})

test('a legacy consumer rejects every managed operation', async () => {
  const consumer = legacyConsumer()
  try {
    await rejects(consumer.recvManaged(), hasCode(MANAGED_ERROR_CODES.MODE))
    await rejects(consumer.commitManaged('topic', 0, 'mc1.1.1', 1), hasCode(MANAGED_ERROR_CODES.MODE))
    throws(() => consumer.cancelManagedReceive(), hasCode(MANAGED_ERROR_CODES.MODE))
    throws(() => consumer.isManagedAssignmentActive('topic', 0, 'mc1.1.1'), hasCode(MANAGED_ERROR_CODES.MODE))
    throws(() => consumer.managedState(), hasCode(MANAGED_ERROR_CODES.MODE))
  } finally {
    await consumer.disconnect()
  }
})

test('only one recvManaged may be pending; cancellation and timeouts resolve as idle', async () => {
  const consumer = managedConsumer()
  try {
    equal(consumer.cancelManagedReceive(), false)
    const pending = consumer.recvManaged()
    await rejects(consumer.recvManaged(), hasCode(MANAGED_ERROR_CODES.RECEIVE_PENDING))
    await rejects(consumer.recvManaged(10), hasCode(MANAGED_ERROR_CODES.RECEIVE_PENDING))
    equal(consumer.cancelManagedReceive(), true)
    deepEqual(await pending, { kind: 'idle', reason: 'cancelled' })

    deepEqual(await consumer.recvManaged(10), { kind: 'idle', reason: 'timeout' })
    deepEqual(await consumer.recvManaged(0), { kind: 'idle', reason: 'timeout' })
    await rejects(consumer.recvManaged(-1), hasCode(MANAGED_ERROR_CODES.INVALID_ARGUMENT))
  } finally {
    await consumer.disconnect()
  }
})

test('commitManaged validates its arguments and the identity before any dispatch', async () => {
  const consumer = managedConsumer()
  const other = managedConsumer()
  try {
    await rejects(consumer.commitManaged('topic', 0, 'not-a-token', 1), hasCode(MANAGED_ERROR_CODES.UNKNOWN_ASSIGNMENT))
    // An identity that this consumer never issued, including another consumer's.
    await rejects(consumer.commitManaged('topic', 0, 'mc1.1.1', 1), hasCode(MANAGED_ERROR_CODES.UNKNOWN_ASSIGNMENT))
    await rejects(consumer.commitManaged('topic', 0, 'mc1.1.1', -1), hasCode(MANAGED_ERROR_CODES.INVALID_ARGUMENT))
    await rejects(consumer.commitManaged('', 0, 'mc1.1.1', 1), hasCode(MANAGED_ERROR_CODES.INVALID_ARGUMENT))
    equal(consumer.isManagedAssignmentActive('topic', 0, 'mc1.1.1'), false)
    equal(consumer.managedState().pendingCommits, 0)
  } finally {
    await Promise.all([consumer.disconnect(), other.disconnect()])
  }
})

test('disconnect resolves the pending recvManaged with null and refuses new managed operations', async () => {
  const consumer = managedConsumer()
  const pending = consumer.recvManaged()
  await sleep(20)
  await consumer.disconnect()
  equal(await pending, null)
  await rejects(consumer.recvManaged(), hasCode(MANAGED_ERROR_CODES.CLOSED))
  await rejects(consumer.commitManaged('topic', 0, 'mc1.1.1', 1), hasCode(MANAGED_ERROR_CODES.CLOSED))
  const state = consumer.managedState()
  equal(state.status, 'closed')
  deepEqual(state.assignments, [])
  equal(state.bufferedMessages, 0)
  equal(consumer.cancelManagedReceive(), false)
})

test('managed records are a discriminated sequence that preserves message contents', async () => {
  const broker = await startMockBroker()
  const topic = `managed-shape-${crypto.randomUUID()}`
  // One key keeps the three messages on one partition of the mock topic.
  await broker.send(topic, [
    { key: 'k', payload: 'first', headers: { trace: 'abc', other: 'def' } },
    { key: 'k', payload: '' },
    { key: 'k', isTombstone: true },
  ])
  const consumer = managedConsumer(createClient(broker.brokers))
  try {
    await consumer.subscribe(topic)
    const assign = await nextRecord(consumer, 'assign')
    ok(assign.kind === 'assign')
    deepEqual(Object.keys(assign).toSorted(), ['kind', 'partitions'])
    ok(assign.partitions.length > 0)
    const owners = new Map<number, string>()
    for (const owned of assign.partitions) {
      equal(owned.topic, topic)
      equal(typeof owned.assignment, 'string')
      equal(owned.previousAssignment, undefined)
      owners.set(owned.partition, owned.assignment ?? '')
    }
    // Each partition has its own identity, visible in native state at once.
    equal(new Set(owners.values()).size, assign.partitions.length)
    deepEqual(
      consumer.managedState().assignments.map(({ partition, assignment }) => [partition, assignment]),
      [...owners].toSorted(([left], [right]) => left - right),
    )

    const messages = []
    while (messages.length < 3) {
      const record = await nextRecord(consumer, 'message')
      ok(record.kind === 'message')
      deepEqual(Object.keys(record).toSorted(), ['assignment', 'kind', 'message'])
      // Every message carries the identity announced by the preceding assign.
      equal(record.assignment, owners.get(record.message.partition))
      messages.push(record.message)
    }
    const [first, empty, tombstone] = messages
    ok(first && empty && tombstone)
    deepEqual(
      messages.map((message) => [message.partition, message.offset]),
      [0, 1, 2].map((index) => [first.partition, first.offset + index]),
    )
    equal(first.payload.toString(), 'first')
    equal(first.key?.toString(), 'k')
    equal(first.headers?.trace?.toString(), 'abc')
    equal(first.headers?.other?.toString(), 'def')
    equal(first.topic, topic)
    equal(first.isTombstone, undefined)
    equal(empty.payload.length, 0)
    equal(empty.isTombstone, undefined)
    equal(tombstone.isTombstone, true)
    equal(tombstone.key?.toString(), 'k')

    const assignment = owners.get(first.partition) ?? ''
    equal(consumer.isManagedAssignmentActive(topic, first.partition, assignment), true)
    // `nextOffset` is the next offset to consume, as in `commit`.
    await consumer.commitManaged(topic, first.partition, assignment, tombstone.offset + 1)
    equal(consumer.managedState().pendingCommits, 0)
    // An identity is bound to its own partition.
    await rejects(
      consumer.commitManaged(topic, first.partition + 100, assignment, 1),
      hasCode(MANAGED_ERROR_CODES.STALE_ASSIGNMENT),
    )
  } finally {
    await consumer.disconnect()
    broker.close()
  }
})

test('managed tracing opens a processing span for message records only', async () => {
  const broker = await startMockBroker()
  const topic = `managed-tracing-${crypto.randomUUID()}`
  await broker.send(topic, [{ payload: 'traced' }])
  const consumer = managedConsumer(createClient(broker.brokers, true))
  const events: string[] = []
  const subscriptions = [
    [consumerReceiveStartChannel, () => events.push('receive:start')],
    [consumerReceiveEndChannel, ({ message }: { message: unknown }) => events.push(`receive:end:${Boolean(message)}`)],
    [consumerProcessStartChannel, () => events.push('process:start')],
    [consumerProcessEndChannel, ({ error }: { error?: Error }) => events.push(`process:end:${error?.message ?? 'ok'}`)],
  ] as const
  for (const [channel, handler] of subscriptions) {
    channel.subscribe(handler as never)
  }
  try {
    await consumer.subscribe(topic)
    deepEqual(await consumer.recvManaged(10), { kind: 'idle', reason: 'timeout' })
    await nextRecord(consumer, 'assign')
    // Control and idle records publish nothing.
    deepEqual(events, [])

    const record = await nextRecord(consumer, 'message')
    ok(record.kind === 'message')
    deepEqual(events, ['receive:start', 'receive:end:true', 'process:start'])
    const { endSpan } = record.message as typeof record.message & { endSpan: (error?: Error) => void }
    endSpan()
    endSpan(new Error('ignored second end'))
    deepEqual(events, ['receive:start', 'receive:end:true', 'process:start', 'process:end:ok'])

    // A failed receive is observable and opens no processing span.
    const pending = consumer.recvManaged()
    await rejects(consumer.recvManaged(), hasCode(MANAGED_ERROR_CODES.RECEIVE_PENDING))
    deepEqual(events.slice(4), ['receive:start', 'receive:end:false'])
    consumer.cancelManagedReceive()
    await pending
    equal(events.length, 6)
  } finally {
    for (const [channel, handler] of subscriptions) {
      channel.unsubscribe(handler as never)
    }
    await consumer.disconnect()
    broker.close()
  }
})
