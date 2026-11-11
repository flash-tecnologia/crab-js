import { execFile, spawn } from 'node:child_process'
import { deepEqual, equal, ok, rejects, throws } from 'node:assert/strict'
import { createInterface } from 'node:readline'
import { promisify } from 'node:util'

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
// Tests that start the mock broker and round-trip commits exceed the default 5 s timeout.
// Slower CI runners (x86_64 macOS) need the longer budget.
const BROKER_TEST = { timeout: 30_000 }

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

    // The call itself reserves the receive until its promise settles.
    // Neither a second call nor a cancel in the same tick can miss it.
    const immediate = consumer.recvManaged(0)
    await rejects(consumer.recvManaged(0), hasCode(MANAGED_ERROR_CODES.RECEIVE_PENDING))
    deepEqual(await immediate, { kind: 'idle', reason: 'timeout' })
    const cancelled = consumer.recvManaged()
    equal(consumer.cancelManagedReceive(), true)
    deepEqual(await cancelled, { kind: 'idle', reason: 'cancelled' })
    equal(consumer.cancelManagedReceive(), false)
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
    await rejects(consumer.commitManaged('', 0, 'mc1.1.1', 1), (error) => {
      hasCode(MANAGED_ERROR_CODES.INVALID_ARGUMENT)(error)
      equal((error as { outcomeUnknown?: boolean }).outcomeUnknown, false)
      return true
    })
    equal(consumer.isManagedAssignmentActive('topic', 0, 'mc1.1.1'), false)
    equal(consumer.managedState().pendingCommits, 0)
  } finally {
    await Promise.all([consumer.disconnect(), other.disconnect()])
  }
})

test('disconnect resolves the pending recvManaged with null and refuses new managed operations', async () => {
  const consumer = managedConsumer()
  // The receive is reserved by the call, so an immediate disconnect ends it.
  const pending = consumer.recvManaged()
  const closed = consumer.disconnect()
  equal(await pending, null)
  await closed
  await rejects(consumer.recvManaged(), hasCode(MANAGED_ERROR_CODES.CLOSED))
  // Every commitManaged rejection carries outcomeUnknown; nothing was sent here.
  await rejects(consumer.commitManaged('topic', 0, 'mc1.1.1', 1), (error) => {
    hasCode(MANAGED_ERROR_CODES.CLOSED)(error)
    equal((error as { outcomeUnknown?: boolean }).outcomeUnknown, false)
    return true
  })
  await rejects(consumer.recvManaged(), (error) => !('outcomeUnknown' in (error as object)))
  const state = consumer.managedState()
  equal(state.status, 'closed')
  deepEqual(state.assignments, [])
  equal(state.bufferedMessages, 0)
  equal(consumer.cancelManagedReceive(), false)
})

test('managed records are a discriminated sequence that preserves message contents', BROKER_TEST, async () => {
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
    // An identity is bound to its own partition. Nothing is sent: a definitive outcome.
    await rejects(consumer.commitManaged(topic, first.partition + 100, assignment, 1), (error) => {
      hasCode(MANAGED_ERROR_CODES.STALE_ASSIGNMENT)(error)
      equal((error as { outcomeUnknown?: boolean }).outcomeUnknown, false)
      return true
    })
  } finally {
    await consumer.disconnect()
    broker.close()
  }
})

/** Subscribes a managed consumer to a one-message topic and returns its owned partition. */
async function ownedPartition(prefix: string) {
  const broker = await startMockBroker()
  const topic = `${prefix}-${crypto.randomUUID()}`
  await broker.send(topic, [{ key: 'k', payload: 'one' }])
  const consumer = managedConsumer(createClient(broker.brokers))
  await consumer.subscribe(topic)
  await nextRecord(consumer, 'assign')
  const record = await nextRecord(consumer, 'message')
  ok(record.kind === 'message')
  const close = async () => {
    await consumer.disconnect()
    broker.close()
  }
  return { broker, close, consumer, topic, partition: record.message.partition, assignment: record.assignment }
}

test('managed commits issued without awaiting are dispatched in call order', BROKER_TEST, async () => {
  const { close, consumer, topic, partition, assignment } = await ownedPartition('managed-order')
  try {
    // An offset below one already authorized is refused, not sent after it.
    const ordered = [10, 20, 20].map(async (offset) => consumer.commitManaged(topic, partition, assignment, offset))
    // How many are still unresolved here depends on the broker, not on this test.
    ok(consumer.managedState().pendingCommits <= 3)
    await rejects(consumer.commitManaged(topic, partition, assignment, 15), (error) => {
      hasCode(MANAGED_ERROR_CODES.COMMIT_REGRESSION)(error)
      equal((error as { outcomeUnknown?: boolean }).outcomeUnknown, false)
      return true
    })
    await Promise.all(ordered)
    equal(consumer.managedState().pendingCommits, 0)
    await rejects(
      consumer.commitManaged(topic, partition, assignment, 1, -1),
      hasCode(MANAGED_ERROR_CODES.INVALID_ARGUMENT),
    )
  } finally {
    await close()
  }
})

test('a managed commit deadline ends only the observation', BROKER_TEST, async () => {
  const { close, consumer, topic, partition, assignment } = await ownedPartition('managed-deadline')
  try {
    // The request waits behind a chain of commits, longer than a zero deadline.
    const ahead = Array.from({ length: 20 }, async (_, index) =>
      consumer.commitManaged(topic, partition, assignment, 10 + index),
    )
    // Whether the deadline wins is up to the broker: both outcomes are valid.
    const deadline: unknown = await consumer.commitManaged(topic, partition, assignment, 40, 0).then(
      () => undefined,
      (error: unknown) => error,
    )
    if (deadline !== undefined) {
      hasCode(MANAGED_ERROR_CODES.COMMIT_TIMEOUT)(deadline)
      equal((deadline as { outcomeUnknown?: boolean }).outcomeUnknown, true)
    }
    await Promise.all(ahead)
    // The timed-out request was not dropped: it is sent and settles natively.
    const settleDeadline = Date.now() + 10_000
    while (consumer.managedState().pendingCommits > 0 && Date.now() < settleDeadline) {
      await new Promise((resolve) => setTimeout(resolve, 10))
    }
    equal(consumer.managedState().pendingCommits, 0)
  } finally {
    await close()
  }
})

test('managed tracing opens a processing span for message records only', BROKER_TEST, async () => {
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
  const timings: [string, number][] = []
  const onStartTiming = () => timings.push(['start', performance.now()])
  const onEndTiming = () => timings.push(['end', performance.now()])
  consumerReceiveStartChannel.subscribe(onStartTiming)
  consumerReceiveEndChannel.subscribe(onEndTiming)
  try {
    // The start event precedes the wait, so a subscriber timing from it (the
    // OTEL duration metric) measures the whole wait, as with `recv()`. Before
    // Subscribing, nothing can arrive, so this receive waits its full timeout.
    deepEqual(await consumer.recvManaged(50), { kind: 'idle', reason: 'timeout' })
    const [start, end] = timings
    ok(start?.[0] === 'start' && end?.[0] === 'end')
    ok(end[1] - start[1] >= 40, `receive timing covered ${end[1] - start[1]} ms of a 50 ms wait`)
    deepEqual(events, ['receive:start', 'receive:end:false'])

    await consumer.subscribe(topic)
    await nextRecord(consumer, 'assign')
    // Control and idle records end their receive pair without a message and open no processing span.
    ok(events.every((event) => event === 'receive:start' || event === 'receive:end:false'))
    events.length = 0

    const record = await nextRecord(consumer, 'message')
    ok(record.kind === 'message')
    // Idle receives may precede the message; it is the last pair.
    deepEqual(events.slice(-3), ['receive:start', 'receive:end:true', 'process:start'])
    events.length = 0
    const { endSpan } = record.message as typeof record.message & { endSpan: (error?: Error) => void }
    endSpan()
    endSpan(new Error('ignored second end'))
    deepEqual(events, ['process:end:ok'])

    // A failed receive is observable and opens no processing span.
    events.length = 0
    const pending = consumer.recvManaged()
    await rejects(consumer.recvManaged(), hasCode(MANAGED_ERROR_CODES.RECEIVE_PENDING))
    deepEqual(events, ['receive:start', 'receive:start', 'receive:end:false'])
    consumer.cancelManagedReceive()
    await pending
    deepEqual(events, ['receive:start', 'receive:start', 'receive:end:false', 'receive:end:false'])
  } finally {
    consumerReceiveStartChannel.unsubscribe(onStartTiming)
    consumerReceiveEndChannel.unsubscribe(onEndTiming)
    for (const [channel, handler] of subscriptions) {
      channel.unsubscribe(handler as never)
    }
    await consumer.disconnect()
    broker.close()
  }
})

test('pending commits are bounded by maxPendingCommits', BROKER_TEST, async () => {
  const broker = await startMockBroker()
  const topic = `managed-backlog-${crypto.randomUUID()}`
  await broker.send(topic, [{ key: 'k', payload: 'one' }])
  const consumer = managedConsumer(createClient(broker.brokers), { managedOptions: { maxPendingCommits: 2 } })
  try {
    await consumer.subscribe(topic)
    await nextRecord(consumer, 'assign')
    const { message, assignment } = (await nextRecord(consumer, 'message')) as Extract<
      ManagedRecord,
      { kind: 'message' }
    >
    const commits = [1, 2].map(async (delta) =>
      consumer.commitManaged(topic, message.partition, assignment, message.offset + delta),
    )
    // The third is refused unless the broker already settled one. Rust covers the bound itself.
    const third: unknown = await consumer.commitManaged(topic, message.partition, assignment, message.offset + 3).then(
      () => undefined,
      (error: unknown) => error,
    )
    if (third !== undefined) {
      hasCode(MANAGED_ERROR_CODES.COMMIT_BACKLOG)(third)
      equal((third as { outcomeUnknown?: boolean }).outcomeUnknown, false)
    }
    await Promise.all(commits)
    await consumer.commitManaged(topic, message.partition, assignment, message.offset + 3)
  } finally {
    await consumer.disconnect()
    broker.close()
  }
})

test('a legacy consumer reports the same configuration as before managed mode existed', async () => {
  const client = createClient()
  const legacy = client.createConsumer({ groupId: 'legacy-config', enableAutoCommit: false })
  const minimal = client.createConsumer({ groupId: 'legacy-config-minimal' })
  try {
    // No managed field appears unless it was set.
    deepEqual(legacy.getConfig(), { groupId: 'legacy-config', enableAutoCommit: false })
    deepEqual(minimal.getConfig(), { groupId: 'legacy-config-minimal' })
    deepEqual(Object.keys(minimal.getConfig()), ['groupId'])
  } finally {
    await Promise.all([legacy.disconnect(), minimal.disconnect()])
  }
})

test('a managed consumer collected without disconnect settles its pending receive', async () => {
  const { stdout } = await promisify(execFile)(
    process.execPath,
    ['--expose-gc', '--import', 'tsx', 'js-tests/fixtures/managed-finalizer-probe.mjs'],
    { cwd: process.cwd(), timeout: 20_000 },
  )
  ok(stdout.includes('MANAGED_FINALIZER null'), stdout)
})
