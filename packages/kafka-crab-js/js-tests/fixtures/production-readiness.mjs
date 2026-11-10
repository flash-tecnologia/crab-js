import { equal, ok } from 'node:assert/strict'
import { execFile } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import { setTimeout as sleep } from 'node:timers/promises'
import { promisify } from 'node:util'
import { KafkaClient } from '../../dist/index.js'
import { expandCompactBatch } from '../../dist/kafka-client.js'

const execute = promisify(execFile)
const runtime = process.env.KAFKA_CONTAINER_RUNTIME ?? 'podman'
ok(['podman', 'docker'].includes(runtime))
const [scenario, mode] = process.argv.slice(2)
const container = process.env.KAFKA_FAULT_CONTAINER
ok(
  /^(?:kafka-crab-readiness-[a-f0-9]+|[a-f0-9]{64})$/.test(container ?? ''),
  'Only an explicitly isolated readiness container may be stopped',
)
const [inspection] = JSON.parse((await execute(runtime, ['inspect', container])).stdout)
equal(inspection.Id, process.env.KAFKA_FAULT_CONTAINER_ID, 'Container identity must match the test-owned broker')
equal(inspection.Config.Labels['codex.task'], 'kafka-crab-readiness')

const topic = `readiness-${randomUUID()}`
const groupId = `readiness-${randomUUID()}`
const client = new KafkaClient({
  brokers: process.env.KAFKA_BROKERS,
  clientId: `readiness-${randomUUID()}`,
  diagnostics: false,
  logLevel: 'error',
  configuration: { 'reconnect.backoff.ms': 50, 'reconnect.backoff.max.ms': 100, 'socket.timeout.ms': 2000 },
})
const admin = client.createAdmin({ timeoutMs: 5000 })
const producerConfiguration = {
  'enable.idempotence': true,
  acks: 'all',
  'message.timeout.ms': 30_000,
  'request.timeout.ms': 2000,
  'linger.ms': 20,
}
const consumerConfiguration = {
  groupId,
  enableAutoCommit: false,
  configuration: {
    'enable.auto.offset.store': false,
    'auto.offset.reset': 'earliest',
    'session.timeout.ms': 6000,
    'heartbeat.interval.ms': 1000,
    'max.poll.interval.ms': 30_000,
    'partition.assignment.strategy': 'range',
  },
}
const producer = client.createProducer({ queueTimeout: 20_000, configuration: producerConfiguration })
const consumers = []
const readers = []
const details = { scenario, mode, topic, groupId }
let brokerDown = false

async function until(check, label, timeoutMs = 25_000) {
  const deadline = Date.now() + timeoutMs
  let lastError
  while (Date.now() < deadline) {
    try {
      if (await check()) return
    } catch (error) {
      lastError = error
    }
    await sleep(50)
  }
  throw new Error(`Timeout waiting for ${label}`, { cause: lastError })
}

async function broker(action) {
  await execute(runtime, [...(action === 'kill' ? ['kill', '--signal', 'KILL'] : ['start']), container], {
    timeout: 15_000,
  })
  brokerDown = action === 'kill'
  if (brokerDown) await execute(runtime, ['wait', container], { timeout: 15_000 })
  if (!brokerDown) {
    await until(async () => {
      const [metadata] = await admin.fetchTopicMetadata([topic])
      return metadata.partitions.length === 3 && metadata.partitions.every((partition) => partition.leader >= 0)
    }, 'broker recovery')
  }
}

function messages(start, count) {
  return Array.from({ length: count }, (_, index) => ({
    partition: (start + index) % 3,
    payload: Buffer.from(String(start + index)),
  }))
}

function assertAcks(metadata, count) {
  equal(metadata.length, count, 'Every successful send must return all delivery confirmations')
  ok(metadata.every((entry) => !entry.error && entry.offset >= 0))
  equal(new Set(metadata.map((entry) => `${entry.partition}:${entry.offset}`)).size, count)
}

async function commitBatch(consumer, batch) {
  const last = new Map()
  for (const message of batch) last.set(message.partition, message)
  for (const message of last.values()) await consumer.commitMessage(message, 'Sync')
}

async function assertCommits(metadata) {
  const expected = new Map()
  for (const entry of metadata)
    expected.set(entry.partition, Math.max(expected.get(entry.partition) ?? 0, entry.offset + 1))
  const [offsets] = await admin.fetchOffsets({ groupId, topics: [topic] })
  for (const [partition, offset] of expected) {
    const actual = offsets.partitions.find((entry) => entry.partition === partition)
    ok(actual && !actual.error)
    equal(actual.offset, String(offset))
  }
  details.committedOffsets = Object.fromEntries(expected)
}

function assertMessages(received, metadata, start = 0) {
  equal(received.length, metadata.length)
  const expected = new Map(metadata.map((entry, index) => [`${entry.partition}:${entry.offset}`, start + index]))
  const seen = new Set()
  for (const message of received) {
    const key = `${message.partition}:${message.offset}`
    equal(Number(message.payload.toString()), expected.get(key))
    equal(message.topic, topic)
    ok(!seen.has(key), `Duplicate delivery ${key}`)
    seen.add(key)
  }
  equal(seen.size, expected.size)
}

function createStream() {
  if (mode === 'compact') {
    const consumer = client.createConsumer(consumerConfiguration)
    consumers.push(consumer)
    return { consumer, stream: consumer.recvBatchStreamCompact(16, 20) }
  }
  const web =
    mode === 'batch'
      ? client.createWebStreamConsumer({ ...consumerConfiguration, batchSize: 16, batchTimeout: 20 })
      : client.createWebStreamConsumer({ ...consumerConfiguration, serialPrefetchSize: 16, serialPrefetchTimeout: 20 })
  consumers.push(web.consumer)
  return web
}

function unpack(value) {
  return mode === 'serial' ? [value] : mode === 'compact' ? expandCompactBatch(value) : value
}

try {
  const [created] = await admin.createTopics({ topics: [{ topic, numPartitions: 3, replicationFactor: 1 }] })
  ok(!created.error)
  await until(async () => (await admin.fetchTopicMetadata([topic]))[0].partitions.length === 3, 'topic metadata')

  if (scenario === 'producer-recovery') {
    await producer.send({ topic, messages: messages(0, 3) })
    const tested = client.createProducer({
      autoFlush: mode === 'auto',
      queueTimeout: 20_000,
      configuration: producerConfiguration,
    })
    await tested.send({ topic, messages: messages(3, 3) })
    if (mode === 'manual') await tested.flush()
    await broker('kill')
    const sends = Array.from({ length: 8 }, (_, index) =>
      tested.send({ topic, messages: messages(6 + index * 25, 25) }),
    )
    const pending =
      mode === 'auto'
        ? Promise.all(sends).then((batches) => batches.flat())
        : Promise.all(sends).then(async () => (await Promise.all([tested.flush(), tested.flush()])).flat())
    let settled = false
    void pending.then(
      () => {
        settled = true
      },
      () => {
        settled = true
      },
    )
    await sleep(250)
    equal(settled, false, 'Success must not be reported while Kafka is unavailable')
    await broker('start')
    const metadata = await pending
    assertAcks(metadata, 200)
    const consumer = client.createConsumer({ ...consumerConfiguration, groupId: randomUUID() })
    consumers.push(consumer)
    await consumer.subscribe([{ topic, allOffsets: { position: 'Beginning' } }])
    const received = []
    await until(async () => {
      received.push(
        ...(await consumer.recvBatch(256, 100)).filter((message) => Number(message.payload.toString()) >= 6),
      )
      return received.length >= 200
    }, 'acknowledged records after broker crash')
    assertMessages(received, metadata, 6)
    const offsets = await admin.fetchTopicOffsets(topic)
    equal(
      offsets.reduce((sum, entry) => sum + Number(entry.high), 0),
      206,
      'Idempotent retries must not append duplicate records',
    )
    details.confirmed = metadata.length
    details.concurrentSends = 8
  } else if (scenario === 'producer-timeout') {
    const tested = client.createProducer({
      autoFlush: mode === 'auto',
      queueTimeout: 2000,
      configuration: { ...producerConfiguration, 'message.timeout.ms': 5000 },
    })
    await tested.send({ topic, messages: messages(0, 1) })
    if (mode === 'manual') await tested.flush()
    await broker('kill')
    const started = performance.now()
    let failure
    try {
      await tested.send({ topic, messages: messages(1, 1) })
      if (mode === 'manual') await tested.flush()
    } catch (error) {
      failure = error
    }
    ok(failure, 'An unconfirmed send/flush must reject, never report an empty success')
    details.timeoutElapsedMs = performance.now() - started
    ok(details.timeoutElapsedMs < 3000, 'The bounded delivery deadline must settle during the outage')
    equal(tested.getLastDeliveryResults().length, 0)
    await until(() => tested.inFlightCount() === 0, 'expired delivery cleanup', 8000)
    await broker('start')
    await sleep(250)
    const metadata = await tested.send({ topic, messages: messages(2, 1) })
    const confirmed = mode === 'auto' ? metadata : await tested.flush()
    assertAcks(confirmed, 1)
    details.recoveredConfirmed = confirmed.length
  } else if (scenario === 'consumer-recovery' || scenario === 'committed-restart') {
    const count = scenario === 'committed-restart' ? 96 : 32
    const metadata = await producer.send({ topic, messages: messages(0, count) })
    assertAcks(metadata, count)
    let { consumer, stream } = createStream()
    await consumer.subscribe(topic)
    let reader = stream.getReader()
    readers.push(reader)
    const received = []
    const firstCount = scenario === 'committed-restart' ? 16 : 32
    while (received.length < firstCount) {
      const { value, done } = await reader.read()
      equal(done, false)
      const batch = unpack(value)
      await commitBatch(consumer, batch)
      received.push(...batch)
    }
    if (scenario === 'committed-restart') {
      const processedBeforeRestart = received.length
      await reader.cancel()
      await consumer.disconnect()
      await reader.closed
      const resumed = client.createConsumer(consumerConfiguration)
      consumers.push(resumed)
      await resumed.subscribe(topic)
      await until(async () => {
        const batch = await resumed.recvBatch(32, 100)
        await commitBatch(resumed, batch)
        received.push(...batch)
        return received.length >= count
      }, 'restart from broker-committed offsets')
      assertMessages(received, metadata)
      await assertCommits(metadata)
      details.confirmed = metadata.length
      details.processedBeforeRestart = processedBeforeRestart
    } else {
      await broker('kill')
      const waiting = reader.read().then(
        (result) => ({ result }),
        (error) => ({ error }),
      )
      const sending = producer.send({ topic, messages: messages(32, 32) })
      void sending.catch(() => undefined)
      await sleep(250)
      await broker('start')
      const after = await sending
      assertAcks(after, 32)
      metadata.push(...after)
      const pending = await waiting
      if (pending.error) {
        ok(/Broker transport failure|BrokerTransportFailure/i.test(pending.error.message))
        details.transportError = pending.error.message
        await reader.cancel().catch(() => undefined)
        await consumer.disconnect()
        const replacement = createStream()
        consumer = replacement.consumer
        stream = replacement.stream
        await consumer.subscribe(topic)
        reader = stream.getReader()
        readers.push(reader)
        details.recreatedConsumer = true
      }
      const next = pending.error ? await reader.read() : pending.result
      equal(next.done, false)
      const batch = unpack(next.value)
      await commitBatch(consumer, batch)
      received.push(...batch)
      while (received.length < 64) {
        const { value, done } = await reader.read()
        equal(done, false)
        const more = unpack(value)
        await commitBatch(consumer, more)
        received.push(...more)
      }
      assertMessages(received, metadata)
      await assertCommits(metadata)
      await consumer.disconnect()
      equal((await reader.read()).done, true)
      await reader.closed
      details.confirmed = metadata.length
    }
  } else if (scenario === 'rebalance') {
    const received = []
    const members = []
    const metadata = []
    function join() {
      const consumer = client.createConsumer(consumerConfiguration)
      consumers.push(consumer)
      const member = { consumer, stopped: false, error: null, postRebalances: 0, received: 0 }
      consumer.onEvents((error, event) => {
        if (!error && event?.name === 'PostRebalance') member.postRebalances++
      })
      members.push(member)
      return member
    }
    async function poll(member) {
      try {
        while (!member.stopped) {
          const batch = await member.consumer.recvBatch(32, 100)
          await commitBatch(member.consumer, batch)
          received.push(...batch)
          member.received += batch.length
        }
      } catch (error) {
        if (!member.stopped) member.error = error
      }
    }
    async function stable(active) {
      await until(async () => {
        for (const member of members) if (member.error) throw member.error
        const [group] = await admin.describeGroups([groupId])
        const partitions = active.map((member) =>
          member.consumer.assignment().flatMap((entry) => entry.partitionOffset.map((part) => part.partition)),
        )
        return (
          group.state === 'Stable' &&
          group.members.length === active.length &&
          partitions.every((parts) => parts.length > 0) &&
          new Set(partitions.flat()).size === 3 &&
          partitions.flat().length === 3
        )
      }, `stable group with ${active.length} members`)
    }
    async function produce(start, count) {
      const sent = await producer.send({ topic, messages: messages(start, count) })
      assertAcks(sent, count)
      metadata.push(...sent)
      await until(() => {
        for (const member of members) if (member.error) throw member.error
        return received.length >= metadata.length
      }, 'processed and committed group messages')
    }
    const first = join()
    await first.consumer.subscribe(topic)
    first.polling = poll(first)
    try {
      await stable([first])
      await produce(0, 30)
      const second = join()
      await second.consumer.subscribe(topic)
      second.polling = poll(second)
      await stable([first, second])
      await produce(30, 60)
      ok(first.received > 30 && second.received > 0, 'Both group members must process assigned partitions')
      first.stopped = true
      await first.consumer.disconnect()
      await first.polling
      await stable([second])
      await produce(90, 30)
      await assertCommits(metadata)
      assertMessages(received, metadata)
      ok(first.postRebalances >= 2 && second.postRebalances >= 2, 'The test must observe real group rebalances')
      details.confirmed = metadata.length
      details.members = members.map(({ received, postRebalances }) => ({ received, postRebalances }))
    } finally {
      for (const member of members) member.stopped = true
      await Promise.all(members.map((member) => member.consumer.disconnect()))
      await Promise.all(members.map((member) => member.polling))
    }
  } else {
    throw new Error(`Unknown readiness scenario: ${scenario}`)
  }
  console.log(`READINESS_RESULT ${JSON.stringify(details)}`)
} finally {
  if (brokerDown) await broker('start')
  await Promise.all(readers.map((reader) => reader.cancel().catch(() => undefined)))
  await Promise.all(consumers.map((consumer) => consumer.disconnect()))
  await admin.close()
}
