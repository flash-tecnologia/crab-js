import { deepEqual, equal, ok, rejects, throws } from 'node:assert/strict'
import { createRequire } from 'node:module'
import { randomUUID } from 'node:crypto'
import test from 'node:test'
import { setTimeout as sleep } from 'node:timers/promises'

import { KafkaAdmin, KafkaClient } from '../../dist/index.js'

const brokers = process.env.KAFKA_BROKERS || 'localhost:9092'
const createClient = (clientId = `admin-${randomUUID()}`) =>
  new KafkaClient({
    brokers,
    clientId,
    diagnostics: false,
    configuration: { 'socket.timeout.ms': 5000 },
  })

test(
  'Admin: topology, partition routing, watermarks, raw records, and committed offsets',
  { timeout: 45_000 },
  async () => {
    const client = createClient()
    const admin = client.createAdmin({ timeoutMs: 10_000 })
    ok(admin instanceof KafkaAdmin)
    const topic = `admin-${randomUUID()}`
    const groupId = `admin-${randomUUID()}`
    const topology = { topic, numPartitions: 3, replicationFactor: 1, config: { 'retention.ms': '60000' } }
    const reader = client.createConsumer({
      groupId,
      enableAutoCommit: false,
      configuration: { 'enable.auto.offset.store': false },
    })
    const producer = client.createProducer({ configuration: { 'message.timeout.ms': 5000 } })
    try {
      const [created] = await admin.createTopics({ topics: [topology] })
      equal(created.created, true)
      ok(!created.error)
      const [existing] = await admin.createTopics({ topics: [topology] })
      equal(existing.created, false)
      ok(!existing.error)
      const [metadata] = await admin.fetchTopicMetadata([topic])
      ok(!metadata.error)
      deepEqual(metadata.partitions.map((partition) => partition.partition).sort(), [0, 1, 2])
      ok(
        metadata.partitions.every(
          (partition) =>
            partition.leader >= 0 && partition.replicas.length === 1 && partition.isr.length === 1 && !partition.error,
        ),
      )
      // A fresh broker may still be electing the coordinator for __consumer_offsets.
      let initial
      const coordinatorDeadline = Date.now() + 5000
      for (;;) {
        try {
          initial = await admin.fetchOffsets({ groupId, topics: [topic] })
          break
        } catch (error) {
          if (!error.message.includes('NotCoordinator') || Date.now() >= coordinatorDeadline) throw error
          await sleep(100)
        }
      }
      ok(
        initial[0].partitions.every((partition) => partition.offset === undefined && !partition.error),
        'uncommitted offsets remain absent',
      )

      const payload = Buffer.from([0, 255, 10, 13, 128])
      const key = Buffer.from([255, 0, 128])
      const headers = { binary: Buffer.from([0, 255]), source: Buffer.from('admin-integration') }
      const receipts = await producer.send({
        topic,
        messages: [
          { partition: 2, payload, key, headers },
          { partition: 0, payload: Buffer.from('zero') },
          { partition: 2, payload: Buffer.from('second') },
        ],
      })
      deepEqual(receipts.map(({ partition, offset }) => [partition, offset]).sort(), [
        [0, 0],
        [2, 0],
        [2, 1],
      ])
      const offsets = (await admin.fetchTopicOffsets(topic)).sort((left, right) => left.partition - right.partition)
      deepEqual(
        offsets.map(({ partition, low, high }) => [partition, low, high]),
        [
          [0, '0', '1'],
          [1, '0', '0'],
          [2, '0', '2'],
        ],
      )
      ok(offsets.every((partition) => !partition.error))

      await reader.subscribe([{ topic, partitionOffset: [{ partition: 2, offset: { position: 'Beginning' } }] }])
      const messages = []
      const deadline = Date.now() + 10_000
      while (messages.length < 2 && Date.now() < deadline) messages.push(...(await reader.recvBatch(2, 250)))
      equal(messages.length, 2)
      deepEqual(messages[0].payload, payload)
      deepEqual(messages[0].key, key)
      deepEqual(messages[0].headers, headers)
      await reader.commit(topic, 0, 0, 'Sync')
      await reader.commit(topic, 2, 2, 'Sync')
      for (let attempt = 0; attempt < 3; attempt += 1) {
        const [committed] = await admin.fetchOffsets({ groupId, topics: [topic] })
        deepEqual(committed.partitions.map(({ partition, offset }) => [partition, offset]).sort(), [
          [0, '0'],
          [1, undefined],
          [2, '2'],
        ])
        ok(committed.partitions.every((partition) => !partition.error))
      }
      const after = await admin.fetchTopicOffsets(topic)
      deepEqual(after, await admin.fetchTopicOffsets(topic), 'observations do not alter broker offsets')
    } finally {
      await reader.disconnect()
      await producer.flush()
      await admin.close()
    }
  },
)

test('Admin: partial creation failures preserve successes and explicit replication', { timeout: 30_000 }, async () => {
  const admin = createClient().createAdmin({ timeoutMs: 10_000 })
  const successful = `admin-ok-${randomUUID()}`
  const failed = `admin-rf-${randomUUID()}`
  try {
    const results = await admin.createTopics({
      topics: [
        { topic: successful, numPartitions: 1, replicationFactor: 1 },
        { topic: failed, numPartitions: 1, replicationFactor: 100 },
      ],
    })
    const accepted = results.find(({ topic }) => topic === successful)
    const rejected = results.find(({ topic }) => topic === failed)
    equal(accepted.created, true)
    ok(!accepted.error)
    equal(rejected.created, false)
    equal(
      rejected.error.code,
      38,
      'Kafka must reject the requested replication factor rather than silently reducing it',
    )
    const metadata = await admin.fetchTopicMetadata([successful, failed])
    ok(!metadata[0].error)
    ok(metadata[1].error)
    const [missing] = await admin.fetchTopicMetadata([`admin-missing-${randomUUID()}`])
    equal(missing.error.code, 3)
    const [created] = await admin.createTopics({
      topics: [{ topic: missing.topic, numPartitions: 1, replicationFactor: 1 }],
    })
    equal(created.created, true, 'metadata reads must not auto-create missing topics')
  } finally {
    await admin.close()
  }
})

test(
  'Compatibility: subscribe keeps its replication fallback while createAdmin respects explicit replication',
  { timeout: 30_000 },
  async () => {
    const client = createClient()
    const admin = client.createAdmin({ timeoutMs: 10_000 })
    const topic = `legacy-subscribe-${randomUUID()}`
    const strictTopic = `strict-admin-${randomUUID()}`
    const consumer = client.createConsumer({
      groupId: `legacy-subscribe-${randomUUID()}`,
      enableAutoCommit: false,
      fetchMetadataTimeout: 10_000,
    })
    try {
      await consumer.subscribe([
        {
          topic,
          createTopic: true,
          numPartitions: 1,
          replicas: 100,
          allOffsets: { position: 'Beginning' },
        },
      ])
      const [metadata] = await admin.fetchTopicMetadata([topic])
      ok(!metadata.error)
      equal(metadata.partitions.length, 1)
      const [partition] = metadata.partitions
      ok(partition.replicas.length > 0 && partition.replicas.length < 100)
      deepEqual(
        consumer.assignment().map((entry) => entry.topic),
        [topic],
      )
      const [rejected] = await admin.createTopics({
        topics: [{ topic: strictTopic, numPartitions: 1, replicationFactor: 100 }],
      })
      equal(rejected.created, false)
      equal(rejected.error.code, 38)
    } finally {
      await consumer.disconnect()
      await admin.close()
    }
  },
)

test('Admin: observing an active group does not join, commit, or rebalance it', { timeout: 40_000 }, async () => {
  const client = createClient()
  const admin = client.createAdmin({ timeoutMs: 5000 })
  const topic = `admin-active-${randomUUID()}`
  const groupId = `admin-active-${randomUUID()}`
  const consumer = client.createConsumer({
    groupId,
    enableAutoCommit: false,
    configuration: { 'auto.offset.reset': 'earliest', 'enable.auto.offset.store': false },
  })
  let postRebalances = 0
  consumer.onEvents((error, event) => {
    if (!error && event?.name === 'PostRebalance') postRebalances += 1
  })
  try {
    await admin.createTopics({ topics: [{ topic, numPartitions: 3, replicationFactor: 1 }] })
    await consumer.subscribe([{ topic }])
    let before
    const deadline = Date.now() + 15_000
    while (Date.now() < deadline) {
      await consumer.recvBatch(1, 100)
      if (consumer.assignment().length === 0) continue
      const [group] = await admin.describeGroups([groupId])
      if (group.state === 'Stable' && group.members.length === 1 && consumer.assignment().length > 0) {
        before = group
        break
      }
    }
    ok(before, 'the real consumer must join and stabilize')
    const assignment = consumer.assignment()
    const rebalances = postRebalances
    for (let attempt = 0; attempt < 3; attempt += 1) {
      const [offsets] = await admin.fetchOffsets({ groupId, topics: [topic] })
      ok(offsets.partitions.every((partition) => partition.offset === undefined && !partition.error))
      const [group] = await admin.describeGroups([groupId])
      equal(group.state, 'Stable')
      deepEqual(
        group.members.map((member) => member.memberId),
        before.members.map((member) => member.memberId),
      )
      ok(Buffer.isBuffer(group.members[0].assignment))
      deepEqual(consumer.assignment(), assignment)
    }
    await consumer.recvBatch(1, 250)
    equal(postRebalances, rebalances)
  } finally {
    await consumer.disconnect()
    await admin.close()
  }
})

test('Admin: invalid inputs, idempotent close, and ESM/CommonJS exports', async () => {
  const client = createClient()
  for (const timeoutMs of [0, -1, 0.5, Number.NaN, Number.POSITIVE_INFINITY, 2147483648]) {
    throws(() => client.createAdmin({ timeoutMs }), /timeoutMs/)
  }
  const require = createRequire(import.meta.url)
  equal(typeof require('../../dist/index.cjs').KafkaAdmin, 'function')
  const admin = client.createAdmin()
  try {
    await rejects(admin.fetchTopicMetadata([]), /topics/)
    await rejects(admin.fetchTopicOffsets(''), /topics/)
    await rejects(admin.describeGroups(['group', 'group']), /groupIds/)
    await rejects(admin.fetchOffsets({ groupId: '', topics: ['topic'] }), /groupId/)
    await rejects(
      admin.createTopics({ topics: [{ topic: 'topic', numPartitions: 1.5, replicationFactor: 1 }] }),
      /positive integers/,
    )
    const producer = client.createProducer({ configuration: { 'message.timeout.ms': 100 } })
    for (const partition of [-1, 0.5, Number.NaN, Number.POSITIVE_INFINITY, 2147483648]) {
      await rejects(
        producer.send({ topic: 'topic', messages: [{ partition, payload: Buffer.from('invalid') }] }),
        /partition must be an integer/,
      )
    }
    await producer.flush()
  } finally {
    await admin.close()
    await admin.close()
  }
  await rejects(admin.fetchTopicOffsets('topic'), /closed/)
  await rejects(admin.createTopics({ topics: [{ topic: 'topic', numPartitions: 1, replicationFactor: 1 }] }), /closed/)
})

test(
  'Admin: unavailable broker deadlines leave the JavaScript event loop responsive',
  { timeout: 10_000 },
  async () => {
    const client = new KafkaClient({ brokers: '127.0.0.1:1', clientId: 'admin-unavailable', diagnostics: false })
    const admin = client.createAdmin({ timeoutMs: 300 })
    let ticks = 0
    const timer = setInterval(() => {
      ticks += 1
    }, 10)
    const started = Date.now()
    try {
      const queries = [
        () => admin.fetchTopicMetadata(['unreachable', 'also-unreachable']),
        () => admin.fetchTopicOffsets('unreachable'),
        () => admin.fetchOffsets({ groupId: 'unreachable', topics: ['unreachable'] }),
        () => admin.describeGroups(['unreachable']),
        () => admin.createTopics({ topics: [{ topic: 'unreachable', numPartitions: 1, replicationFactor: 1 }] }),
      ]
      for (const query of queries) {
        const ticksBefore = ticks
        const queryStarted = Date.now()
        await rejects(query(), /metadata|timed out|transport|create Kafka topics|describe Kafka group/i)
        ok(ticks - ticksBefore >= 5, 'blocking native I/O must run outside the JS thread')
        ok(Date.now() - queryStarted < 2500, 'the operation deadline must bound the entire query')
      }
      ok(Date.now() - started < 5000)
      const pending = admin.fetchTopicMetadata(['unreachable'])
      const settling = Promise.allSettled([pending])
      await admin.close()
      const [result] = await settling
      equal(result.status, 'rejected')
      await rejects(admin.describeGroups(['group']), /closed/)
    } finally {
      clearInterval(timer)
      await admin.close()
    }
  },
)
