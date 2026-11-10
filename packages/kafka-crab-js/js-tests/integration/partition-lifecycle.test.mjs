import { deepEqual, equal, ok } from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import test from 'node:test'
import { KafkaClient } from '../../dist/index.js'

const brokers = process.env.KAFKA_BROKERS ?? '127.0.0.1:9092'

const join = async (consumer) => {
  const deadline = Date.now() + 12_000
  while (consumer.assignment().length === 0 && Date.now() < deadline) {
    await consumer.recvBatch(1, 100)
  }
  ok(consumer.assignment().length > 0, 'consumer must join the group')
}

test('pauses only the selected partition and resumes it without losing messages', { timeout: 25_000 }, async () => {
  const topic = `partition-pause-${randomUUID()}`
  const client = new KafkaClient({ brokers, clientId: topic })
  const admin = client.createAdmin()
  const producer = client.createProducer({ autoFlush: true })
  const consumer = client.createConsumer({
    groupId: topic,
    enableAutoCommit: false,
    configuration: { 'auto.offset.reset': 'earliest', 'session.timeout.ms': '6000' },
  })
  try {
    const [created] = await admin.createTopics({ topics: [{ topic, numPartitions: 2, replicationFactor: 1 }] })
    ok(!created.error)
    await consumer.subscribe(topic)
    await join(consumer)
    consumer.pausePartitions([{ topic, partition: 0 }])
    await producer.send({
      topic,
      messages: [0, 1].map((partition) => ({ partition, payload: Buffer.from(String(partition)) })),
    })
    const free = await consumer.recvBatch(10, 1000)
    deepEqual(
      free.map((message) => message.partition),
      [1],
    )
    consumer.resumePartitions([{ topic, partition: 0 }])
    const resumed = await consumer.recvBatch(10, 1000)
    deepEqual(
      resumed.map((message) => message.partition),
      [0],
    )
    equal(resumed[0].offset, 0)
  } finally {
    await consumer.disconnect()
    await producer.flush(1000)
    await admin.close()
  }
})

test(
  'disconnect resolves only after leaving the group, including concurrent callers',
  { timeout: 25_000 },
  async () => {
    const topic = `disconnect-group-${randomUUID()}`
    const client = new KafkaClient({ brokers, clientId: topic })
    const admin = client.createAdmin()
    const consumer = client.createConsumer({
      groupId: topic,
      enableAutoCommit: false,
      configuration: { 'session.timeout.ms': '6000', 'heartbeat.interval.ms': '1000' },
    })
    try {
      const [created] = await admin.createTopics({ topics: [{ topic, numPartitions: 1, replicationFactor: 1 }] })
      ok(!created.error)
      await consumer.subscribe(topic)
      await join(consumer)
      const [before] = await admin.describeGroups([topic])
      equal(before.members.length, 1)
      const pending = consumer.recv()
      await Promise.all([consumer.disconnect(), consumer.disconnect()])
      equal(await pending, null)
      const [after] = await admin.describeGroups([topic])
      equal(after.members.length, 0, 'disconnect must leave while the JS consumer object is still alive')
      await consumer.disconnect()
    } finally {
      await consumer.disconnect()
      await admin.close()
    }
  },
)
