import { equal, ok } from 'node:assert/strict'
import test from 'node:test'
import { KafkaClient } from '../../dist/index.js'
import { cleanupConsumer, createConsumerConfig, createTestTopic, setupTestEnvironment } from './utils.mjs'

const SEND_FAILURE_PAYLOAD_MARKER = '\n--kafka-crab-send-failure--\n'

function spySlotReads(producer) {
  const originalGet = producer.getLastDeliveryResults.bind(producer)
  let slotReads = 0
  producer.getLastDeliveryResults = () => {
    slotReads += 1
    return originalGet()
  }
  return {
    get slotReads() {
      return slotReads
    },
  }
}

function createPartialProducer(client) {
  return client.createProducer({
    configuration: {
      'queue.buffering.max.messages': 1,
      'message.timeout.ms': '10000',
      'request.timeout.ms': '10000',
    },
  })
}

function assertFailureBoundToTopic(error, topic) {
  ok(error instanceof Error, 'partial send must throw an Error')
  equal(error.message.includes(SEND_FAILURE_PAYLOAD_MARKER), false, 'wrapper must strip the native payload marker')
  equal(error.totalCount, 2)
  ok(error.enqueuedCount === 0 || error.enqueuedCount === 1)
  equal(error.confirmedCount, error.confirmedMessages?.length ?? 0)
  for (const metadata of error.confirmedMessages ?? []) {
    equal(metadata.topic, topic)
    ok(typeof metadata.partition === 'number')
    ok(typeof metadata.offset === 'number')
    ok(metadata.offset >= 0)
  }
}

async function sendPartial(producer, topic, payloads) {
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
    assertFailureBoundToTopic(error, topic)
    return error
  }
}

await test('Producer send-failure isolation against real Kafka', async (t) => {
  const { config } = await setupTestEnvironment()
  const client = new KafkaClient(config)

  await t.test('partial enqueue recovers a broker ack without reading getLastDeliveryResults', async () => {
    const topic = createTestTopic('partial-one')
    const producer = createPartialProducer(client)
    const spy = spySlotReads(producer)
    const payload = `partial-one-${topic}`

    const error = await sendPartial(producer, topic, [payload, 'dropped'])

    equal(error.enqueuedCount, 1)
    equal(error.confirmedCount, 1)
    equal(error.confirmedMessages[0].topic, topic)
    equal(spy.slotReads, 0)

    const confirmed = error.confirmedMessages[0]
    const consumer = client.createConsumer(
      createConsumerConfig(`send-failure-one-${topic}`, {
        enableAutoCommit: false,
        configuration: {
          'enable.auto.commit': 'false',
          'auto.offset.reset': 'earliest',
        },
      }),
    )

    try {
      await consumer.subscribe([
        {
          topic,
          partitionOffset: [{ partition: confirmed.partition, offset: { offset: confirmed.offset } }],
        },
      ])
      const message = await consumer.recv()
      ok(message, 'broker must still have the confirmed message')
      equal(message.topic, topic)
      equal(message.partition, confirmed.partition)
      equal(message.offset, confirmed.offset)
      equal(message.payload.toString(), payload)
    } finally {
      await cleanupConsumer(consumer)
    }
  })

  await t.test('concurrent partial sends keep per-send broker metadata', async () => {
    const topicA = createTestTopic('partial-a')
    const topicB = createTestTopic('partial-b')
    const producer = createPartialProducer(client)
    const spy = spySlotReads(producer)

    const [errorA, errorB] = await Promise.all([
      sendPartial(producer, topicA, ['a-1', 'a-2']),
      sendPartial(producer, topicB, ['b-1', 'b-2']),
    ])

    ok((errorA.enqueuedCount ?? 0) + (errorB.enqueuedCount ?? 0) >= 1)
    ok(
      (errorA.confirmedCount ?? 0) + (errorB.confirmedCount ?? 0) >= 1,
      'at least one send must be acknowledged by the real broker',
    )
    equal(spy.slotReads, 0)
  })
})
