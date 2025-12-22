import { equal, ok } from 'node:assert/strict'
import { Buffer } from 'node:buffer'
import test from 'node:test'
import { KafkaClient } from '../../dist/index.js'
import {
  cleanupProducer,
  createProducerConfig,
  createTestMessages,
  createTestTopic,
  setupTestEnvironment,
} from './utils.mjs'

await test('Producer Integration Tests', async (t) => {
  let client
  let producer

  await t.test('Setup: Create KafkaClient', async () => {
    const { config } = await setupTestEnvironment()
    client = new KafkaClient(config)
    ok(client, 'KafkaClient should be created')
  })

  await t.test('Producer: Create producer instance', async () => {
    producer = client.createProducer(createProducerConfig())
    ok(producer, 'Producer should be created')
    ok(typeof producer.send === 'function', 'Producer should have send method')
  })

  await t.test('Producer: Send single message', async () => {
    const { topic, testId } = await setupTestEnvironment()
    const message = {
      key: Buffer.from(`single-key-${testId}`),
      payload: Buffer.from(JSON.stringify({ testId, type: 'single' })),
      headers: { 'message-type': Buffer.from('single') },
    }

    const results = await producer.send({
      topic,
      messages: [message],
    })

    ok(Array.isArray(results), 'Send should return array of results')
    equal(results.length, 1, 'Should have one result')
    ok(typeof results[0] === 'number' || (results[0] && typeof results[0].offset === 'number'),
      'Result should contain offset')
  })

  await t.test('Producer: Send batch of messages', async () => {
    const { topic, messages } = await setupTestEnvironment()

    const results = await producer.send({
      topic,
      messages,
    })

    ok(Array.isArray(results), 'Send should return array of results')
    equal(results.length, messages.length, 'Should have results for all messages')

    for (const result of results) {
      ok(typeof result === 'number' || (result && typeof result.offset === 'number'),
        'Each result should contain offset')
    }
  })

  await t.test('Producer: Send messages with complex headers', async () => {
    const topic = createTestTopic('headers')
    const complexMessage = {
      key: Buffer.from('complex-key'),
      payload: Buffer.from(JSON.stringify({
        data: 'complex payload',
        nested: { value: 42 },
      })),
      headers: {
        'content-type': Buffer.from('application/json'),
        'correlation-id': Buffer.from('12345-67890'),
        'timestamp': Buffer.from(Date.now().toString()),
        'binary-data': Buffer.from([0x01, 0x02, 0x03, 0x04]),
      },
    }

    const results = await producer.send({
      topic,
      messages: [complexMessage],
    })

    ok(Array.isArray(results), 'Send should return array of results')
    equal(results.length, 1, 'Should have one result')
  })

  await t.test('Producer: Send to multiple topics', async () => {
    const topic1 = createTestTopic('multi1')
    const topic2 = createTestTopic('multi2')
    const messages1 = createTestMessages(2, 'multi-test-1')
    const messages2 = createTestMessages(2, 'multi-test-2')

    const [results1, results2] = await Promise.all([
      producer.send({ topic: topic1, messages: messages1 }),
      producer.send({ topic: topic2, messages: messages2 }),
    ])

    ok(Array.isArray(results1), 'First send should return array')
    ok(Array.isArray(results2), 'Second send should return array')
    equal(results1.length, messages1.length, 'Should have results for first topic')
    equal(results2.length, messages2.length, 'Should have results for second topic')
  })

  await t.test('Producer: Handle large message batch', async () => {
    const topic = createTestTopic('large-batch')
    const largeMessages = createTestMessages(50, 'large-batch-test')

    const results = await producer.send({
      topic,
      messages: largeMessages,
    })

    ok(Array.isArray(results), 'Send should return array of results')
    equal(results.length, largeMessages.length, 'Should have results for all 50 messages')
  })

  await t.test('Producer: Send message with no key', async () => {
    const topic = createTestTopic('no-key')
    const message = {
      payload: Buffer.from(JSON.stringify({ type: 'no-key-message' })),
    }

    const results = await producer.send({
      topic,
      messages: [message],
    })

    ok(Array.isArray(results), 'Send should return array of results')
    equal(results.length, 1, 'Should have one result')
  })

  await t.test('Producer: Send message with empty payload', async () => {
    const topic = createTestTopic('empty-payload')
    const message = {
      key: Buffer.from('empty-key'),
      payload: Buffer.alloc(0),
    }

    const results = await producer.send({
      topic,
      messages: [message],
    })

    ok(Array.isArray(results), 'Send should return array of results')
    equal(results.length, 1, 'Should have one result')
  })

  await t.test('Cleanup: Disconnect producer', async () => {
    await cleanupProducer(producer)
  })
})
