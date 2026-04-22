import { equal, ok } from 'node:assert/strict'
import test from 'node:test'
import { setTimeout as sleep } from 'node:timers/promises'

import { KafkaClient } from '../../dist/index.js'
import {
  cleanupConsumer,
  createConsumerConfig,
  createProducerConfig,
  createTestTopic,
  isTestMessage,
  setupTestEnvironment,
} from './utils.mjs'

async function readSerialMessages(stream, expectedCount, testId, timeoutMs = 15000) {
  const reader = stream.getReader()
  const messages = []
  const deadline = Date.now() + timeoutMs

  try {
    while (messages.length < expectedCount) {
      const remainingMs = deadline - Date.now()
      if (remainingMs <= 0) {
        throw new Error(`Timeout waiting for ${expectedCount} serial messages after ${timeoutMs}ms`)
      }

      const result = await Promise.race([
        reader.read(),
        sleep(remainingMs).then(() => {
          throw new Error(`Timeout waiting for ${expectedCount} serial messages after ${timeoutMs}ms`)
        }),
      ])

      if (result.done) {
        break
      }

      if (result.value && isTestMessage(result.value, testId)) {
        messages.push(result.value)
      }
    }

    return messages
  } finally {
    await reader.cancel().catch(() => undefined)
  }
}

async function readBatchMessages(stream, expectedCount, testId, timeoutMs = 15000) {
  const reader = stream.getReader()
  const messages = []
  const deadline = Date.now() + timeoutMs

  try {
    while (messages.length < expectedCount) {
      const remainingMs = deadline - Date.now()
      if (remainingMs <= 0) {
        throw new Error(`Timeout waiting for ${expectedCount} batch messages after ${timeoutMs}ms`)
      }

      const result = await Promise.race([
        reader.read(),
        sleep(remainingMs).then(() => {
          throw new Error(`Timeout waiting for ${expectedCount} batch messages after ${timeoutMs}ms`)
        }),
      ])

      if (result.done) {
        break
      }

      if (!Array.isArray(result.value)) {
        continue
      }

      for (const message of result.value) {
        if (isTestMessage(message, testId)) {
          messages.push(message)
        }
      }
    }

    return messages
  } finally {
    await reader.cancel().catch(() => undefined)
  }
}

test('Web Stream Consumer Integration Tests', async (t) => {
  let client
  let producer

  await t.test('Setup: Create KafkaClient and Producer', async () => {
    const { config } = await setupTestEnvironment()
    client = new KafkaClient(config)
    producer = client.createProducer(createProducerConfig())

    ok(client, 'KafkaClient should be created')
    ok(producer, 'Producer should be created')
  })

  await t.test('KafkaConsumer.recvStream preserves metadata fields', async () => {
    const { topic, messages, testId } = await setupTestEnvironment()

    await producer.send({ topic, messages })
    await sleep(1000)

    const consumer = client.createConsumer(createConsumerConfig(`native-stream-serial-${testId}`))
    const stream = consumer.recvStream(32, 2)

    try {
      await consumer.subscribe([{ topic, allOffsets: { position: 'Beginning' } }])
      const receivedMessages = await readSerialMessages(stream, messages.length, testId)

      equal(receivedMessages.length, messages.length, 'Should receive all sent messages')

      for (const message of receivedMessages) {
        ok(Object.keys(message).includes('headers'), 'Message should expose headers as an enumerable property')
        ok(Object.keys(message).includes('key'), 'Message should expose key as an enumerable property')
        ok(message.payload, 'Message should have payload')
        ok(message.key, 'Message should have key metadata')
        ok(message.headers, 'Message should have headers metadata')
        equal(message.topic, topic, 'Message should preserve topic metadata')
        ok(Number.isInteger(message.partition), 'Message should preserve partition metadata')
        ok(Number.isInteger(message.offset), 'Message should preserve offset metadata')
        ok(message.headers['test-header'], 'Message should preserve header metadata')
      }
    } finally {
      await cleanupConsumer(consumer)
    }
  })

  await t.test('KafkaConsumer.recvBatchStream preserves metadata fields', async () => {
    const topic = createTestTopic('native-batch')
    const testId = `native-batch-${Date.now()}`
    const messages = Array.from({ length: 6 }, (_, index) => ({
      key: Buffer.from(`key-${testId}-${index}`),
      headers: {
        'test-header': Buffer.from(`header-value-${index}`),
      },
      payload: Buffer.from(
        JSON.stringify({
          index,
          testId,
          type: 'native-batch-metadata',
        }),
      ),
    }))

    await producer.send({ topic, messages })
    await sleep(1000)

    const consumer = client.createConsumer(createConsumerConfig(`native-stream-batch-${testId}`))
    const stream = consumer.recvBatchStream(3, 25)

    try {
      await consumer.subscribe([{ topic, allOffsets: { position: 'Beginning' } }])
      const receivedMessages = await readBatchMessages(stream, messages.length, testId)

      equal(receivedMessages.length, messages.length, 'Should receive all batch messages')

      for (const message of receivedMessages) {
        ok(Object.keys(message).includes('headers'), 'Batch message should expose headers as an enumerable property')
        ok(Object.keys(message).includes('key'), 'Batch message should expose key as an enumerable property')
        ok(message.payload, 'Batch message should have payload')
        ok(message.key, 'Batch message should have key metadata')
        ok(message.headers, 'Batch message should have headers metadata')
        equal(message.topic, topic, 'Batch message should preserve topic metadata')
        ok(Number.isInteger(message.partition), 'Batch message should preserve partition metadata')
        ok(Number.isInteger(message.offset), 'Batch message should preserve offset metadata')
        ok(message.headers['test-header'], 'Batch message should preserve header metadata')
      }
    } finally {
      await cleanupConsumer(consumer)
    }
  })

  await t.test('KafkaConsumer.recvBatchStreamCompact compacts repeated keys and header values', async () => {
    const topic = createTestTopic('compact-batch')
    const testId = `compact-batch-${Date.now()}`
    const repeatedHeaderValue = Buffer.from('shared-header-value')
    const repeatedKeys = ['partition-0', 'partition-1', 'partition-2'].map((value) => Buffer.from(value))
    const messages = Array.from({ length: 12 }, (_, index) => ({
      key: repeatedKeys[index % repeatedKeys.length],
      headers: {
        'test-header': repeatedHeaderValue,
      },
      payload: Buffer.from(
        JSON.stringify({
          index,
          testId,
          type: 'compact-batch-metadata',
        }),
      ),
    }))

    await producer.send({ topic, messages })
    await sleep(1000)

    const consumer = client.createConsumer(createConsumerConfig(`compact-stream-batch-${testId}`))
    const stream = consumer.recvBatchStreamCompact(messages.length, 25)
    const reader = stream.getReader()

    try {
      await consumer.subscribe([{ topic, allOffsets: { position: 'Beginning' } }])
      const { value, done } = await reader.read()

      equal(done, false, 'Compact batch stream should yield a batch')
      ok(value, 'Compact batch value should be present')
      equal(value.payloads.length, messages.length, 'Compact batch should contain every payload')
      equal(value.topic, topic, 'Compact batch should preserve shared topic')
      equal(value.keyDictionary?.length, repeatedKeys.length, 'Compact batch should dictionary-encode repeated keys')
      equal(value.keyDictionaryIndexes?.length, messages.length, 'Compact batch should emit key dictionary indexes')
      equal(value.sharedHeaderKey, 'test-header', 'Compact batch should preserve the shared header key')
      ok(value.sharedHeaderValue?.equals(repeatedHeaderValue), 'Compact batch should share the repeated header value')
      equal(value.denseKeys, undefined, 'Compact batch should avoid dense key payloads for repeated keys')
      equal(
        value.denseSharedHeaderValues,
        undefined,
        'Compact batch should avoid dense shared header values when one value repeats',
      )
    } finally {
      await reader.cancel().catch(() => undefined)
      await cleanupConsumer(consumer)
    }
  })

  await t.test('Web Stream Consumer: Serial metadata preserves message fields', async () => {
    const { topic, messages, testId } = await setupTestEnvironment()

    await producer.send({ topic, messages })
    await sleep(1000)

    const webConsumer = client.createWebStreamConsumer({
      ...createConsumerConfig(`web-stream-serial-${testId}`),
      serialPrefetchSize: 32,
      serialPrefetchTimeout: 2,
    })

    equal(webConsumer.mode, 'serial', 'Serial web consumer should report serial mode')

    try {
      await webConsumer.consumer.subscribe([{ topic, allOffsets: { position: 'Beginning' } }])
      const receivedMessages = await readSerialMessages(webConsumer.stream, messages.length, testId)

      equal(receivedMessages.length, messages.length, 'Should receive all sent messages')

      for (const message of receivedMessages) {
        ok(message.payload, 'Message should have payload')
        ok(message.key, 'Message should have key metadata')
        ok(message.headers, 'Message should have headers metadata')
        equal(message.topic, topic, 'Message should preserve topic metadata')
        ok(Number.isInteger(message.partition), 'Message should preserve partition metadata')
        ok(Number.isInteger(message.offset), 'Message should preserve offset metadata')
        ok(message.headers['test-header'], 'Message should preserve header metadata')
      }
    } finally {
      await cleanupConsumer(webConsumer.consumer)
    }
  })

  await t.test('Web Stream Consumer: Batch metadata preserves message fields', async () => {
    const topic = createTestTopic('web-batch')
    const testId = `web-batch-${Date.now()}`
    const messages = Array.from({ length: 6 }, (_, index) => ({
      key: Buffer.from(`key-${testId}-${index}`),
      headers: {
        'test-header': Buffer.from(`header-value-${index}`),
      },
      payload: Buffer.from(
        JSON.stringify({
          index,
          testId,
          type: 'web-batch-metadata',
        }),
      ),
    }))

    await producer.send({ topic, messages })
    await sleep(1000)

    const webConsumer = client.createWebStreamConsumer({
      ...createConsumerConfig(`web-stream-batch-${testId}`),
      batchSize: 3,
      batchTimeout: 25,
    })

    equal(webConsumer.mode, 'batch', 'Batch web consumer should report batch mode')

    try {
      await webConsumer.consumer.subscribe([{ topic, allOffsets: { position: 'Beginning' } }])
      const receivedMessages = await readBatchMessages(webConsumer.stream, messages.length, testId)

      equal(receivedMessages.length, messages.length, 'Should receive all batch messages')

      for (const message of receivedMessages) {
        ok(message.payload, 'Batch message should have payload')
        ok(message.key, 'Batch message should have key metadata')
        ok(message.headers, 'Batch message should have headers metadata')
        equal(message.topic, topic, 'Batch message should preserve topic metadata')
        ok(Number.isInteger(message.partition), 'Batch message should preserve partition metadata')
        ok(Number.isInteger(message.offset), 'Batch message should preserve offset metadata')
        ok(message.headers['test-header'], 'Batch message should preserve header metadata')
      }
    } finally {
      await cleanupConsumer(webConsumer.consumer)
    }
  })
})
