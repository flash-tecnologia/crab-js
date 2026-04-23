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

async function readCompactBatch(stream, timeoutMs = 15000) {
  const reader = stream.getReader()

  try {
    const result = await Promise.race([
      reader.read(),
      sleep(timeoutMs).then(() => {
        throw new Error(`Timeout waiting for compact batch after ${timeoutMs}ms`)
      }),
    ])

    if (result.done || !result.value) {
      throw new Error('Compact batch stream ended before yielding a batch')
    }

    return result.value
  } finally {
    await reader.cancel().catch(() => undefined)
  }
}

function getPayloadIndex(message) {
  return JSON.parse(message.payload.toString()).index
}

function assertHeadersEqual(actualHeaders, expectedHeaders, assertionMessage) {
  ok(actualHeaders, assertionMessage)

  const actualEntries = Object.entries(actualHeaders)
  const expectedEntries = Object.entries(expectedHeaders)

  equal(actualEntries.length, expectedEntries.length, `${assertionMessage}: header count should match`)

  for (const [key, expectedValue] of expectedEntries) {
    ok(actualHeaders[key]?.equals(expectedValue), `${assertionMessage}: header "${key}" should match`)
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
    try {
      const stream = consumer.recvBatchStreamCompact(messages.length, 25)
      await consumer.subscribe([{ topic, allOffsets: { position: 'Beginning' } }])
      const value = await readCompactBatch(stream)

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
      await cleanupConsumer(consumer)
    }
  })

  await t.test('KafkaConsumer.recvBatchStreamCompact uses shared key encoding with shared header values', async () => {
    const topic = createTestTopic('compact-shared-key')
    const testId = `compact-shared-key-${Date.now()}`
    const sharedKey = Buffer.from(`shared-key-${testId}`)
    const sharedHeaderValue = Buffer.from(`shared-header-${testId}`)
    const messages = Array.from({ length: 10 }, (_, index) => ({
      key: sharedKey,
      headers: {
        'test-header': sharedHeaderValue,
      },
      payload: Buffer.from(
        JSON.stringify({
          index,
          testId,
          type: 'compact-shared-key',
        }),
      ),
    }))

    await producer.send({ topic, messages })
    await sleep(1000)

    const compactConsumer = client.createConsumer(createConsumerConfig(`compact-shared-key-raw-${testId}`))

    try {
      const compactStream = compactConsumer.recvBatchStreamCompact(messages.length, 25)
      await compactConsumer.subscribe([{ topic, allOffsets: { position: 'Beginning' } }])
      const compactBatch = await readCompactBatch(compactStream)

      equal(compactBatch.payloads.length, messages.length, 'Compact batch should contain every payload')
      equal(compactBatch.topic, topic, 'Compact batch should preserve shared topic')
      ok(compactBatch.sharedKey?.equals(sharedKey), 'Compact batch should expose the shared key')
      equal(compactBatch.keys, undefined, 'Compact batch should avoid sparse keys for a shared key')
      equal(compactBatch.denseKeys, undefined, 'Compact batch should avoid dense keys for a shared key')
      equal(compactBatch.keyDictionary, undefined, 'Compact batch should avoid key dictionaries for a shared key')
      equal(
        compactBatch.keyDictionaryIndexes,
        undefined,
        'Compact batch should avoid key dictionary indexes for a shared key',
      )
      equal(compactBatch.sharedHeaderKey, 'test-header', 'Compact batch should preserve the shared header key')
      ok(
        compactBatch.sharedHeaderValue?.equals(sharedHeaderValue),
        'Compact batch should expose the shared header value',
      )
      equal(
        compactBatch.sharedHeaderValues,
        undefined,
        'Compact batch should avoid sparse shared header values when one value repeats',
      )
      equal(
        compactBatch.denseSharedHeaderValues,
        undefined,
        'Compact batch should avoid dense shared header values when one value repeats',
      )
      equal(
        compactBatch.headerValueDictionary,
        undefined,
        'Compact batch should avoid header dictionaries when one value repeats',
      )
      equal(
        compactBatch.headerValueDictionaryIndexes,
        undefined,
        'Compact batch should avoid header dictionary indexes when one value repeats',
      )
      equal(compactBatch.headers, undefined, 'Compact batch should avoid per-message header maps for shared headers')
    } finally {
      await cleanupConsumer(compactConsumer)
    }

    const webConsumer = client.createWebStreamConsumer({
      ...createConsumerConfig(`compact-shared-key-web-${testId}`),
      batchSize: messages.length,
      batchTimeout: 25,
    })

    try {
      await webConsumer.consumer.subscribe([{ topic, allOffsets: { position: 'Beginning' } }])
      const receivedMessages = await readBatchMessages(webConsumer.stream, messages.length, testId)

      equal(receivedMessages.length, messages.length, 'Should receive all shared-key batch messages')

      for (const message of receivedMessages) {
        ok(message.payload, 'Decoded shared-key message should have payload')
        ok(message.key?.equals(sharedKey), 'Decoded shared-key message should preserve the shared key')
        equal(message.topic, topic, 'Decoded shared-key message should preserve topic metadata')
        ok(Number.isInteger(message.partition), 'Decoded shared-key message should preserve partition metadata')
        ok(Number.isInteger(message.offset), 'Decoded shared-key message should preserve offset metadata')
        assertHeadersEqual(
          message.headers,
          {
            'test-header': sharedHeaderValue,
          },
          'Decoded shared-key message should preserve shared headers',
        )
      }
    } finally {
      await cleanupConsumer(webConsumer.consumer)
    }
  })

  await t.test('KafkaConsumer.recvBatchStreamCompact dictionary-encodes repeated header values', async () => {
    const topic = createTestTopic('compact-header-dictionary')
    const testId = `compact-header-dictionary-${Date.now()}`
    const headerValues = ['header-a', 'header-b', 'header-c'].map((value) => Buffer.from(`${value}-${testId}`))
    const messages = Array.from({ length: 12 }, (_, index) => ({
      key: Buffer.from(`key-${testId}-${index}`),
      headers: {
        'test-header': headerValues[index % headerValues.length],
      },
      payload: Buffer.from(
        JSON.stringify({
          index,
          testId,
          type: 'compact-header-dictionary',
        }),
      ),
    }))

    await producer.send({ topic, messages })
    await sleep(1000)

    const compactConsumer = client.createConsumer(createConsumerConfig(`compact-header-dictionary-raw-${testId}`))

    try {
      const compactStream = compactConsumer.recvBatchStreamCompact(messages.length, 25)
      await compactConsumer.subscribe([{ topic, allOffsets: { position: 'Beginning' } }])
      const compactBatch = await readCompactBatch(compactStream)

      equal(compactBatch.payloads.length, messages.length, 'Compact batch should contain every payload')
      equal(compactBatch.topic, topic, 'Compact batch should preserve shared topic')
      equal(compactBatch.sharedHeaderKey, 'test-header', 'Compact batch should preserve the shared header key')
      equal(
        compactBatch.headerValueDictionary?.length,
        headerValues.length,
        'Compact batch should dictionary-encode repeated header values',
      )
      equal(
        compactBatch.headerValueDictionaryIndexes?.length,
        messages.length,
        'Compact batch should emit header dictionary indexes',
      )
      equal(compactBatch.sharedHeaderValue, undefined, 'Compact batch should avoid a single shared header value')
      equal(compactBatch.sharedHeaderValues, undefined, 'Compact batch should avoid sparse shared header values')
      equal(
        compactBatch.denseSharedHeaderValues,
        undefined,
        'Compact batch should avoid dense shared header values for repeated values',
      )
      equal(
        compactBatch.headers,
        undefined,
        'Compact batch should avoid per-message header maps for shared header keys',
      )
    } finally {
      await cleanupConsumer(compactConsumer)
    }

    const webConsumer = client.createWebStreamConsumer({
      ...createConsumerConfig(`compact-header-dictionary-web-${testId}`),
      batchSize: messages.length,
      batchTimeout: 25,
    })

    try {
      await webConsumer.consumer.subscribe([{ topic, allOffsets: { position: 'Beginning' } }])
      const receivedMessages = await readBatchMessages(webConsumer.stream, messages.length, testId)

      equal(receivedMessages.length, messages.length, 'Should receive all header-dictionary batch messages')

      for (const message of receivedMessages) {
        const payloadIndex = getPayloadIndex(message)
        const expectedHeaderValue = headerValues[payloadIndex % headerValues.length]

        ok(message.payload, 'Decoded header-dictionary message should have payload')
        ok(message.key, 'Decoded header-dictionary message should preserve key metadata')
        equal(message.topic, topic, 'Decoded header-dictionary message should preserve topic metadata')
        ok(Number.isInteger(message.partition), 'Decoded header-dictionary message should preserve partition metadata')
        ok(Number.isInteger(message.offset), 'Decoded header-dictionary message should preserve offset metadata')
        assertHeadersEqual(
          message.headers,
          {
            'test-header': expectedHeaderValue,
          },
          'Decoded header-dictionary message should preserve the expected header value',
        )
      }
    } finally {
      await cleanupConsumer(webConsumer.consumer)
    }
  })

  await t.test(
    'KafkaConsumer.recvBatchStreamCompact falls back to per-message headers for heterogeneous batches',
    async () => {
      const topic = createTestTopic('compact-header-fallback')
      const testId = `compact-header-fallback-${Date.now()}`
      const expectedHeadersByIndex = [
        {
          'test-header': Buffer.from(`test-header-0-${testId}`),
        },
        {
          'test-header': Buffer.from(`test-header-1-${testId}`),
          'extra-header': Buffer.from(`extra-header-1-${testId}`),
        },
        {
          'another-header': Buffer.from(`another-header-2-${testId}`),
        },
        {
          'test-header': Buffer.from(`test-header-3-${testId}`),
          'message-index': Buffer.from('3'),
        },
        {
          'final-header': Buffer.from(`final-header-4-${testId}`),
          'test-header': Buffer.from(`test-header-4-${testId}`),
        },
      ]
      const messages = expectedHeadersByIndex.map((headers, index) => ({
        key: Buffer.from(`key-${testId}-${index}`),
        headers,
        payload: Buffer.from(
          JSON.stringify({
            index,
            testId,
            type: 'compact-header-fallback',
          }),
        ),
      }))

      await producer.send({ topic, messages })
      await sleep(1000)

      const compactConsumer = client.createConsumer(createConsumerConfig(`compact-header-fallback-raw-${testId}`))

      try {
        const compactStream = compactConsumer.recvBatchStreamCompact(messages.length, 25)
        await compactConsumer.subscribe([{ topic, allOffsets: { position: 'Beginning' } }])
        const compactBatch = await readCompactBatch(compactStream)

        equal(compactBatch.payloads.length, messages.length, 'Compact batch should contain every payload')
        equal(compactBatch.topic, topic, 'Compact batch should preserve shared topic')
        equal(compactBatch.headers?.length, messages.length, 'Compact batch should preserve one header map per message')
        equal(
          compactBatch.sharedHeaderKey,
          undefined,
          'Compact batch should avoid shared header keys for heterogeneous headers',
        )
        equal(
          compactBatch.sharedHeaderValue,
          undefined,
          'Compact batch should avoid shared header values for heterogeneous headers',
        )
        equal(
          compactBatch.sharedHeaderValues,
          undefined,
          'Compact batch should avoid sparse shared header values for heterogeneous headers',
        )
        equal(
          compactBatch.denseSharedHeaderValues,
          undefined,
          'Compact batch should avoid dense shared header values for heterogeneous headers',
        )
        equal(
          compactBatch.headerValueDictionary,
          undefined,
          'Compact batch should avoid header dictionaries for heterogeneous headers',
        )
        equal(
          compactBatch.headerValueDictionaryIndexes,
          undefined,
          'Compact batch should avoid header dictionary indexes for heterogeneous headers',
        )
      } finally {
        await cleanupConsumer(compactConsumer)
      }

      const webConsumer = client.createWebStreamConsumer({
        ...createConsumerConfig(`compact-header-fallback-web-${testId}`),
        batchSize: messages.length,
        batchTimeout: 25,
      })

      try {
        await webConsumer.consumer.subscribe([{ topic, allOffsets: { position: 'Beginning' } }])
        const receivedMessages = await readBatchMessages(webConsumer.stream, messages.length, testId)

        equal(receivedMessages.length, messages.length, 'Should receive all heterogeneous-header batch messages')

        for (const message of receivedMessages) {
          const payloadIndex = getPayloadIndex(message)
          const expectedHeaders = expectedHeadersByIndex[payloadIndex]

          ok(message.payload, 'Decoded heterogeneous-header message should have payload')
          ok(message.key, 'Decoded heterogeneous-header message should preserve key metadata')
          equal(message.topic, topic, 'Decoded heterogeneous-header message should preserve topic metadata')
          ok(
            Number.isInteger(message.partition),
            'Decoded heterogeneous-header message should preserve partition metadata',
          )
          ok(Number.isInteger(message.offset), 'Decoded heterogeneous-header message should preserve offset metadata')
          assertHeadersEqual(
            message.headers,
            expectedHeaders,
            'Decoded heterogeneous-header message should preserve the original header map',
          )
        }
      } finally {
        await cleanupConsumer(webConsumer.consumer)
      }
    },
  )

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

  await t.test('Web Stream Consumer: Serial pipeTo settles promptly on early stop', async () => {
    const { topic, messages, testId } = await setupTestEnvironment()

    await producer.send({ topic, messages })
    await sleep(1000)

    const webConsumer = client.createWebStreamConsumer({
      ...createConsumerConfig(`web-stream-serial-stop-${testId}`),
      serialPrefetchSize: 32,
      serialPrefetchTimeout: 2,
    })
    const stopError = new Error(`stop-${testId}`)
    let seen = 0

    try {
      await webConsumer.consumer.subscribe([{ topic, allOffsets: { position: 'Beginning' } }])

      await Promise.race([
        webConsumer.stream
          .pipeTo(
            new WritableStream({
              write(message) {
                if (!isTestMessage(message, testId)) {
                  return
                }

                seen += 1
                if (seen >= 3) {
                  throw stopError
                }
              },
            }),
          )
          .then(
            () => {
              throw new Error('Serial pipeTo should reject when stopped early')
            },
            (error) => {
              if (error !== stopError) {
                throw error
              }
            },
          ),
        sleep(2000).then(() => {
          throw new Error('Serial pipeTo did not settle after early stop within 2000ms')
        }),
      ])

      ok(seen >= 3, 'Serial pipeTo should process a few messages before stopping')
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
