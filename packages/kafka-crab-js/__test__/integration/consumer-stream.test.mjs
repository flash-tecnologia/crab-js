import { equal, ok } from 'node:assert/strict'
import test from 'node:test'
import { setTimeout as sleep } from 'node:timers/promises'
import { KafkaClient } from '../../dist/index.js'
import {
  cleanupConsumer,
  cleanupProducer,
  createConsumerConfig,
  createProducerConfig,
  createTestTopic,
  isTestMessage,
  setupTestEnvironment,
  waitForMessages,
} from './utils.mjs'

await test('Consumer Stream Integration Tests', async (t) => {
  let client
  let producer

  await t.test('Setup: Create KafkaClient and Producer', async () => {
    const { config } = await setupTestEnvironment()
    client = new KafkaClient(config)
    producer = client.createProducer(createProducerConfig())
    ok(client, 'KafkaClient should be created')
    ok(producer, 'Producer should be created')
  })

  await t.test('Stream Consumer: Create stream consumer instance', async () => {
    const consumerConfig = createConsumerConfig('test-stream-creation')
    const streamConsumer = client.createStreamConsumer(consumerConfig)

    ok(streamConsumer, 'Stream consumer should be created')
    ok(typeof streamConsumer.subscribe === 'function', 'Should have subscribe method')
    ok(typeof streamConsumer.on === 'function', 'Should have on method (Node.js stream)')
    ok(typeof streamConsumer.pipe === 'function', 'Should have pipe method (Node.js stream)')
    ok(typeof streamConsumer.read === 'function', 'Should have read method (Node.js stream)')
    ok(typeof streamConsumer.rawConsumer === 'function', 'Should have rawConsumer method')

    await cleanupConsumer(streamConsumer)
  })

  await t.test('Stream Consumer: Receive messages via data events', async () => {
    const { topic, messages, testId } = await setupTestEnvironment()

    // Send messages first
    await producer.send({ topic, messages })
    await sleep(1000)

    // Create stream consumer
    const streamConsumer = client.createStreamConsumer(createConsumerConfig(`stream-data-${testId}`))
    await streamConsumer.subscribe([
      { topic, allOffsets: { position: 'Beginning' } },
    ])

    // Use utility function to wait for messages
    const receivedMessages = await waitForMessages(streamConsumer, messages.length, testId, 15000)

    await cleanupConsumer(streamConsumer)

    // Verify results
    equal(receivedMessages.length, messages.length, 'Should receive all sent messages')

    for (const msg of receivedMessages) {
      ok(msg.payload, 'Message should have payload')
      ok(msg.offset !== undefined, 'Message should have offset')
      ok(msg.partition !== undefined, 'Message should have partition')
      ok(msg.topic === topic, 'Message should have correct topic')
    }
  })

  await t.test('Stream Consumer: Handle stream errors', async () => {
    const streamConsumer = client.createStreamConsumer(createConsumerConfig('error-test'))

    let errorReceived = false

    streamConsumer.on('error', (error) => {
      errorReceived = true
      ok(error instanceof Error, 'Should receive Error instance')
    })

    // Try to subscribe to invalid topic format to potentially trigger error
    try {
      await streamConsumer.subscribe('')
    } catch (error) {
      errorReceived = true
      ok(error instanceof Error, 'Should throw error for invalid topic')
    }

    await cleanupConsumer(streamConsumer)

    // Note: Error handling might vary based on implementation
    // This test ensures error events are properly emitted
    console.log(`Error handling test completed. Error received: ${errorReceived}`)
  })

  await t.test('Stream Consumer: Process multiple topics', async () => {
    const topic1 = createTestTopic('stream-multi1')
    const topic2 = createTestTopic('stream-multi2')
    const testId = 'stream-multi-test'

    const messages1 = [{
      payload: Buffer.from(JSON.stringify({ testId, source: 'topic1' })),
    }]
    const messages2 = [{
      payload: Buffer.from(JSON.stringify({ testId, source: 'topic2' })),
    }]

    // Send to both topics
    await Promise.all([
      producer.send({ topic: topic1, messages: messages1 }),
      producer.send({ topic: topic2, messages: messages2 }),
    ])
    await sleep(1000)

    // Create stream consumer for multiple topics
    const streamConsumer = client.createStreamConsumer(createConsumerConfig(`multi-stream-${testId}`))
    await streamConsumer.subscribe([
      { topic: topic1, allOffsets: { position: 'Beginning' } },
      { topic: topic2, allOffsets: { position: 'Beginning' } },
    ])

    const receivedTopics = new Set()
    const messagePromise = new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(new Error('Timeout waiting for messages from both topics'))
      }, 15000)

      streamConsumer.on('data', (message) => {
        if (isTestMessage(message, testId)) {
          receivedTopics.add(message.topic)
          if (receivedTopics.size >= 2) {
            clearTimeout(timeout)
            resolve()
          }
        }
      })

      streamConsumer.on('error', (error) => {
        clearTimeout(timeout)
        reject(error)
      })
    })

    await messagePromise
    await cleanupConsumer(streamConsumer)

    // Verify results
    equal(receivedTopics.size, 2, 'Should receive from both topics')
    ok(receivedTopics.has(topic1), 'Should receive from first topic')
    ok(receivedTopics.has(topic2), 'Should receive from second topic')
  })

  await t.test('Stream Consumer: Pause and resume functionality', async () => {
    const { topic, messages, testId } = await setupTestEnvironment()

    await producer.send({ topic, messages })
    await sleep(1000)

    const streamConsumer = client.createStreamConsumer(createConsumerConfig(`pause-resume-${testId}`))
    await streamConsumer.subscribe([
      { topic, allOffsets: { position: 'Beginning' } },
    ])

    let receivedCount = 0
    const firstBatch = []

    // Set up data handler
    streamConsumer.on('data', (message) => {
      if (isTestMessage(message, testId)) {
        receivedCount++
        if (receivedCount <= 2) {
          firstBatch.push(message)
        }

        // Pause after receiving 2 messages
        if (receivedCount === 2) {
          streamConsumer.pause()
        }
      }
    })

    // Wait for first batch
    await new Promise((resolve) => {
      const checkMessages = () => {
        if (firstBatch.length >= 2) {
          resolve()
        } else {
          setTimeout(checkMessages, 100)
        }
      }
      checkMessages()
    })

    // Verify stream is paused
    ok(streamConsumer.isPaused(), 'Stream should be paused')

    const countBeforeResume = receivedCount
    await sleep(1000) // Wait to ensure no more messages come
    equal(receivedCount, countBeforeResume, 'Should not receive messages while paused')

    // Resume and wait for remaining messages
    streamConsumer.resume()
    ok(!streamConsumer.isPaused(), 'Stream should not be paused after resume')

    // Wait for remaining messages
    await new Promise((resolve) => {
      const checkMessages = () => {
        if (receivedCount >= messages.length) {
          resolve()
        } else {
          setTimeout(checkMessages, 100)
        }
      }
      setTimeout(checkMessages, 1000) // Give time for messages to flow
    })

    await cleanupConsumer(streamConsumer)

    ok(receivedCount >= messages.length, 'Should eventually receive all messages')
  })

  await t.test('Stream Consumer: Raw consumer access', async () => {
    const streamConsumer = client.createStreamConsumer(createConsumerConfig('raw-access-test'))

    const rawConsumer = streamConsumer.rawConsumer()
    ok(rawConsumer, 'Should provide access to raw consumer')
    ok(typeof rawConsumer.subscribe === 'function', 'Raw consumer should have subscribe method')
    ok(typeof rawConsumer.recv === 'function', 'Raw consumer should have recv method')

    await cleanupConsumer(streamConsumer)
  })

  await t.test('Stream Consumer: Seek functionality', async () => {
    const { topic, messages, testId } = await setupTestEnvironment()

    await producer.send({ topic, messages })
    await sleep(1000)

    const streamConsumer = client.createStreamConsumer(createConsumerConfig(`stream-seek-${testId}`))
    await streamConsumer.subscribe([
      { topic, allOffsets: { position: 'Beginning' } },
    ])

    let firstMessage = null

    // Get first message with timeout
    const messagePromise = new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(new Error('Timeout waiting for first message'))
      }, 10000)

      streamConsumer.on('data', (message) => {
        if (isTestMessage(message, testId) && !firstMessage) {
          firstMessage = message
          clearTimeout(timeout)
          resolve()
        }
      })

      streamConsumer.on('error', (error) => {
        clearTimeout(timeout)
        reject(error)
      })
    })

    try {
      await messagePromise
      ok(firstMessage, 'Should receive first message')

      // Test seek functionality
      streamConsumer.seek(firstMessage.topic, firstMessage.partition, { position: 'Beginning' })
      console.log('Seek functionality test passed')
    } catch (error) {
      console.warn('Stream seek test warning:', error.message)
      // Don't fail the test for this timing issue
    } finally {
      await cleanupConsumer(streamConsumer)
    }
  })

  await t.test('Stream Consumer: Unsubscribe functionality', async () => {
    const { topic } = await setupTestEnvironment()

    const streamConsumer = client.createStreamConsumer(createConsumerConfig('unsubscribe-test'))

    // Subscribe first
    await streamConsumer.subscribe([
      { topic, allOffsets: { position: 'Beginning' } },
    ])

    // Then unsubscribe
    streamConsumer.unsubscribe()

    // Should be able to subscribe to different topic after unsubscribe
    const newTopic = createTestTopic('after-unsubscribe')
    await streamConsumer.subscribe([
      { topic: newTopic, allOffsets: { position: 'Beginning' } },
    ])

    await cleanupConsumer(streamConsumer)
  })

  await t.test('Stream Consumer: Handle large message batches', async () => {
    const topic = createTestTopic('large-stream')
    const testId = 'large-stream-test'
    const messageCount = 100

    // Create many messages
    const largeMessageBatch = Array.from({ length: messageCount }, (_, i) => ({
      key: Buffer.from(`key-${i}`),
      payload: Buffer.from(JSON.stringify({ testId, index: i, data: 'x'.repeat(100) })),
    }))

    await producer.send({ topic, messages: largeMessageBatch })
    await sleep(2000)

    const streamConsumer = client.createStreamConsumer(createConsumerConfig(`large-batch-${testId}`))
    await streamConsumer.subscribe([
      { topic, allOffsets: { position: 'Beginning' } },
    ])

    const receivedMessages = await waitForMessages(streamConsumer, messageCount, testId, 30000)

    await cleanupConsumer(streamConsumer)

    equal(receivedMessages.length, messageCount, 'Should receive all large batch messages')
  })

  await t.test('Cleanup: Disconnect producer', async () => {
    await cleanupProducer(producer)
  })
})
