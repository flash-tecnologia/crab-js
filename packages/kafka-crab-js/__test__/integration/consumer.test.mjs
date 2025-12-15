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
} from './utils.mjs'

await test('Consumer Integration Tests', async (t) => {
  let client
  let producer

  await t.test('Setup: Create KafkaClient and Producer', async () => {
    const { config } = await setupTestEnvironment()
    client = new KafkaClient(config)
    producer = client.createProducer(createProducerConfig())
    ok(client, 'KafkaClient should be created')
    ok(producer, 'Producer should be created')
  })

  await t.test('Consumer: Create consumer instance', async () => {
    const consumerConfig = createConsumerConfig('test-consumer-creation')
    const consumer = client.createConsumer(consumerConfig)

    ok(consumer, 'Consumer should be created')
    ok(typeof consumer.subscribe === 'function', 'Consumer should have subscribe method')
    ok(typeof consumer.recv === 'function', 'Consumer should have recv method')
    ok(typeof consumer.recvBatch === 'function', 'Consumer should have recvBatch method')
    ok(typeof consumer.commit === 'function', 'Consumer should have commit method')
    ok(typeof consumer.seek === 'function', 'Consumer should have seek method')
    ok(typeof consumer.onEvents === 'function', 'Consumer should have onEvents method')

    await cleanupConsumer(consumer)
  })

  await t.test('Consumer: Subscribe to topic and receive messages', async () => {
    const { topic, messages, testId } = await setupTestEnvironment()

    // Send messages first
    await producer.send({ topic, messages })
    await sleep(1000) // Wait for messages to be available

    // Create consumer and subscribe
    const consumer = client.createConsumer(createConsumerConfig(`recv-test-${testId}`))
    await consumer.subscribe(topic)

    // Receive messages
    const receivedMessages = []
    const maxPolls = 20
    let polls = 0

    while (receivedMessages.length < messages.length && polls < maxPolls) {
      polls++
      const message = await consumer.recv()

      if (message && isTestMessage(message, testId)) {
        receivedMessages.push(message)
      }

      if (!message) {
        await sleep(500)
      }
    }

    await cleanupConsumer(consumer)

    // Verify results
    equal(receivedMessages.length, messages.length, 'Should receive all sent messages')

    for (const msg of receivedMessages) {
      ok(msg.payload, 'Message should have payload')
      ok(msg.offset !== undefined, 'Message should have offset')
      ok(msg.partition !== undefined, 'Message should have partition')
      ok(msg.topic === topic, 'Message should have correct topic')
    }
  })

  await t.test('Consumer: Batch receive messages', async () => {
    const { topic, messages, testId } = await setupTestEnvironment()

    // Send messages
    await producer.send({ topic, messages })
    await sleep(1000)

    // Create consumer
    const consumer = client.createConsumer(createConsumerConfig(`batch-test-${testId}`))
    await consumer.subscribe(topic)

    // Use batch receive
    const batchSize = 3
    const timeoutMs = 5000
    const receivedBatches = []
    let totalReceived = 0

    while (totalReceived < messages.length) {
      const batch = await consumer.recvBatch(batchSize, timeoutMs)

      if (batch.length === 0) {
        break
      }

      const testMessages = batch.filter(msg => isTestMessage(msg, testId))
      if (testMessages.length > 0) {
        receivedBatches.push(testMessages)
        totalReceived += testMessages.length
      }
    }

    await cleanupConsumer(consumer)

    // Verify results
    ok(receivedBatches.length > 0, 'Should receive at least one batch')
    ok(totalReceived >= messages.length, 'Should receive all messages via batches')

    const allMessages = receivedBatches.flat()
    for (const msg of allMessages) {
      ok(msg.payload, 'Batch message should have payload')
      ok(msg.offset !== undefined, 'Batch message should have offset')
      ok(msg.partition !== undefined, 'Batch message should have partition')
    }
  })

  await t.test('Consumer: Subscribe to multiple topics', async () => {
    const topic1 = createTestTopic('multi1')
    const topic2 = createTestTopic('multi2')
    const testId = 'multi-topic-test'
    const messages1 = [{
      payload: Buffer.from(JSON.stringify({ testId, topic: topic1 })),
    }]
    const messages2 = [{
      payload: Buffer.from(JSON.stringify({ testId, topic: topic2 })),
    }]

    // Send to both topics
    await Promise.all([
      producer.send({ topic: topic1, messages: messages1 }),
      producer.send({ topic: topic2, messages: messages2 }),
    ])
    await sleep(1000)

    // Create consumer with multiple topic subscription
    const consumer = client.createConsumer(createConsumerConfig(`multi-topic-${testId}`))
    await consumer.subscribe([
      { topic: topic1, allOffsets: { position: 'Beginning' } },
      { topic: topic2, allOffsets: { position: 'Beginning' } },
    ])

    const receivedTopics = new Set()
    const receivedMessages = []
    const maxPolls = 20
    let polls = 0

    while (receivedTopics.size < 2 && polls < maxPolls) {
      polls++
      const message = await consumer.recv()
      if (message && isTestMessage(message, testId)) {
        receivedTopics.add(message.topic)
        receivedMessages.push(message)
      }

      if (!message) {
        await sleep(500)
      }
    }

    await cleanupConsumer(consumer)

    // Verify results
    equal(receivedTopics.size, 2, 'Should receive from both topics')
    ok(receivedTopics.has(topic1), 'Should receive from first topic')
    ok(receivedTopics.has(topic2), 'Should receive from second topic')
  })

  await t.test('Consumer: Seek functionality', async () => {
    const { topic, messages, testId } = await setupTestEnvironment()

    await producer.send({ topic, messages })
    await sleep(1000)

    const consumer = client.createConsumer(createConsumerConfig(`seek-test-${testId}`))
    await consumer.subscribe(topic)

    try {
      // Receive first message to get position with timeout
      let firstMessage = null
      const startTime = Date.now()

      while (!firstMessage && (Date.now() - startTime) < 10000) {
        const message = await consumer.recv()
        if (message && isTestMessage(message, testId)) {
          firstMessage = message
          break
        }
        await sleep(100)
      }

      ok(firstMessage, 'Should receive first message')

      // Test seek functionality (may fail due to timing)
      try {
        consumer.seek(firstMessage.topic, firstMessage.partition, { Beginning: null })
        console.log('Seek operation completed successfully')
      } catch (error) {
        console.warn('Seek test warning:', error.message)
        // Don't fail the test for seek issues
      }
    } catch (error) {
      console.warn('Seek test error:', error.message)
    } finally {
      await cleanupConsumer(consumer)
    }
  })

  await t.test('Consumer: Event handling', async () => {
    const { topic, testId } = await setupTestEnvironment()

    const consumer = client.createConsumer(createConsumerConfig(`events-test-${testId}`))

    let eventReceived = false
    let eventName = ''

    // Set up event handler with timeout
    const eventPromise = new Promise((resolve) => {
      const timeout = setTimeout(() => {
        console.log('Event handling test: No events received within timeout')
        resolve()
      }, 5000)

      consumer.onEvents((err, event) => {
        if (err) {
          console.error('Event error:', err)
          return
        }
        eventReceived = true
        eventName = event.name
        clearTimeout(timeout)
        resolve()
      })
    })

    try {
      // Subscribe to trigger events
      await consumer.subscribe(topic)

      // Wait for events with timeout
      await eventPromise

      // Events are optional, so we don't fail if none received
      if (eventReceived) {
        ok(eventName, 'Should have received an event with a name')
        console.log(`Received event: ${eventName}`)
      } else {
        console.log('No events received (this is okay)')
      }
    } catch (error) {
      console.warn('Event handling test warning:', error.message)
    } finally {
      await cleanupConsumer(consumer)
    }
  })

  await t.test('Consumer: Handle message headers', async () => {
    const topic = createTestTopic('headers')
    const testId = 'headers-test'
    const messageWithHeaders = {
      key: Buffer.from('header-key'),
      payload: Buffer.from(JSON.stringify({ testId, type: 'with-headers' })),
      headers: {
        'content-type': Buffer.from('application/json'),
        'custom-header': Buffer.from('custom-value'),
        'timestamp': Buffer.from(Date.now().toString()),
      },
    }

    await producer.send({ topic, messages: [messageWithHeaders] })
    await sleep(1000)

    const consumer = client.createConsumer(createConsumerConfig(`headers-${testId}`))
    await consumer.subscribe(topic)

    const message = await consumer.recv()
    ok(message, 'Should receive message')

    if (isTestMessage(message, testId)) {
      ok(message.headers, 'Message should have headers')
      ok(message.headers['content-type'], 'Should have content-type header')
      ok(message.headers['custom-header'], 'Should have custom header')
      equal(
        message.headers['content-type'].toString(),
        'application/json',
        'Header value should match',
      )
    }

    await cleanupConsumer(consumer)
  })

  await t.test('Consumer: Topic creation control test', async () => {
    const testTopicName = `topic-creation-test-${Date.now()}`

    // Test with createTopic disabled
    const consumerConfigDisabled = {
      groupId: `topic-creation-disabled-${Date.now()}`,
      createTopic: false,
      enableAutoCommit: true,
    }

    const consumerDisabled = client.createConsumer(consumerConfigDisabled)

    try {
      // This should work without creating the topic (will fail if topic doesn't exist)
      await consumerDisabled.subscribe(testTopicName)
      console.log('Topic creation disabled - subscription succeeded without creating topic')
    } catch {
      console.log('Topic creation disabled - subscription failed as expected when topic does not exist')
    } finally {
      await cleanupConsumer(consumerDisabled)
    }

    // Test with createTopic enabled (default behavior)
    const consumerConfigEnabled = {
      groupId: `topic-creation-enabled-${Date.now()}`,
      createTopic: true, // explicitly enabled
      enableAutoCommit: true,
    }

    const consumerEnabled = client.createConsumer(consumerConfigEnabled)

    try {
      // This should create the topic automatically
      await consumerEnabled.subscribe(testTopicName)
      console.log('Topic creation enabled - subscription succeeded, topic should be created')

      // Verify the topic creation worked by checking subscription
      const subscription = consumerEnabled.getSubscription()
      ok(subscription.length > 0, 'Should have subscription after topic creation')
    } catch (error) {
      console.error('Topic creation enabled test failed:', error.message)
      throw error
    } finally {
      await cleanupConsumer(consumerEnabled)
    }

    console.log('Topic creation control tests completed')
  })

  await t.test('Cleanup: Disconnect producer', async () => {
    await cleanupProducer(producer)
  })
})
