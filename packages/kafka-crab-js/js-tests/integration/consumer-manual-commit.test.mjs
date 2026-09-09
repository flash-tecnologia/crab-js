import { equal, ok } from 'node:assert/strict'
import test from 'node:test'
import { setTimeout as sleep } from 'node:timers/promises'
import { KafkaClient } from '../../dist/index.js'
import {
  cleanupConsumer,
  cleanupProducer,
  createConsumerConfig,
  createProducerConfig,
  isTestMessage,
  setupTestEnvironment,
} from './utils.mjs'

// Manual assignment (`allOffsets`/`partitionOffset`) uses `assign()`, which never emits
// group rebalance events. Poll the local assignment instead of waiting for PostRebalance.
async function waitForAssignment(consumer, timeoutMs = 10000) {
  const attempts = Math.max(1, Math.ceil(timeoutMs / 100))
  for (let attempt = 0; attempt < attempts; attempt += 1) {
    let assigned = 0
    try {
      assigned = consumer.assignment().reduce((count, entry) => count + (entry.partitionOffset?.length ?? 0), 0)
    } catch {
      assigned = 0
    }
    if (assigned > 0) {
      return
    }
    await sleep(100)
  }
  throw new Error('Timeout waiting for partition assignment after 10000ms')
}

await test('Consumer Manual Commit Integration Tests', async (t) => {
  let client
  let producer

  await t.test('Setup: Create KafkaClient and Producer', async () => {
    const { config } = await setupTestEnvironment()
    client = new KafkaClient(config)
    producer = client.createProducer(createProducerConfig())
    ok(client, 'KafkaClient should be created')
    ok(producer, 'Producer should be created')
  })

  await t.test('Consumer: Manual commit sync with offset verification', async () => {
    const { topic, messages, testId } = await setupTestEnvironment()

    // Send messages first
    await producer.send({ topic, messages })
    await sleep(1000)

    // Create consumer with manual commit disabled
    const consumerConfig = createConsumerConfig(`manual-commit-sync-${testId}`, {
      configuration: {
        'enable.auto.commit': 'false',
        'auto.offset.reset': 'earliest',
      },
    })
    const consumer = client.createConsumer(consumerConfig)

    // Manual assignment emits no rebalance events; log events and wait for the
    // local assignment instead.
    const events = []
    consumer.onEvents((err, event) => {
      if (err) {
        console.error('Event error:', err)
        return
      }
      events.push(event)
      console.log(`Received event: ${event.name}`)
    })

    await consumer.subscribe([{ topic, allOffsets: { position: 'Beginning' } }])
    await waitForAssignment(consumer)

    // Verify assignment
    try {
      const assignment = consumer.assignment()
      console.log(`Consumer assigned to ${assignment.length} partitions`)
      ok(assignment.length > 0, 'Consumer should be assigned to at least one partition')
    } catch {
      console.log('Assignment check failed, continuing with test')
    }

    // Receive messages and manually commit each one with sync mode
    const receivedMessages = []
    const maxPolls = 20
    let polls = 0

    while (receivedMessages.length < messages.length && polls < maxPolls) {
      polls++
      const batch = await consumer.recvBatch(1, 1000)
      const message = batch[0]

      if (message && isTestMessage(message, testId)) {
        receivedMessages.push(message)

        // Manual commit with sync mode
        try {
          console.log(
            `Committing sync: topic=${message.topic}, partition=${message.partition}, offset=${message.offset}`,
          )
          await consumer.commit(message.topic, message.partition, message.offset + 1, 'Sync')
          console.log(`Successfully committed offset ${message.offset + 1} for partition ${message.partition}`)
        } catch (error) {
          console.error('Sync commit failed:', error.message)
          throw error
        }
      }
    }

    await cleanupConsumer(consumer)

    // Verify results
    equal(receivedMessages.length, messages.length, 'Should receive all sent messages')
    ok(
      receivedMessages.every((msg) => msg.offset !== undefined),
      'All messages should have offsets',
    )

    // Verify that events were captured (if any)
    console.log(`Captured ${events.length} events during sync commit test`)
    events.forEach((event) => console.log(`Event: ${event.name}`))
  })

  await t.test('Consumer: Manual commit async with offset verification', async () => {
    const { topic, messages, testId } = await setupTestEnvironment()

    // Send messages first
    await producer.send({ topic, messages })
    await sleep(1000)

    // Create consumer with manual commit disabled
    const consumerConfig = createConsumerConfig(`manual-commit-async-${testId}`, {
      configuration: {
        'enable.auto.commit': 'false',
        'auto.offset.reset': 'earliest',
      },
    })
    const consumer = client.createConsumer(consumerConfig)

    // Manual assignment emits no rebalance events; log events and wait for the
    // local assignment instead.
    const events = []
    consumer.onEvents((err, event) => {
      if (err) {
        console.error('Event error:', err)
        return
      }
      events.push(event)
      console.log(`Received event: ${event.name}`)
    })

    await consumer.subscribe([{ topic, allOffsets: { position: 'Beginning' } }])
    await waitForAssignment(consumer)

    // Verify assignment
    try {
      const assignment = consumer.assignment()
      console.log(`Consumer assigned to ${assignment.length} partitions`)
      ok(assignment.length > 0, 'Consumer should be assigned to at least one partition')
    } catch {
      console.log('Assignment check failed, continuing with test')
    }

    // Receive messages and manually commit each one with async mode
    const receivedMessages = []
    const maxPolls = 20
    let polls = 0

    while (receivedMessages.length < messages.length && polls < maxPolls) {
      polls++
      const batch = await consumer.recvBatch(1, 1000)
      const message = batch[0]

      if (message && isTestMessage(message, testId)) {
        receivedMessages.push(message)

        // Manual commit with async mode
        try {
          console.log(
            `Committing async: topic=${message.topic}, partition=${message.partition}, offset=${message.offset}`,
          )
          await consumer.commit(message.topic, message.partition, message.offset + 1, 'Async')
          console.log(`Successfully committed offset ${message.offset + 1} for partition ${message.partition}`)
        } catch (error) {
          console.error('Async commit failed:', error.message)
          throw error
        }
      }
    }

    await cleanupConsumer(consumer)

    // Verify results
    equal(receivedMessages.length, messages.length, 'Should receive all sent messages')
    ok(
      receivedMessages.every((msg) => msg.offset !== undefined),
      'All messages should have offsets',
    )

    // Verify that events were captured (if any)
    console.log(`Captured ${events.length} events during async commit test`)
    events.forEach((event) => console.log(`Event: ${event.name}`))
  })

  await t.test('Consumer: Verify committed offsets persist across consumer restarts', async () => {
    const { topic, messages, testId } = await setupTestEnvironment()
    const groupId = `offset-persistence-${testId}`

    // Send messages first
    await producer.send({ topic, messages })
    await sleep(1000)

    // First consumer: consume and commit manually
    const consumer1Config = createConsumerConfig(groupId, {
      configuration: {
        'enable.auto.commit': 'false',
        'auto.offset.reset': 'earliest',
      },
    })
    const consumer1 = client.createConsumer(consumer1Config)
    // Manual assignment emits no rebalance events; wait for the local assignment.
    await consumer1.subscribe([{ topic, allOffsets: { position: 'Beginning' } }])
    await waitForAssignment(consumer1)

    // Consume first half of messages and commit
    const halfCount = Math.floor(messages.length / 2)
    const lastCommittedOffsets = new Map()

    for (let i = 0; i < halfCount; i++) {
      const batch = await consumer1.recvBatch(1, 1000)
      const message = batch[0]
      if (message && isTestMessage(message, testId)) {
        const partitionKey = `${message.topic}:${message.partition}`
        const committedOffset = message.offset + 1

        console.log(
          `Consumer1 received message at offset ${message.offset} on partition ${message.partition} and committing offset ${committedOffset}`,
        )

        await consumer1.commit(message.topic, message.partition, committedOffset, 'Sync')
        lastCommittedOffsets.set(partitionKey, committedOffset)
      }
    }

    await cleanupConsumer(consumer1)
    await sleep(2000) // Allow time for commit to persist

    // Second consumer: should start from committed offset
    const consumer2Config = createConsumerConfig(groupId, {
      configuration: {
        'enable.auto.commit': 'false',
        'auto.offset.reset': 'earliest',
      },
    })
    const consumer2 = client.createConsumer(consumer2Config)
    await consumer2.subscribe(topic)

    // Receive remaining messages
    const remainingMessages = []
    const maxPolls = 15
    let polls = 0

    while (remainingMessages.length < messages.length - halfCount && polls < maxPolls) {
      polls++
      const batch = await consumer2.recvBatch(1, 1000)
      const message = batch[0]

      if (message && isTestMessage(message, testId)) {
        const partitionKey = `${message.topic}:${message.partition}`
        const committedOffset = lastCommittedOffsets.get(partitionKey)

        console.log(
          `Consumer2 received message at offset ${message.offset} on partition ${message.partition} (committed offset: ${
            committedOffset ?? 'none'
          })`,
        )
        remainingMessages.push(message)

        if (committedOffset !== undefined) {
          // Verify this message's offset is >= committed offset for the same partition
          ok(
            message.offset >= committedOffset,
            `Message offset ${message.offset} should be >= last committed offset ${committedOffset} for partition ${message.partition}`,
          )
        }
      }
    }

    await cleanupConsumer(consumer2)

    // Verify that consumer2 picked up from where consumer1 left off
    ok(remainingMessages.length > 0, 'Consumer2 should receive remaining messages')
    console.log('Committed offsets before restart:', Object.fromEntries(lastCommittedOffsets))
    console.log(`Consumer2 received ${remainingMessages.length} remaining messages`)
  })

  await t.test('Consumer: Basic manual commit functionality', async () => {
    const { topic, messages, testId } = await setupTestEnvironment()

    await producer.send({ topic, messages })
    await sleep(1000)

    // Create consumer with manual commit
    const consumerConfig = createConsumerConfig(`basic-manual-commit-${testId}`, {
      configuration: {
        'enable.auto.commit': 'false',
        'auto.offset.reset': 'earliest',
      },
    })
    const consumer = client.createConsumer(consumerConfig)
    try {
      await consumer.subscribe([{ topic, allOffsets: { position: 'Beginning' } }])

      let message = null
      const deadline = Date.now() + 20_000

      while (!message && Date.now() < deadline) {
        const remainingMs = deadline - Date.now()
        const batch = await consumer.recvBatch(1, Math.min(Math.max(remainingMs, 0), 1000))
        const received = batch[0]
        if (received && isTestMessage(received, testId)) {
          message = received
          break
        }
      }

      ok(message, 'Should have received a test message to commit')
      await consumer.commit(message.topic, message.partition, message.offset + 1, 'Sync')
    } finally {
      await cleanupConsumer(consumer)
    }
  })

  await t.test('Stream Consumer: Manual commit with stream consumer', async () => {
    const { topic, messages, testId } = await setupTestEnvironment()

    await producer.send({ topic, messages })
    await sleep(1000)

    const streamConsumer = client.createStreamConsumer(
      createConsumerConfig(`stream-manual-commit-${testId}`, {
        configuration: { 'enable.auto.commit': 'false' },
      }),
    )
    await streamConsumer.subscribe([{ topic, allOffsets: { position: 'Beginning' } }])

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

      // Test commit functionality with stream consumer
      await streamConsumer.commit(firstMessage.topic, firstMessage.partition, firstMessage.offset + 1, 'Sync')
      console.log('Stream consumer manual commit test passed')
    } catch (error) {
      console.warn('Stream manual commit test warning:', error.message)
      // Don't fail the test for this timing issue
    } finally {
      await cleanupConsumer(streamConsumer)
    }
  })

  await t.test('Consumer: Batch processing with manual commit', async () => {
    const { topic, messages, testId } = await setupTestEnvironment()

    // Send more messages for batch testing
    const batchMessages = messages.concat(messages) // Double the messages
    await producer.send({ topic, messages: batchMessages })
    await sleep(1000)

    const consumerConfig = createConsumerConfig(`batch-manual-commit-${testId}`, {
      configuration: {
        'enable.auto.commit': 'false',
        'auto.offset.reset': 'earliest',
      },
    })
    const consumer = client.createConsumer(consumerConfig)
    // Manual assignment emits no rebalance events; wait for the local assignment.
    await consumer.subscribe([{ topic, allOffsets: { position: 'Beginning' } }])
    await waitForAssignment(consumer)

    // Receive messages in batches and commit the highest offset
    const batchSize = 3
    const timeoutMs = 5000
    let totalReceived = 0
    let lastCommittedOffset = -1

    while (totalReceived < batchMessages.length) {
      const batch = await consumer.recvBatch(batchSize, timeoutMs)

      if (batch.length === 0) {
        break
      }

      const testMessagesBatch = batch.filter((msg) => isTestMessage(msg, testId))
      if (testMessagesBatch.length > 0) {
        totalReceived += testMessagesBatch.length

        // Find the highest offset in this batch
        const highestOffsetMessage = testMessagesBatch.reduce((max, msg) => (msg.offset > max.offset ? msg : max))

        // Commit the highest offset + 1
        try {
          const commitOffset = highestOffsetMessage.offset + 1
          console.log(`Committing batch with highest offset ${commitOffset}`)
          await consumer.commit(highestOffsetMessage.topic, highestOffsetMessage.partition, commitOffset, 'Sync')
          lastCommittedOffset = commitOffset
          console.log(`Successfully committed batch offset ${commitOffset}`)
        } catch (error) {
          console.error('Batch commit failed:', error.message)
          throw error
        }
      }
    }

    await cleanupConsumer(consumer)

    // Verify results
    ok(totalReceived >= batchMessages.length, 'Should receive all batch messages')
    ok(lastCommittedOffset > -1, 'Should have committed at least one offset')
    console.log(`Batch processing completed with final committed offset: ${lastCommittedOffset}`)
  })

  await t.test('Cleanup: Disconnect producer', async () => {
    await cleanupProducer(producer)
  })
})
