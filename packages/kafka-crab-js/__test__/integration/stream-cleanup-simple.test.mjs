import { strict as assert } from 'node:assert'
import { test } from 'node:test'
import { KafkaClient } from '../../dist/index.js'
import { createConsumerConfig, createTestTopic, TEST_CONFIG } from './utils.mjs'

test('Stream Cleanup Validation', async (t) => {
  const client = new KafkaClient(TEST_CONFIG)

  await t.test('Stream destroy calls disconnect', async () => {
    const topic = createTestTopic()
    const consumerConfig = createConsumerConfig(`cleanup-${Date.now()}`)

    // Create stream consumer
    const streamConsumer = client.createStreamConsumer(consumerConfig)

    // Subscribe to topic
    await streamConsumer.subscribe(topic)

    // Track if disconnect was called
    let disconnectCalled = false
    const rawConsumer = streamConsumer.rawConsumer()
    const originalDisconnect = rawConsumer.disconnect
    rawConsumer.disconnect = async function() {
      disconnectCalled = true
      return originalDisconnect.call(this)
    }

    // Destroy the stream and wait for completion
    streamConsumer.destroy()

    // Give time for async cleanup to complete
    await new Promise(resolve => setTimeout(resolve, 100))

    // Verify disconnect was called
    assert.ok(disconnectCalled, 'Consumer disconnect should be called during stream destroy')
    assert.ok(streamConsumer.destroyed, 'Stream should be marked as destroyed')
  })

  await t.test('Multiple stream cleanup works', async () => {
    const streams = []

    // Create multiple streams
    for (let i = 0; i < 3; i++) {
      const topic = createTestTopic()
      const consumerConfig = createConsumerConfig(`multi-cleanup-${Date.now()}-${i}`)
      const streamConsumer = client.createStreamConsumer(consumerConfig)
      await streamConsumer.subscribe(topic)
      streams.push(streamConsumer)
    }

    // Destroy all streams
    for (const stream of streams) {
      stream.destroy()
    }

    // Wait for cleanup
    await new Promise(resolve => setTimeout(resolve, 200))

    // Verify all are destroyed
    for (let i = 0; i < streams.length; i++) {
      assert.ok(streams[i].destroyed, `Stream ${i} should be destroyed`)
    }
  })

  await t.test('Batch stream cleanup works', async () => {
    const topic = createTestTopic()
    const consumerConfig = createConsumerConfig(`batch-cleanup-${Date.now()}`)

    // Create batch stream (batchSize > 1)
    const batchStream = client.createStreamConsumer({
      ...consumerConfig,
      batchSize: 5,
      batchTimeout: 100,
    })

    await batchStream.subscribe(topic)

    // Track disconnect calls
    let disconnectCalled = false
    const rawConsumer = batchStream.rawConsumer()
    const originalDisconnect = rawConsumer.disconnect
    rawConsumer.disconnect = async function() {
      disconnectCalled = true
      return originalDisconnect.call(this)
    }

    // Destroy and wait
    batchStream.destroy()
    await new Promise(resolve => setTimeout(resolve, 100))

    // Verify cleanup
    assert.ok(disconnectCalled, 'Batch stream should call disconnect during cleanup')
    assert.ok(batchStream.destroyed, 'Batch stream should be destroyed')
  })
})
