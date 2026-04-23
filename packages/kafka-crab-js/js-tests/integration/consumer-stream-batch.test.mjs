import { strict as assert } from 'node:assert'
import { test } from 'node:test'
import { KafkaClient } from '../../dist/index.js'
import { cleanupConsumer, createConsumerConfig, createTestTopic, TEST_CONFIG } from './utils.mjs'

test('Consumer Stream Batch Mode Integration Tests', async (t) => {
  const client = new KafkaClient(TEST_CONFIG)

  await t.test('Setup: Create KafkaClient and Producer', async () => {
    // Basic setup test
    assert.ok(client, 'KafkaClient should be created')
  })

  await t.test('Stream Batch: Create batch consumer', async () => {
    const consumerConfig = createConsumerConfig('batch-create')

    // Create single mode consumer (batchSize = 1 or undefined)
    const singleConsumer = client.createStreamConsumer(consumerConfig)
    assert.ok(singleConsumer, 'Single consumer should be created')
    await cleanupConsumer(singleConsumer)

    // Create batch mode consumer (batchSize > 1)
    const batchConsumer = client.createStreamConsumer({
      ...consumerConfig,
      batchSize: 10,
      batchTimeout: 1000,
    })
    assert.ok(batchConsumer, 'Batch consumer should be created')
    await cleanupConsumer(batchConsumer)
  })

  await t.test('Stream Batch: Default batch configuration', async () => {
    const consumerConfig = createConsumerConfig('batch-defaults')
    const streamConsumer = client.createStreamConsumer({
      ...consumerConfig,
      batchSize: 10, // > 1 enables batch mode with defaults
    })

    const config = streamConsumer.getBatchConfig()
    assert.equal(config.batchSize, 10, 'Batch size should match what was set')
    assert.equal(config.batchTimeout, 1000, 'Default timeout should be 1000ms')

    await cleanupConsumer(streamConsumer)
  })

  await t.test('Stream Batch: Custom batch configuration', async () => {
    const consumerConfig = createConsumerConfig('batch-custom')
    const streamConsumer = client.createStreamConsumer({
      ...consumerConfig,
      batchSize: 5,
      batchTimeout: 2000,
    })

    const config = streamConsumer.getBatchConfig()
    assert.equal(config.batchSize, 5, 'Batch size should match custom value')
    assert.equal(config.batchTimeout, 2000, 'Timeout should match custom value')

    await cleanupConsumer(streamConsumer)
  })

  await t.test('Stream Batch: Batch consumer functionality', async () => {
    const topic = createTestTopic()
    const consumerConfig = createConsumerConfig('batch-functionality')

    // Create batch consumer
    const batchConsumer = client.createStreamConsumer({
      ...consumerConfig,
      batchSize: 3,
      batchTimeout: 1000,
    })

    // Subscribe to topic
    await batchConsumer.subscribe(topic)

    // Verify it's a stream
    assert.ok(typeof batchConsumer.on === 'function', 'Should have stream methods')
    assert.ok(typeof batchConsumer.read === 'function', 'Should have stream methods')

    await cleanupConsumer(batchConsumer)
  })

  await t.test('Stream Batch: Large batch size accepted', async () => {
    const consumerConfig = createConsumerConfig('batch-large')
    const streamConsumer = client.createStreamConsumer({
      ...consumerConfig,
      batchSize: 1000, // Large batch size
      batchTimeout: 5000,
    })

    const config = streamConsumer.getBatchConfig()
    assert.equal(config.batchSize, 1000, 'Large batch size should be accepted')
    assert.equal(config.batchTimeout, 5000, 'Custom timeout should be set')

    await cleanupConsumer(streamConsumer)
  })

  await t.test('Cleanup: Test completion', async () => {
    // Cleanup test
    assert.ok(true, 'All batch consumer tests completed')
  })
})
