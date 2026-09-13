import { equal, ok } from 'node:assert/strict'
import test from 'node:test'
import { KafkaClient } from '../../dist/index.js'
import {
  cleanupConsumer,
  cleanupProducer,
  createConsumerConfig,
  createProducerConfig,
  setupTestEnvironment,
} from '../integration/utils.mjs'

// Manual reproduction; the release integration suite also verifies this broker
// rejection, a Sync control, and callback delivery without receive polling.
test('Diagnostic: observe broker error context in an Async commit callback', { timeout: 30000 }, async () => {
  const { config, topic, messages, testId } = await setupTestEnvironment()
  const client = new KafkaClient(config)
  const producer = client.createProducer(createProducerConfig())
  let consumer
  try {
    const metadata = await producer.send({ topic, messages })
    const invalidPartition = metadata[0].partition + 10_000
    const committedOffset = metadata[0].offset + 1
    consumer = client.createConsumer(
      createConsumerConfig(`async-commit-error-${testId}`, {
        enableAutoCommit: false,
        configuration: { 'enable.auto.commit': 'false' },
      }),
    )
    let event
    let listenerError
    consumer.onEvents((error, received) => {
      if (error) listenerError = error
      if (received?.name === 'CommitCallback') {
        console.log(`ASYNC_COMMIT_CALLBACK ${JSON.stringify(received)}`)
        if (received.payload.error) event = received
      }
    })
    await consumer.subscribe(topic)
    await consumer.recvBatch(1, 1000)
    await consumer.commit(topic, invalidPartition, committedOffset, 'Async')
    const deadline = Date.now() + 10_000
    while (!event && !listenerError && Date.now() < deadline) await consumer.recvBatch(1, 100)
    if (listenerError) throw listenerError
    ok(event, 'Missing failed commit callback within 10s while polling')
    ok(event.payload.error.includes('Broker:'), `Expected broker error, received: ${event.payload.error}`)
    const topicEntry = event.payload.tpl.find((entry) => entry.topic === topic)
    ok(topicEntry, 'Callback must preserve the committed topic')
    const partitionEntry = topicEntry.partitionOffset.find((entry) => entry.partition === invalidPartition)
    ok(partitionEntry, 'Callback must preserve the committed partition')
    equal(partitionEntry.offset.offset, committedOffset)
  } finally {
    await cleanupConsumer(consumer)
    await cleanupProducer(producer)
  }
})
