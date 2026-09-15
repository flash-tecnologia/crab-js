import { deepEqual, equal, match, ok, rejects } from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { setTimeout as sleep } from 'node:timers/promises'
import test from 'node:test'

import { KafkaClient } from '../../dist/index.js'
import { cleanupProducer } from './utils.mjs'

await test(
  'deleteRecords reports a partition failure while preserving partial success',
  { timeout: 90_000 },
  async () => {
    const topic = `delete-records-${randomUUID()}`
    const client = new KafkaClient({
      brokers: process.env.KAFKA_BROKERS || 'localhost:9092',
      clientId: topic,
      configuration: { 'socket.timeout.ms': 10_000 },
    })
    const newConsumer = () =>
      client.createConsumer({
        groupId: `delete-records-${randomUUID()}`,
        enableAutoCommit: false,
        fetchMetadataTimeout: 10_000,
        configuration: {
          'allow.auto.create.topics': false,
          'auto.offset.reset': 'earliest',
        },
      })

    let topicReady = false
    for (let attempt = 0; attempt < 6 && !topicReady; attempt += 1) {
      const setupConsumer = newConsumer()
      try {
        await setupConsumer.subscribe([
          {
            topic,
            createTopic: true,
            numPartitions: 1,
            replicas: 1,
            allOffsets: { position: 'Beginning' },
          },
        ])
        deepEqual(
          setupConsumer.assignment().flatMap((entry) => entry.partitionOffset.map((partition) => partition.partition)),
          [0],
        )
        topicReady = true
      } catch (error) {
        if (attempt === 5) throw error
      } finally {
        await setupConsumer.disconnect()
      }
      if (!topicReady) await sleep(500)
    }
    ok(topicReady, 'the one-partition topic must become available before producing')

    const producer = client.createProducer({
      configuration: { 'message.timeout.ms': 10_000 },
    })
    try {
      const [removed] = await producer.send({ topic, messages: [{ payload: Buffer.from('remove') }] })
      const [retained] = await producer.send({ topic, messages: [{ payload: Buffer.from('keep') }] })
      equal(removed.partition, 0)
      equal(retained.partition, 0)
      equal(removed.offset, 0)
      equal(retained.offset, 1)

      await rejects(
        () =>
          client.deleteRecords([
            {
              topic,
              partitionOffset: [{ partition: 0, offset: { offset: retained.offset } }],
            },
            {
              topic,
              partitionOffset: [{ partition: 1, offset: { offset: retained.offset } }],
            },
          ]),
        (error) => {
          ok(error instanceof Error, 'deleteRecords should reject with an Error')
          ok(error.message.includes(topic), 'error must identify the topic')
          match(error.message, /partition\D*1\b/i, 'error must identify the failed partition')
          match(error.message, /unknown.*partition|partition.*unknown/i, 'error must preserve the cause')
          match(error.message, /partially succeeded/i, 'error must warn about possible partial success')
          return true
        },
      )

      // Use a fresh consumer so messages prefetched before deletion cannot hide the result.
      const reader = newConsumer()
      try {
        await reader.subscribe([{ topic, allOffsets: { position: 'Beginning' } }])
        let first
        const deadline = Date.now() + 10_000
        while (!first && Date.now() < deadline) {
          const batch = await reader.recvBatch(1, 1_000)
          first = batch[0]
        }
        ok(first, 'the retained record must remain readable')
        equal(first.partition, 0)
        equal(first.offset, retained.offset)
        equal(first.payload.toString(), 'keep')
      } finally {
        await reader.disconnect()
      }
    } finally {
      await cleanupProducer(producer)
    }
  },
)
