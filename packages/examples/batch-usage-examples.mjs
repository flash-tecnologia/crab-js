#!/usr/bin/env node

/**
 * Kafka Crab JS - Simple Batch Processing Demo
 *
 * Produces test messages and demonstrates batch consumption.
 */

import { KafkaClient } from 'kafka-crab-js'

// Configuration
const config = {
  brokers: process.env.KAFKA_BROKERS || 'localhost:9092',
  clientId: 'batch-demo',
  logLevel: 'info',
  brokerAddressFamily: 'v4',
}

const TOPIC_NAME = 'batch-test-topic'
const MESSAGE_COUNT = 100000

/**
 * Populate the topic with test messages
 */
async function populateTestTopic() {
  console.log(`📝 Populating ${TOPIC_NAME} with ${MESSAGE_COUNT.toLocaleString()} messages...`)

  const kafkaClient = new KafkaClient(config)
  const producer = kafkaClient.createProducer()

  const startTime = Date.now()
  const batchSize = 10000

  for (let i = 0; i < MESSAGE_COUNT; i += batchSize) {
    const messages = []
    const currentBatchSize = Math.min(batchSize, MESSAGE_COUNT - i)

    for (let j = 0; j < currentBatchSize; j++) {
      messages.push({
        payload: Buffer.from(`Message ${i + j}: ${Date.now()}`),
      })
    }

    await producer.send({ topic: TOPIC_NAME, messages })

    if ((i + currentBatchSize) % 50000 === 0) {
      const progress = ((i + currentBatchSize) / MESSAGE_COUNT * 100).toFixed(1)
      console.log(`   Progress: ${progress}%`)
    }
  }

  const totalTime = Date.now() - startTime
  const rate = (MESSAGE_COUNT / totalTime) * 1000
  console.log(`✅ Sent ${MESSAGE_COUNT.toLocaleString()} messages in ${totalTime}ms (${rate.toFixed(0)} msg/sec)\n`)
}

/**
 * Demonstrate batch consumption
 */
async function batchConsumptionDemo() {
  console.log('🚀 Batch consumption demo...')

  const kafkaClient = new KafkaClient(config)
  const consumer = kafkaClient.createConsumer({
    groupId: `batch-demo-${Date.now()}`, // Unique group ID to avoid offset issues
    enableAutoCommit: true,
    configuration: {
      'auto.offset.reset': 'earliest', // Start from beginning
    },
  })

  await consumer.subscribe([{ topic: TOPIC_NAME }])

  let totalProcessed = 0
  let emptyBatches = 0
  const startTime = Date.now()

  console.log('📊 Reading messages in batches of 10...')

  while (totalProcessed < MESSAGE_COUNT && emptyBatches < 5) {
    const messages = await consumer.recvBatch(10, 2000)

    if (messages.length === 0) {
      emptyBatches++
      console.log(`   Empty batch ${emptyBatches}/5 - waiting for messages...`)
      continue
    }

    emptyBatches = 0 // Reset counter on successful batch
    totalProcessed += messages.length

    if (totalProcessed % 1000 === 0 || totalProcessed < 100) {
      const elapsed = Date.now() - startTime
      const rate = (totalProcessed / elapsed) * 1000
      const progress = (totalProcessed / MESSAGE_COUNT * 100).toFixed(1)
      console.log(
        `   Processed: ${totalProcessed.toLocaleString()}/${MESSAGE_COUNT.toLocaleString()} (${progress}%) - ${
          rate.toFixed(0)
        } msg/sec`,
      )
    }
  }

  const totalTime = Date.now() - startTime
  const finalRate = totalProcessed > 0 ? (totalProcessed / totalTime) * 1000 : 0
  console.log(
    `✅ Batch processing: ${totalProcessed.toLocaleString()} messages in ${totalTime}ms (${
      finalRate.toFixed(0)
    } msg/sec)\n`,
  )

  consumer.unsubscribe()
}

// Main demo
console.log('🦀 Kafka Crab JS - Batch Processing Demo')
console.log('='.repeat(50))

try {
  await populateTestTopic()

  console.log('⏳ Waiting for commit...')
  await new Promise(resolve => setTimeout(resolve, 2000))

  await batchConsumptionDemo()

  console.log('🎉 Demo completed!')
} catch (error) {
  console.error('❌ Error:', error)
}
