import { KafkaClient, type MessageProducer } from 'kafka-crab-js'
import { Buffer } from 'node:buffer'
import { randomUUID } from 'node:crypto'
import { brokers, topic } from './definitions.ts'

const client = new KafkaClient({
  brokers: brokers.join(','),
  clientId: 'benchmark-setup',
  securityProtocol: 'Plaintext',
  logLevel: 'info',
  brokerAddressFamily: 'v4',
})

export async function prepareTopics() {
  console.log(`Preparing topic: ${topic}`)

  // Create a temporary consumer with createTopic: true to ensure topic exists
  // This leverages kafka-crab's built-in topic creation functionality
  const tempConsumer = client.createConsumer({
    groupId: `setup-${randomUUID()}`,
    configuration: {
      'auto.offset.reset': 'earliest',
    },
  })

  try {
    // Subscribe to the topic to trigger topic creation if needed
    await tempConsumer.subscribe([{ topic, createTopic: true, numPartitions: brokers.length }])
    console.log(`Topic ${topic} is ready`)

    // Wait a moment for topic to be fully created
    await new Promise(resolve => setTimeout(resolve, 2000))
  } catch (error) {
    console.error('Failed to prepare topic:', error)
    throw error
  } finally {
    tempConsumer.unsubscribe()
    await tempConsumer.disconnect()
  }
}

export async function prepareConsumerData() {
  const producer = client.createProducer()

  const max = 1e6
  const batchSize = 50_000

  console.log(`Starting to produce ${max} messages...`)

  for (let i = 0; i < max; i += batchSize) {
    const messages: MessageProducer[] = []
    const batchEnd = Math.min(i + batchSize, max)

    for (let j = i; j < batchEnd; j++) {
      messages.push({
        payload: Buffer.from(`{"message": "message index ${j}", "index": ${j}, "date": "${new Date().toISOString()}"}`),
        key: Buffer.from(`key-${j}`),
        headers: {
          [`header-key-${j}`]: Buffer.from(`header-value-${j}`),
        },
      })
    }

    try {
      await producer.send({ topic, messages })

      if ((i + batchEnd - i) % 2000 === 0) {
        const progress = ((i + batchEnd - i) / max * 100).toFixed(1)
        console.log(`Produced ${i + batchEnd - i}/${max} messages (${progress}%)`)
      }
    } catch (error) {
      console.error(`Failed to send batch starting at ${i}:`, error)
      throw error
    }
  }

  console.log(`Successfully produced ${max} messages`)
}

export async function setup() {
  console.log('Setting up benchmark environment...')

  try {
    await prepareTopics()
    await prepareConsumerData()
    console.log('Benchmark setup completed successfully!')
  } catch (error) {
    console.error('Benchmark setup failed:', error)
    throw error
  }
}

// If this file is run directly, execute setup
if (import.meta.url === `file://${process.argv[1]}`) {
  setup()
    .then(() => {
      console.log('Setup complete!')
      process.exit(0)
    })
    .catch(error => {
      console.error('Setup failed:', error)
      process.exit(1)
    })
}
