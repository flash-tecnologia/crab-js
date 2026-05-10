import { KafkaClient, type MessageProducer } from 'kafka-crab-js'
import { randomUUID } from 'node:crypto'
import { brokers, partitionCount, topic } from './definitions.js'
import { createBenchmarkMessage, createBenchmarkPartitionKeys } from './messages.js'

const client = new KafkaClient({
  brokers: brokers.join(','),
  clientId: 'benchmark-setup',
  securityProtocol: 'Plaintext',
  logLevel: 'info',
  brokerAddressFamily: 'v4',
})

function readPositiveInteger(name: string, fallback: number): number {
  const raw = process.env[name]
  if (!raw) {
    return fallback
  }

  const parsed = Number(raw)
  return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : fallback
}

function readNonNegativeInteger(name: string, fallback: number): number {
  const raw = process.env[name]
  if (!raw) {
    return fallback
  }

  const parsed = Number(raw)
  return Number.isFinite(parsed) && parsed >= 0 ? Math.floor(parsed) : fallback
}

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
    await tempConsumer.subscribe([{ topic, createTopic: true, numPartitions: partitionCount }])
    console.log(`Topic ${topic} is ready`)

    // Wait a moment for topic to be fully created
    await new Promise((resolve) => setTimeout(resolve, 2000))
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
  const partitionKeys = createBenchmarkPartitionKeys(partitionCount)

  const benchmarkIterations = readPositiveInteger('BENCHMARK_ITERATIONS', 100_000)
  const warmupMessages = readNonNegativeInteger('BENCHMARK_WARMUP_MESSAGES', 0)
  const max = readPositiveInteger('BENCHMARK_SETUP_MESSAGES', benchmarkIterations + warmupMessages)
  const batchSize = readPositiveInteger('BENCHMARK_SETUP_BATCH_SIZE', 10_000)

  console.log(`Starting to produce ${max} messages...`)

  for (let i = 0; i < max; i += batchSize) {
    const messages: MessageProducer[] = []
    const batchEnd = Math.min(i + batchSize, max)

    for (let j = i; j < batchEnd; j++) {
      messages.push(createBenchmarkMessage(j, partitionKeys))
    }

    try {
      await producer.send({ topic, messages })

      const produced = batchEnd
      const progress = ((produced / max) * 100).toFixed(1)
      console.log(`Produced ${produced}/${max} messages (${progress}%)`)
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
    .catch((error) => {
      console.error('Setup failed:', error)
      process.exit(1)
    })
}
