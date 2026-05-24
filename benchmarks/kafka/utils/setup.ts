import { KafkaClient, type MessageProducer } from 'kafka-crab-js'
import { brokers, partitionCount, topic } from './definitions.js'
import { readPositiveInteger } from './env.js'
import { createBenchmarkMessage, createBenchmarkPartitionKeys } from './messages.js'

const topicPrepareTimeoutMs = readPositiveInteger('BENCHMARK_TOPIC_PREPARE_TIMEOUT_MS', 30_000)

const client = new KafkaClient({
  brokers: brokers.join(','),
  clientId: 'benchmark-setup',
  securityProtocol: 'Plaintext',
  logLevel: 'info',
  brokerAddressFamily: 'v4',
})

export async function prepareTopics() {
  console.log(`Ensuring topic ${topic} exists (${partitionCount} partitions when created)`)

  const consumer = client.createConsumer({
    groupId: `benchmark-setup-${process.pid}`,
    enableAutoCommit: false,
    fetchMetadataTimeout: topicPrepareTimeoutMs,
    configuration: {
      'auto.offset.reset': 'earliest',
      'enable.auto.commit': false,
    },
  })

  try {
    await consumer.subscribe([
      {
        topic,
        createTopic: true,
        numPartitions: partitionCount,
        replicas: 1,
        allOffsets: { position: 'Beginning' },
      },
    ])
    console.log(`Topic ${topic} is ready`)
  } catch (error) {
    console.error('Failed to prepare topic:', error)
    throw error
  } finally {
    try {
      await consumer.disconnect()
    } catch {
      // Ignore cleanup failures so the original setup error is preserved.
    }
  }
}

export async function prepareConsumerData() {
  const producer = client.createProducer()
  const partitionKeys = createBenchmarkPartitionKeys(partitionCount)

  const benchmarkIterations = readPositiveInteger('BENCHMARK_ITERATIONS', 100_000)
  const max = readPositiveInteger('BENCHMARK_SETUP_MESSAGES', benchmarkIterations)
  const batchSize = readPositiveInteger('BENCHMARK_SETUP_BATCH_SIZE', 10_000)

  console.log(`Starting to produce ${max} messages...`)

  try {
    for (let i = 0; i < max; i += batchSize) {
      const messages: MessageProducer[] = []
      const batchEnd = Math.min(i + batchSize, max)

      for (let j = i; j < batchEnd; j++) {
        messages.push(createBenchmarkMessage(j, partitionKeys))
      }

      await producer.send({ topic, messages })

      const produced = batchEnd
      const progress = ((produced / max) * 100).toFixed(1)
      console.log(`Produced ${produced}/${max} messages (${progress}%)`)
    }
  } catch (error) {
    console.error('Failed to produce benchmark data:', error)
    throw error
  } finally {
    await producer.flush()
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
