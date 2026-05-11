import { Admin as PlatformaticKafkaAdmin } from '@platformatic/kafka'
import { KafkaClient, type MessageProducer } from 'kafka-crab-js'
import { setTimeout as sleep } from 'node:timers/promises'
import { brokers, partitionCount, topic } from './definitions.js'
import { readNonNegativeInteger, readPositiveInteger } from './env.js'
import { createBenchmarkMessage, createBenchmarkPartitionKeys } from './messages.js'

type PlatformaticKafkaAdminClient = InstanceType<typeof PlatformaticKafkaAdmin>

const topicRecreateTimeoutMs = readPositiveInteger('BENCHMARK_TOPIC_RECREATE_TIMEOUT_MS', 30_000)
const topicRecreatePollMs = readPositiveInteger('BENCHMARK_TOPIC_RECREATE_POLL_MS', 500)

const client = new KafkaClient({
  brokers: brokers.join(','),
  clientId: 'benchmark-setup',
  securityProtocol: 'Plaintext',
  logLevel: 'info',
  brokerAddressFamily: 'v4',
})

export async function prepareTopics() {
  console.log(`Preparing topic: ${topic}`)

  const admin = new PlatformaticKafkaAdmin({
    clientId: 'benchmark-setup-admin',
    bootstrapBrokers: brokers,
    strict: true,
    timeout: topicRecreateTimeoutMs,
  })

  try {
    const existingTopics = await admin.listTopics()
    if (existingTopics.includes(topic)) {
      console.log(`Deleting existing topic: ${topic}`)
      await admin.deleteTopics({ topics: [topic] })
      await waitForTopicState(admin, false)
    }

    console.log(`Creating topic ${topic} with ${partitionCount} partitions`)
    await admin.createTopics({
      topics: [topic],
      partitions: partitionCount,
      replicas: 1,
    })
    await waitForTopicState(admin, true)
    console.log(`Topic ${topic} is ready`)
  } catch (error) {
    console.error('Failed to prepare topic:', error)
    throw error
  } finally {
    try {
      await admin.close()
    } catch {
      // Ignore cleanup failures so the original setup error is preserved.
    }
  }
}

async function waitForTopicState(admin: PlatformaticKafkaAdminClient, shouldExist: boolean) {
  const deadline = Date.now() + topicRecreateTimeoutMs

  while (Date.now() < deadline) {
    const topics = await admin.listTopics()
    if (topics.includes(topic) === shouldExist) {
      return
    }

    await sleep(topicRecreatePollMs)
  }

  const expectation = shouldExist ? 'be created' : 'be deleted'
  throw new Error(`Timed out waiting for topic "${topic}" to ${expectation}`)
}

export async function prepareConsumerData() {
  const producer = client.createProducer()
  const partitionKeys = createBenchmarkPartitionKeys(partitionCount)

  const benchmarkIterations = readPositiveInteger('BENCHMARK_ITERATIONS', 100_000)
  const warmupMessages = readNonNegativeInteger('BENCHMARK_WARMUP_MESSAGES', 0)
  const max = readPositiveInteger('BENCHMARK_SETUP_MESSAGES', benchmarkIterations + warmupMessages)
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
