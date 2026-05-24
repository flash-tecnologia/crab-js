import { KafkaClient } from 'kafka-crab-js'

const kafkaClient = new KafkaClient({
  brokers: 'localhost:9092',
  clientId: 'reconnect-test',
  logLevel: 'debug',
  brokerAddressFamily: 'v4',
  configuration: {
    'auto.offset.reset': 'earliest',
  },
})

/**
 * Creates and configures a new Kafka stream consumer
 */
async function createConsumer() {
  const kafkaStream = kafkaClient.createStreamConsumer({
    groupId: `reconnect-test-3`,
    enableAutoCommit: true,
    configuration: {
      'auto.offset.reset': 'earliest',
    },
  })
  await kafkaStream.subscribe([{ topic: 'foo' }, { topic: 'bar' }])
  return kafkaStream
}

/**
 * Starts a Kafka consumer with auto-restart capability
 */
async function startConsumer() {
  let counter = 0
  let retryCount = 0
  const MAX_RETRIES = 5
  const RETRY_DELAY = 5000 // 5 seconds

  async function handleRetry() {
    if (retryCount < MAX_RETRIES) {
      retryCount++
      console.log(
        `Attempting to restart consumer (attempt ${retryCount}/${MAX_RETRIES}) in ${RETRY_DELAY / 1000} seconds...`,
      )
      setTimeout(setupConsumerWithRetry, RETRY_DELAY)
    } else {
      console.error(`Maximum retry attempts (${MAX_RETRIES}) reached. Stopping consumer.`)
      process.exit(1)
    }
  }

  async function setupConsumerWithRetry() {
    try {
      const kafkaStream = await createConsumer()
      retryCount = 0 // Reset retry count on successful connection

      console.log('Starting consumer')

      kafkaStream.on('data', (message) => {
        counter++
        console.log('>>> Message received:', {
          counter,
          payload: message.payload.toString(),
          offset: message.offset,
          partition: message.partition,
          topic: message.topic,
        })
      })

      kafkaStream.on('error', async (error) => {
        console.error('Stream error:', error)
        handleRetry()
      })

      kafkaStream.on('close', () => {
        console.log('Stream ended')
        try {
          kafkaStream.unsubscribe()
        } catch (unsubError) {
          console.error('Error unsubscribing:', unsubError)
        }
      })
    } catch (error) {
      console.error('Error setting up consumer:', error)
      handleRetry()
    }
  }

  await setupConsumerWithRetry()
}

await startConsumer()
