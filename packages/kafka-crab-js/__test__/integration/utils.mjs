import { nanoid } from 'nanoid'
import { Buffer } from 'node:buffer'

export const TEST_CONFIG = {
  brokers: process.env.KAFKA_BROKERS || 'localhost:9092',
  clientId: `kafka-crab-integration-${nanoid(6)}`,
  logLevel: process.env.KAFKA_LOG_LEVEL || 'info',
  brokerAddressFamily: 'v4',
}

export function createTestTopic(suffix = '') {
  return `test-topic-${nanoid(6)}${suffix ? `-${suffix}` : ''}`
}

export function createTestMessages(count = 3, testId = nanoid(6)) {
  return Array.from({ length: count }, (_, i) => ({
    key: Buffer.from(`key-${testId}-${i}`),
    headers: {
      'test-header': Buffer.from(`header-value-${i}`),
      'test-id': Buffer.from(testId),
      'message-index': Buffer.from(i.toString()),
    },
    payload: Buffer.from(JSON.stringify({
      _id: i,
      testId,
      name: `Test Message ${i}`,
      timestamp: Date.now(),
    })),
  }))
}

export function isTestMessage(message, testId) {
  try {
    const payload = JSON.parse(message.payload.toString())
    return payload.testId === testId
  } catch {
    return false
  }
}

export function createConsumerConfig(groupId, options = {}) {
  return {
    groupId,
    logLevel: 'debug',
    configuration: {
      'auto.offset.reset': 'earliest',
      'enable.auto.commit': 'true',
      'auto.commit.interval.ms': '1000',
      ...options.configuration,
    },
    ...options,
  }
}

export function createProducerConfig(options = {}) {
  return {
    configuration: {
      'message.timeout.ms': '30000',
      'request.timeout.ms': '30000',
      ...options.configuration,
    },
    ...options,
  }
}

export function waitForMessages(consumer, expectedCount, testId, timeoutMs = 30000) {
  return new Promise((resolve, reject) => {
    const messages = []
    const timeout = setTimeout(() => {
      reject(new Error(`Timeout waiting for ${expectedCount} messages after ${timeoutMs}ms. Got ${messages.length}`))
    }, timeoutMs)

    const handleMessage = (message) => {
      if (isTestMessage(message, testId)) {
        messages.push(message)
        if (messages.length >= expectedCount) {
          clearTimeout(timeout)
          resolve(messages)
        }
      }
    }

    const handleError = (error) => {
      clearTimeout(timeout)
      reject(error)
    }

    if (consumer.on) {
      consumer.on('data', handleMessage)
      consumer.on('error', handleError)
    } else {
      const pollMessages = async () => {
        try {
          while (messages.length < expectedCount) {
            const message = await consumer.recv()
            if (message && isTestMessage(message, testId)) {
              messages.push(message)
            }
          }
          clearTimeout(timeout)
          resolve(messages)
        } catch (error) {
          clearTimeout(timeout)
          reject(error)
        }
      }
      pollMessages()
    }
  })
}

export async function setupTestEnvironment() {
  const testId = nanoid(6)
  const topic = createTestTopic()
  const messages = createTestMessages(5, testId)

  return {
    testId,
    topic,
    messages,
    config: TEST_CONFIG,
  }
}

export async function cleanupConsumer(consumer) {
  try {
    if (consumer && typeof consumer.disconnect === 'function') {
      await consumer.disconnect()
    }
  } catch (error) {
    console.warn('Error disconnecting consumer:', error.message)
  }
}

export async function cleanupProducer(producer) {
  try {
    if (producer && typeof producer.disconnect === 'function') {
      await producer.disconnect()
    }
  } catch (error) {
    console.warn('Error disconnecting producer:', error.message)
  }
}
