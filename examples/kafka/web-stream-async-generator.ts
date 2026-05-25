#!/usr/bin/env node

import { KafkaClient, type Message } from 'kafka-crab-js'
import { nanoid } from 'nanoid'
import { Buffer } from 'node:buffer'

const brokers = process.env.KAFKA_BROKERS || 'localhost:9092'
const topic = process.env.KAFKA_TOPIC || `web-stream-generator-${nanoid()}`
const messageCount = readPositiveInteger(process.argv[2] ?? process.env.MESSAGE_COUNT, 10)
const progressInterval = readPositiveInteger(process.env.PROGRESS_INTERVAL, 1000)

const kafkaClient = new KafkaClient({
  brokers,
  clientId: 'web-stream-generator-example',
  logLevel: process.env.KAFKA_LOG_LEVEL || 'info',
  brokerAddressFamily: process.env.KAFKA_BROKER_ADDRESS_FAMILY || 'v4',
})

function readPositiveInteger(value: string | undefined, fallback: number): number {
  const parsed = Number.parseInt(value ?? '', 10)
  return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : fallback
}

async function produceMessages(count: number): Promise<void> {
  const producer = kafkaClient.createProducer({
    configuration: {
      'message.timeout.ms': '5000',
    },
  })

  const messages = Array.from({ length: count }, (_, index) => ({
    key: Buffer.from(`message-${index + 1}`),
    payload: Buffer.from(
      JSON.stringify({
        id: index + 1,
        source: 'web-stream-async-generator',
      }),
    ),
  }))

  await producer.send({ topic, messages })
  await producer.flush()
}

function decodePayload(message: Message): unknown {
  const raw = message.payload.toString()

  try {
    return JSON.parse(raw)
  } catch {
    return raw
  }
}

const webConsumer = kafkaClient.createWebStreamConsumer({
  groupId: `web-stream-generator-${nanoid()}`,
  enableAutoCommit: false,
  configuration: {
    'auto.offset.reset': 'earliest',
  },
})

const shutdown = (): void => {
  void webConsumer.consumer.disconnect().finally(() => process.exit(0))
}

process.once('SIGINT', shutdown)
process.once('SIGTERM', shutdown)

try {
  await webConsumer.consumer.subscribe([
    {
      topic,
      createTopic: true,
      numPartitions: 1,
      replicas: 1,
      allOffsets: { position: 'Beginning' },
    },
  ])

  console.log('Web stream consumer ready:', {
    brokers,
    topic,
    mode: webConsumer.mode,
    messageCount,
  })

  await produceMessages(messageCount)

  console.log('Messages produced:', messageCount)

  let received = 0
  for await (const message of webConsumer.stream) {
    received += 1

    if (received <= 5 || received === messageCount || received % progressInterval === 0) {
      console.log('Message received:', {
        received,
        payload: decodePayload(message),
        topic: message.topic,
        partition: message.partition,
        offset: message.offset,
      })
    }

    await webConsumer.consumer.commitMessage(message, 'Async')

    if (received >= messageCount) {
      break
    }
  }

  console.log(`Consumed ${received}/${messageCount} messages.`)
} finally {
  process.off('SIGINT', shutdown)
  process.off('SIGTERM', shutdown)
  await webConsumer.consumer.disconnect()
}
