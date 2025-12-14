import { printResults, type Result, Tracker } from 'cronometro'
import { Kafka as KafkaJS, logLevel } from 'kafkajs'
import RDKafka from 'node-rdkafka'
import assert from 'node:assert'
import { randomUUID } from 'node:crypto'
import { setTimeout as sleep } from 'node:timers/promises'
import { KafkaClient } from '../dist/index.js'
import type { Message } from '../js-binding.js'
import { brokers, topic } from './utils/definitions.ts'

const iterations = 50_000

const maxBytes = 200

async function assertPayload(payload: Buffer | undefined | null) {
  if (!payload) {
    throw new Error('Payload is undefined')
  }
  const result = JSON.parse(payload.toString())
  const index = result.index as number
  assert(Number.isInteger(index))
  assert(new Date(result.date) instanceof Date)
  assert(result.message === `message index ${index}`)
  await sleep(1)
}

async function kafkaCrabJs(useBatchMode = false): Promise<Result> {
  const { promise, resolve, reject } = Promise.withResolvers<Result>()
  const tracker = new Tracker()

  const client = new KafkaClient({
    brokers: brokers.join(','),
    clientId: 'benchmarks',
    securityProtocol: 'Plaintext',
    logLevel: 'warn',
    brokerAddressFamily: 'v4',
    otel: {
      enabled: false,
    },
  })

  const consumer = client.createStreamConsumer({
    groupId: randomUUID(),
    enableAutoCommit: false,
    batchSize: useBatchMode ? 1024 : 1,
    configuration: {
      'auto.offset.reset': 'earliest',
      'enable.auto.commit': false,
      'fetch.min.bytes': 1,
      'fetch.message.max.bytes': maxBytes,
      'fetch.wait.max.ms': 10,
    },
  })

  await consumer.subscribe([{ topic, allOffsets: { position: 'Beginning' } }])

  let i = 0
  let last = process.hrtime.bigint()

  consumer.on('data', async ({ payload }: Message) => {
    await assertPayload(payload)

    i++
    tracker.track(last)
    last = process.hrtime.bigint()

    if (i === iterations) {
      consumer.removeAllListeners('data')
      consumer.pause()
      consumer.destroy()
      resolve(tracker.results)
    }
  })

  consumer.on('error', (err: Error) => {
    reject(err)
  })

  return promise
}

function rdkafkaEvented(): Promise<Result> {
  const { promise, resolve, reject } = Promise.withResolvers<Result>()
  const tracker = new Tracker()

  const consumer = new RDKafka.KafkaConsumer(
    {
      'client.id': 'benchmarks',
      'group.id': randomUUID(),
      'metadata.broker.list': brokers.join(','),
      'enable.auto.commit': false,
      'fetch.min.bytes': 1,
      'fetch.message.max.bytes': maxBytes,
      'fetch.wait.max.ms': 10,
    },
    { 'auto.offset.reset': 'earliest' },
  )

  let i = 0
  let last = process.hrtime.bigint()
  consumer.on('data', async (message: RDKafka.Message) => {
    await assertPayload(message.value)

    i++
    tracker.track(last)
    last = process.hrtime.bigint()

    if (i === iterations) {
      consumer.removeAllListeners('data')
      consumer.pause([
        {
          topic,
          partition: 0,
        },
        {
          topic,
          partition: 1,
        },
        {
          topic,
          partition: 2,
        },
      ])

      setTimeout(() => {
        consumer.disconnect()
        resolve(tracker.results)
      }, 20)
    }
  })

  consumer.on('ready', () => {
    consumer.subscribe([topic])
    consumer.consume()
  })

  consumer.on('event.error', reject)

  consumer.connect()

  return promise
}

function rdkafkaStream(): Promise<Result> {
  const { promise, resolve, reject } = Promise.withResolvers<Result>()
  const tracker = new Tracker()

  const stream = RDKafka.KafkaConsumer.createReadStream(
    {
      'client.id': 'benchmarks',
      'group.id': randomUUID(),
      'metadata.broker.list': brokers.join(','),
      'enable.auto.commit': false,
      'fetch.min.bytes': 1,
      'fetch.message.max.bytes': maxBytes,
      'fetch.wait.max.ms': 10,
    },
    { 'auto.offset.reset': 'earliest' },
    { topics: [topic], waitInterval: 0, highWaterMark: 1024, objectMode: true },
  )

  let i = 0
  let last = process.hrtime.bigint()
  stream.on('data', async (message: RDKafka.Message) => {
    await assertPayload(message.value)

    i++
    tracker.track(last)
    last = process.hrtime.bigint()

    if (i === iterations) {
      stream.removeAllListeners('data')
      stream.pause()

      stream.destroy()
      resolve(tracker.results)
    }
  })

  stream.on('error', reject)

  return promise
}

async function kafkajs(): Promise<Result> {
  const { promise, resolve, reject } = Promise.withResolvers<Result>()
  const tracker = new Tracker()

  const client = new KafkaJS({ clientId: 'benchmarks', brokers, logLevel: logLevel.ERROR })
  const consumer = client.consumer({ groupId: randomUUID(), maxBytes, maxWaitTimeInMs: 10 })

  await consumer.connect()
  await consumer.subscribe({ topics: [topic], fromBeginning: true })

  consumer.on('consumer.crash', reject)

  let i = 0
  let last = process.hrtime.bigint()
  await consumer.run({
    autoCommit: false,
    partitionsConsumedConcurrently: 3,
    async eachMessage({ pause, message }) {
      await assertPayload(message.value)

      i++
      tracker.track(last)
      last = process.hrtime.bigint()

      if (i === iterations) {
        pause()
        consumer.disconnect()
        resolve(tracker.results)
      }
    },
  })

  return promise
}

console.log('Starting consumer benchmark...')

const results = {
  'node-rdkafka (evented)': await rdkafkaEvented(),
  'node-rdkafka (stream)': await rdkafkaStream(),
  kafkajs: await kafkajs(),
  'kafka-crab-js (serial)': await kafkaCrabJs(false),
  'kafka-crab-js (batch)': await kafkaCrabJs(true),
}

printResults(results, true, true, 'previous')
