import { fakerPT_BR } from '@faker-js/faker'
import { KafkaClient } from 'kafka-crab-js'

const kafkaClient = new KafkaClient({
  brokers: 'localhost:9092',
  clientId: 'my-js-group-11',
  logLevel: 'debug',
  brokerAddressFamily: 'v4',
})

async function produce(topic, messages = 1) {
  const producer = kafkaClient.createProducer({ configuration: { 'message.timeout.ms': '5000' } })
  const index = Math.floor(Math.random() * 100_000)
  const records = []
  for (let i = index ?? 0; i < index + messages; i++) {
    const payload = {
      '_id': i,
      'name': fakerPT_BR.person.fullName(),
      'phone': fakerPT_BR.phone.number(),
    }
    records.push({
      payload: Buffer.from(JSON.stringify(payload)),
    })
    console.log('Payload to send', payload)
  }

  try {
    const result = await producer.send({ topic, messages: records })
    console.log('Number of messages:', result.length)
    console.log('Js message sent. Offset:', result)
  } catch (error) {
    console.error('Js Error on send', error)
  }
}

async function startConsumer(topic) {
  const kafkaStream = kafkaClient.createStreamConsumer({
    groupId: `crab-test`,
    enableAutoCommit: true,
  })

  await kafkaStream.subscribe([{
    topic,
    createTopic: true,
    allOffsets: { position: 'Beginning' },
  }])

  let counter = 0
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

  kafkaStream.on('error', (error) => {
    console.error('Stream error', error)
  })

  kafkaStream.on('close', () => {
    console.log('Stream ended')
    kafkaStream.unsubscribe()
  })
}

const usage = () => {
  console.log('Usage:')
  console.log('  node stream-sample.mjs send <topic> [count]')
  console.log('  node stream-sample.mjs <topic>')
}

if (process.argv[2] === 'send') {
  const topic = process.argv[3] || process.env.KAFKA_TOPIC
  const messages = process.argv[4] ? Number(process.argv[4]) : 1
  if (!topic) {
    usage()
    process.exit(1)
  }
  console.log('Sending', messages, 'messages to', topic)
  await produce(topic, messages)
} else {
  const topic = process.argv[2] || process.env.KAFKA_TOPIC
  if (!topic) {
    usage()
    process.exit(1)
  }
  await startConsumer(topic)
}

// Usage:
// Node stream-sample.mjs send foo 10 // send 10 messages on topic foo
// Node stream-sample.mjs foo // process messages on topic foo
