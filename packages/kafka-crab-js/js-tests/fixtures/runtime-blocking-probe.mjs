import { KafkaClient } from '../../js-src/index.js'

const client = new KafkaClient({
  brokers: '127.0.0.1:1',
  clientId: 'kafka-crab-runtime-blocking-probe',
  diagnostics: false,
  logLevel: 'error',
})
const consumer = client.createConsumer({
  groupId: 'kafka-crab-runtime-blocking-probe',
  enableAutoCommit: false,
  configuration: {
    'test.mock.num.brokers': 1,
  },
})
const producer = client.createProducer({
  queueTimeout: 500,
  configuration: {
    'message.timeout.ms': 1000,
  },
})

try {
  await consumer.recvBatch(1, 50)

  const startedAt = performance.now()
  const read = consumer.recvBatch(1, 50).then(() => performance.now() - startedAt)
  const send = producer
    .send({
      topic: 'kafka-crab-runtime-blocking-probe',
      messages: [{ payload: Buffer.from('probe') }],
    })
    .catch(() => undefined)

  const elapsedMs = await read
  await send
  console.log(`PROBE_RESULT ${JSON.stringify({ elapsedMs })}`)
} finally {
  await consumer.disconnect()
}
