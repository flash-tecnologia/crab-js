import { createInterface } from 'node:readline'

import { KafkaClient } from '../../js-src/index.js'

const client = new KafkaClient({
  brokers: '127.0.0.1:1',
  clientId: 'kafka-crab-test-mock-broker',
  configuration: {
    'test.mock.num.brokers': 1,
  },
  diagnostics: false,
  logLevel: 'info',
})
const producer = client.createProducer({
  configuration: {
    'message.timeout.ms': 3000,
  },
})
const input = createInterface({ input: process.stdin })

for await (const line of input) {
  const request = JSON.parse(line)

  try {
    const messages = request.messages.map((message) => ({
      key: Buffer.from(message.key ?? 'shared-key'),
      ...(message.payload === undefined ? {} : { payload: Buffer.from(message.payload) }),
      ...(message.isTombstone === undefined ? {} : { isTombstone: message.isTombstone }),
      ...(message.headers === undefined
        ? {}
        : {
            headers: Object.fromEntries(
              Object.entries(message.headers).map(([key, value]) => [key, Buffer.from(value)]),
            ),
          }),
    }))
    const result = await producer.send({ topic: request.topic, messages })
    console.log(`MOCK_RESPONSE ${JSON.stringify({ id: request.id, result })}`)
  } catch (error) {
    console.log(
      `MOCK_RESPONSE ${JSON.stringify({
        id: request.id,
        error: error instanceof Error ? error.message : String(error),
      })}`,
    )
  }
}
