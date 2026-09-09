import { setTimeout as sleep } from 'node:timers/promises'

import { KafkaClient } from '../../js-src/index.js'

const secret = process.argv[2]
if (!secret) {
  throw new Error('A fake credential is required')
}

const client = new KafkaClient({
  brokers: '127.0.0.1:1',
  clientId: 'credential-log-probe',
  logLevel: 'debug',
  configuration: {
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'fake-user',
    'sasl.password': secret,
  },
})
const consumer = client.createConsumer({
  groupId: 'credential-log-probe',
  enableAutoCommit: false,
})

await sleep(100)
await consumer.disconnect().catch(() => undefined)
console.log('PROBE_DONE')
