import { setTimeout as sleep } from 'node:timers/promises'

import { KafkaClient } from '../../js-src/index.js'

const client = new KafkaClient({ brokers: '127.0.0.1:1', logLevel: 'error' })
let consumer = client.createConsumer({
  groupId: 'consumer-finalizer-probe',
  enableAutoCommit: false,
  configuration: { 'session.timeout.ms': 6000 },
})
const reference = new WeakRef(consumer)

// An enqueued commit against an unavailable coordinator makes native close
// wait for the session timeout even after our terminal disconnect completes.
consumer.onEvents(() => undefined)
await consumer.commit('unavailable-topic', 0, 1, 'Async')
await consumer.disconnect()
consumer = null
await sleep(100)

const started = performance.now()
global.gc()
await sleep(50)

console.log(
  `CONSUMER_FINALIZER_RESULT ${JSON.stringify({
    elapsedMs: performance.now() - started,
    collected: reference.deref() === undefined,
  })}`,
)
