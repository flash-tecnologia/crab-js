// A managed consumer that is garbage collected without disconnect must settle
// its pending receive instead of leaving the promise pending forever.
import { KafkaClient } from '../../js-src/index.js'

const startReceive = () => {
  const consumer = new KafkaClient({
    brokers: '127.0.0.1:1',
    clientId: 'managed-finalizer-probe',
    diagnostics: false,
    logLevel: 'error',
  }).createConsumer({ groupId: 'managed-finalizer-probe', managed: true })
  return consumer.recvManaged()
}

const pending = startReceive()
const collector = setInterval(() => globalThis.gc(), 20)
const result = await Promise.race([
  pending.then((record) => JSON.stringify(record)),
  new Promise((resolve) => setTimeout(() => resolve('still pending'), 8000)),
])
clearInterval(collector)
console.log(`MANAGED_FINALIZER ${result}`)
process.exit(0)
