import { KafkaClient } from '../../js-src/index.js'

const client = new KafkaClient({
  brokers: '127.0.0.1:1',
  clientId: 'kafka-crab-metadata-offload-probe',
  diagnostics: false,
  logLevel: 'error',
})
const metadataConsumer = client.createConsumer({
  groupId: 'kafka-crab-metadata-offload-slow',
  enableAutoCommit: false,
  fetchMetadataTimeout: 600,
})
const receiveConsumer = client.createConsumer({
  groupId: 'kafka-crab-metadata-offload-fast',
  enableAutoCommit: false,
  configuration: {
    'test.mock.num.brokers': 1,
  },
})

try {
  let metadataSettled = false
  const metadataStartedAt = performance.now()
  const metadata = metadataConsumer
    .subscribe([
      {
        topic: 'kafka-crab-metadata-offload-missing',
        allOffsets: { position: 'Beginning' },
      },
    ])
    .catch(() => undefined)
    .finally(() => {
      metadataSettled = true
    })

  const receiveStartedAt = performance.now()
  await receiveConsumer.recvBatch(1, 50)
  const receiveElapsedMs = performance.now() - receiveStartedAt
  const metadataPendingAfterReceive = !metadataSettled

  await metadata
  const metadataElapsedMs = performance.now() - metadataStartedAt
  console.log(
    `METADATA_OFFLOAD_RESULT ${JSON.stringify({
      receiveElapsedMs,
      metadataElapsedMs,
      metadataPendingAfterReceive,
    })}`,
  )
} finally {
  await Promise.allSettled([metadataConsumer.disconnect(), receiveConsumer.disconnect()])
}
