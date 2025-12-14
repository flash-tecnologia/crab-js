import { nanoid } from 'nanoid'
import { Buffer } from 'node:buffer'
import { endSpan, KafkaClient } from '../dist/index.js'

// Minimal OTEL SDK bootstrap so spans are actually exported.
// Configure with:
//   OTEL_EXPORTER_OTLP_ENDPOINT (default: http://localhost:4317)
//   OTEL_SERVICE_NAME (default: kafka-crab-js-example)
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-grpc'
import { resourceFromAttributes } from '@opentelemetry/resources'
import { NodeSDK } from '@opentelemetry/sdk-node'
import { SEMRESATTRS_SERVICE_NAME } from '@opentelemetry/semantic-conventions'

process.env.NAPI_RS_TOKIO_RUNTIME = '1'

const otelEndpoint = process.env.OTEL_EXPORTER_OTLP_ENDPOINT || 'http://localhost:4317'
const otelServiceName = process.env.OTEL_SERVICE_NAME || 'kafka-crab-js-example'
const sdk = new NodeSDK({
  traceExporter: new OTLPTraceExporter({ url: otelEndpoint }),
  resource: resourceFromAttributes({
    [SEMRESATTRS_SERVICE_NAME]: otelServiceName,
  }),
})
await sdk.start()

const kafkaClient = new KafkaClient({
  brokers: 'localhost:9092',
  clientId: 'my-js-group',
  securityProtocol: 'Plaintext',
  logLevel: 'debug',
  brokerAddressFamily: 'v4',
})
const topic = `topic-${nanoid()}`

async function produce() {
  const producer = kafkaClient.createProducer({ topic, configuration: { 'message.timeout.ms': '5000' } })
  for (let i = 0; i < 10; i++) {
    try {
      const result = await producer.send(
        {
          topic,
          messages: [{
            key: Buffer.from(nanoid()),
            headers: { 'correlation-id': Buffer.from(nanoid()) },
            payload: Buffer.from(`{"_id":"${i}","name":"Elizeu Drummond","phone":"1234567890"}`),
          }],
        },
      )
      console.log('Js message sent. Offset:', result)
    } catch (error) {
      console.error('Js Error on send', error)
    }
  }
}

async function startConsumer() {
  const consumer = kafkaClient.createConsumer({
    topic,
    groupId: 'my-js-group2',
    configuration: {
      'auto.offset.reset': 'earliest',
    },
  })
  await consumer.subscribe(topic)
  while (true) {
    const message = await consumer.recv()
    const { partition, offset, headers, payload } = message
    console.log(
      'Message received! Partition:',
      partition,
      'Offset:',
      offset,
      'headers:',
      Object.entries(headers).map(([k, v]) => ({ [k]: v.toString() })),
      'Message => ',
      payload.toString(),
    )
    endSpan(message)
  }
}

await produce()
await startConsumer()
