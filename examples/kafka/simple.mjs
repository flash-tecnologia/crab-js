import { KafkaClient } from 'kafka-crab-js'
import { enableOtelInstrumentation, endSpan } from 'kafka-crab-js-otel'
import { nanoid } from 'nanoid'
import { Buffer } from 'node:buffer'

// Minimal OTEL SDK bootstrap so spans are actually exported.
// Configure with:
//   OTEL_EXPORTER_OTLP_ENDPOINT (default: http://localhost:4317)
//   OTEL_SERVICE_NAME (default: kafka-crab-js-example)
import otlpTraceGrpcPkg from '@opentelemetry/exporter-trace-otlp-grpc'
import resourcesPkg from '@opentelemetry/resources'
import sdkNodePkg from '@opentelemetry/sdk-node'
import semconvPkg from '@opentelemetry/semantic-conventions'

const { OTLPTraceExporter } = otlpTraceGrpcPkg
const { Resource, resourceFromAttributes } = resourcesPkg
const { NodeSDK } = sdkNodePkg
const { SEMRESATTRS_SERVICE_NAME } = semconvPkg

process.env.NAPI_RS_TOKIO_RUNTIME = '1'

if (process.env.KAFKA_AVAILABLE !== 'true') {
  console.error('Set KAFKA_AVAILABLE=true to run this example.')
  process.exit(1)
}

const otelEndpoint = process.env.OTEL_EXPORTER_OTLP_ENDPOINT || 'http://localhost:4317'
const otelServiceName = process.env.OTEL_SERVICE_NAME || 'kafka-crab-js-example'

const resource =
  typeof resourceFromAttributes === 'function'
    ? resourceFromAttributes({ [SEMRESATTRS_SERVICE_NAME]: otelServiceName })
    : new Resource({ [SEMRESATTRS_SERVICE_NAME]: otelServiceName })

const sdk = new NodeSDK({
  traceExporter: new OTLPTraceExporter({ url: otelEndpoint }),
  resource,
})
sdk.start()

enableOtelInstrumentation({
  captureMessageHeaders: true,
  captureMessagePayload: true,
})

const kafkaClient = new KafkaClient({
  brokers: process.env.KAFKA_BROKERS || 'localhost:9092',
  clientId: 'my-js-group',
  securityProtocol: 'Plaintext',
  logLevel: 'debug',
  brokerAddressFamily: 'v6',
})
const topic = `topic-${nanoid()}`

async function produce(count) {
  const producer = kafkaClient.createProducer({ configuration: { 'message.timeout.ms': '5000' } })
  for (let i = 0; i < count; i++) {
    try {
      const result = await producer.send({
        topic,
        messages: [
          {
            key: Buffer.from(nanoid()),
            headers: { 'correlation-id': Buffer.from(nanoid()) },
            payload: Buffer.from(`{"_id":"${i}","name":"Elizeu Drummond","phone":"1234567890"}`),
          },
        ],
      })
      console.log('Js message sent. Offset:', result)
    } catch (error) {
      console.error('Js Error on send', error)
    }
  }

  await producer.flush()
}

async function startConsumer(expectedMessages) {
  const consumer = kafkaClient.createConsumer({
    topic,
    groupId: 'my-js-group2',
    configuration: {
      'auto.offset.reset': 'earliest',
    },
  })
  process.once('SIGINT', () => consumer.disconnect())
  process.once('SIGTERM', () => consumer.disconnect())

  await consumer.subscribe(topic)

  let received = 0
  while (received < expectedMessages) {
    const message = await consumer.recv()
    if (!message) {
      break
    }

    let processingError
    try {
      const { partition, offset, headers, payload } = message
      console.log(
        'Message received! Partition:',
        partition,
        'Offset:',
        offset,
        'headers:',
        Object.entries(headers ?? {}).map(([k, v]) => ({ [k]: v.toString() })),
        'Message => ',
        payload.toString(),
      )
      received++
    } catch (error) {
      processingError = error
      throw error
    } finally {
      endSpan(message, processingError instanceof Error ? processingError : undefined)
    }
  }

  await consumer.disconnect()
}

try {
  const messageCount = 10
  await produce(messageCount)
  await startConsumer(messageCount)
} finally {
  await sdk.shutdown()
}
