/**
 * OpenTelemetry Tracing Example for kafka-crab-js
 *
 * This example demonstrates how to:
 * 1. Configure OpenTelemetry tracing and metrics
 * 2. Set up exporters (Console, Jaeger, etc.)
 * 3. Use automatic instrumentation for Kafka operations
 * 4. Create custom spans and add attributes
 * 5. Propagate trace context between producer and consumer
 *
 * Prerequisites:
 * - Running Kafka broker at localhost:9092
 * - (Optional) Jaeger for trace visualization: docker run -d -p 16686:16686 -p 4318:4318 jaegertracing/all-in-one:latest
 *
 * Run: KAFKA_AVAILABLE=true node example/otel-tracing-example.mjs
 */

import { nanoid } from 'nanoid'
import { Buffer } from 'node:buffer'
import { KafkaClient } from '../dist/index.js'

// OpenTelemetry SDK imports (mix of ESM and CommonJS)
import { context, SpanStatusCode, trace } from '@opentelemetry/api'
import otlpTracePkg from '@opentelemetry/exporter-trace-otlp-http'
import resourcesPkg from '@opentelemetry/resources'
import sdkNodePkg from '@opentelemetry/sdk-node'
import traceNodePkg from '@opentelemetry/sdk-trace-node'
import semconvPkg from '@opentelemetry/semantic-conventions'

const { OTLPTraceExporter } = otlpTracePkg
const { resourceFromAttributes } = resourcesPkg
const { NodeSDK } = sdkNodePkg
const { ConsoleSpanExporter } = traceNodePkg
const { SEMRESATTRS_SERVICE_NAME } = semconvPkg

process.env.NAPI_RS_TOKIO_RUNTIME = '1'

// ============================================================================
// 1. Configure OpenTelemetry SDK
// ============================================================================

console.log('🔧 Configuring OpenTelemetry SDK...\n')

// Choose your exporter configuration:
// - ConsoleSpanExporter: Logs traces to console (good for development)
// - OTLPTraceExporter: Sends to OTLP-compatible backend (Jaeger, Grafana Tempo, etc.)

const traceExporter = process.env.OTEL_EXPORTER_TYPE === 'otlp'
  ? new OTLPTraceExporter({
    url: process.env.OTEL_EXPORTER_OTLP_ENDPOINT || 'http://localhost:4318/v1/traces',
  })
  : new ConsoleSpanExporter()

const sdk = new NodeSDK({
  resource: resourceFromAttributes({
    [SEMRESATTRS_SERVICE_NAME]: 'kafka-crab-otel-example',
  }),
  traceExporter,
})

sdk.start()
console.log('✅ OpenTelemetry SDK initialized')
console.log(`📊 Trace Exporter: ${process.env.OTEL_EXPORTER_TYPE === 'otlp' ? 'OTLP' : 'Console'}\n`)

// Handle graceful shutdown
process.on('SIGTERM', () => {
  sdk.shutdown()
    .then(() => console.log('✅ OpenTelemetry SDK shut down successfully'))
    .catch((error) => console.error('❌ Error shutting down OpenTelemetry SDK', error))
    .finally(() => process.exit(0))
})

// ============================================================================
// 2. Create Kafka Client with OTEL Configuration
// ============================================================================

console.log('🔧 Creating Kafka client with OpenTelemetry enabled...\n')

const kafkaClient = new KafkaClient({
  brokers: process.env.KAFKA_BROKERS || 'localhost:9092',
  clientId: 'otel-example-client',
  securityProtocol: 'Plaintext',
  logLevel: 'info',

  // OpenTelemetry Configuration
  otel: {
    enabled: true, // Enable OTEL instrumentation (default: true)
    serviceName: 'kafka-crab-otel-example', // Service name for traces

    // Span configuration
    captureMessagePayload: true, // Include message payload in spans (default: false)
    maxPayloadSize: 1024, // Max payload size to capture in bytes (default: 1024)
    captureMessageHeaders: true, // Include message headers in spans (default: true)
    enableBatchInstrumentation: true, // Enable batch operation instrumentation (default: true)

    // Topic filtering
    ignoreTopics: ['__consumer_offsets'], // Topics to exclude from tracing

    // Metrics configuration (disabled for this tracing-focused example)
    metrics: {
      enabled: false, // Metrics disabled - see otel-metrics-example.mjs for metrics
    },

    // Custom hooks for advanced scenarios
    producerHook: (span, record, metadata) => {
      // Add custom attributes to producer spans
      span.setAttribute('custom.message_count', record.messages.length)
      if (metadata) {
        span.setAttribute('custom.broker_partition', metadata.partition)
      }
    },

    messageHook: (span, message) => {
      // Add custom attributes to consumer message spans
      span.setAttribute('custom.message_size', message.payload.length)

      // You can also extract business-level data
      try {
        const data = JSON.parse(message.payload.toString())
        if (data.userId) {
          span.setAttribute('custom.user_id', data.userId)
        }
      } catch {
        // Ignore parse errors
      }
    },
  },
})

console.log('✅ Kafka client created with OTEL enabled\n')

// ============================================================================
// 3. Producer Example with Automatic Tracing
// ============================================================================

const topic = process.env.KAFKA_TOPIC || `otel-example-${nanoid()}`
console.log(`📝 Using topic: ${topic}\n`)

async function produceMessagesWithTracing() {
  console.log('🚀 Starting producer with automatic tracing...\n')

  const producer = kafkaClient.createProducer()
  const tracer = trace.getTracer('kafka-producer-example')

  // Create a parent span to demonstrate trace context propagation
  const parentSpan = tracer.startSpan('produce-batch-operation')

  try {
    // All producer.send() calls within this context will be child spans
    await context.with(trace.setSpan(context.active(), parentSpan), async () => {
      for (let i = 0; i < 5; i++) {
        const messageId = nanoid()

        // Each send operation automatically creates a span
        // Trace context is automatically injected into message headers
        const result = await producer.send({
          topic,
          messages: [{
            key: Buffer.from(`key-${i}`),
            headers: {
              'correlation-id': Buffer.from(messageId),
              'user-id': Buffer.from('user-123'),
            },
            payload: Buffer.from(JSON.stringify({
              id: i,
              userId: 'user-123',
              timestamp: Date.now(),
              data: `Message ${i}`,
            })),
          }],
        })

        console.log(`✅ Message ${i} sent to partition ${result[0].partition}, offset ${result[0].offset}`)
      }
    })

    parentSpan.setStatus({ code: SpanStatusCode.OK })
    console.log('\n✅ All messages produced successfully\n')
  } catch (error) {
    parentSpan.recordException(error)
    parentSpan.setStatus({
      code: SpanStatusCode.ERROR,
      message: error.message,
    })
    console.error('❌ Producer error:', error)
    throw error
  } finally {
    parentSpan.end()
  }
}

// ============================================================================
// 4. Consumer Example with Automatic Tracing
// ============================================================================

async function consumeMessagesWithTracing() {
  console.log('🚀 Starting consumer with automatic tracing...\n')

  const consumer = kafkaClient.createConsumer({
    groupId: 'otel-example-consumer-group',
    enableAutoCommit: false,
    configuration: {
      'auto.offset.reset': 'earliest',
    },
  })

  await consumer.subscribe(topic)
  console.log(`✅ Subscribed to topic: ${topic}\n`)

  const tracer = trace.getTracer('kafka-consumer-example')
  let messageCount = 0
  const maxMessages = 5

  while (messageCount < maxMessages) {
    // recv() automatically creates spans with trace context extracted from headers
    const message = await consumer.recv()

    if (!message) {
      console.log('No more messages, exiting...')
      break
    }

    messageCount++

    // Create a custom span for message processing
    // This span will be a child of the automatically created span
    const processingSpan = tracer.startSpan('process-message-business-logic', {
      attributes: {
        'message.topic': message.topic,
        'message.partition': message.partition,
        'message.offset': message.offset,
      },
    })

    try {
      // Simulate business logic processing
      const data = JSON.parse(message.payload.toString())
      console.log(`📨 Received message ${messageCount}/${maxMessages}:`)
      console.log(`   Topic: ${message.topic}`)
      console.log(`   Partition: ${message.partition}`)
      console.log(`   Offset: ${message.offset}`)
      console.log(`   Data: ${JSON.stringify(data)}`)
      console.log(`   Headers:`, Object.fromEntries(
        Object.entries(message.headers || {}).map(([k, v]) => [k, v.toString()]),
      ))
      console.log()

      // Add custom attributes to the processing span
      processingSpan.setAttribute('business.user_id', data.userId)
      processingSpan.setAttribute('business.message_id', data.id)

      // Simulate some processing time
      await new Promise(resolve => setTimeout(resolve, 100))

      // Commit the offset
      await consumer.commitMessage(message, 'Async')

      processingSpan.setStatus({ code: SpanStatusCode.OK })
    } catch (error) {
      processingSpan.recordException(error)
      processingSpan.setStatus({
        code: SpanStatusCode.ERROR,
        message: error.message,
      })
      console.error('❌ Error processing message:', error)
    } finally {
      processingSpan.end()
    }
  }

  console.log(`\n✅ Consumed ${messageCount} messages\n`)
  await consumer.disconnect()
}

// ============================================================================
// 5. Manual OTEL Context Usage
// ============================================================================

async function demonstrateManualOTELUsage() {
  console.log('🔧 Demonstrating manual OTEL context usage...\n')

  const producer = kafkaClient.createProducer()

  // Access the OTEL context directly
  const otelContext = kafkaClient.otel

  console.log('OTEL Context properties:')
  console.log(`  - enabled: ${otelContext.enabled}`)
  console.log(`  - tracer: ${otelContext.tracer ? 'Available' : 'Not available'}`)
  console.log(`  - context: ${otelContext.context ? 'Active' : 'Not active'}`)
  console.log()

  // Manually create a span
  const manualSpan = otelContext.startSpan('manual-kafka-operation', {
    'operation.type': 'custom',
    'custom.attribute': 'example',
  })

  try {
    // Manually inject trace context into headers
    const headers = {}
    otelContext.inject(headers)

    console.log('Manually injected trace headers:', Object.keys(headers))
    console.log()

    await producer.send({
      topic,
      messages: [{
        key: Buffer.from('manual-key'),
        headers: {
          ...headers,
          'custom-header': Buffer.from('custom-value'),
        },
        payload: Buffer.from(JSON.stringify({
          manual: true,
          message: 'Manually traced message',
        })),
      }],
    })

    otelContext.endSpan(manualSpan)
    console.log('✅ Manual span completed\n')
  } catch (error) {
    otelContext.endSpan(manualSpan, error)
    console.error('❌ Manual operation failed:', error)
  }
}

// ============================================================================
// 6. Run the Example
// ============================================================================

async function main() {
  try {
    console.log('='.repeat(80))
    console.log('OpenTelemetry Tracing Example for kafka-crab-js')
    console.log('='.repeat(80))
    console.log()

    // Step 1: Produce messages with automatic tracing
    await produceMessagesWithTracing()

    // Step 2: Consume messages with automatic tracing
    await consumeMessagesWithTracing()

    // Step 3: Demonstrate manual OTEL usage
    await demonstrateManualOTELUsage()

    console.log('='.repeat(80))
    console.log('✅ Example completed successfully!')
    console.log('='.repeat(80))
    console.log()
    console.log('📊 Next steps:')
    console.log('  1. Check console output for trace spans')
    console.log('  2. If using Jaeger, open http://localhost:16686 to view traces')
    console.log('  3. Search for service: kafka-crab-otel-example')
    console.log()
  } catch (error) {
    console.error('❌ Example failed:', error)
    process.exit(1)
  } finally {
    // Shutdown SDK to flush remaining spans/metrics
    await sdk.shutdown()
    process.exit(0)
  }
}

// Run the example
main().catch(console.error)
