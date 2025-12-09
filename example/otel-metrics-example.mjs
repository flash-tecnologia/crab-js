/**
 * OpenTelemetry Metrics Example for kafka-crab-js
 *
 * This example demonstrates how to:
 * 1. Enable and configure metrics collection
 * 2. Monitor Kafka operations with OTEL metrics
 * 3. Export metrics to various backends
 * 4. Use custom histogram buckets
 *
 * Metrics collected:
 * - messaging.client.operation.duration (Histogram) - Producer/consumer operation duration
 * - messaging.client.sent.messages (Counter) - Number of messages sent
 * - messaging.client.consumed.messages (Counter) - Number of messages consumed
 * - messaging.process.duration (Histogram) - Message processing duration
 *
 * Prerequisites:
 * - Running Kafka broker at localhost:9092
 * - Prometheus/Grafana with OTLP gRPC receiver at localhost:4317 (or set OTEL_EXPORTER_OTLP_ENDPOINT)
 * - Or use console exporter: OTEL_EXPORTER_TYPE=console
 *
 * Run: KAFKA_AVAILABLE=true node example/otel-metrics-example.mjs
 */

import { nanoid } from 'nanoid'
import { Buffer } from 'node:buffer'
import { KafkaClient } from '../dist/index.js'

// OpenTelemetry SDK imports (CommonJS modules, need default import)
import otlpMetricsGrpcPkg from '@opentelemetry/exporter-metrics-otlp-grpc'
import resourcesPkg from '@opentelemetry/resources'
import metricsPkg from '@opentelemetry/sdk-metrics'
import semconvPkg from '@opentelemetry/semantic-conventions'

const { resourceFromAttributes } = resourcesPkg
const { ConsoleMetricExporter, MeterProvider, PeriodicExportingMetricReader } = metricsPkg
const { OTLPMetricExporter } = otlpMetricsGrpcPkg
const { SEMRESATTRS_SERVICE_NAME } = semconvPkg

process.env.NAPI_RS_TOKIO_RUNTIME = '1'

// ============================================================================
// 1. Configure OpenTelemetry Metrics
// ============================================================================

console.log('🔧 Configuring OpenTelemetry Metrics...\n')

// Choose your exporter configuration:
// - ConsoleMetricExporter: Logs metrics to console (good for development)
// - OTLPMetricExporter: Sends to OTLP-compatible backend via gRPC (Prometheus, Grafana, etc.)
//
// Default: OTLP gRPC to Prometheus/Grafana at localhost:4317
// Set OTEL_EXPORTER_TYPE=console to use console exporter instead

const metricExporter = process.env.OTEL_EXPORTER_TYPE === 'console'
  ? new ConsoleMetricExporter()
  : new OTLPMetricExporter({
    url: process.env.OTEL_EXPORTER_OTLP_ENDPOINT || 'http://localhost:4317',
  })

const meterProvider = new MeterProvider({
  resource: resourceFromAttributes({
    [SEMRESATTRS_SERVICE_NAME]: 'kafka-crab-metrics-example',
  }),
  readers: [
    new PeriodicExportingMetricReader({
      exporter: metricExporter,
      exportIntervalMillis: 10000, // Export metrics every 10 seconds
    }),
  ],
})

console.log('✅ Metrics provider configured')
console.log(
  `📊 Metric Exporter: ${process.env.OTEL_EXPORTER_TYPE === 'console' ? 'Console' : 'OTLP (Prometheus/Grafana)'}\n`,
)

// ============================================================================
// 2. Create Kafka Client with Metrics Configuration
// ============================================================================

console.log('🔧 Creating Kafka client with metrics enabled...\n')

const kafkaClient = new KafkaClient({
  brokers: process.env.KAFKA_BROKERS || 'localhost:9092',
  clientId: 'metrics-example-client',
  securityProtocol: 'Plaintext',
  logLevel: 'info',

  // OpenTelemetry Configuration
  otel: {
    enabled: true,
    serviceName: 'kafka-crab-metrics-example',

    // Metrics-specific configuration
    metrics: {
      enabled: true,
      meterProvider: meterProvider, // Use our custom meter provider
      includePartitionId: true, // Include partition in metric labels
      serverAddress: 'localhost', // Broker address for attribution
      serverPort: 9092, // Broker port for attribution

      // Custom histogram buckets for duration metrics (in seconds)
      // Default: [0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10]
      // Customize based on your expected latencies:
      histogramBuckets: [
        0.001, // 1ms
        0.005, // 5ms
        0.01, // 10ms
        0.05, // 50ms
        0.1, // 100ms
        0.5, // 500ms
        1, // 1s
        2, // 2s
        5, // 5s
      ],
    },
  },
})

console.log('✅ Kafka client created with metrics enabled\n')

// ============================================================================
// 3. Producer with Metrics
// ============================================================================

const topic = process.env.KAFKA_TOPIC || `metrics-example-${nanoid()}`
console.log(`📝 Using topic: ${topic}\n`)

async function produceWithMetrics() {
  console.log('🚀 Producing messages (metrics will be collected)...\n')

  const producer = kafkaClient.createProducer()
  const messageCount = 20

  for (let i = 0; i < messageCount; i++) {
    try {
      await producer.send({
        topic,
        messages: [{
          key: Buffer.from(`key-${i}`),
          headers: {
            'message-id': Buffer.from(nanoid()),
          },
          payload: Buffer.from(JSON.stringify({
            id: i,
            timestamp: Date.now(),
            data: `Message ${i}`,
          })),
        }],
      })

      if ((i + 1) % 5 === 0) {
        console.log(`✅ Produced ${i + 1}/${messageCount} messages`)
      }

      // Simulate variable latency
      await new Promise(resolve => setTimeout(resolve, Math.random() * 100))
    } catch (error) {
      console.error(`❌ Failed to send message ${i}:`, error.message)
    }
  }

  console.log(`\n✅ Finished producing ${messageCount} messages\n`)

  // Metrics collected:
  // - messaging.client.operation.duration: Histogram of send operation duration
  // - messaging.client.sent.messages: Counter of messages sent
}

// ============================================================================
// 4. Consumer with Metrics
// ============================================================================

async function consumeWithMetrics() {
  console.log('🚀 Consuming messages (metrics will be collected)...\n')

  const consumer = kafkaClient.createConsumer({
    groupId: 'metrics-example-consumer-group',
    enableAutoCommit: false,
    configuration: {
      'auto.offset.reset': 'earliest',
    },
  })

  await consumer.subscribe(topic)

  let messageCount = 0
  const maxMessages = 20

  while (messageCount < maxMessages) {
    try {
      const message = await consumer.recv()

      if (!message) {
        break
      }

      messageCount++

      // Simulate processing time
      const processingTime = Math.random() * 200
      await new Promise(resolve => setTimeout(resolve, processingTime))

      await consumer.commitMessage(message, 'Async')

      if (messageCount % 5 === 0) {
        console.log(`✅ Consumed ${messageCount}/${maxMessages} messages`)
      }
    } catch (error) {
      console.error('❌ Consumer error:', error.message)
    }
  }

  console.log(`\n✅ Finished consuming ${messageCount} messages\n`)
  await consumer.disconnect()

  // Metrics collected:
  // - messaging.client.operation.duration: Histogram of receive operation duration
  // - messaging.client.consumed.messages: Counter of messages consumed
  // - messaging.process.duration: Histogram of message processing duration
}

// ============================================================================
// 5. Batch Processing with Metrics
// ============================================================================

async function batchProcessWithMetrics() {
  console.log('🚀 Batch processing messages (metrics will be collected)...\n')

  const producer = kafkaClient.createProducer()

  // Produce batch
  console.log('📤 Producing batch of messages...')
  await producer.send({
    topic,
    messages: Array.from({ length: 10 }, (_, i) => ({
      key: Buffer.from(`batch-key-${i}`),
      payload: Buffer.from(JSON.stringify({ batch: true, id: i })),
    })),
  })
  console.log('✅ Batch produced\n')

  // Consume batch
  console.log('📥 Consuming messages in batches...')
  const consumer = kafkaClient.createConsumer({
    groupId: 'metrics-batch-consumer-group',
    enableAutoCommit: false,
    configuration: {
      'auto.offset.reset': 'earliest',
    },
  })

  await consumer.subscribe(topic)

  let totalMessages = 0
  const batchSize = 5
  const maxBatches = 2

  for (let batch = 0; batch < maxBatches; batch++) {
    const messages = await consumer.recvBatch(batchSize, 5000)
    totalMessages += messages.length

    console.log(`✅ Batch ${batch + 1}: Received ${messages.length} messages`)

    // Process batch
    await Promise.all(messages.map(async (message) => {
      await new Promise(resolve => setTimeout(resolve, 50))
      await consumer.commitMessage(message, 'Async')
    }))
  }

  console.log(`\n✅ Processed ${totalMessages} messages in batches\n`)
  await consumer.disconnect()

  // Metrics collected:
  // - messaging.process.duration: Histogram for batch processing duration
  // - messaging.batch.message_count: Batch size attribute
}

// ============================================================================
// 6. Understanding Metrics
// ============================================================================

function explainMetrics() {
  console.log('='.repeat(80))
  console.log('📊 Metrics Collected by kafka-crab-js')
  console.log('='.repeat(80))
  console.log()

  console.log('1. messaging.client.operation.duration (Histogram)')
  console.log('   - Measures: Time taken for producer/consumer operations')
  console.log('   - Unit: seconds')
  console.log('   - Attributes:')
  console.log('     * messaging.system: "kafka"')
  console.log('     * messaging.operation.name: "send" | "receive"')
  console.log('     * messaging.operation.type: "send" | "receive"')
  console.log('     * messaging.destination.name: topic name')
  console.log('     * messaging.destination.partition.id: partition (optional)')
  console.log('     * messaging.client.id: client ID')
  console.log('     * server.address: broker address')
  console.log('     * server.port: broker port')
  console.log('     * error.type: error type (only on error)')
  console.log()

  console.log('2. messaging.client.sent.messages (Counter)')
  console.log('   - Measures: Number of messages sent by producer')
  console.log('   - Unit: messages')
  console.log('   - Attributes: Same as operation.duration')
  console.log()

  console.log('3. messaging.client.consumed.messages (Counter)')
  console.log('   - Measures: Number of messages consumed')
  console.log('   - Unit: messages')
  console.log('   - Attributes: Same as operation.duration + consumer.group.name')
  console.log()

  console.log('4. messaging.process.duration (Histogram)')
  console.log('   - Measures: Time taken to process messages')
  console.log('   - Unit: seconds')
  console.log('   - Attributes: Same as consumed.messages')
  console.log()

  console.log('='.repeat(80))
  console.log()
}

// ============================================================================
// 7. Run the Example
// ============================================================================

async function main() {
  try {
    console.log('='.repeat(80))
    console.log('OpenTelemetry Metrics Example for kafka-crab-js')
    console.log('='.repeat(80))
    console.log()

    explainMetrics()

    // Run examples
    await produceWithMetrics()
    await consumeWithMetrics()
    await batchProcessWithMetrics()

    console.log('='.repeat(80))
    console.log('⏳ Waiting 15 seconds for final metrics export...')
    console.log('='.repeat(80))
    console.log()

    // Wait for final metrics export
    await new Promise(resolve => setTimeout(resolve, 15000))

    console.log('='.repeat(80))
    console.log('✅ Example completed!')
    console.log('='.repeat(80))
    console.log()
    console.log('📊 Check the console output above for exported metrics')
    console.log()
    console.log('💡 Tips:')
    console.log('  - Look for metrics with names starting with "messaging."')
    console.log('  - Each metric includes labels (attributes) for filtering')
    console.log('  - Histogram metrics show distribution of latencies')
    console.log('  - Counter metrics show total counts')
    console.log()
    console.log('🚀 Next steps:')
    console.log('  1. Open Grafana at http://localhost:3000')
    console.log('  2. Go to Explore → Select Prometheus data source')
    console.log('  3. Query metrics: messaging_client_operation_duration_bucket')
    console.log('  4. Create dashboards to visualize Kafka operation metrics')
    console.log('  5. Set up alerts based on metric thresholds')
    console.log()
  } catch (error) {
    console.error('❌ Example failed:', error)
  } finally {
    // Shutdown meter provider to flush metrics
    await meterProvider.shutdown()
    process.exit(0)
  }
}

main().catch(console.error)
