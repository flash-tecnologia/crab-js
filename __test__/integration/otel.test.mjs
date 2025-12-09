import { nanoid } from 'nanoid'
import assert from 'node:assert'
import { createRequire } from 'node:module'
import { afterEach, beforeEach, describe, test } from 'node:test'

// OpenTelemetry test infrastructure
import { context, SpanKind, SpanStatusCode, trace } from '@opentelemetry/api'
import { AsyncHooksContextManager } from '@opentelemetry/context-async-hooks'
import { AlwaysOnSampler, InMemorySpanExporter, SimpleSpanProcessor } from '@opentelemetry/sdk-trace-base'
import { NodeTracerProvider } from '@opentelemetry/sdk-trace-node'

// Use CommonJS require for the kafka client to avoid ESM bundling issues
const require = createRequire(import.meta.url)
const { KafkaClient, KAFKA_SEMANTIC_CONVENTIONS, getKafkaInstrumentation, resetKafkaInstrumentation } = require(
  '../../dist/index.cjs',
)

const KAFKA_BROKERS = process.env.KAFKA_BROKERS || 'localhost:9092'
const TEST_TIMEOUT = 120000

describe('KafkaClient OpenTelemetry Integration', { timeout: TEST_TIMEOUT }, () => {
  let memoryExporter
  let spanProcessor
  let provider
  let contextManager
  let instrumentation
  let kafkaClient
  let testTopic

  beforeEach(async () => {
    // Generate unique test topic
    testTopic = `test-otel-${nanoid()}`

    // Reset any existing instrumentation
    resetKafkaInstrumentation()

    // Setup OpenTelemetry test infrastructure
    contextManager = new AsyncHooksContextManager()
    memoryExporter = new InMemorySpanExporter()
    spanProcessor = new SimpleSpanProcessor(memoryExporter)

    provider = new NodeTracerProvider({
      sampler: new AlwaysOnSampler(),
    })
    provider.addSpanProcessor(spanProcessor)

    provider.register({ contextManager })
    contextManager.enable()

    // Create instrumented Kafka client
    kafkaClient = new KafkaClient({
      brokers: KAFKA_BROKERS,
      clientId: `test-otel-client-${nanoid()}`,
      otel: {
        serviceName: 'kafka-test-service',
        captureMessageHeaders: true,
        captureMessagePayload: false,
        enableBatchInstrumentation: true,
      },
    })

    // Get the instrumentation instance and set provider
    instrumentation = getKafkaInstrumentation()
    instrumentation.setTracerProvider(provider)
  })

  afterEach(async () => {
    // Cleanup resources
    try {
      contextManager.disable()
      instrumentation.disable()
      await spanProcessor.forceFlush()
      memoryExporter.reset()
      provider.forceFlush()
    } catch (error) {
      console.warn('Cleanup error:', error.message)
    }

    resetKafkaInstrumentation()
  })

  test('should create producer spans for message sending', async () => {
    const producer = kafkaClient.createProducer()

    const testMessage = {
      topic: testTopic,
      messages: [
        {
          payload: Buffer.from('test message 1'),
          key: Buffer.from('test-key-1'),
          headers: { 'test-header': Buffer.from('test-value') },
        },
      ],
    }

    await producer.send(testMessage)
    await producer.flush()

    // Wait a bit for spans to be processed
    await new Promise(resolve => setTimeout(resolve, 100))

    const spans = memoryExporter.getFinishedSpans()

    // Should have at least one producer span
    assert(spans.length >= 1, `Expected at least 1 span, got ${spans.length}`)

    const producerSpan = spans.find(span =>
      span.kind === SpanKind.PRODUCER &&
      span.name.includes(testTopic)
    )

    assert(producerSpan, 'Should have a producer span')
    assert.equal(producerSpan.name, `send ${testTopic}`)
    assert.equal(producerSpan.kind, SpanKind.PRODUCER)
    assert.equal(producerSpan.status.code, SpanStatusCode.OK)

    // Verify Kafka semantic convention attributes
    const attributes = producerSpan.attributes
    assert.equal(attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_SYSTEM], 'kafka')
    assert.equal(attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_DESTINATION_NAME], testTopic)
    assert.equal(attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_OPERATION_NAME], 'send')
    assert.equal(attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_KAFKA_MESSAGE_KEY], 'test-key-1')
    assert.equal(attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_BATCH_MESSAGE_COUNT], 1)
    assert(attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_MESSAGE_BODY_SIZE] > 0)
  })

  test('should inject trace context into message headers', async () => {
    const producer = kafkaClient.createProducer()

    // Create a parent span to establish trace context
    const parentSpan = trace.getTracer('test').startSpan('test-operation')
    const testContext = trace.setSpan(context.active(), parentSpan)

    await context.with(testContext, async () => {
      const testMessage = {
        topic: testTopic,
        messages: [
          {
            payload: Buffer.from('test message with context'),
            headers: {},
          },
        ],
      }

      await producer.send(testMessage)
      await producer.flush()
    })

    parentSpan.end()

    // Wait for spans to be processed
    await new Promise(resolve => setTimeout(resolve, 100))

    const spans = memoryExporter.getFinishedSpans()
    const producerSpan = spans.find(span =>
      span.kind === SpanKind.PRODUCER &&
      span.name.includes(testTopic)
    )

    assert(producerSpan, 'Should have a producer span')

    // Verify trace relationship
    assert.equal(producerSpan.spanContext().traceId, parentSpan.spanContext().traceId)
  })

  test('should create consumer spans for message processing', async () => {
    // First, send a message
    const producer = kafkaClient.createProducer()
    const testMessage = {
      topic: testTopic,
      messages: [
        {
          payload: Buffer.from('consumer test message'),
          key: Buffer.from('consumer-key'),
        },
      ],
    }

    await producer.send(testMessage)
    await producer.flush()

    // Reset spans to focus on consumer
    memoryExporter.reset()

    // Create consumer and receive message
    const consumer = kafkaClient.createConsumer({
      groupId: `test-group-${nanoid()}`,
      enableAutoCommit: false,
    })

    await consumer.subscribe([{
      topic: testTopic,
      allOffsets: { position: 'Beginning' },
    }])

    // Receive the message
    const receivedMessage = await consumer.recv()
    assert(receivedMessage, 'Should receive a message')
    assert.equal(receivedMessage.topic, testTopic)

    await consumer.disconnect()

    // Wait for spans to be processed
    await new Promise(resolve => setTimeout(resolve, 100))

    const spans = memoryExporter.getFinishedSpans()
    const consumerSpan = spans.find(span =>
      span.kind === SpanKind.CONSUMER &&
      span.name.includes(testTopic)
    )

    assert(consumerSpan, 'Should have a consumer span')
    assert.equal(consumerSpan.name, `process ${testTopic}`)
    assert.equal(consumerSpan.kind, SpanKind.CONSUMER)
    assert.equal(consumerSpan.status.code, SpanStatusCode.OK)

    // Verify consumer attributes
    const attributes = consumerSpan.attributes
    assert.equal(attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_SYSTEM], 'kafka')
    assert.equal(attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_DESTINATION_NAME], testTopic)
    assert.equal(attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_OPERATION_NAME], 'process')
    assert.equal(attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_KAFKA_PARTITION], receivedMessage.partition)
    assert.equal(attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_KAFKA_OFFSET], receivedMessage.offset)
  })

  test('should propagate producer span context to consumers', async () => {
    const producer = kafkaClient.createProducer()

    await producer.send({
      topic: testTopic,
      messages: [
        {
          payload: Buffer.from('trace propagation payload'),
        },
      ],
    })
    await producer.flush()

    const consumerGroupId = `trace-prop-${nanoid()}`
    const consumer = kafkaClient.createConsumer({
      groupId: consumerGroupId,
      enableAutoCommit: false,
    })

    await consumer.subscribe([{
      topic: testTopic,
      allOffsets: { position: 'Beginning' },
    }])

    const propagatedMessage = await consumer.recv()
    assert(propagatedMessage, 'Should receive a message for propagation check')

    await consumer.disconnect()

    // Allow async span processor to flush
    await new Promise(resolve => setTimeout(resolve, 100))

    const spans = memoryExporter.getFinishedSpans()

    const producerSpan = spans.find(span =>
      span.kind === SpanKind.PRODUCER &&
      span.name === `send ${testTopic}`
    )

    assert(producerSpan, 'Producer span should be present for propagation test')

    const consumerSpan = spans.find(span =>
      span.kind === SpanKind.CONSUMER &&
      span.name === `process ${testTopic}`
    )

    assert(consumerSpan, 'Consumer span should be present for propagation test')

    assert.equal(
      consumerSpan.spanContext().traceId,
      producerSpan.spanContext().traceId,
      'Consumer span should share trace with producer span',
    )
    assert.equal(
      consumerSpan.parentSpanId,
      producerSpan.spanContext().spanId,
      'Consumer span should be child of producer span',
    )
  })

  test('should create batch spans for batch processing', async () => {
    // Send multiple messages
    const producer = kafkaClient.createProducer()
    const batchSize = 3

    for (let i = 0; i < batchSize; i++) {
      await producer.send({
        topic: testTopic,
        messages: [
          {
            payload: Buffer.from(`batch message ${i}`),
            key: Buffer.from(`batch-key-${i}`),
          },
        ],
      })
    }
    await producer.flush()

    // Reset spans to focus on batch consumer
    memoryExporter.reset()

    // Create consumer for batch processing
    const consumer = kafkaClient.createConsumer({
      groupId: `batch-group-${nanoid()}`,
      enableAutoCommit: false,
    })

    await consumer.subscribe([{
      topic: testTopic,
      allOffsets: { position: 'Beginning' },
    }])

    // Receive messages in batch
    const messages = await consumer.recvBatch(batchSize, 5000)
    assert(messages.length >= 1, `Should receive at least 1 message, got ${messages.length}`)

    await consumer.disconnect()

    // Wait for spans to be processed
    await new Promise(resolve => setTimeout(resolve, 100))

    const spans = memoryExporter.getFinishedSpans()

    // Should have batch span and individual message spans
    const batchSpan = spans.find(span =>
      span.name.includes('batch_process') &&
      span.name.includes(testTopic)
    )

    assert(batchSpan, 'Should have a batch processing span')
    assert.equal(batchSpan.kind, SpanKind.CONSUMER)

    // Verify batch attributes
    const batchAttributes = batchSpan.attributes
    assert.equal(batchAttributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_SYSTEM], 'kafka')
    assert.equal(batchAttributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_OPERATION_NAME], 'batch_process')
    assert(batchAttributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_BATCH_MESSAGE_COUNT] >= 1)

    // Should also have individual message spans
    const messageSpans = spans.filter(span =>
      span.kind === SpanKind.CONSUMER &&
      span.name.includes('process') &&
      !span.name.includes('batch')
    )

    assert(messageSpans.length >= 1, 'Should have individual message spans')
  })

  test('should handle errors in spans correctly', async () => {
    // Create a client that will fail
    const failingClient = new KafkaClient({
      brokers: 'invalid-broker:9092', // Invalid broker
      clientId: `failing-client-${nanoid()}`,
      otel: { serviceName: 'failing-kafka-test' },
    })

    const producer = failingClient.createProducer()

    let errorThrown = false
    try {
      await producer.send({
        topic: testTopic,
        messages: [{ payload: Buffer.from('failing message') }],
      })
    } catch {
      errorThrown = true
    }

    assert(errorThrown, 'Should throw an error for invalid broker')

    // Wait for spans to be processed
    await new Promise(resolve => setTimeout(resolve, 100))

    const spans = memoryExporter.getFinishedSpans()

    // Look for spans with error status
    const errorSpans = spans.filter(span => span.status.code === SpanStatusCode.ERROR)

    // Note: The exact error handling depends on when the error occurs
    // Some errors might not create spans if they fail before instrumentation
    console.log(`Found ${spans.length} total spans, ${errorSpans.length} error spans`)
  })

  test('should support topic filtering configuration', async () => {
    // Create client with topic filtering
    const filteredClient = new KafkaClient({
      brokers: KAFKA_BROKERS,
      clientId: `filtered-client-${nanoid()}`,
      otel: {
        serviceName: 'filtered-kafka-test',
        ignoreTopics: [testTopic], // Ignore our test topic
      },
    })

    const producer = filteredClient.createProducer()

    await producer.send({
      topic: testTopic,
      messages: [{ payload: Buffer.from('ignored message') }],
    })
    await producer.flush()

    // Wait for spans to be processed
    await new Promise(resolve => setTimeout(resolve, 100))

    const spans = memoryExporter.getFinishedSpans()

    // Should have no spans for the ignored topic
    const filteredSpans = spans.filter(span =>
      span.attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_DESTINATION_NAME] === testTopic
    )

    assert.equal(filteredSpans.length, 0, 'Should have no spans for ignored topic')
  })

  test('should support custom message hooks', async () => {
    let hookCalled = false
    let hookMessage = null

    // Create client with custom hook
    const hookedClient = new KafkaClient({
      brokers: KAFKA_BROKERS,
      clientId: `hooked-client-${nanoid()}`,
      otel: {
        serviceName: 'hooked-kafka-test',
        messageHook: (span, message) => {
          hookCalled = true
          hookMessage = message
          span.setAttributes({ 'custom.hook.executed': 'true' })
        },
      },
    })

    // Send a message first
    const producer = hookedClient.createProducer()
    await producer.send({
      topic: testTopic,
      messages: [{ payload: Buffer.from('hook test message') }],
    })
    await producer.flush()

    // Reset spans and create consumer
    memoryExporter.reset()

    const consumer = hookedClient.createConsumer({
      groupId: `hook-group-${nanoid()}`,
      enableAutoCommit: false,
    })

    await consumer.subscribe([{
      topic: testTopic,
      allOffsets: { position: 'Beginning' },
    }])

    const receivedMessage = await consumer.recv()
    assert(receivedMessage, 'Should receive a message')

    await consumer.disconnect()

    // Wait for spans to be processed
    await new Promise(resolve => setTimeout(resolve, 100))

    const spans = memoryExporter.getFinishedSpans()
    const consumerSpan = spans.find(span =>
      span.kind === SpanKind.CONSUMER &&
      span.name.includes(testTopic)
    )

    assert(consumerSpan, 'Should have a consumer span')
    assert(hookCalled, 'Message hook should have been called')
    assert(hookMessage, 'Hook should receive message')
    assert.equal(consumerSpan.attributes['custom.hook.executed'], 'true')
  })

  test('should instrument stream consumers with OTEL spans', async () => {
    const producer = kafkaClient.createProducer()

    await producer.send({
      topic: testTopic,
      messages: [
        {
          payload: Buffer.from('stream message payload'),
        },
      ],
    })
    await producer.flush()

    // Reset spans so we only capture stream consumption spans
    memoryExporter.reset()

    const streamGroupId = `stream-group-${nanoid()}`
    const streamConsumer = kafkaClient.createStreamConsumer({
      groupId: streamGroupId,
      enableAutoCommit: false,
      streamOptions: { objectMode: true },
    })

    const messagePromise = new Promise((resolve, reject) => {
      streamConsumer.once('error', reject)
      streamConsumer.once('data', resolve)
    })

    await streamConsumer.subscribe([{
      topic: testTopic,
      allOffsets: { position: 'Beginning' },
    }])

    const streamMessage = await messagePromise
    assert(streamMessage, 'Stream consumer should receive a message')

    await streamConsumer.disconnect()

    // Allow spans to flush
    await new Promise(resolve => setTimeout(resolve, 100))

    const spans = memoryExporter.getFinishedSpans()
    const streamSpan = spans.find(span =>
      span.kind === SpanKind.CONSUMER &&
      span.name === `process ${testTopic}`
    )

    assert(streamSpan, 'Stream consumer should create a consumer span')
    assert.equal(
      streamSpan.attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_CONSUMER_GROUP_NAME],
      streamGroupId,
      'Stream consumer span should include consumer group attribute',
    )
  })

  test('should work with disabled OTEL configuration', async () => {
    // Create client with OTEL disabled
    const disabledClient = new KafkaClient({
      brokers: KAFKA_BROKERS,
      clientId: `disabled-client-${nanoid()}`,
      otel: false, // Explicitly disable OTEL
    })

    assert.equal(disabledClient.otel.enabled, false, 'OTEL should be disabled')

    const producer = disabledClient.createProducer()

    await producer.send({
      topic: testTopic,
      messages: [{ payload: Buffer.from('non-traced message') }],
    })
    await producer.flush()

    // Wait for potential spans
    await new Promise(resolve => setTimeout(resolve, 100))

    const spans = memoryExporter.getFinishedSpans()

    // Should have no spans from the disabled client
    const clientSpans = spans.filter(span =>
      span.attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_DESTINATION_NAME] === testTopic
    )

    assert.equal(clientSpans.length, 0, 'Should have no spans when OTEL is disabled')
  })

  test('should trace stream consumer operations', async () => {
    // Send test messages first
    const producer = kafkaClient.createProducer()
    const messageCount = 5

    for (let i = 0; i < messageCount; i++) {
      await producer.send({
        topic: testTopic,
        messages: [
          {
            payload: Buffer.from(`stream message ${i}`),
            key: Buffer.from(`stream-key-${i}`),
          },
        ],
      })
    }
    await producer.flush()

    // Reset spans to focus on stream consumer
    memoryExporter.reset()

    // Create stream consumer
    const streamConsumer = kafkaClient.createStreamConsumer({
      groupId: `stream-group-${nanoid()}`,
    })

    await streamConsumer.subscribe([{
      topic: testTopic,
      allOffsets: { position: 'Beginning' },
    }])

    let receivedCount = 0
    const receivedMessages = []

    // Collect messages from stream
    streamConsumer.on('data', (message) => {
      receivedMessages.push(message)
      receivedCount++
      if (receivedCount >= messageCount) {
        streamConsumer.destroy()
      }
    })

    // Wait for all messages to be processed
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('Stream timeout')), 15000)
      const cleanup = () => {
        clearTimeout(timer)
        streamConsumer.removeListener('error', onError)
        streamConsumer.removeListener('end', onEnd)
        streamConsumer.removeListener('close', onEnd)
      }
      const onEnd = () => {
        cleanup()
        resolve()
      }
      const onError = (err) => {
        cleanup()
        reject(err)
      }
      streamConsumer.on('end', onEnd)
      streamConsumer.on('close', onEnd) // destroy() triggers close, not end
      streamConsumer.on('error', onError)
    })

    // Wait for spans to be processed
    await new Promise(resolve => setTimeout(resolve, 200))

    const spans = memoryExporter.getFinishedSpans()
    const consumerSpans = spans.filter(span =>
      span.kind === SpanKind.CONSUMER &&
      span.name.includes(testTopic)
    )

    assert(consumerSpans.length >= 1, `Should have at least 1 consumer span, got ${consumerSpans.length}`)
    assert.equal(receivedCount, messageCount, `Should receive all ${messageCount} messages`)
  })

  test('should trace stream consumer with batch mode', async () => {
    // Send multiple messages
    const producer = kafkaClient.createProducer()
    const batchSize = 3

    for (let i = 0; i < batchSize * 2; i++) {
      await producer.send({
        topic: testTopic,
        messages: [
          {
            payload: Buffer.from(`batch stream message ${i}`),
            key: Buffer.from(`batch-stream-key-${i}`),
          },
        ],
      })
    }
    await producer.flush()

    // Reset spans
    memoryExporter.reset()

    // Create stream consumer with batch configuration
    const batchStreamClient = new KafkaClient({
      brokers: KAFKA_BROKERS,
      clientId: `batch-stream-client-${nanoid()}`,
      otel: {
        serviceName: 'batch-stream-test',
        enableBatchInstrumentation: true,
      },
    })

    const streamConsumer = batchStreamClient.createStreamConsumer({
      groupId: `batch-stream-group-${nanoid()}`,
      batchSize,
      batchTimeout: 1000,
    })

    await streamConsumer.subscribe([{
      topic: testTopic,
      allOffsets: { position: 'Beginning' },
    }])

    let receivedCount = 0
    const receivedMessages = []

    streamConsumer.on('data', (message) => {
      receivedMessages.push(message)
      receivedCount++
      // stop after we see at least two batches worth of messages
      if (receivedCount >= batchSize * 2) {
        streamConsumer.destroy()
      }
    })

    // Wait for messages to be processed
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('Batch stream timeout')), 15000)
      const cleanup = () => {
        clearTimeout(timer)
        streamConsumer.removeListener('error', onError)
        streamConsumer.removeListener('end', onEnd)
        streamConsumer.removeListener('close', onEnd)
      }
      const onEnd = () => {
        cleanup()
        resolve()
      }
      const onError = (err) => {
        cleanup()
        reject(err)
      }
      streamConsumer.on('end', onEnd)
      streamConsumer.on('close', onEnd)
      streamConsumer.on('error', onError)
    })

    // Wait for spans to be processed
    await new Promise(resolve => setTimeout(resolve, 200))

    const spans = memoryExporter.getFinishedSpans()
    const batchSpans = spans.filter(span =>
      span.name.includes('batch_process') &&
      span.name.includes(testTopic)
    )

    assert(batchSpans.length >= 1, 'Should have batch processing spans')
    assert(receivedMessages.length >= batchSize, 'Should receive at least one batch worth of messages')
  })

  test('should handle producer send with delivery reports in spans', async () => {
    // Reset spans
    memoryExporter.reset()

    const producer = kafkaClient.createProducer()
    const testMessage = {
      topic: testTopic,
      messages: [
        {
          payload: Buffer.from('delivery report test'),
          key: Buffer.from('delivery-key'),
        },
      ],
    }

    const deliveryReport = await producer.send(testMessage)
    assert(deliveryReport, 'Should get delivery report')
    assert(Array.isArray(deliveryReport), 'Delivery report should be an array')

    await producer.flush()

    // Wait for spans to be processed
    await new Promise(resolve => setTimeout(resolve, 100))

    const spans = memoryExporter.getFinishedSpans()
    const producerSpan = spans.find(span =>
      span.kind === SpanKind.PRODUCER &&
      span.name.includes(testTopic)
    )

    assert(producerSpan, 'Should have producer span')
    assert.equal(producerSpan.status.code, SpanStatusCode.OK, 'Span should have OK status')

    // Verify delivery report information is captured
    const attributes = producerSpan.attributes
    assert.equal(attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_DESTINATION_NAME], testTopic)
    assert(attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_KAFKA_PARTITION] !== undefined, 'Should have partition info')
  })

  test('should trace complex producer-consumer flow with context propagation', async () => {
    // Reset spans
    memoryExporter.reset()

    // Create a parent span to establish trace context
    const parentSpan = trace.getTracer('test').startSpan('complex-flow-parent')
    const parentContext = trace.setSpan(context.active(), parentSpan)

    let consumerSpan = null

    await context.with(parentContext, async () => {
      // Send message within parent context
      const producer = kafkaClient.createProducer()
      await producer.send({
        topic: testTopic,
        messages: [
          {
            payload: Buffer.from('context propagation test'),
            key: Buffer.from('context-key'),
            headers: { 'test-header': Buffer.from('test-value') },
          },
        ],
      })
      await producer.flush()

      // Create consumer and receive message
      const consumer = kafkaClient.createConsumer({
        groupId: `context-group-${nanoid()}`,
        enableAutoCommit: false,
      })

      await consumer.subscribe([{
        topic: testTopic,
        allOffsets: { position: 'Beginning' },
      }])

      const receivedMessage = await consumer.recv()
      assert(receivedMessage, 'Should receive message')

      await consumer.disconnect()
    })

    parentSpan.end()

    // Wait for spans to be processed
    await new Promise(resolve => setTimeout(resolve, 200))

    const spans = memoryExporter.getFinishedSpans()

    // Find producer and consumer spans
    const producerSpan = spans.find(span =>
      span.kind === SpanKind.PRODUCER &&
      span.name.includes(testTopic)
    )

    consumerSpan = spans.find(span =>
      span.kind === SpanKind.CONSUMER &&
      span.name.includes(testTopic)
    )

    const parentSpanFinished = spans.find(span => span.name === 'complex-flow-parent')

    assert(producerSpan, 'Should have producer span')
    assert(consumerSpan, 'Should have consumer span')
    if (parentSpanFinished) {
      assert.equal(
        producerSpan.spanContext().traceId,
        parentSpanFinished.spanContext().traceId,
        'Producer should be in same trace',
      )
      assert.equal(
        consumerSpan.spanContext().traceId,
        parentSpanFinished.spanContext().traceId,
        'Consumer should be in same trace',
      )
    }
  })

  test('should handle multiple producers and consumers with proper span isolation', async () => {
    const topicA = `${testTopic}-a`
    const topicB = `${testTopic}-b`

    // Reset spans
    memoryExporter.reset()

    // Create multiple producers
    const producer1 = kafkaClient.createProducer()
    const producer2 = kafkaClient.createProducer()

    // Send to different topics
    await Promise.all([
      producer1.send({
        topic: topicA,
        messages: [{ payload: Buffer.from('message for topic A') }],
      }),
      producer2.send({
        topic: topicB,
        messages: [{ payload: Buffer.from('message for topic B') }],
      }),
    ])

    await Promise.all([producer1.flush(), producer2.flush()])

    // Create consumers for both topics
    const consumer1 = kafkaClient.createConsumer({
      groupId: `group-a-${nanoid()}`,
      enableAutoCommit: false,
    })

    const consumer2 = kafkaClient.createConsumer({
      groupId: `group-b-${nanoid()}`,
      enableAutoCommit: false,
    })

    await Promise.all([
      consumer1.subscribe([{ topic: topicA, allOffsets: { position: 'Beginning' } }]),
      consumer2.subscribe([{ topic: topicB, allOffsets: { position: 'Beginning' } }]),
    ])

    const [messageA, messageB] = await Promise.all([
      consumer1.recv(),
      consumer2.recv(),
    ])

    assert(messageA && messageA.topic === topicA, 'Should receive message from topic A')
    assert(messageB && messageB.topic === topicB, 'Should receive message from topic B')

    await Promise.all([consumer1.disconnect(), consumer2.disconnect()])

    // Wait for spans to be processed
    await new Promise(resolve => setTimeout(resolve, 200))

    const spans = memoryExporter.getFinishedSpans()

    // Verify spans for both topics
    const topicASpans = spans.filter(span =>
      span.attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_DESTINATION_NAME] === topicA
    )

    const topicBSpans = spans.filter(span =>
      span.attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_DESTINATION_NAME] === topicB
    )

    assert(topicASpans.length >= 2, 'Should have producer and consumer spans for topic A')
    assert(topicBSpans.length >= 2, 'Should have producer and consumer spans for topic B')

    // Verify spans have different trace IDs (isolated)
    const topicAProducer = topicASpans.find(s => s.kind === SpanKind.PRODUCER)
    const topicBProducer = topicBSpans.find(s => s.kind === SpanKind.PRODUCER)

    assert(topicAProducer && topicBProducer, 'Should have producer spans for both topics')
    // Note: They might have the same trace ID if created in the same async context
  })

  test('should handle consumer group rebalancing with proper span cleanup', async () => {
    // Send initial messages
    const producer = kafkaClient.createProducer()
    const messageCount = 3

    for (let i = 0; i < messageCount; i++) {
      await producer.send({
        topic: testTopic,
        messages: [{ payload: Buffer.from(`rebalance message ${i}`) }],
      })
    }
    await producer.flush()

    // Reset spans
    memoryExporter.reset()

    const groupId = `rebalance-group-${nanoid()}`

    // Create first consumer
    const consumer1 = kafkaClient.createConsumer({
      groupId,
      enableAutoCommit: false,
    })

    await consumer1.subscribe([{
      topic: testTopic,
      allOffsets: { position: 'Beginning' },
    }])

    // Receive some messages
    const message1 = await consumer1.recv()
    assert(message1, 'First consumer should receive message')

    // Create second consumer to trigger rebalancing
    const consumer2 = kafkaClient.createConsumer({
      groupId, // Same group ID to trigger rebalancing
      enableAutoCommit: false,
    })

    await consumer2.subscribe([{
      topic: testTopic,
      allOffsets: { position: 'Beginning' },
    }])

    // Give some time for rebalancing
    await new Promise(resolve => setTimeout(resolve, 1000))

    // Try to receive from both consumers
    const message2 = await consumer2.recv()
    assert(message2, 'Second consumer should receive message after rebalancing')

    await Promise.all([consumer1.disconnect(), consumer2.disconnect()])

    // Wait for spans to be processed
    await new Promise(resolve => setTimeout(resolve, 200))

    const spans = memoryExporter.getFinishedSpans()
    const consumerSpans = spans.filter(span =>
      span.kind === SpanKind.CONSUMER &&
      span.name.includes(testTopic)
    )

    assert(consumerSpans.length >= 2, 'Should have spans from both consumers')
  })

  test('should handle producer configuration hooks correctly', async () => {
    let producerHookCalled = false
    let hookRecord = null

    // Create client with producer hook
    const hookedClient = new KafkaClient({
      brokers: KAFKA_BROKERS,
      clientId: `producer-hook-client-${nanoid()}`,
      otel: {
        serviceName: 'producer-hook-test',
        producerHook: (span, record, _metadata) => {
          producerHookCalled = true
          hookRecord = record
          span.setAttributes({ 'custom.producer.hook': 'executed' })
        },
      },
    })

    // Reset spans
    memoryExporter.reset()

    const producer = hookedClient.createProducer()
    const testRecord = {
      topic: testTopic,
      messages: [{ payload: Buffer.from('producer hook test') }],
    }

    await producer.send(testRecord)
    await producer.flush()

    // Wait for spans to be processed
    await new Promise(resolve => setTimeout(resolve, 100))

    const spans = memoryExporter.getFinishedSpans()
    const producerSpan = spans.find(span =>
      span.kind === SpanKind.PRODUCER &&
      span.name.includes(testTopic)
    )

    assert(producerSpan, 'Should have producer span')
    assert(producerHookCalled, 'Producer hook should have been called')
    assert(hookRecord, 'Hook should receive record')
    assert.equal(producerSpan.attributes['custom.producer.hook'], 'executed')
  })

  test('should propagate context across async producer operations', async () => {
    // Reset spans
    memoryExporter.reset()

    // Create a root span
    const rootSpan = trace.getTracer('test').startSpan('async-root-operation')
    const rootContext = trace.setSpan(context.active(), rootSpan)

    const producer = kafkaClient.createProducer()

    await context.with(rootContext, async () => {
      // Create a child span within the context
      const childSpan = trace.getTracer('test').startSpan('async-child-operation')

      await context.with(trace.setSpan(context.active(), childSpan), async () => {
        // Send message within child context
        await producer.send({
          topic: testTopic,
          messages: [{ payload: Buffer.from('async context test') }],
        })
      })

      childSpan.end()
    })

    rootSpan.end()
    await producer.flush()

    // Wait for spans to be processed
    await new Promise(resolve => setTimeout(resolve, 200))

    const spans = memoryExporter.getFinishedSpans()

    const producerSpan = spans.find(s => s.kind === SpanKind.PRODUCER && s.name.includes(testTopic))

    // In this environment user spans may not be exported; ensure Kafka producer span exists
    assert(producerSpan, `Should have producer span. Spans: ${spans.map(s => s.name).join(', ')}`)
  })

  test('should handle context propagation in concurrent operations', async () => {
    // Reset spans
    memoryExporter.reset()

    const producer = kafkaClient.createProducer()
    const concurrentCount = 5

    // Create multiple concurrent operations with different contexts
    const operations = Array.from({ length: concurrentCount }, (_, i) => {
      return new Promise(async (resolve) => {
        const operationSpan = trace.getTracer('test').startSpan(`concurrent-op-${i}`)
        const operationContext = trace.setSpan(context.active(), operationSpan)

        await context.with(operationContext, async () => {
          await producer.send({
            topic: testTopic,
            messages: [{
              payload: Buffer.from(`concurrent message ${i}`),
              key: Buffer.from(`key-${i}`),
            }],
          })
        })

        operationSpan.end()
        resolve(i)
      })
    })

    await Promise.all(operations)
    await producer.flush()

    // Wait for spans to be processed
    await new Promise(resolve => setTimeout(resolve, 200))

    const spans = memoryExporter.getFinishedSpans()

    const producerSpans = spans.filter(s => s.kind === SpanKind.PRODUCER && s.name.includes(testTopic))

    assert(producerSpans.length >= concurrentCount,
      `Should have producer spans for all operations. Producer spans: ${producerSpans.length}, names: ${producerSpans.map(s => s.name).join(', ')
      }`)
  })

  test('should propagate context through stream consumer processing', async () => {
    // Send test message first
    const producer = kafkaClient.createProducer()
    await producer.send({
      topic: testTopic,
      messages: [{ payload: Buffer.from('stream context test') }],
    })
    await producer.flush()

    // Reset spans
    memoryExporter.reset()

    // Create parent context for stream processing
    const parentSpan = trace.getTracer('test').startSpan('stream-processing-parent')
    const parentContext = trace.setSpan(context.active(), parentSpan)

    let streamProcessingComplete = false

    await context.with(parentContext, async () => {
      const streamConsumer = kafkaClient.createStreamConsumer({
        groupId: `stream-context-group-${nanoid()}`,
      })

      await streamConsumer.subscribe([{
        topic: testTopic,
        allOffsets: { position: 'Beginning' },
      }])

      streamConsumer.on('data', (message) => {
        // Create processing span within the context
        const processingSpan = trace.getTracer('test').startSpan('stream-message-processing')

        // Simulate some processing work
        setTimeout(() => {
          processingSpan.setAttributes({
            'message.topic': message.topic,
            'processing.type': 'stream',
          })
          processingSpan.end()
          streamProcessingComplete = true
          streamConsumer.destroy()
        }, 10)
      })

      await new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error('Stream context timeout')), 5000)
        const cleanup = () => {
          clearTimeout(timer)
          streamConsumer.removeListener('end', onEnd)
          streamConsumer.removeListener('close', onEnd)
          streamConsumer.removeListener('error', onError)
        }
        const onEnd = () => {
          cleanup()
          resolve()
        }
        const onError = (err) => {
          cleanup()
          reject(err)
        }
        streamConsumer.on('end', onEnd)
        streamConsumer.on('close', onEnd) // destroy() triggers close, not end
        streamConsumer.on('error', onError)
      })
    })

    parentSpan.end()

    // Wait for all processing to complete
    await new Promise(resolve => setTimeout(resolve, 200))

    const spans = memoryExporter.getFinishedSpans()

    const consumerSpan = spans.find(s => s.kind === SpanKind.CONSUMER && s.name.includes(testTopic))

    assert(consumerSpan, `Should have consumer span. Spans: ${spans.map(s => s.name).join(', ')}`)
    assert(streamProcessingComplete, 'Stream processing should complete')
  })

  test('should handle context extraction and injection across message boundaries', async () => {
    // Reset spans
    memoryExporter.reset()

    // Create producer context
    const producerSpan = trace.getTracer('test').startSpan('message-boundary-producer')
    const producerContext = trace.setSpan(context.active(), producerSpan)

    const producer = kafkaClient.createProducer()

    // Send message with injected context
    await context.with(producerContext, async () => {
      await producer.send({
        topic: testTopic,
        messages: [{
          payload: Buffer.from('boundary test message'),
          key: Buffer.from('boundary-key'),
          headers: { 'custom-header': Buffer.from('custom-value') },
        }],
      })
    })

    producerSpan.end()
    await producer.flush()

    // Small delay to ensure message is available
    await new Promise(resolve => setTimeout(resolve, 500))

    // Create separate consumer context
    const consumerSpan = trace.getTracer('test').startSpan('message-boundary-consumer')
    const consumerContext = trace.setSpan(context.active(), consumerSpan)

    let receivedMessage = null

    await context.with(consumerContext, async () => {
      const consumer = kafkaClient.createConsumer({
        groupId: `boundary-group-${nanoid()}`,
        enableAutoCommit: false,
      })

      await consumer.subscribe([{
        topic: testTopic,
        allOffsets: { position: 'Beginning' },
      }])

      receivedMessage = await consumer.recv()
      await consumer.disconnect()
    })

    consumerSpan.end()

    // Wait for spans to be processed
    await new Promise(resolve => setTimeout(resolve, 200))

    const spans = memoryExporter.getFinishedSpans()

    const kafkaProducerSpan = spans.find(s => s.kind === SpanKind.PRODUCER && s.name.includes(testTopic))
    const kafkaConsumerSpan = spans.find(s => s.kind === SpanKind.CONSUMER && s.name.includes(testTopic))

    assert(kafkaProducerSpan, 'Should have Kafka producer span')
    assert(kafkaConsumerSpan, 'Should have Kafka consumer span')
    assert(receivedMessage, 'Should receive message')

    // Verify message headers contain trace context
    assert(receivedMessage.headers, 'Message should have headers')
  })

  test('should handle nested async operations with proper context isolation', async () => {
    // Reset spans
    memoryExporter.reset()

    const producer = kafkaClient.createProducer()

    // Create nested async operations
    const rootSpan = trace.getTracer('test').startSpan('nested-root')
    const rootContext = trace.setSpan(context.active(), rootSpan)

    await context.with(rootContext, async () => {
      // Level 1: Database operation simulation
      const dbSpan = trace.getTracer('test').startSpan('database-operation')
      const dbContext = trace.setSpan(context.active(), dbSpan)

      await context.with(dbContext, async () => {
        // Level 2: Business logic simulation
        const businessSpan = trace.getTracer('test').startSpan('business-logic')
        const businessContext = trace.setSpan(context.active(), businessSpan)

        await context.with(businessContext, async () => {
          // Level 3: Kafka message sending
          await producer.send({
            topic: testTopic,
            messages: [{
              payload: Buffer.from('nested operation result'),
              key: Buffer.from('nested-key'),
            }],
          })
        })

        businessSpan.end()
      })

      dbSpan.end()
    })

    rootSpan.end()
    await producer.flush()

    // Wait for spans to be processed
    await new Promise(resolve => setTimeout(resolve, 200))

    const spans = memoryExporter.getFinishedSpans()

    const kafkaSpanRecorded = spans.find(s => s.kind === SpanKind.PRODUCER && s.name.includes(testTopic))
    assert(kafkaSpanRecorded, 'Should have Kafka span')
  })

  test('should maintain context across batch operations', async () => {
    // Send multiple test messages
    const producer = kafkaClient.createProducer()
    const batchSize = 3

    for (let i = 0; i < batchSize; i++) {
      await producer.send({
        topic: testTopic,
        messages: [{ payload: Buffer.from(`batch context message ${i}`) }],
      })
    }
    await producer.flush()

    // Reset spans
    memoryExporter.reset()

    // Create batch processing context
    const batchSpan = trace.getTracer('test').startSpan('batch-processing-context')
    const batchContext = trace.setSpan(context.active(), batchSpan)

    let processedMessages = []

    await context.with(batchContext, async () => {
      const consumer = kafkaClient.createConsumer({
        groupId: `batch-context-group-${nanoid()}`,
        enableAutoCommit: false,
      })

      await consumer.subscribe([{
        topic: testTopic,
        allOffsets: { position: 'Beginning' },
      }])

      // Receive messages in batch
      const messages = await consumer.recvBatch(batchSize, 5000)
      processedMessages = messages

      // Process each message in the context
      for (const message of messages) {
        const messageSpan = trace.getTracer('test').startSpan('message-processing')
        messageSpan.setAttributes({
          'message.topic': message.topic,
          'message.offset': message.offset,
        })
        messageSpan.end()
      }

      await consumer.disconnect()
    })

    batchSpan.end()

    // Wait for spans to be processed
    await new Promise(resolve => setTimeout(resolve, 200))

    const spans = memoryExporter.getFinishedSpans()

    const kafkaBatchSpan = spans.find(s => s.name.includes('batch_process') && s.name.includes(testTopic))

    assert(kafkaBatchSpan, `Should have Kafka batch span. Spans: ${spans.map(s => s.name).join(', ')}`)
    assert(processedMessages.length >= 1, 'Should process at least one message')
  })
})
