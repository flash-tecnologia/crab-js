import { nanoid } from 'nanoid'
import assert from 'node:assert'
import * as net from 'node:net'
import { afterEach, beforeEach, describe, test } from 'node:test'

// OpenTelemetry test infrastructure
import { context, type Span, SpanKind, SpanStatusCode, trace } from '@opentelemetry/api'
import { AsyncHooksContextManager } from '@opentelemetry/context-async-hooks'
import { AlwaysOnSampler, InMemorySpanExporter, SimpleSpanProcessor } from '@opentelemetry/sdk-trace-base'
import { NodeTracerProvider } from '@opentelemetry/sdk-trace-node'

// ESM imports for Kafka client
import { KafkaClient } from 'kafka-crab-js'

// ESM imports for OTEL package
import {
  enableOtelInstrumentation,
  getKafkaInstrumentation,
  KAFKA_SEMANTIC_CONVENTIONS,
  type KafkaCrabInstrumentation,
  type OtelAdapter,
  resetKafkaInstrumentation,
  resetOtelAdapter,
} from '../../dist/index.js'

const KAFKA_BROKERS = process.env.KAFKA_BROKERS || 'localhost:9092'
const TEST_TIMEOUT = 120000

async function isKafkaReachable(brokers: string, timeoutMs = 750): Promise<boolean> {
  const first = String(brokers).split(',')[0]?.trim()
  if (!first) {
    return false
  }

  const [host, portRaw] = first.split(':')
  const port = portRaw ? Number(portRaw) : 9092

  if (!host || !Number.isFinite(port)) {
    return false
  }

  return new Promise(resolve => {
    const socket = net.connect({ host, port })

    let done = false
    const finish = (ok: boolean) => {
      if (done) {
        return
      }
      done = true
      socket.removeAllListeners()
      socket.destroy()
      resolve(ok)
    }

    socket.once('error', () => finish(false))
    socket.setTimeout(timeoutMs, () => finish(false))

    socket.once('connect', () => {
      try {
        // Kafka protocol sanity check (ApiVersionsRequest v0)
        // This avoids false positives when the port is open but isn't Kafka.
        const clientId = 'kafka-crab-js-test'
        const clientIdBytes = Buffer.from(clientId, 'utf8')

        const header = Buffer.alloc(2 + 2 + 4 + 2 + clientIdBytes.length)
        let offset = 0
        header.writeInt16BE(18, offset) // ApiKey: ApiVersions
        offset += 2
        header.writeInt16BE(0, offset) // ApiVersion: 0
        offset += 2
        header.writeInt32BE(1, offset) // CorrelationId
        offset += 4
        header.writeInt16BE(clientIdBytes.length, offset) // ClientId length
        offset += 2
        clientIdBytes.copy(header, offset)

        const frame = Buffer.alloc(4 + header.length)
        frame.writeInt32BE(header.length, 0) // Length prefix excludes itself
        header.copy(frame, 4)

        socket.write(frame)
      } catch {
        finish(false)
      }
    })

    let buffered = Buffer.alloc(0)
    socket.on('data', (chunk) => {
      buffered = Buffer.concat([buffered, chunk])
      if (buffered.length < 4) {
        return
      }

      const size = buffered.readInt32BE(0)
      // Kafka responses start with a sane frame size; cap to 1MB for reachability checks.
      if (size <= 0 || size > 1024 * 1024) {
        finish(false)
        return
      }

      if (buffered.length < 4 + size) {
        return
      }

      const payload = buffered.subarray(4, 4 + size)
      if (payload.length < 6) {
        finish(false)
        return
      }

      const correlationId = payload.readInt32BE(0)
      if (correlationId !== 1) {
        finish(false)
        return
      }

      // ApiVersionsResponse v0 begins with error_code (int16) after correlationId
      const errorCode = payload.readInt16BE(4)
      if (errorCode < 0 || errorCode > 1000) {
        finish(false)
        return
      }

      finish(true)
    })
  })
}

interface MessageWithEndSpan {
  endSpan?: () => void
  topic?: string
  partition?: number
  offset?: number
  headers?: Record<string, Buffer>
}

type MessageOrBatch = MessageWithEndSpan | MessageWithEndSpan[] | null

function endOtelSpans(target: MessageOrBatch): void {
  if (!target) {
    return
  }

  if (Array.isArray(target)) {
    if (typeof (target as unknown as MessageWithEndSpan).endSpan === 'function') {
      const maybeSpan = target as unknown as MessageWithEndSpan
      maybeSpan.endSpan?.()
      return
    }

    for (const item of target) {
      if (item?.endSpan) {
        item.endSpan()
      }
    }
    return
  }

  if (typeof target.endSpan === 'function') {
    target.endSpan()
  }
}

const kafkaAvailable = await (async () => {
  if (process.env.KAFKA_AVAILABLE === 'true') return true
  if (process.env.KAFKA_AVAILABLE === 'false') return false
  return isKafkaReachable(KAFKA_BROKERS)
})()

const describeKafka = kafkaAvailable ? describe : describe.skip

describeKafka('KafkaClient OpenTelemetry Integration', { timeout: TEST_TIMEOUT }, () => {
  let memoryExporter: InMemorySpanExporter
  let spanProcessor: SimpleSpanProcessor
  let provider: NodeTracerProvider
  let contextManager: AsyncHooksContextManager
  let instrumentation: KafkaCrabInstrumentation
  let otelAdapter: OtelAdapter
  let kafkaClient: KafkaClient
  let testTopic: string

  const flushSpans = async () => {
    await spanProcessor.forceFlush()
    await provider.forceFlush()
    await new Promise(resolve => setTimeout(resolve, 10))
    return memoryExporter.getFinishedSpans()
  }

  beforeEach(async () => {
    // Generate unique test topic
    testTopic = `test-otel-${nanoid()}`

    // Reset any existing instrumentation
    resetKafkaInstrumentation()
    resetOtelAdapter()

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

    // Enable OTEL instrumentation via the adapter (subscribes to diagnostic channels)
    otelAdapter = enableOtelInstrumentation({
      tracerProvider: provider,
      captureMessageHeaders: true,
      captureMessagePayload: true,
    })

    // Create Kafka client (no otel option needed - adapter listens to diagnostic channels)
    kafkaClient = new KafkaClient({
      brokers: KAFKA_BROKERS,
      clientId: `test-otel-client-${nanoid()}`,
    })

    // Get the instrumentation instance for reference
    instrumentation = getKafkaInstrumentation()
    instrumentation.setTracerProvider(provider)
  })

  afterEach(async () => {
    // Cleanup resources
    try {
      contextManager.disable()
      if (otelAdapter) {
        otelAdapter.disable()
      }
      await spanProcessor.forceFlush()
      memoryExporter.reset()
      provider.forceFlush()
    } catch (error) {
      console.warn('Cleanup error:', (error as Error).message)
    }

    resetKafkaInstrumentation()
    resetOtelAdapter()
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

    const spans = await flushSpans()

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
    assert((attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_MESSAGE_BODY_SIZE] as number) > 0)
  })

  test('should omit message body size when payload capture is disabled', async () => {
    // Reconfigure adapter with payload capture disabled
    otelAdapter.disable()
    resetOtelAdapter()
    otelAdapter = enableOtelInstrumentation({
      tracerProvider: provider,
      captureMessageHeaders: true,
      captureMessagePayload: false,
    })

    // Recreate client
    kafkaClient = new KafkaClient({
      brokers: KAFKA_BROKERS,
      clientId: `test-otel-client-no-payload-${nanoid()}`,
    })

    const producer = kafkaClient.createProducer()

    const testMessage = {
      topic: testTopic,
      messages: [
        {
          payload: Buffer.from('test message without payload capture'),
          headers: { 'test-header': Buffer.from('test-value') },
        },
      ],
    }

    await producer.send(testMessage)
    await producer.flush()

    const spans = await flushSpans()
    const producerSpan = spans.find(span =>
      span.kind === SpanKind.PRODUCER &&
      span.name.includes(testTopic)
    )

    assert(producerSpan, 'Should have a producer span')
    const attributes = producerSpan.attributes
    assert.equal(attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_SYSTEM], 'kafka')
    assert.equal(attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_DESTINATION_NAME], testTopic)
    assert.equal(attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_OPERATION_NAME], 'send')
    assert.equal(attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_BATCH_MESSAGE_COUNT], 1)
    assert.strictEqual(attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_MESSAGE_BODY_SIZE], undefined)
  })

  test('should inject trace context into message headers', async () => {
    const producer = kafkaClient.createProducer()

    // Create a parent span to establish trace context
    const parentSpan = trace.getTracer('test').startSpan('test-operation')
    const testContext = trace.setSpan(context.active(), parentSpan)
    let receivedMessage: MessageWithEndSpan | null = null

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

    const consumer = kafkaClient.createConsumer({
      groupId: `trace-header-${nanoid()}`,
      enableAutoCommit: false,
    })

    await consumer.subscribe([{
      topic: testTopic,
      allOffsets: { position: 'Beginning' },
    }])

    receivedMessage = await consumer.recv() as MessageWithEndSpan
    endOtelSpans(receivedMessage)
    await consumer.disconnect()

    parentSpan.end()

    const spans = await flushSpans()
    const producerSpan = spans.find(span =>
      span.kind === SpanKind.PRODUCER &&
      span.name.includes(testTopic)
    )

    const consumerSpan = spans.find(span =>
      span.kind === SpanKind.CONSUMER &&
      span.name === `process ${testTopic}`
    )

    const traceparentHeader = receivedMessage?.headers?.traceparent?.toString('utf8')
    assert(traceparentHeader, 'Message headers should include traceparent')
    assert(traceparentHeader.includes(parentSpan.spanContext().traceId),
      'traceparent header should carry parent trace id')

    assert(producerSpan, 'Should have a producer span')

    // Verify trace relationship
    assert.equal(producerSpan.spanContext().traceId, parentSpan.spanContext().traceId)
    assert(consumerSpan, 'Should have consumer span for injected context')
    assert.equal(consumerSpan.spanContext().traceId, producerSpan.spanContext().traceId)
    assert.equal(consumerSpan.parentSpanId, producerSpan.spanContext().spanId)
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
    const receivedMessage = await consumer.recv() as MessageWithEndSpan
    endOtelSpans(receivedMessage)
    assert(receivedMessage, 'Should receive a message')
    assert.equal(receivedMessage.topic, testTopic)

    await consumer.disconnect()

    const spans = await flushSpans()
    if (process.env.DEBUG_OTEL_TESTS === 'true') {
      console.log('message-boundary spans', spans.map(s => ({
        name: s.name,
        kind: s.kind,
        traceId: s.spanContext().traceId,
        parent: s.parentSpanId,
      })))
    }
    const consumerSpan = spans.find(span =>
      span.kind === SpanKind.CONSUMER &&
      span.name.startsWith('process ') &&
      span.name.endsWith(testTopic)
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
    assert.equal(attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_DESTINATION_PARTITION_ID], receivedMessage.partition)
    assert.equal(attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_KAFKA_OFFSET], receivedMessage.offset)
  })

  test('should not finish process span until endSpan is invoked', async () => {
    const producer = kafkaClient.createProducer()
    await producer.send({
      topic: testTopic,
      messages: [{ payload: Buffer.from('delayed endSpan message') }],
    })
    await producer.flush()

    memoryExporter.reset()

    const consumer = kafkaClient.createConsumer({
      groupId: `no-endspan-group-${nanoid()}`,
      enableAutoCommit: false,
    })

    await consumer.subscribe([{
      topic: testTopic,
      allOffsets: { position: 'Beginning' },
    }])

    const receivedMessage = await consumer.recv() as MessageWithEndSpan
    assert(receivedMessage, 'Should receive a message')
    assert.equal(typeof receivedMessage.endSpan, 'function', 'Message should expose endSpan when OTEL is enabled')

    await consumer.disconnect()

    const spansBefore = await flushSpans()
    const processBefore = spansBefore.find(span =>
      span.kind === SpanKind.CONSUMER &&
      span.name === `process ${testTopic}`
    )
    assert.equal(processBefore, undefined, 'Process span should not be finished without calling endSpan()')

    // Cleanup: end span after assertion so we don't leak spans across tests
    endOtelSpans(receivedMessage)
    const spansAfter = await flushSpans()
    const processAfter = spansAfter.find(span =>
      span.kind === SpanKind.CONSUMER &&
      span.name === `process ${testTopic}`
    )
    assert(processAfter, 'Process span should finish after calling endSpan()')
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

    const propagatedMessage = await consumer.recv() as MessageWithEndSpan
    endOtelSpans(propagatedMessage)
    assert(propagatedMessage, 'Should receive a message for propagation check')

    await consumer.disconnect()

    const spans = await flushSpans()

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

  test('should create a new consumer trace when message has no traceparent and ambient span exists', async () => {
    // Use a producer without diagnostics instrumentation so headers are not auto-injected.
    const rawKafkaClient = new KafkaClient({
      brokers: KAFKA_BROKERS,
      clientId: `raw-producer-${nanoid()}`,
      diagnostics: false,
    })
    const rawProducer = rawKafkaClient.createProducer()

    await rawProducer.send({
      topic: testTopic,
      messages: [
        {
          payload: Buffer.from('message-without-traceparent'),
          headers: {},
        },
      ],
    })
    await rawProducer.flush()

    memoryExporter.reset()

    const consumer = kafkaClient.createConsumer({
      groupId: `no-traceparent-group-${nanoid()}`,
      enableAutoCommit: false,
    })

    await consumer.subscribe([{
      topic: testTopic,
      allOffsets: { position: 'Beginning' },
    }])

    const ambientSpan = trace.getTracer('test').startSpan('ambient-consumer-parent')
    let receivedMessage: MessageWithEndSpan | null = null
    try {
      await context.with(trace.setSpan(context.active(), ambientSpan), async () => {
        receivedMessage = await consumer.recv() as MessageWithEndSpan
      })
    } finally {
      ambientSpan.end()
    }

    endOtelSpans(receivedMessage)
    await consumer.disconnect()

    const spans = await flushSpans()
    const consumerSpan = spans.find(span =>
      span.kind === SpanKind.CONSUMER &&
      span.name === `process ${testTopic}`
    )

    assert(consumerSpan, 'Should have consumer span for no-traceparent message')
    assert.notEqual(
      consumerSpan.spanContext().traceId,
      ambientSpan.spanContext().traceId,
      'consumer span should not inherit ambient trace when traceparent is absent',
    )
  })

  test('should continue trace when consumer receives mixed-case TraceParent header', async () => {
    // Use a producer without diagnostics instrumentation and set a mixed-case TraceParent header manually.
    const rawKafkaClient = new KafkaClient({
      brokers: KAFKA_BROKERS,
      clientId: `raw-producer-mixed-${nanoid()}`,
      diagnostics: false,
    })
    const rawProducer = rawKafkaClient.createProducer()

    const upstreamSpan = trace.getTracer('test').startSpan('upstream-mixed-case-parent')
    const upstreamContext = upstreamSpan.spanContext()
    const traceparent = `00-${upstreamContext.traceId}-${upstreamContext.spanId}-01`

    await rawProducer.send({
      topic: testTopic,
      messages: [
        {
          payload: Buffer.from('message-with-mixed-case-traceparent'),
          headers: { TraceParent: Buffer.from(traceparent) } as Record<string, Buffer>,
        },
      ],
    })
    await rawProducer.flush()
    upstreamSpan.end()

    memoryExporter.reset()

    const consumer = kafkaClient.createConsumer({
      groupId: `mixed-traceparent-group-${nanoid()}`,
      enableAutoCommit: false,
    })

    await consumer.subscribe([{
      topic: testTopic,
      allOffsets: { position: 'Beginning' },
    }])

    const receivedMessage = await consumer.recv() as MessageWithEndSpan
    endOtelSpans(receivedMessage)
    await consumer.disconnect()

    const spans = await flushSpans()
    const consumerSpan = spans.find(span =>
      span.kind === SpanKind.CONSUMER &&
      span.name === `process ${testTopic}`
    )

    assert(consumerSpan, 'Should have consumer span for mixed-case TraceParent message')
    assert.equal(
      consumerSpan.spanContext().traceId,
      upstreamContext.traceId,
      'consumer span should continue upstream trace from mixed-case TraceParent header',
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
    const messages = await consumer.recvBatch(batchSize, 5000) as MessageWithEndSpan[]
    endOtelSpans(messages)
    assert(messages.length >= 1, `Should receive at least 1 message, got ${messages.length}`)

    await consumer.disconnect()

    const spans = await flushSpans()

    // Should have batch span and individual message spans
    const batchSpan = spans.find(span =>
      span.kind === SpanKind.CONSUMER &&
      span.name === `process ${testTopic}` &&
      (span.attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_BATCH_MESSAGE_COUNT] as number) >= 1 &&
      span.attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_DESTINATION_PARTITION_ID] === undefined
    )

    assert(batchSpan, 'Should have a batch processing span')
    assert.equal(batchSpan.kind, SpanKind.CONSUMER)

    // Verify batch attributes
    const batchAttributes = batchSpan.attributes
    assert.equal(batchAttributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_SYSTEM], 'kafka')
    assert.equal(batchAttributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_OPERATION_NAME], 'process')
    assert((batchAttributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_BATCH_MESSAGE_COUNT] as number) >= 1)

    // Should also have individual message spans
    const messageSpans = spans.filter(span =>
      span.kind === SpanKind.CONSUMER &&
      span.name === `process ${testTopic}` &&
      span.attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_DESTINATION_PARTITION_ID] !== undefined
    )

    assert(messageSpans.length >= 1, 'Should have individual message spans')
  })

  test('should handle errors in spans correctly', async () => {
    // Create a client that will fail (OTEL is already enabled via adapter)
    const failingClient = new KafkaClient({
      brokers: 'invalid-broker:9092', // Invalid broker
      clientId: `failing-client-${nanoid()}`,
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

    const spans = await flushSpans()

    assert(spans.length >= 1, 'Expected at least one span for failing producer operation')

    // Look for spans with error status
    const errorSpans = spans.filter(span => span.status.code === SpanStatusCode.ERROR)
    assert(errorSpans.length >= 1,
      `Expected error spans to be recorded. Spans: ${
        spans.map(s => `${s.name}:${SpanStatusCode[s.status.code]}`).join(', ')
      }`)
  })

  test('should support topic filtering configuration', async () => {
    // Reconfigure adapter with topic filtering
    otelAdapter.disable()
    resetOtelAdapter()
    otelAdapter = enableOtelInstrumentation({
      tracerProvider: provider,
      ignoreTopics: [testTopic], // Ignore our test topic
    })

    // Create client (OTEL adapter will filter topics)
    const filteredClient = new KafkaClient({
      brokers: KAFKA_BROKERS,
      clientId: `filtered-client-${nanoid()}`,
    })

    const producer = filteredClient.createProducer()

    await producer.send({
      topic: testTopic,
      messages: [{ payload: Buffer.from('ignored message') }],
    })
    await producer.flush()

    const spans = await flushSpans()

    // Should have no spans for the ignored topic
    const filteredSpans = spans.filter(span =>
      span.attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_DESTINATION_NAME] === testTopic
    )

    assert.equal(filteredSpans.length, 0, 'Should have no spans for ignored topic')
  })

  test('should support custom message hooks', async () => {
    let hookCalled = false
    let hookMessage: unknown = null

    // Reconfigure adapter with custom hook
    otelAdapter.disable()
    resetOtelAdapter()
    otelAdapter = enableOtelInstrumentation({
      tracerProvider: provider,
      messageHook: (span: Span, message: unknown) => {
        hookCalled = true
        hookMessage = message
        span.setAttributes({ 'custom.hook.executed': 'true' })
      },
    })

    // Create client
    const hookedClient = new KafkaClient({
      brokers: KAFKA_BROKERS,
      clientId: `hooked-client-${nanoid()}`,
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

    const receivedMessage = await consumer.recv() as MessageWithEndSpan
    endOtelSpans(receivedMessage)
    assert(receivedMessage, 'Should receive a message')

    await consumer.disconnect()

    const spans = await flushSpans()
    const consumerSpan = spans.find(span =>
      span.kind === SpanKind.CONSUMER &&
      span.name.startsWith('process ') &&
      span.name.endsWith(testTopic)
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

    const messagePromise = new Promise<MessageWithEndSpan>((resolve, reject) => {
      streamConsumer.once('error', reject)
      streamConsumer.once('data', resolve)
    })

    await streamConsumer.subscribe([{
      topic: testTopic,
      allOffsets: { position: 'Beginning' },
    }])

    const streamMessage = await messagePromise
    endOtelSpans(streamMessage)
    assert(streamMessage, 'Stream consumer should receive a message')

    await streamConsumer.disconnect()

    const spans = await flushSpans()
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
    // Disable the OTEL adapter entirely
    otelAdapter.disable()

    // Create client (no OTEL instrumentation since adapter is disabled)
    const disabledClient = new KafkaClient({
      brokers: KAFKA_BROKERS,
      clientId: `disabled-client-${nanoid()}`,
    })

    const producer = disabledClient.createProducer()

    await producer.send({
      topic: testTopic,
      messages: [{ payload: Buffer.from('non-traced message') }],
    })
    await producer.flush()

    const spans = await flushSpans()

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
    const receivedMessages: MessageWithEndSpan[] = []

    // Collect messages from stream
    streamConsumer.on('data', (message: MessageWithEndSpan) => {
      endOtelSpans(message)
      receivedMessages.push(message)
      receivedCount++
      if (receivedCount >= messageCount) {
        streamConsumer.destroy()
      }
    })

    // Wait for all messages to be processed
    await new Promise<void>((resolve, reject) => {
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
      const onError = (err: Error) => {
        cleanup()
        reject(err)
      }
      streamConsumer.on('end', onEnd)
      streamConsumer.on('close', onEnd) // Destroy() triggers close, not end
      streamConsumer.on('error', onError)
    })

    const spans = await flushSpans()
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
    const streamParent = trace.getTracer('test').startSpan('stream-batch-parent')
    const streamParentContext = trace.setSpan(context.active(), streamParent) // Bound to producer sends for trace linkage

    for (let i = 0; i < batchSize * 2; i++) {
      await context.with(streamParentContext, () =>
        producer.send({
          topic: testTopic,
          messages: [
            {
              payload: Buffer.from(`batch stream message ${i}`),
              key: Buffer.from(`batch-stream-key-${i}`),
            },
          ],
        }))
    }
    await producer.flush()
    streamParent.end()

    // Reset spans
    memoryExporter.reset()

    // Create stream consumer with batch configuration using main kafkaClient
    const streamConsumer = kafkaClient.createStreamConsumer({
      groupId: `batch-stream-group-${nanoid()}`,
      enableAutoCommit: false,
      batchSize,
      batchTimeout: 1000,
      streamOptions: { objectMode: true },
    })

    await streamConsumer.subscribe([{
      topic: testTopic,
      allOffsets: { position: 'Beginning' },
    }])

    let receivedCount = 0
    const receivedMessages: MessageWithEndSpan[] = []

    streamConsumer.on('data', (message: MessageWithEndSpan) => {
      endOtelSpans(message)
      receivedMessages.push(message)
      receivedCount++
      // Stop after we see at least two batches worth of messages
      if (receivedCount >= batchSize * 2) {
        streamConsumer.destroy()
      }
    })

    // Wait for messages to be processed
    await new Promise<void>((resolve, reject) => {
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
      const onError = (err: Error) => {
        cleanup()
        reject(err)
      }
      streamConsumer.on('end', onEnd)
      streamConsumer.on('close', onEnd)
      streamConsumer.on('error', onError)
    })

    const spans = await flushSpans()

    const consumerSpans = spans.filter(span => span.kind === SpanKind.CONSUMER && span.name === `process ${testTopic}`)

    assert(consumerSpans.length >= 1, `Should have consumer spans from stream batch mode, got ${consumerSpans.length}`)
    assert(receivedMessages.length >= batchSize, 'Should receive at least one batch worth of messages')
    const allOnParentTrace = consumerSpans.every(span =>
      span.spanContext().traceId === streamParent.spanContext().traceId
    )
    assert(allOnParentTrace, 'Stream batch consumer spans should stay on parent context trace')
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

    const spans = await flushSpans()
    const producerSpan = spans.find(span =>
      span.kind === SpanKind.PRODUCER &&
      span.name.includes(testTopic)
    )

    assert(producerSpan, 'Should have producer span')
    assert.equal(producerSpan.status.code, SpanStatusCode.OK, 'Span should have OK status')

    // Verify delivery report information is captured
    const attributes = producerSpan.attributes
    assert.equal(attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_DESTINATION_NAME], testTopic)
    assert(attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_DESTINATION_PARTITION_ID] !== undefined,
      'Should have partition info')
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

      const receivedMessage = await consumer.recv() as MessageWithEndSpan
      endOtelSpans(receivedMessage)
      assert(receivedMessage, 'Should receive message')

      await consumer.disconnect()
    })

    parentSpan.end()

    const spans = await flushSpans()

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
    ]) as [MessageWithEndSpan, MessageWithEndSpan]

    endOtelSpans(messageA)
    endOtelSpans(messageB)
    assert(messageA && messageA.topic === topicA, 'Should receive message from topic A')
    assert(messageB && messageB.topic === topicB, 'Should receive message from topic B')

    await Promise.all([consumer1.disconnect(), consumer2.disconnect()])

    const spans = await flushSpans()

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
    assert.notEqual(topicAProducer.spanContext().traceId, topicBProducer.spanContext().traceId,
      'Producer spans for different topics should have isolated traces')
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
    const message1 = await consumer1.recv() as MessageWithEndSpan
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
    const message2 = await consumer2.recv() as MessageWithEndSpan
    endOtelSpans(message1)
    endOtelSpans(message2)
    assert(message2, 'Second consumer should receive message after rebalancing')

    await Promise.all([consumer1.disconnect(), consumer2.disconnect()])

    const spans = await flushSpans()
    const consumerSpans = spans.filter(span =>
      span.kind === SpanKind.CONSUMER &&
      span.name.includes(testTopic)
    )

    assert(consumerSpans.length >= 2, 'Should have spans from both consumers')
  })

  test('should handle producer configuration hooks correctly', async () => {
    let producerHookCalled = false
    let hookRecord: unknown = null

    // Reconfigure adapter with producer hook
    otelAdapter.disable()
    resetOtelAdapter()
    otelAdapter = enableOtelInstrumentation({
      tracerProvider: provider,
      producerHook: (span: Span, record: unknown, _metadata?: unknown) => {
        producerHookCalled = true
        hookRecord = record
        span.setAttributes({ 'custom.producer.hook': 'executed' })
      },
    })

    // Create client
    const hookedClient = new KafkaClient({
      brokers: KAFKA_BROKERS,
      clientId: `producer-hook-client-${nanoid()}`,
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

    const spans = await flushSpans()
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
    let childSpan: Span | null = null

    await context.with(rootContext, async () => {
      // Create a child span within the context
      childSpan = trace.getTracer('test').startSpan('async-child-operation')

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

    const spans = await flushSpans()
    const producerSpan = spans.find(s => s.kind === SpanKind.PRODUCER && s.name.includes(testTopic))
    const rootSpanFinished = spans.find(s => s.name === 'async-root-operation')

    assert(producerSpan, `Should have producer span. Spans: ${spans.map(s => s.name).join(', ')}`)
    assert(childSpan, 'Child span should be created')
    const childSpanContext = childSpan.spanContext()
    assert(childSpanContext, 'Child span should have context')
    assert.equal(producerSpan.spanContext().traceId, childSpanContext.traceId)
    assert.equal(producerSpan.parentSpanId, childSpanContext.spanId)
    if (rootSpanFinished) {
      assert.equal(producerSpan.spanContext().traceId, rootSpanFinished.spanContext().traceId)
    }
  })

  test('should handle context propagation in concurrent operations', async () => {
    // Reset spans
    memoryExporter.reset()

    const producer = kafkaClient.createProducer()
    const concurrentCount = 5

    // Create multiple concurrent operations with different contexts
    const operationSpans: Span[] = []
    const operations = Array.from({ length: concurrentCount }, (_, i) => {
      return new Promise<number>(async (resolve) => {
        const operationSpan = trace.getTracer('test').startSpan(`concurrent-op-${i}`)
        operationSpans[i] = operationSpan
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

    const spans = await flushSpans()
    const producerSpans = spans.filter(s => s.kind === SpanKind.PRODUCER && s.name.includes(testTopic))

    assert(producerSpans.length >= concurrentCount,
      `Should have producer spans for all operations. Producer spans: ${producerSpans.length}, names: ${
        producerSpans.map(s => s.name).join(', ')
      }`)

    for (let i = 0; i < concurrentCount; i++) {
      const opSpan = operationSpans[i]
      assert(opSpan, `Missing operation span for op ${i}`)

      const expectedKey = `key-${i}`
      const producerSpan = producerSpans.find(s =>
        s.attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_KAFKA_MESSAGE_KEY] === expectedKey
      )

      assert(producerSpan, `Missing producer span for op ${i}`)
      assert.equal(producerSpan.spanContext().traceId, opSpan.spanContext().traceId,
        `Producer span for op ${i} should share trace with operation span`)
      assert.equal(producerSpan.parentSpanId, opSpan.spanContext().spanId,
        `Producer span for op ${i} should be child of operation span`)
    }
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

      streamConsumer.on('data', (message: MessageWithEndSpan) => {
        endOtelSpans(message)
        // Create processing span within the context
        const processingSpan = trace.getTracer('test').startSpan('stream-message-processing')

        // Simulate some processing work
        setTimeout(() => {
          assert(message.topic, 'Message topic should be defined')
          processingSpan.setAttributes({
            'message.topic': message.topic,
            'processing.type': 'stream',
          })
          processingSpan.end()
          streamProcessingComplete = true
          streamConsumer.destroy()
        }, 10)
      })

      await new Promise<void>((resolve, reject) => {
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
        const onError = (err: Error) => {
          cleanup()
          reject(err)
        }
        streamConsumer.on('end', onEnd)
        streamConsumer.on('close', onEnd) // Destroy() triggers close, not end
        streamConsumer.on('error', onError)
      })
    })

    parentSpan.end()

    const spans = await flushSpans()

    const consumerSpan = spans.find(s => s.kind === SpanKind.CONSUMER && s.name.includes(testTopic))
    const parentSpanFinished = spans.find(s => s.name === 'stream-processing-parent')

    assert(consumerSpan, `Should have consumer span. Spans: ${spans.map(s => s.name).join(', ')}`)
    if (parentSpanFinished) {
      assert.equal(consumerSpan.spanContext().traceId, parentSpanFinished.spanContext().traceId,
        'Consumer span should propagate parent stream context')
      assert.equal(consumerSpan.parentSpanId, parentSpanFinished.spanContext().spanId,
        'Consumer span should be child of stream parent span')
    }
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

    let receivedMessage: MessageWithEndSpan | null = null

    const consumer = kafkaClient.createConsumer({
      groupId: `boundary-group-${nanoid()}`,
      enableAutoCommit: false,
    })

    await consumer.subscribe([{
      topic: testTopic,
      allOffsets: { position: 'Beginning' },
    }])

    receivedMessage = await consumer.recv() as MessageWithEndSpan
    endOtelSpans(receivedMessage)
    await consumer.disconnect()

    const spans = await flushSpans()

    const kafkaProducerSpan = spans.find(s => s.kind === SpanKind.PRODUCER && s.name.includes(testTopic))
    const kafkaConsumerSpan = spans.find(s => s.kind === SpanKind.CONSUMER && s.name.includes(testTopic))

    assert(kafkaProducerSpan, 'Should have Kafka producer span')
    assert(kafkaConsumerSpan, 'Should have Kafka consumer span')
    assert(receivedMessage, 'Should receive message')

    // Verify message headers contain trace context
    assert(receivedMessage.headers, 'Message should have headers')
    const traceparentHeader = receivedMessage.headers?.traceparent?.toString('utf8')
    assert(traceparentHeader, 'Traceparent header should be present on received message')
    assert(traceparentHeader.includes(kafkaProducerSpan.spanContext().traceId),
      'Traceparent header should include producer trace id')
  })

  test('should handle nested async operations with proper context isolation', async () => {
    // Reset spans
    memoryExporter.reset()

    const producer = kafkaClient.createProducer()

    // Create nested async operations
    const rootSpan = trace.getTracer('test').startSpan('nested-root')
    const rootContext = trace.setSpan(context.active(), rootSpan)
    let businessSpan: Span | null = null

    await context.with(rootContext, async () => {
      // Level 1: Database operation simulation
      const dbSpan = trace.getTracer('test').startSpan('database-operation')
      const dbContext = trace.setSpan(context.active(), dbSpan)

      await context.with(dbContext, async () => {
        // Level 2: Business logic simulation
        businessSpan = trace.getTracer('test').startSpan('business-logic')
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

    const spans = await flushSpans()

    const kafkaSpanRecorded = spans.find(s => s.kind === SpanKind.PRODUCER && s.name.includes(testTopic))
    assert(kafkaSpanRecorded, 'Should have Kafka span')
    assert(businessSpan, 'Business span should be created')
    assert.equal(kafkaSpanRecorded.spanContext().traceId, rootSpan.spanContext().traceId,
      'Kafka span should stay on root trace')
    assert.equal(kafkaSpanRecorded.parentSpanId, businessSpan.spanContext().spanId,
      'Kafka span should be child of business logic span')
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

    let processedMessages: MessageWithEndSpan[] = []

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
      const messages = await consumer.recvBatch(batchSize, 5000) as MessageWithEndSpan[]
      processedMessages = messages

      // Process each message in the context
      for (const message of messages) {
        const messageSpan = trace.getTracer('test').startSpan('message-processing')
        assert(message.topic, 'Message topic should be defined for batch processing')
        assert(message.offset !== undefined, 'Message offset should be defined for batch processing')
        messageSpan.setAttributes({
          'message.topic': message.topic,
          'message.offset': message.offset,
        })
        messageSpan.end()
      }

      endOtelSpans(messages)
      await consumer.disconnect()
    })

    batchSpan.end()

    const spans = await flushSpans()

    const kafkaBatchSpan = spans.find(s =>
      s.kind === SpanKind.CONSUMER &&
      s.name === `process ${testTopic}` &&
      (s.attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_BATCH_MESSAGE_COUNT] as number) >= 1 &&
      s.attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_DESTINATION_PARTITION_ID] === undefined
    )

    assert(kafkaBatchSpan, `Should have Kafka batch span. Spans: ${spans.map(s => s.name).join(', ')}`)
    const messageSpans = spans.filter(s =>
      s.kind === SpanKind.CONSUMER &&
      s.name === `process ${testTopic}` &&
      s.attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_DESTINATION_PARTITION_ID] !== undefined
    )
    assert(messageSpans.length >= 1, 'Should have individual message spans within batch processing')
    assert(processedMessages.length >= 1, 'Should process at least one message')
  })

  test('should handle large batch consumption with spans', async () => {
    // Produce a large batch
    const producer = kafkaClient.createProducer()
    const batchSize = 100
    for (let i = 0; i < batchSize; i++) {
      await producer.send({
        topic: testTopic,
        messages: [{ payload: Buffer.from(`large batch message ${i}`) }],
      })
    }
    await producer.flush()

    // Reset spans
    memoryExporter.reset()

    // Consume the entire batch in one call
    const consumer = kafkaClient.createConsumer({
      groupId: `large-batch-group-${nanoid()}`,
      enableAutoCommit: false,
    })

    await consumer.subscribe([{
      topic: testTopic,
      allOffsets: { position: 'Beginning' },
    }])

    const messages = await consumer.recvBatch(batchSize, 10000) as MessageWithEndSpan[]
    endOtelSpans(messages)
    await consumer.disconnect()

    const spans = await flushSpans()

    const batchSpan = spans.find(span =>
      span.kind === SpanKind.CONSUMER &&
      span.name === `process ${testTopic}` &&
      (span.attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_BATCH_MESSAGE_COUNT] as number) >= batchSize &&
      span.attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_DESTINATION_PARTITION_ID] === undefined
    )

    const messageSpans = spans.filter(span =>
      span.kind === SpanKind.CONSUMER &&
      span.name === `process ${testTopic}` &&
      span.attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_DESTINATION_PARTITION_ID] !== undefined
    )

    assert(messages.length >= batchSize, `Should receive at least ${batchSize} messages`)
    assert(batchSpan, 'Should have a batch span for large batch consumption')
    assert(messageSpans.length >= batchSize, 'Should have per-message spans for each message in the batch')
  })
})
