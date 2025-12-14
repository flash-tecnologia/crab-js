import { context, propagation, SpanKind, SpanStatusCode, trace } from '@opentelemetry/api'
import { AsyncHooksContextManager } from '@opentelemetry/context-async-hooks'
import { W3CTraceContextPropagator } from '@opentelemetry/core'
import { InMemorySpanExporter, SimpleSpanProcessor } from '@opentelemetry/sdk-trace-base'
import { NodeTracerProvider } from '@opentelemetry/sdk-trace-node'
import assert from 'node:assert/strict'
import { createRequire } from 'node:module'
import { test } from 'node:test'

const require = createRequire(import.meta.url)

const {
  KafkaClient,
  KafkaClientConfig,
  KafkaStreamReadable,
  KafkaBatchStreamReadable,
  EndSpan,
  endSpan,
  getKafkaInstrumentation,
  getOtelAdapter,
  instrumentBatchReceive,
  instrumentConsumerReceive,
  instrumentProducerSend,
  peekKafkaInstrumentation,
  resetKafkaInstrumentation,
} = require('../../dist/index.cjs')

function setupOtelProvider() {
  resetKafkaInstrumentation()

  const contextManager = new AsyncHooksContextManager()
  context.setGlobalContextManager(contextManager.enable())
  propagation.setGlobalPropagator(new W3CTraceContextPropagator())

  const exporter = new InMemorySpanExporter()
  const provider = new NodeTracerProvider()
  provider.addSpanProcessor(new SimpleSpanProcessor(exporter))
  provider.register()

  return { contextManager, exporter, provider }
}

function teardownOtelProvider({ contextManager, exporter, provider }) {
  contextManager.disable()
  exporter.reset()
  provider.shutdown().catch(() => undefined)
  resetKafkaInstrumentation()
}

test('producer spans propagate context to consumer spans via Kafka headers', async () => {
  const otel = setupOtelProvider()
  getOtelAdapter({ tracerProvider: otel.provider })

  const instrumentation = getKafkaInstrumentation()

  let capturedRecord
  const originalSend = async function(record) {
    capturedRecord = record
    return []
  }

  const instrumentedSend = instrumentProducerSend(originalSend, { clientId: 'client-a' })

  await instrumentedSend.call({}, {
    topic: 'test-topic',
    messages: [
      {
        payload: Buffer.from('hello world'),
        headers: {},
      },
    ],
  })

  assert(capturedRecord, 'Original send should receive the instrumented record')
  const headers = capturedRecord.messages[0].headers
  assert(headers.traceparent, 'Trace context should be injected into headers')
  assert(Buffer.isBuffer(headers.traceparent), 'Injected trace header should be a Buffer')
  const traceparentHeader = headers.traceparent.toString('utf8')
  const injectedTraceId = traceparentHeader.split('-')[1]

  const otelContext = instrumentation.createOtelContext()
  const extractedSpanContext = trace.getSpanContext(otelContext.extract(headers))
  assert(extractedSpanContext, 'Extracted span context should exist')
  assert.equal(extractedSpanContext.traceId, injectedTraceId, 'Extracted trace id should match injected header')

  await otel.provider.forceFlush()

  const originalReceive = async () => ({
    topic: 'test-topic',
    partition: 0,
    offset: 0,
    payload: Buffer.from('hello world'),
    headers,
  })

  const instrumentedReceive = instrumentConsumerReceive(originalReceive, 'group-a', { clientId: 'client-a' })
  const receivedMessage = await instrumentedReceive.call({})

  receivedMessage?.endSpan?.()

  await otel.provider.forceFlush()

  const spans = otel.exporter.getFinishedSpans()
  const producerSpan = spans.find(span => span.kind === SpanKind.PRODUCER)
  const consumerSpan = spans.find(span =>
    span.kind === SpanKind.CONSUMER &&
    span.name.startsWith('process ')
  )

  assert(producerSpan, 'Producer span should be recorded')
  assert(consumerSpan, 'Consumer span should be recorded')
  assert.equal(
    consumerSpan.spanContext().traceId,
    producerSpan.spanContext().traceId,
    'Consumer span should share trace with producer span',
  )
  assert.equal(
    consumerSpan.parentSpanId,
    producerSpan.spanContext().spanId,
    'Consumer span should be the child of the producer span',
  )

  teardownOtelProvider(otel)
})

test('ignoreTopics suppresses consumer receive spans', async () => {
  const otel = setupOtelProvider()
  getOtelAdapter({ tracerProvider: otel.provider, ignoreTopics: ['ignored-topic'] })

  const originalReceive = async () => ({
    topic: 'ignored-topic',
    partition: 0,
    offset: 0,
    payload: Buffer.from('hello world'),
    headers: {},
  })

  const instrumentedReceive = instrumentConsumerReceive(originalReceive, 'group-a', { clientId: 'client-a' })
  const message = await instrumentedReceive.call({})
  message?.endSpan?.()

  await otel.provider.forceFlush()

  const spans = otel.exporter.getFinishedSpans()
  assert.equal(spans.length, 0, 'Should not create spans for ignored topic')

  teardownOtelProvider(otel)
})

test('ignoreTopics suppresses batch receive spans', async () => {
  const otel = setupOtelProvider()
  getOtelAdapter({ tracerProvider: otel.provider, ignoreTopics: ['ignored-topic'] })

  const originalBatchReceive = async () => ([{
    topic: 'ignored-topic',
    partition: 0,
    offset: 0,
    payload: Buffer.from('hello world'),
    headers: {},
  }])

  const instrumentedBatchReceive = instrumentBatchReceive(originalBatchReceive, 'group-a', { clientId: 'client-a' })
  const messages = await instrumentedBatchReceive.call({}, 1, 1000)

  assert(Array.isArray(messages), 'Should receive messages array')
  assert.equal(messages.length, 1, 'Should receive one message')

  messages.endSpan?.()

  await otel.provider.forceFlush()

  const spans = otel.exporter.getFinishedSpans()
  assert.equal(spans.length, 0, 'Should not create spans for ignored topic batch')

  teardownOtelProvider(otel)
})

test('captureMessageHeaders captures header keys on producer spans', async () => {
  const otel = setupOtelProvider()
  getOtelAdapter({ tracerProvider: otel.provider, captureMessageHeaders: true })

  const originalSend = async () => []
  const instrumentedSend = instrumentProducerSend(originalSend, { clientId: 'client-a' })

  await instrumentedSend.call({}, {
    topic: 'test-topic',
    messages: [{
      payload: Buffer.from('hello'),
      headers: {
        'x-custom': Buffer.from('1'),
      },
    }],
  })

  await otel.provider.forceFlush()

  const spans = otel.exporter.getFinishedSpans()
  const producerSpan = spans.find(span => span.kind === SpanKind.PRODUCER)

  assert(producerSpan, 'Producer span should be recorded')

  const headerNames = producerSpan.attributes['kafka_crab.message.header_names']
  assert(Array.isArray(headerNames), 'Header names should be an array')
  assert(headerNames.includes('x-custom'), 'Should capture custom header key')
  assert(headerNames.includes('traceparent'), 'Should capture injected traceparent header key')
  assert.equal(
    producerSpan.attributes['kafka_crab.message.header_count'] >= 2,
    true,
    'Header count should include injected trace headers',
  )

  teardownOtelProvider(otel)
})

test('captureMessageHeaders captures header keys on consumer spans', async () => {
  const otel = setupOtelProvider()
  getOtelAdapter({ tracerProvider: otel.provider, captureMessageHeaders: true })

  const originalReceive = async () => ({
    topic: 'test-topic',
    partition: 0,
    offset: 0,
    payload: Buffer.from('hello world'),
    headers: { 'x-custom': Buffer.from('1') },
  })

  const instrumentedReceive = instrumentConsumerReceive(originalReceive, 'group-a', { clientId: 'client-a' })
  const message = await instrumentedReceive.call({})
  message?.endSpan?.()

  await otel.provider.forceFlush()

  const spans = otel.exporter.getFinishedSpans()
  const consumerSpan = spans.find(span => span.kind === SpanKind.CONSUMER && span.name.startsWith('process '))

  assert(consumerSpan, 'Consumer span should be recorded')

  const headerNames = consumerSpan.attributes['kafka_crab.message.header_names']
  assert(Array.isArray(headerNames), 'Header names should be an array')
  assert(headerNames.includes('x-custom'), 'Should capture custom header key')
  assert.equal(consumerSpan.attributes['kafka_crab.message.header_count'], 1)

  teardownOtelProvider(otel)
})

test('endSpan helper ends processing spans without optional chaining', async () => {
  const otel = setupOtelProvider()
  getOtelAdapter({ tracerProvider: otel.provider })

  const originalReceive = async () => ({
    topic: 'test-topic',
    partition: 0,
    offset: 0,
    payload: Buffer.from('hello world'),
    headers: {},
  })

  const instrumentedReceive = instrumentConsumerReceive(originalReceive, 'group-a', { clientId: 'client-a' })
  const message = await instrumentedReceive.call({})

  endSpan(message)

  const message2 = await instrumentedReceive.call({})
  EndSpan(message2)

  await otel.provider.forceFlush()

  const spans = otel.exporter.getFinishedSpans()
  const consumerSpans = spans.filter(span => span.kind === SpanKind.CONSUMER && span.name.startsWith('process '))
  assert.equal(consumerSpans.length, 2, 'Should have ended both consumer process spans')

  endSpan(null)
  EndSpan(undefined)

  teardownOtelProvider(otel)
})

test('endSpan helper does not initialize instrumentation when OTEL is disabled', async () => {
  resetKafkaInstrumentation()

  assert.equal(peekKafkaInstrumentation(), null)

  const message = {
    topic: 'test-topic',
    partition: 0,
    offset: 0,
    payload: Buffer.from('hello world'),
    headers: {},
  }

  endSpan(message)
  EndSpan(message)

  assert.equal(peekKafkaInstrumentation(), null)
})

test('KafkaClient with otel=false does not instrument consumer recv/recvBatch', async () => {
  resetKafkaInstrumentation()

  const originalCreateConsumer = KafkaClientConfig.prototype.createConsumer

  const stubConsumer = {
    recv: async () => ({
      topic: 'test-topic',
      partition: 0,
      offset: 0,
      payload: Buffer.from('hello world'),
      headers: {},
    }),
    recvBatch: async () => ([{
      topic: 'test-topic',
      partition: 0,
      offset: 0,
      payload: Buffer.from('hello world'),
      headers: {},
    }]),
    subscribe: async () => undefined,
    unsubscribe: () => undefined,
    disconnect: async () => undefined,
    commit: async () => undefined,
    seek: () => undefined,
  }
  stubConsumer._originalRecv = stubConsumer.recv
  stubConsumer._originalRecvBatch = stubConsumer.recvBatch

  KafkaClientConfig.prototype.createConsumer = function mockCreateConsumer() {
    return stubConsumer
  }

  try {
    const client = new KafkaClient({
      brokers: 'localhost:9092',
      clientId: 'otel-disabled-client',
      otel: false,
    })

    const consumer = client.createConsumer({ topic: 'test-topic', groupId: 'group-a' })
    assert.equal(consumer.recv, stubConsumer._originalRecv)
    assert.equal(consumer.recvBatch, stubConsumer._originalRecvBatch)

    const message = await consumer.recv()
    assert.equal(Object.hasOwn(message, 'endSpan'), false)

    const batch = await consumer.recvBatch(1, 1000)
    assert.equal(Object.hasOwn(batch, 'endSpan'), false)
    assert.equal(Object.hasOwn(batch[0], 'endSpan'), false)
  } finally {
    KafkaClientConfig.prototype.createConsumer = originalCreateConsumer
    resetKafkaInstrumentation()
  }
})

test('otelContext.processMessage ends spans and records errors', async () => {
  const otel = setupOtelProvider()
  getOtelAdapter({ tracerProvider: otel.provider })

  const instrumentation = getKafkaInstrumentation({ decorateMessages: false })
  const otelContext = instrumentation.createOtelContext()

  const originalReceive = async () => ({
    topic: 'test-topic',
    partition: 0,
    offset: 0,
    payload: Buffer.from('hello'),
    headers: {},
  })

  const instrumentedReceive = instrumentConsumerReceive(originalReceive, 'group-a', { clientId: 'client-a' })
  const message = await instrumentedReceive.call({})

  const value = await otelContext.processMessage(message, async (msg) => msg.payload.toString())
  assert.equal(value, 'hello')

  const message2 = await instrumentedReceive.call({})
  await assert.rejects(
    async () => {
      await otelContext.processMessage(message2, async () => {
        throw new Error('boom')
      })
    },
    /boom/,
  )

  await otel.provider.forceFlush()

  const spans = otel.exporter.getFinishedSpans()
  const processSpans = spans.filter(span => span.kind === SpanKind.CONSUMER && span.name === 'process test-topic')
  assert.equal(processSpans.length, 2, 'Should end both process spans')

  const okSpan = processSpans.find(span => span.status.code === SpanStatusCode.OK)
  const errorSpan = processSpans.find(span => span.status.code === SpanStatusCode.ERROR)

  assert(okSpan, 'Should have an OK process span')
  assert(errorSpan, 'Should have an ERROR process span')
  assert(errorSpan.events.some(e => e.name === 'exception'), 'Error process span should record an exception event')

  teardownOtelProvider(otel)
})

test('otelContext.processBatch ends batch + message spans', async () => {
  const otel = setupOtelProvider()
  getOtelAdapter({ tracerProvider: otel.provider })

  const instrumentation = getKafkaInstrumentation({ decorateMessages: false })
  const otelContext = instrumentation.createOtelContext()

  const originalBatchReceive = async () => ([
    {
      topic: 'test-topic',
      partition: 0,
      offset: 0,
      payload: Buffer.from('m0'),
      headers: {},
    },
    {
      topic: 'test-topic',
      partition: 0,
      offset: 1,
      payload: Buffer.from('m1'),
      headers: {},
    },
  ])

  const instrumentedBatchReceive = instrumentBatchReceive(originalBatchReceive, 'group-a', { clientId: 'client-a' })
  const batch = await instrumentedBatchReceive.call({}, 2, 1000)

  await otelContext.processBatch(batch, async (messages) => {
    assert.equal(messages.length, 2)
  })

  await otel.provider.forceFlush()

  const spans = otel.exporter.getFinishedSpans()
  const processSpans = spans.filter(span => span.kind === SpanKind.CONSUMER && span.name === 'process test-topic')
  assert.equal(processSpans.length, 3, 'Should end batch span + per-message spans')

  teardownOtelProvider(otel)
})

test('createStreamConsumer instruments underlying consumer when OTEL is enabled', async () => {
  resetKafkaInstrumentation()

  const originalCreateConsumer = KafkaClientConfig.prototype.createConsumer

  const makeStubConsumer = () => {
    const consumer = {
      recv: async () => null,
      recvBatch: async () => [],
      subscribe: async () => undefined,
      unsubscribe: () => undefined,
      disconnect: async () => undefined,
      commit: async () => undefined,
      seek: () => undefined,
    }

    consumer._originalRecv = consumer.recv
    consumer._originalRecvBatch = consumer.recvBatch
    return consumer
  }

  const singleConsumer = makeStubConsumer()
  const batchConsumer = makeStubConsumer()

  const consumerQueue = [singleConsumer, batchConsumer]

  KafkaClientConfig.prototype.createConsumer = function mockCreateConsumer() {
    const next = consumerQueue.shift()
    if (!next) {
      throw new Error('No stub consumers left')
    }
    return next
  }

  try {
    const client = new KafkaClient({
      brokers: 'localhost:9092',
      clientId: 'otel-test-client',
      otel: {
        serviceName: 'otel-stream-test',
      },
    })

    const stream = client.createStreamConsumer({
      groupId: 'stream-group',
      enableAutoCommit: false,
      streamOptions: { objectMode: true },
    })

    assert(stream instanceof KafkaStreamReadable, 'Should create a KafkaStreamReadable instance')

    const instrumentedSingle = stream.rawConsumer()
    assert.notStrictEqual(
      instrumentedSingle.recv,
      instrumentedSingle._originalRecv,
      'Single message consumer should have instrumented recv',
    )

    await stream.disconnect()

    const batchStream = client.createStreamConsumer({
      groupId: 'batch-group',
      enableAutoCommit: false,
      batchSize: 5,
      batchTimeout: 50,
      streamOptions: { objectMode: true },
    })

    assert(batchStream instanceof KafkaBatchStreamReadable, 'Should create a KafkaBatchStreamReadable instance')

    const instrumentedBatch = batchStream.rawConsumer()
    assert.notStrictEqual(
      instrumentedBatch.recv,
      instrumentedBatch._originalRecv,
      'Batch consumer should have instrumented recv',
    )
    assert.notStrictEqual(
      instrumentedBatch.recvBatch,
      instrumentedBatch._originalRecvBatch,
      'Batch consumer should have instrumented recvBatch',
    )

    await batchStream.disconnect()
  } finally {
    KafkaClientConfig.prototype.createConsumer = originalCreateConsumer
    resetKafkaInstrumentation()
  }
})
