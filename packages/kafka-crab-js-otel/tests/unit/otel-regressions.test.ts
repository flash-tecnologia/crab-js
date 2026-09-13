import assert from 'node:assert/strict'
import { afterEach, beforeEach, describe, test } from 'vite-plus/test'
import { context, propagation, ROOT_CONTEXT, trace } from '@opentelemetry/api'
import { AsyncHooksContextManager } from '@opentelemetry/context-async-hooks'
import { W3CTraceContextPropagator } from '@opentelemetry/core'
import { InMemorySpanExporter, SimpleSpanProcessor } from '@opentelemetry/sdk-trace-base'
import { NodeTracerProvider } from '@opentelemetry/sdk-trace-node'
import type { KafkaConsumer, Message, ProducerRecord } from 'kafka-crab-js'
import {
  enableOtelInstrumentation,
  endSpan,
  extractTraceContext,
  getBatchContext,
  getKafkaInstrumentation,
  injectTraceContext,
  KafkaMetrics,
  resetKafkaInstrumentation,
} from '../../src/index.js'
import { instrumentBatchReceive } from '../../../kafka-crab-js/js-src/diagnostics/instrumentation.js'
import { getMessageAttributes } from '../../src/utils.js'
import {
  batchReceiveStartChannel,
  batchReceiveEndChannel,
  consumerReceiveStartChannel,
  consumerReceiveEndChannel,
  producerSendEndChannel,
  producerSendStartChannel,
} from '../../src/kafka-channels.js'
import type { Meter, MeterProvider } from '../../src/types.js'

function makeMessage(partition = 0, headers: Record<string, Buffer> = {}): Message {
  return { topic: 'orders', partition, offset: 0, payload: Buffer.from('data'), headers }
}

function makeMeterProvider(
  records: { name: string; value: number; attributes: Record<string, unknown> }[],
): MeterProvider {
  const meter: Meter = {
    createCounter: (name) => ({ add: (value, attributes = {}) => records.push({ name, value, attributes }) }),
    createHistogram: (name) => ({ record: (value, attributes = {}) => records.push({ name, value, attributes }) }),
    createUpDownCounter: () => ({ add() {} }),
    createObservableGauge: () => ({ addCallback() {} }),
  }
  return { getMeter: () => meter }
}

describe('OTEL data and context regressions', () => {
  let exporter: InMemorySpanExporter
  let provider: NodeTracerProvider

  beforeEach(() => {
    resetKafkaInstrumentation()
    trace.disable()
    context.disable()
    propagation.disable()
    exporter = new InMemorySpanExporter()
    provider = new NodeTracerProvider({ spanProcessors: [new SimpleSpanProcessor(exporter)] })
    provider.register({ contextManager: new AsyncHooksContextManager(), propagator: new W3CTraceContextPropagator() })
  })

  afterEach(async () => {
    resetKafkaInstrumentation()
    await provider.shutdown()
    trace.disable()
    context.disable()
    propagation.disable()
  })

  test('trace injection preserves arbitrary binary headers and caller-owned headers', () => {
    const span = provider.getTracer('test').startSpan('upstream')
    const binary = Buffer.from([255, 254, 0x00, 0x80, 192])
    const headers = Object.freeze({ binary, empty: Buffer.alloc(0) })
    const injected = injectTraceContext(headers, trace.setSpan(ROOT_CONTEXT, span))
    assert.deepEqual(injected.binary, binary)
    assert.equal(injected.binary, binary)
    assert.deepEqual(injected.empty, Buffer.alloc(0))
    assert.equal('traceparent' in headers, false)
    assert(Buffer.isBuffer(injected.traceparent))
    assert.equal(trace.getSpanContext(extractTraceContext(injected))?.traceId, span.spanContext().traceId)
    span.end()
  })

  test('an invalid incoming traceparent does not inherit an unrelated ambient span', () => {
    const ambient = provider.getTracer('test').startSpan('ambient')
    context.with(trace.setSpan(ROOT_CONTEXT, ambient), () => {
      assert.equal(trace.getSpanContext(extractTraceContext({ traceparent: 'invalid' })), undefined)
    })
    ambient.end()
  })

  test('tombstone telemetry respects isTombstone while preserving an ordinary empty value', () => {
    const tombstone = { ...makeMessage(), payload: Buffer.alloc(0), isTombstone: true }
    const empty = { ...makeMessage(), payload: Buffer.alloc(0) }
    assert.equal(getMessageAttributes(tombstone, 'process', 'process')['messaging.kafka.message.tombstone'], true)
    assert.equal(getMessageAttributes(empty, 'process', 'process')['messaging.kafka.message.tombstone'], undefined)
  })

  test('public batches retain independent producer contexts and expose their own batch context', async () => {
    enableOtelInstrumentation({ tracerProvider: provider })
    const tracer = provider.getTracer('test')
    const producers = [tracer.startSpan('producer-a'), tracer.startSpan('producer-b')]
    const messages = producers.map((span, partition) =>
      makeMessage(partition, {
        traceparent: Buffer.from(`00-${span.spanContext().traceId}-${span.spanContext().spanId}-01`),
        binary: Buffer.from([255, partition, 0x80]),
      }),
    )
    const originalHeaders = messages.map((message) => message.headers)
    const receive = instrumentBatchReceive(async () => messages, 'workers')
    const batch = await receive.call({} as KafkaConsumer, 64, 5)
    const batchSpan = trace.getSpan(getBatchContext(batch))
    assert(batchSpan)
    assert.equal(batchSpan, (batch as Message[] & { span?: unknown }).span)
    for (const [index, message] of batch.entries()) {
      assert.equal(message.headers, originalHeaders[index])
      assert.equal(
        trace.getSpanContext(extractTraceContext(message.headers))?.traceId,
        producers[index].spanContext().traceId,
      )
      const processingSpan = trace.getSpan(
        (message as Message & { otelContext: ReturnType<typeof context.active> }).otelContext,
      )
      assert.equal(processingSpan?.spanContext().traceId, producers[index].spanContext().traceId)
    }
    endSpan(batch)
    for (const producer of producers) producer.end()
    await provider.forceFlush()
    const finishedBatch = exporter
      .getFinishedSpans()
      .find((span) => span.spanContext().spanId === batchSpan.spanContext().spanId)
    assert(finishedBatch)
    assert.deepEqual(
      new Set(finishedBatch.links.map((link) => link.context.traceId)),
      new Set(producers.map((span) => span.spanContext().traceId)),
    )
  })

  test('ignored first messages cannot replace the context of a tracked batch', async () => {
    enableOtelInstrumentation({ tracerProvider: provider, ignoreTopics: ['ignored'] })
    const producer = provider.getTracer('test').startSpan('tracked-producer')
    const headers = {
      traceparent: Buffer.from(`00-${producer.spanContext().traceId}-${producer.spanContext().spanId}-01`),
    }
    const messages = [{ ...makeMessage(), topic: 'ignored' }, makeMessage(0, headers)]
    const receive = instrumentBatchReceive(async () => messages, 'workers')
    const batch = await receive.call({} as KafkaConsumer, 64, 5)
    assert.equal(trace.getSpanContext(getBatchContext(batch))?.traceId, producer.spanContext().traceId)
    assert.equal(batch[1].headers, headers)
    endSpan(batch)
    producer.end()
  })

  test('producer completion hook attributes are exported before the span ends', async () => {
    enableOtelInstrumentation({
      tracerProvider: provider,
      producerHook: (span, _record, metadata) => {
        if (metadata) span.setAttribute('delivery.offset', metadata.offset)
      },
    })
    const eventContext = {}
    const record: ProducerRecord = { topic: 'orders', messages: [{ payload: Buffer.from('data') }] }
    producerSendStartChannel.publish({
      timestamp: Date.now(),
      topic: record.topic,
      record,
      messageCount: 1,
      context: eventContext,
    })
    producerSendEndChannel.publish({
      timestamp: Date.now(),
      topic: record.topic,
      record,
      metadata: [{ topic: 'orders', partition: 0, offset: 42 }],
      durationMs: 1,
      context: eventContext,
    })
    await provider.forceFlush()
    assert.equal(
      exporter.getFinishedSpans().find((span) => span.name === 'send orders')?.attributes['delivery.offset'],
      42,
    )
  })

  test('enabling after disable resumes channel instrumentation', () => {
    const adapter = enableOtelInstrumentation({ tracerProvider: provider })
    adapter.disable()
    assert.equal(enableOtelInstrumentation(), adapter)
    assert.equal(adapter.isEnabled(), true)
  })

  test('batch wrapper helpers preserve the public batch context', async () => {
    enableOtelInstrumentation({ tracerProvider: provider })
    const receive = instrumentBatchReceive(async () => [makeMessage(), makeMessage()], 'workers')
    const batch = await receive.call({} as KafkaConsumer, 64, 5)
    const wrapped = getKafkaInstrumentation().createOtelContext().toInstrumentedBatch(batch)
    assert.equal(getBatchContext(wrapped), getBatchContext(batch))
    endSpan(wrapped)
  })

  test('consumer message counters attribute each partition correctly', () => {
    const records: { name: string; value: number; attributes: Record<string, unknown> }[] = []
    const metrics = new KafkaMetrics({ enabled: true, meterProvider: makeMeterProvider(records) })
    metrics.enable()
    metrics.recordMessagesConsumed([makeMessage(0), makeMessage(1), makeMessage(1)], { groupId: 'workers' })
    assert.deepEqual(
      records.map((record) => [record.attributes['messaging.destination.partition.id'], record.value]),
      [
        ['0', 1],
        ['1', 2],
      ],
    )
    metrics.dispose()
  })

  test('updating the meter provider sends subsequent measurements to the new provider', () => {
    const first: { name: string; value: number; attributes: Record<string, unknown> }[] = []
    const second: typeof first = []
    const metrics = new KafkaMetrics({ enabled: true, meterProvider: makeMeterProvider(first) })
    metrics.enable()
    metrics.recordMessagesConsumed(makeMessage())
    metrics.updateConfig({ meterProvider: makeMeterProvider(second) })
    metrics.recordMessagesConsumed(makeMessage())
    assert.equal(first.length, 1)
    assert.equal(second.length, 1)
    metrics.dispose()
  })
  test('batch durations never attribute mixed destinations to the first message', () => {
    const records: { name: string; value: number; attributes: Record<string, unknown> }[] = []
    const metrics = new KafkaMetrics({ enabled: true, meterProvider: makeMeterProvider(records) })
    metrics.enable()
    metrics.recordBatchProcessDuration([makeMessage(0), makeMessage(1)], 0.1)
    metrics.recordBatchProcessDuration([makeMessage(0), { ...makeMessage(0), topic: 'other' }], 0.2)
    assert.equal(records.length, 2)
    assert.equal(records[0].attributes['messaging.destination.name'], 'orders')
    assert.equal(records[0].attributes['messaging.destination.partition.id'], undefined)
    assert.equal(records[1].attributes['messaging.destination.name'], undefined)
    assert.equal(records[1].attributes['messaging.destination.partition.id'], undefined)
    metrics.dispose()
  })

  test('batch poll spans and metrics omit ambiguous partition labels', async () => {
    const records: { name: string; value: number; attributes: Record<string, unknown> }[] = []
    enableOtelInstrumentation({
      tracerProvider: provider,
      metrics: { enabled: true, meterProvider: makeMeterProvider(records) },
    })
    const receive = instrumentBatchReceive(async () => [makeMessage(0), makeMessage(1)], 'workers')
    const batch = await receive.call({} as KafkaConsumer, 64, 5)
    endSpan(batch)
    await provider.forceFlush()
    const poll = exporter.getFinishedSpans().find((span) => span.name === 'poll orders')
    assert(poll)
    assert.equal(poll.attributes['messaging.destination.partition.id'], undefined)
    for (const metric of records.filter((record) => record.name.endsWith('.duration'))) {
      assert.equal(metric.attributes['messaging.destination.partition.id'], undefined)
    }
  })

  test('failed receives record error durations without inventing a destination', () => {
    const records: { name: string; value: number; attributes: Record<string, unknown> }[] = []
    enableOtelInstrumentation({
      tracerProvider: provider,
      metrics: { enabled: true, meterProvider: makeMeterProvider(records) },
    })
    const event = { timestamp: Date.now(), context: {}, groupId: 'workers' }
    consumerReceiveStartChannel.publish(event)
    consumerReceiveEndChannel.publish({ ...event, message: null, durationMs: 1, error: new Error('TIMEOUT') })
    const batchEvent = { ...event, context: {}, requestedSize: 64, timeoutMs: 5 }
    batchReceiveStartChannel.publish(batchEvent)
    batchReceiveEndChannel.publish({ ...batchEvent, messages: [], durationMs: 1, error: new Error('TIMEOUT') })
    assert.equal(records.length, 2)
    for (const record of records) {
      assert.equal(record.name, 'messaging.client.operation.duration')
      assert.equal(record.attributes['error.type'], 'KAFKA_TIMEOUT')
      assert.equal(record.attributes['messaging.destination.name'], undefined)
    }
  })

  test('a send across partitions does not report a single partition or offset', async () => {
    const records: { name: string; value: number; attributes: Record<string, unknown> }[] = []
    enableOtelInstrumentation({
      tracerProvider: provider,
      metrics: { enabled: true, meterProvider: makeMeterProvider(records) },
    })
    const eventContext = {}
    const record: ProducerRecord = {
      topic: 'orders',
      messages: [{ payload: Buffer.from('a') }, { payload: Buffer.from('b') }],
    }
    producerSendStartChannel.publish({
      timestamp: Date.now(),
      topic: record.topic,
      record,
      messageCount: 2,
      context: eventContext,
    })
    producerSendEndChannel.publish({
      timestamp: Date.now(),
      topic: record.topic,
      record,
      metadata: [
        { topic: 'orders', partition: 0, offset: 42 },
        { topic: 'orders', partition: 1, offset: 43 },
      ],
      durationMs: 1,
      context: eventContext,
    })
    await provider.forceFlush()
    const span = exporter.getFinishedSpans().find((candidate) => candidate.name === 'send orders')
    assert(span)
    assert.equal(span.attributes['messaging.destination.partition.id'], undefined)
    assert.equal(span.attributes['messaging.kafka.offset'], undefined)
    assert.equal(records.find((metric) => metric.name === 'messaging.client.sent.messages')?.value, 2)
    for (const metric of records) assert.equal(metric.attributes['messaging.destination.partition.id'], undefined)
  })
})
