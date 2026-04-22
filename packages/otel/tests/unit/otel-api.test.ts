import assert from 'node:assert'
import { afterEach, beforeEach, describe, test } from 'vite-plus/test'

// OpenTelemetry test infrastructure
import { context, propagation, trace } from '@opentelemetry/api'
import { AsyncHooksContextManager } from '@opentelemetry/context-async-hooks'
import { AlwaysOnSampler, InMemorySpanExporter, SimpleSpanProcessor } from '@opentelemetry/sdk-trace-base'
import { NodeTracerProvider } from '@opentelemetry/sdk-trace-node'
import {
  batchProcessEndChannel,
  batchProcessStartChannel,
  consumerProcessEndChannel,
  consumerProcessStartChannel,
  type Message,
} from 'kafka-crab-js'

// Import from source so unit tests do not depend on a prebuilt dist/
import {
  enableOtelInstrumentation,
  endSpan,
  extractTraceContext,
  getBatchContext,
  getKafkaInstrumentation,
  getMessageContext,
  getOtelAdapter,
  KAFKA_OPERATION_TYPES,
  KAFKA_SEMANTIC_CONVENTIONS,
  resetKafkaInstrumentation,
  resetOtelAdapter,
  withBatchContext,
  withMessageContext,
} from '../../src/index.ts'

describe('kafka-crab-js-otel Public API Tests', () => {
  let memoryExporter: InMemorySpanExporter
  let spanProcessor: SimpleSpanProcessor
  let provider: NodeTracerProvider
  let contextManager: AsyncHooksContextManager

  beforeEach(() => {
    // Reset any existing instrumentation
    resetKafkaInstrumentation()
    resetOtelAdapter()

    // Setup OpenTelemetry test infrastructure
    contextManager = new AsyncHooksContextManager()
    memoryExporter = new InMemorySpanExporter()
    spanProcessor = new SimpleSpanProcessor(memoryExporter)

    provider = new NodeTracerProvider({
      sampler: new AlwaysOnSampler(),
      spanProcessors: [spanProcessor],
    })

    provider.register()
    context.setGlobalContextManager(contextManager)
    contextManager.enable()
  })

  afterEach(async () => {
    try {
      contextManager.disable()
      await spanProcessor.forceFlush()
      memoryExporter.reset()
    } catch (error) {
      console.warn('Cleanup error:', (error as Error).message)
    }

    resetKafkaInstrumentation()
    resetOtelAdapter()
  })

  describe('enableOtelInstrumentation()', () => {
    test('should enable OTEL instrumentation with default config', () => {
      const adapter = enableOtelInstrumentation()

      assert(adapter, 'Should return an adapter instance')
      assert.equal(adapter.isEnabled(), true, 'Adapter should be enabled')
    })

    test('should enable OTEL instrumentation with custom config', () => {
      const adapter = enableOtelInstrumentation({
        captureMessagePayload: true,
        captureMessageHeaders: true,
      })

      assert(adapter, 'Should return an adapter instance')
      assert.equal(adapter.isEnabled(), true, 'Adapter should be enabled')
    })

    test('should enable metrics when configured', () => {
      const adapter = enableOtelInstrumentation({
        metrics: { enabled: true },
      })

      assert(adapter, 'Should return an adapter instance')
      assert.equal(adapter.isMetricsEnabled(), true, 'Metrics should be enabled')
    })

    test('metrics should be opt-in (disabled by default)', () => {
      const adapter = enableOtelInstrumentation()

      assert.equal(adapter.isMetricsEnabled(), false, 'Metrics should be disabled by default')
    })

    test('should return same adapter on subsequent calls', () => {
      const adapter1 = enableOtelInstrumentation()
      const adapter2 = enableOtelInstrumentation()

      assert.equal(adapter1, adapter2, 'Should return the same adapter instance (singleton)')
    })
  })

  describe('getOtelAdapter()', () => {
    test('should return adapter after enableOtelInstrumentation', () => {
      enableOtelInstrumentation()
      const adapter = getOtelAdapter()

      assert(adapter, 'Should return an adapter')
      assert.equal(adapter.isEnabled(), true, 'Adapter should be enabled')
    })

    test('should create adapter with config if not exists', () => {
      const adapter = getOtelAdapter()

      assert(adapter, 'Should return an adapter')
      // Note: getOtelAdapter creates but doesn't auto-enable
    })

    test('should provide tracer getter', () => {
      enableOtelInstrumentation()
      const adapter = getOtelAdapter()

      const tracer = adapter.tracer
      assert(tracer, 'Should have a tracer')
    })
  })

  describe('getKafkaInstrumentation()', () => {
    test('should return instrumentation instance', () => {
      const instrumentation = getKafkaInstrumentation()

      assert(instrumentation, 'Should return instrumentation instance')
      assert.equal(typeof instrumentation.enable, 'function', 'Should have enable method')
      assert.equal(typeof instrumentation.disable, 'function', 'Should have disable method')
      assert.equal(typeof instrumentation.isEnabled, 'function', 'Should have isEnabled method')
    })

    test('should support enable/disable lifecycle', () => {
      const instrumentation = getKafkaInstrumentation()

      assert.equal(instrumentation.isEnabled(), true, 'Should be enabled by default')

      instrumentation.disable()
      assert.equal(instrumentation.isEnabled(), false, 'Should be disabled after disable()')

      instrumentation.enable()
      assert.equal(instrumentation.isEnabled(), true, 'Should be enabled after enable()')
    })

    test('should return singleton instance', () => {
      const inst1 = getKafkaInstrumentation()
      const inst2 = getKafkaInstrumentation()

      assert.equal(inst1, inst2, 'Should return the same instrumentation instance')
    })
  })

  describe('resetKafkaInstrumentation()', () => {
    test('should reset instrumentation instance', () => {
      const inst1 = getKafkaInstrumentation()
      resetKafkaInstrumentation()
      const inst2 = getKafkaInstrumentation()

      assert.notEqual(inst1, inst2, 'Should return different instance after reset')
    })
  })

  describe('resetOtelAdapter()', () => {
    test('should reset adapter instance', () => {
      const adapter1 = enableOtelInstrumentation()
      resetOtelAdapter()
      const adapter2 = enableOtelInstrumentation()

      assert.notEqual(adapter1, adapter2, 'Should return different instance after reset')
    })
  })

  describe('KAFKA_SEMANTIC_CONVENTIONS', () => {
    test('should export standard messaging semantic conventions', () => {
      assert.equal(KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_SYSTEM, 'messaging.system')
      assert.equal(KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_DESTINATION_NAME, 'messaging.destination.name')
      assert.equal(KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_OPERATION_NAME, 'messaging.operation.name')
      assert.equal(KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_OPERATION_TYPE, 'messaging.operation.type')
      assert.equal(KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_KAFKA_OFFSET, 'messaging.kafka.offset')
      assert.equal(KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_DESTINATION_PARTITION_ID, 'messaging.destination.partition.id')
    })
  })

  describe('KAFKA_OPERATION_TYPES', () => {
    test('should export standard operation types', () => {
      assert.equal(KAFKA_OPERATION_TYPES.CREATE, 'create')
      assert.equal(KAFKA_OPERATION_TYPES.SEND, 'send')
      assert.equal(KAFKA_OPERATION_TYPES.RECEIVE, 'receive')
      assert.equal(KAFKA_OPERATION_TYPES.PROCESS, 'process')
      assert.equal(KAFKA_OPERATION_TYPES.SETTLE, 'settle')
    })
  })

  describe('endSpan()', () => {
    test('should be a callable function', () => {
      assert.equal(typeof endSpan, 'function', 'endSpan should be a function')
    })

    test('should handle null/undefined message gracefully', () => {
      // Should not throw
      endSpan(null)
      endSpan(undefined)
    })

    test('should handle message without span gracefully', () => {
      const message = {
        topic: 'test-topic',
        partition: 0,
        offset: 0,
        payload: Buffer.from('test'),
      }

      // Should not throw
      endSpan(message)
    })
  })

  describe('extractTraceContext()', () => {
    test('should use root context when traceparent header is missing', () => {
      const tracer = provider.getTracer('kafka-crab-js-otel-test')
      const activeParent = tracer.startSpan('active-parent')

      try {
        let childTraceId = ''

        context.with(trace.setSpan(context.active(), activeParent), () => {
          const extractedContext = extractTraceContext({})
          const childSpan = tracer.startSpan('child-no-traceparent', undefined, extractedContext)
          childTraceId = childSpan.spanContext().traceId
          childSpan.end()
        })

        assert.notEqual(
          childTraceId,
          activeParent.spanContext().traceId,
          'child span should not inherit active trace id when traceparent is missing',
        )
      } finally {
        activeParent.end()
      }
    })

    test('should continue existing trace when traceparent header is present', () => {
      const tracer = provider.getTracer('kafka-crab-js-otel-test')
      const parentSpan = tracer.startSpan('upstream-parent')
      const parentContext = parentSpan.spanContext()
      const traceparent = `00-${parentContext.traceId}-${parentContext.spanId}-01`

      const extractedContext = extractTraceContext({ traceparent })
      const childSpan = tracer.startSpan('child-with-traceparent', undefined, extractedContext)

      assert.equal(
        childSpan.spanContext().traceId,
        parentContext.traceId,
        'child span should continue upstream trace id when traceparent is provided',
      )

      childSpan.end()
      parentSpan.end()
    })

    test('should extract traceparent from mixed-case array header', () => {
      const tracer = provider.getTracer('kafka-crab-js-otel-test')
      const parentSpan = tracer.startSpan('upstream-parent-mixed-case')
      const parentContext = parentSpan.spanContext()
      const traceparent = `00-${parentContext.traceId}-${parentContext.spanId}-01`

      const extractedContext = extractTraceContext({
        TraceParent: [traceparent],
      } as unknown as Record<string, Buffer | string | string[] | undefined>)
      const childSpan = tracer.startSpan('child-with-mixed-case-traceparent', undefined, extractedContext)

      assert.equal(
        childSpan.spanContext().traceId,
        parentContext.traceId,
        'child span should continue upstream trace id from mixed-case traceparent header',
      )

      childSpan.end()
      parentSpan.end()
    })

    test('should fallback to root context when extraction throws and traceparent is missing', () => {
      const tracer = provider.getTracer('kafka-crab-js-otel-test')
      const ambientSpan = tracer.startSpan('ambient-parent-no-traceparent')

      const throwingPropagator = {
        inject: () => undefined,
        extract: () => {
          throw new Error('extract failed')
        },
        fields: () => ['traceparent', 'tracestate'],
      }

      let childTraceId = ''

      propagation.setGlobalPropagator(throwingPropagator)
      try {
        context.with(trace.setSpan(context.active(), ambientSpan), () => {
          const extractedContext = extractTraceContext({})
          const childSpan = tracer.startSpan('child-fallback-root', undefined, extractedContext)
          childTraceId = childSpan.spanContext().traceId
          childSpan.end()
        })
      } finally {
        propagation.disable()
        ambientSpan.end()
      }

      assert.notEqual(
        childTraceId,
        ambientSpan.spanContext().traceId,
        'child span should use root fallback context when traceparent is missing and extraction throws',
      )
    })

    test('should fallback to active context when extraction throws and traceparent is present', () => {
      const tracer = provider.getTracer('kafka-crab-js-otel-test')
      const ambientSpan = tracer.startSpan('ambient-parent-with-traceparent')
      const traceparent = `00-${ambientSpan.spanContext().traceId}-${ambientSpan.spanContext().spanId}-01`

      const throwingPropagator = {
        inject: () => undefined,
        extract: () => {
          throw new Error('extract failed')
        },
        fields: () => ['traceparent', 'tracestate'],
      }

      let childTraceId = ''

      propagation.setGlobalPropagator(throwingPropagator)
      try {
        context.with(trace.setSpan(context.active(), ambientSpan), () => {
          const extractedContext = extractTraceContext({ traceparent })
          const childSpan = tracer.startSpan('child-fallback-active', undefined, extractedContext)
          childTraceId = childSpan.spanContext().traceId
          childSpan.end()
        })
      } finally {
        propagation.disable()
        ambientSpan.end()
      }

      assert.equal(
        childTraceId,
        ambientSpan.spanContext().traceId,
        'child span should use active fallback context when traceparent exists and extraction throws',
      )
    })
  })

  describe('message/batch context helpers', () => {
    test('withMessageContext should use decorated message span when available', () => {
      const tracer = provider.getTracer('kafka-crab-js-otel-test')
      const parentSpan = tracer.startSpan('decorated-message-parent')
      const message = {
        topic: 'test-topic',
        partition: 0,
        offset: 0,
        payload: Buffer.from('test'),
        span: parentSpan,
      } as const

      let childTraceId = ''
      withMessageContext(message, () => {
        const childSpan = tracer.startSpan('child-from-decorated-message')
        childTraceId = childSpan.spanContext().traceId
        childSpan.end()
      })

      assert.equal(childTraceId, parentSpan.spanContext().traceId)
      parentSpan.end()
    })

    test('withMessageContext should create a new trace when traceparent is missing', () => {
      const tracer = provider.getTracer('kafka-crab-js-otel-test')
      const ambientSpan = tracer.startSpan('ambient-parent')
      const message = {
        topic: 'test-topic',
        partition: 0,
        offset: 1,
        payload: Buffer.from('test'),
      } as const

      let childTraceId = ''

      context.with(trace.setSpan(context.active(), ambientSpan), () => {
        withMessageContext(message, () => {
          const childSpan = tracer.startSpan('child-without-traceparent')
          childTraceId = childSpan.spanContext().traceId
          childSpan.end()
        })
      })

      assert.notEqual(childTraceId, ambientSpan.spanContext().traceId)
      ambientSpan.end()
    })

    test('withBatchContext should use decorated batch span when available', () => {
      const tracer = provider.getTracer('kafka-crab-js-otel-test')
      const batchSpan = tracer.startSpan('decorated-batch-parent')
      const batch = [
        {
          topic: 'test-topic',
          partition: 0,
          offset: '0',
          payload: Buffer.from('test'),
        },
      ] as unknown as Record<string, unknown>[]
      Object.defineProperty(batch, 'span', {
        value: batchSpan,
        enumerable: false,
        configurable: true,
      })

      let childTraceId = ''
      withBatchContext(batch as unknown as Message[], () => {
        const childSpan = tracer.startSpan('child-from-decorated-batch')
        childTraceId = childSpan.spanContext().traceId
        childSpan.end()
      })

      assert.equal(childTraceId, batchSpan.spanContext().traceId)
      batchSpan.end()
    })

    test('getMessageContext/getBatchContext should be callable', () => {
      const message = {
        topic: 'test-topic',
        partition: 0,
        offset: 0,
        payload: Buffer.from('test'),
      } as const

      const messageContext = getMessageContext(message)
      const batchContext = getBatchContext([message])

      assert(messageContext, 'getMessageContext should return a context')
      assert(batchContext, 'getBatchContext should return a context')
    })

    test('adapter should decorate process-start message with non-enumerable span and otelContext', () => {
      enableOtelInstrumentation()

      const message = {
        topic: 'test-topic',
        partition: 0,
        offset: 0,
        payload: Buffer.from('test'),
        headers: {},
      }
      const eventContext: Record<PropertyKey, unknown> = {}

      consumerProcessStartChannel.publish({
        timestamp: Date.now(),
        message,
        context: eventContext,
      })

      const decoratedMessage = message as {
        span?: unknown
        otelContext?: unknown
      }

      assert(decoratedMessage.span, 'message should have span decoration')
      assert(decoratedMessage.otelContext, 'message should have otelContext decoration')
      assert.equal(Object.prototype.propertyIsEnumerable.call(message, 'span'), false)
      assert.equal(Object.prototype.propertyIsEnumerable.call(message, 'otelContext'), false)
      assert.equal(
        trace.getSpan(decoratedMessage.otelContext as Parameters<typeof trace.getSpan>[0]),
        decoratedMessage.span,
      )

      consumerProcessEndChannel.publish({
        timestamp: Date.now(),
        message,
        durationMs: 1,
        context: eventContext,
      })
    })

    test('adapter should decorate batch and contained messages with non-enumerable span and otelContext', () => {
      enableOtelInstrumentation()

      const messages = [
        {
          topic: 'test-topic',
          partition: 0,
          offset: 1,
          payload: Buffer.from('batch-1'),
          headers: {},
        },
        {
          topic: 'test-topic',
          partition: 0,
          offset: 2,
          payload: Buffer.from('batch-2'),
          headers: {},
        },
      ]
      const eventContext: Record<PropertyKey, unknown> = {}

      batchProcessStartChannel.publish({
        timestamp: Date.now(),
        messages,
        context: eventContext,
      })

      const decoratedBatch = Object.getOwnPropertySymbols(eventContext)
        .map((symbolKey) => eventContext[symbolKey])
        .find(
          (value) =>
            Array.isArray(value) &&
            value.length === messages.length &&
            value[0] === messages[0] &&
            value[1] === messages[1],
        ) as {
        span?: unknown
        otelContext?: unknown
      } & {
        span?: unknown
        otelContext?: unknown
      }[]

      assert(decoratedBatch, 'event context should include instrumented batch array')
      assert.notEqual(decoratedBatch, messages, 'instrumented batch should be a filtered array instance')
      assert(decoratedBatch.span, 'batch should have span decoration')
      assert(decoratedBatch.otelContext, 'batch should have otelContext decoration')
      assert.equal(Object.prototype.propertyIsEnumerable.call(decoratedBatch, 'span'), false)
      assert.equal(Object.prototype.propertyIsEnumerable.call(decoratedBatch, 'otelContext'), false)
      assert.equal(
        trace.getSpan(decoratedBatch.otelContext as Parameters<typeof trace.getSpan>[0]),
        decoratedBatch.span,
      )

      const decoratedMessages = decoratedBatch as {
        span?: unknown
        otelContext?: unknown
      }[]
      for (const message of decoratedMessages) {
        assert(message.span, 'message in batch should have span decoration')
        assert(message.otelContext, 'message in batch should have otelContext decoration')
        assert.equal(Object.prototype.propertyIsEnumerable.call(message, 'span'), false)
        assert.equal(Object.prototype.propertyIsEnumerable.call(message, 'otelContext'), false)
        assert.equal(trace.getSpan(message.otelContext as Parameters<typeof trace.getSpan>[0]), message.span)
      }

      batchProcessEndChannel.publish({
        timestamp: Date.now(),
        messages,
        durationMs: 1,
        context: eventContext,
      })
    })
  })

  describe('OtelAdapter enable/disable', () => {
    test('should support enable and disable', () => {
      const adapter = enableOtelInstrumentation()

      assert.equal(adapter.isEnabled(), true, 'Should be enabled after enableOtelInstrumentation')

      adapter.disable()
      assert.equal(adapter.isEnabled(), false, 'Should be disabled after disable()')

      adapter.enable()
      assert.equal(adapter.isEnabled(), true, 'Should be enabled after enable()')
    })
  })
})
