import assert from 'node:assert'
import { afterEach, beforeEach, describe, test } from 'node:test'

// OpenTelemetry test infrastructure
import { context } from '@opentelemetry/api'
import { AsyncHooksContextManager } from '@opentelemetry/context-async-hooks'
import { AlwaysOnSampler, InMemorySpanExporter, SimpleSpanProcessor } from '@opentelemetry/sdk-trace-base'
import { NodeTracerProvider } from '@opentelemetry/sdk-trace-node'

// Import from the built package
import {
    enableOtelInstrumentation,
    endSpan,
    getKafkaInstrumentation,
    getOtelAdapter,
    KAFKA_OPERATION_TYPES,
    KAFKA_SEMANTIC_CONVENTIONS,
    resetKafkaInstrumentation,
    resetOtelAdapter,
} from '../../dist/index.js'

describe('kafka-crab-js-otel Public API Tests', () => {
    let memoryExporter
    let spanProcessor
    let provider
    let contextManager

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

    afterEach(() => {
        try {
            contextManager.disable()
            spanProcessor.forceFlush()
            memoryExporter.reset()
        } catch (error) {
            console.warn('Cleanup error:', error.message)
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
                offset: '0',
                payload: Buffer.from('test'),
            }

            // Should not throw
            endSpan(message)
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
