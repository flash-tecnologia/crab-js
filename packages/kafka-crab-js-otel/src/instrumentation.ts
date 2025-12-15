import { type Attributes, type Context, context, diag, type Span, trace, type Tracer } from '@opentelemetry/api'

import type { Message } from 'kafka-crab-js'
import { resetOtelAdapter } from './otel-adapter.js'

import { PACKAGE_INFO } from './constants.js'
import { resetKafkaMetrics } from './metrics.js'
import {
  DEFAULT_OTEL_CONFIG,
  type InstrumentedMessage,
  type InstrumentedMessageBatch,
  type KafkaOtelContext,
  type KafkaOtelInstrumentationConfig,
  type TracerProvider,
} from './types.js'
import { extractTraceContext, getTracer, injectTraceContext, setSpanStatus } from './utils.js'

function cloneWithDescriptors<TValue extends object>(value: TValue): TValue {
  try {
    const clone = Object.create(Object.getPrototypeOf(value)) as TValue
    Object.defineProperties(clone, Object.getOwnPropertyDescriptors(value))
    return clone
  } catch {
    // Best-effort fallback for plain objects
    return { ...(value as unknown as Record<string, unknown>) } as TValue
  }
}

export class KafkaCrabInstrumentation {
  private _kafkaTracer: Tracer | null = null
  private _kafkaConfig: KafkaOtelInstrumentationConfig
  private _enabled = false

  constructor(config: KafkaOtelInstrumentationConfig = {}) {
    const metricsConfig = {
      ...DEFAULT_OTEL_CONFIG.metrics,
      ...(config.metrics && typeof config.metrics === 'object' ? config.metrics : {}),
    }

    this._kafkaConfig = { ...DEFAULT_OTEL_CONFIG, ...config, metrics: metricsConfig }
  }

  public get kafkaConfig(): KafkaOtelInstrumentationConfig {
    return this._kafkaConfig
  }

  public get kafkaTracer(): Tracer | null {
    return this._kafkaTracer
  }

  public updateConfig(config: KafkaOtelInstrumentationConfig): void {
    if (config.maxPayloadSize !== undefined && config.maxPayloadSize <= 0) {
      diag.warn(`Invalid maxPayloadSize: ${config.maxPayloadSize}. Must be positive. Ignoring update.`)
      // We don't throw to avoid crashing app on config update, just ignore the invalid value
      // eslint-disable-next-line no-param-reassign
      delete config.maxPayloadSize
    }

    const metricsConfig = config.metrics && typeof config.metrics === 'object'
      ? { ...(this._kafkaConfig.metrics ?? DEFAULT_OTEL_CONFIG.metrics), ...config.metrics }
      : undefined

    this._kafkaConfig = {
      ...this._kafkaConfig,
      ...config,
      ...(metricsConfig ? { metrics: metricsConfig } : {}),
    }

    if (this._kafkaConfig.enabled && !this._enabled) {
      this.enable()
    } else if (this._kafkaConfig.enabled === false && this._enabled) {
      this.disable()
    }
  }

  public setTracerProvider(provider: TracerProvider): void {
    this._kafkaTracer = provider.getTracer(PACKAGE_INFO.NAME, PACKAGE_INFO.VERSION)
  }

  public enable(): void {
    this._kafkaTracer = getTracer(PACKAGE_INFO.NAME, PACKAGE_INFO.VERSION)
    this._enabled = true

    if (this._kafkaConfig?.registerOnInitialization && this._kafkaTracer) {
      diag.debug('Kafka OTEL instrumentation enabled')
    }
  }

  public disable(): void {
    this._kafkaTracer = null
    this._enabled = false

    // Clear hook references to prevent memory leaks
    this._kafkaConfig.producerHook = undefined
    this._kafkaConfig.messageHook = undefined

    diag.debug('Kafka OTEL instrumentation disabled')
  }

  public isEnabled(): boolean {
    return this._enabled && this._kafkaTracer !== null
  }

  public createOtelContext(): KafkaOtelContext {
    if (!this.isEnabled() || !this._kafkaTracer) {
      return this._createDisabledContext()
    }

    const tracer = this._kafkaTracer

    const endMessageSpan = (message: Message | InstrumentedMessage | null | undefined, error?: Error) => {
      if (!message) {
        return
      }

      const existingEndSpan = (message as InstrumentedMessage).endSpan
      if (typeof existingEndSpan === 'function') {
        existingEndSpan(error)
      }
    }

    const endBatchSpan = (batch: Message[] | InstrumentedMessageBatch | null | undefined, error?: Error) => {
      if (!batch) {
        return
      }

      const existingEndSpan = (batch as unknown as { endSpan?: (error?: Error) => void }).endSpan
      if (typeof existingEndSpan === 'function') {
        existingEndSpan(error)
        return
      }

      if (!Array.isArray(batch)) {
        return
      }

      for (const message of batch) {
        endMessageSpan(message, error)
      }
    }

    const toInstrumentedMessage = (message: Message): InstrumentedMessage =>
      cloneWithDescriptors(message) as InstrumentedMessage

    const toInstrumentedBatch = (batch: Message[]): InstrumentedMessageBatch => {
      const cloned = batch.map(message => toInstrumentedMessage(message)) as InstrumentedMessageBatch

      const spanDescriptor = Object.getOwnPropertyDescriptor(batch, 'span')
      if (spanDescriptor) {
        Object.defineProperty(cloned, 'span', spanDescriptor)
      }

      const endSpanDescriptor = Object.getOwnPropertyDescriptor(batch, 'endSpan')
      if (endSpanDescriptor) {
        Object.defineProperty(cloned, 'endSpan', endSpanDescriptor)
      }

      return cloned
    }

    const processMessage = async <TResult>(
      message: Message | InstrumentedMessage,
      handler: (message: Message | InstrumentedMessage) => TResult | Promise<TResult>,
    ): Promise<TResult> => {
      let capturedError: Error | undefined

      try {
        return await handler(message)
      } catch (error) {
        capturedError = error instanceof Error ? error : new Error(String(error))
        throw error
      } finally {
        endMessageSpan(message, capturedError)
      }
    }

    const processBatch = async <TResult>(
      batch: Message[] | InstrumentedMessageBatch,
      handler: (batch: Message[] | InstrumentedMessageBatch) => TResult | Promise<TResult>,
    ): Promise<TResult> => {
      let capturedError: Error | undefined

      try {
        return await handler(batch)
      } catch (error) {
        capturedError = error instanceof Error ? error : new Error(String(error))
        throw error
      } finally {
        endBatchSpan(batch, capturedError)
      }
    }

    return {
      enabled: true,
      span: trace.getActiveSpan() || null,
      tracer,
      context: context.active(),
      inject: (carrier, spanToInject?: Span) => {
        const applyInjection = (ctx: Context) => {
          const injectedHeaders = injectTraceContext(carrier, ctx)
          if (injectedHeaders && injectedHeaders !== carrier) {
            Object.assign(carrier, injectedHeaders)
          }
        }

        if (spanToInject) {
          const spanContext = trace.setSpan(context.active(), spanToInject)
          applyInjection(spanContext)
          return
        }

        const activeSpan = trace.getActiveSpan()
        if (activeSpan) {
          const spanContext = trace.setSpan(context.active(), activeSpan)
          applyInjection(spanContext)
        } else {
          applyInjection(context.active())
        }
      },
      extract: (carrier) => extractTraceContext(carrier),
      startSpan: (name, attributes: Attributes = {}) => tracer.startSpan(name, { attributes }),
      endSpan: (span, error) => {
        if (!span) {
          return
        }
        setSpanStatus(span, error)
        span.end()
      },
      endMessageSpan,
      endBatchSpan,
      toInstrumentedMessage,
      toInstrumentedBatch,
      processMessage,
      processBatch,
    }
  }

  // eslint-disable-next-line class-methods-use-this
  private _createDisabledContext(): KafkaOtelContext {
    return {
      enabled: false,
      span: null,
      tracer: null,
      context: context.active(),
      inject: () => {
        /* no-op */
      },
      extract: () => context.active(),
      startSpan: () => null,
      endSpan: () => {
        /* no-op */
      },
      endMessageSpan: (message, error) => {
        if (!message) {
          return
        }
        const existingEndSpan = (message as InstrumentedMessage).endSpan
        if (typeof existingEndSpan === 'function') {
          existingEndSpan(error)
        }
      },
      endBatchSpan: (batch, error) => {
        if (!batch) {
          return
        }
        const existingEndSpan = (batch as unknown as { endSpan?: (error?: Error) => void }).endSpan
        if (typeof existingEndSpan === 'function') {
          existingEndSpan(error)
        }
      },
      toInstrumentedMessage: message => cloneWithDescriptors(message) as InstrumentedMessage,
      toInstrumentedBatch: batch => {
        const cloned = batch.map(message =>
          cloneWithDescriptors(message) as InstrumentedMessage
        ) as InstrumentedMessageBatch

        const spanDescriptor = Object.getOwnPropertyDescriptor(batch, 'span')
        if (spanDescriptor) {
          Object.defineProperty(cloned, 'span', spanDescriptor)
        }

        const endSpanDescriptor = Object.getOwnPropertyDescriptor(batch, 'endSpan')
        if (endSpanDescriptor) {
          Object.defineProperty(cloned, 'endSpan', endSpanDescriptor)
        }

        return cloned
      },
      processMessage: async (message, handler) => {
        let capturedError: Error | undefined
        try {
          return await handler(message)
        } catch (error) {
          capturedError = error instanceof Error ? error : new Error(String(error))
          throw error
        } finally {
          const existingEndSpan = message
            ? (message as InstrumentedMessage).endSpan
            : undefined
          if (typeof existingEndSpan === 'function') {
            existingEndSpan(capturedError)
          }
        }
      },
      processBatch: async (batch, handler) => {
        let capturedError: Error | undefined
        try {
          return await handler(batch)
        } catch (error) {
          capturedError = error instanceof Error ? error : new Error(String(error))
          throw error
        } finally {
          const existingEndSpan = batch
            ? (batch as unknown as { endSpan?: (error?: Error) => void }).endSpan
            : undefined
          if (typeof existingEndSpan === 'function') {
            existingEndSpan(capturedError)
          }
        }
      },
    }
  }
}

// Singleton instance for global use
let globalInstrumentation: KafkaCrabInstrumentation | null = null

export function getKafkaInstrumentation(config?: KafkaOtelInstrumentationConfig): KafkaCrabInstrumentation {
  if (!globalInstrumentation) {
    globalInstrumentation = new KafkaCrabInstrumentation(config)
    if (globalInstrumentation.kafkaConfig.enabled !== false) {
      globalInstrumentation.enable()
    }
  } else if (config) {
    globalInstrumentation.updateConfig(config)
  }
  return globalInstrumentation
}

export function peekKafkaInstrumentation(): KafkaCrabInstrumentation | null {
  return globalInstrumentation
}

export function resetKafkaInstrumentation(): void {
  if (globalInstrumentation) {
    globalInstrumentation.disable()
    globalInstrumentation = null
  }

  resetOtelAdapter()
  resetKafkaMetrics()
}
