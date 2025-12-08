import { type Attributes, context, diag, type Span, trace, type Tracer } from '@opentelemetry/api'

import type { KafkaConsumer, KafkaProducer, Message, ProducerRecord, RecordMetadata } from '../../js-binding.js'
import { KAFKA_OPERATION_NAMES, KAFKA_OPERATION_TYPES, PACKAGE_INFO } from './constants.js'
import { getKafkaMetrics, KafkaMetrics, resetKafkaMetrics } from './metrics.js'
import {
  DEFAULT_OTEL_CONFIG,
  type KafkaMetricsConfig,
  type KafkaOtelContext,
  type KafkaOtelInstrumentationConfig,
  type TracerProvider,
} from './types.js'
import {
  createBatchSpan,
  createConsumerSpan,
  createProducerSpan,
  extractTraceContext,
  getTracer,
  injectTraceContext,
  normalizeHeadersToBuffer,
  setSpanStatus,
  shouldIgnoreTopic,
} from './utils.js'

export class KafkaCrabInstrumentation {
  private _kafkaTracer: Tracer | null = null
  private _kafkaConfig: KafkaOtelInstrumentationConfig
  private _kafkaMetrics: KafkaMetrics | null = null
  private _enabled = false

  constructor(config: KafkaOtelInstrumentationConfig = {}) {
    this._kafkaConfig = { ...DEFAULT_OTEL_CONFIG, ...config }
  }

  public get kafkaConfig(): KafkaOtelInstrumentationConfig {
    return this._kafkaConfig
  }

  public get kafkaTracer(): Tracer | null {
    return this._kafkaTracer
  }

  public get kafkaMetrics(): KafkaMetrics | null {
    return this._kafkaMetrics
  }

  public updateConfig(config: KafkaOtelInstrumentationConfig): void {
    this._kafkaConfig = { ...this._kafkaConfig, ...config }

    // Update metrics config if provided
    if (config.metrics && this._kafkaMetrics) {
      this._kafkaMetrics.updateConfig(config.metrics)
    }
  }

  public setTracerProvider(provider: TracerProvider): void {
    this._kafkaTracer = provider.getTracer(PACKAGE_INFO.NAME, PACKAGE_INFO.VERSION)
  }

  public setMetricsConfig(config: KafkaMetricsConfig): void {
    if (this._kafkaMetrics) {
      this._kafkaMetrics.updateConfig(config)
    } else if (this._enabled) {
      this._kafkaMetrics = getKafkaMetrics(config)
    }
  }

  public enable(): void {
    this._kafkaTracer = getTracer(PACKAGE_INFO.NAME, PACKAGE_INFO.VERSION)
    this._enabled = true

    // Enable metrics if configured
    const metricsConfig = this._kafkaConfig.metrics ?? DEFAULT_OTEL_CONFIG.metrics
    if (metricsConfig.enabled !== false) {
      this._kafkaMetrics = getKafkaMetrics(metricsConfig)
    }

    if (this._kafkaConfig?.registerOnInitialization && this._kafkaTracer) {
      diag.debug('Kafka OTEL instrumentation enabled')
    }
  }

  public disable(): void {
    this._kafkaTracer = null
    this._enabled = false

    if (this._kafkaMetrics) {
      this._kafkaMetrics.dispose()
      this._kafkaMetrics = null
    }

    // Clear hook references to prevent memory leaks
    this._kafkaConfig.producerHook = undefined
    this._kafkaConfig.messageHook = undefined

    diag.debug('Kafka OTEL instrumentation disabled')
  }

  public isEnabled(): boolean {
    return this._enabled && this._kafkaTracer !== null
  }

  public isMetricsEnabled(): boolean {
    return this._kafkaMetrics?.isEnabled() ?? false
  }

  public createOtelContext(): KafkaOtelContext {
    if (!this.isEnabled() || !this._kafkaTracer) {
      return this._createDisabledContext()
    }

    const tracer = this._kafkaTracer

    return {
      enabled: true,
      span: trace.getActiveSpan() || null,
      tracer,
      context: context.active(),
      inject: (carrier, spanToInject?: Span) => {
        if (spanToInject) {
          const spanContext = trace.setSpan(context.active(), spanToInject)
          injectTraceContext(carrier, spanContext)
          return
        }

        const activeSpan = trace.getActiveSpan()
        if (activeSpan) {
          const spanContext = trace.setSpan(context.active(), activeSpan)
          injectTraceContext(carrier, spanContext)
        } else {
          injectTraceContext(carrier, context.active())
        }
      },
      extract: (carrier) => extractTraceContext(carrier),
      startSpan: (name, attributes: Attributes = {}) => {
        const span = tracer.startSpan(name, { attributes })
        return span
      },
      endSpan: (span, error) => {
        if (!span) {
          return
        }
        setSpanStatus(span, error)
        span.end()
      },
    }
  }

  public instrumentProducerSend(
    originalSend: (record: ProducerRecord) => Promise<RecordMetadata[]>,
    clientId?: string,
  ): (producerRecord: ProducerRecord) => Promise<RecordMetadata[]> {
    if (!this.isEnabled()) {
      return originalSend
    }

    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const instrumentation = this
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    const tracer = this._kafkaTracer!
    const metrics = this._kafkaMetrics
    const { serverAddress, serverPort } = this._kafkaConfig

    return async function instrumentedSend(this: KafkaProducer, record: ProducerRecord) {
      if (!record) {
        return originalSend.call(this, record)
      }

      const callerContext = context.active()

      // Check if topic should be ignored
      if (shouldIgnoreTopic(record.topic, instrumentation._kafkaConfig.ignoreTopics)) {
        return originalSend.call(this, record)
      }

      // Start timer for metrics - must match span duration per OTEL semantic conventions
      const spanTimer = metrics ? KafkaMetrics.startTimer() : undefined

      const span = createProducerSpan(tracer, record, {
        operationName: KAFKA_OPERATION_NAMES.SEND,
        parentContext: callerContext,
        clientId,
        serverAddress,
        serverPort,
      })

      if (!span) {
        // Call timer to release closure reference even though we won't use the duration
        if (spanTimer) {
          spanTimer()
        }
        return originalSend.call(this, record)
      }

      const spanContext = trace.setSpan(callerContext, span)

      const instrumentedRecord: ProducerRecord = {
        ...record,
        messages: (record.messages ?? []).map(message => {
          const originalHeaders = message.headers ?? {}
          const injectedHeaders = injectTraceContext(originalHeaders, spanContext)
          const normalizedHeaders = normalizeHeadersToBuffer(injectedHeaders)

          return {
            ...message,
            headers: normalizedHeaders,
          }
        }),
      }

      if (instrumentation._kafkaConfig.producerHook) {
        try {
          context.with(spanContext, () => {
            instrumentation._kafkaConfig.producerHook?.(span, record)
          })
        } catch (error) {
          diag.warn('Producer hook failed:', error)
        }
      }

      try {
        const result = await context.with(callerContext, async () => originalSend.call(this, instrumentedRecord))

        const metadataArray = Array.isArray(result) ? result : []

        // Set partition and offset attributes from metadata
        if (metadataArray.length > 0) {
          const [metadata] = metadataArray
          if (metadata) {
            if (metadata.partition !== undefined) {
              span.setAttribute('messaging.destination.partition.id', String(metadata.partition))
            }
            if (metadata.offset !== undefined) {
              span.setAttribute('messaging.kafka.offset', metadata.offset)
            }
          }
          setSpanStatus(span, metadata?.error ? new Error(metadata.error.message) : undefined)
        } else {
          setSpanStatus(span)
        }

        span.end()

        // Record metrics after span ends so duration matches span duration
        if (metrics && spanTimer) {
          try {
            const duration = spanTimer()
            metrics.recordProducerDuration(record.topic, duration, {
              partition: metadataArray[0]?.partition,
              clientId,
            })
            metrics.recordMessagesSent(record, metadataArray, { clientId })
          } catch (error) {
            diag.warn('Failed to record producer metrics:', error)
          }
        }

        if (instrumentation._kafkaConfig.producerHook && metadataArray.length > 0) {
          try {
            instrumentation._kafkaConfig.producerHook(span, record, metadataArray[0])
          } catch (error) {
            diag.warn('Producer hook failed with metadata:', error)
          }
        }

        return result
      } catch (error) {
        const errorInstance = error instanceof Error ? error : new Error(String(error))

        setSpanStatus(span, errorInstance)
        span.end()

        // Record metrics after span ends so duration matches span duration
        if (metrics && spanTimer) {
          try {
            const duration = spanTimer()
            metrics.recordProducerDuration(record.topic, duration, {
              error: errorInstance,
              clientId,
            })
            metrics.recordMessagesSent(record, undefined, {
              error: errorInstance,
              clientId,
            })
          } catch (error) {
            diag.warn('Failed to record producer error metrics:', error)
          }
        }
        throw error
      }
    }
  }

  public instrumentConsumerReceive(
    originalReceive: () => Promise<Message | null>,
    groupId?: string,
    clientId?: string,
  ): () => Promise<Message | null> {
    if (!this.isEnabled()) {
      return originalReceive
    }

    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const instrumentation = this
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    const tracer = this._kafkaTracer!
    const metrics = this._kafkaMetrics
    const { serverAddress, serverPort } = this._kafkaConfig

    return async function instrumentedReceive(this: KafkaConsumer) {
      // Start timer for metrics
      const timer = metrics ? KafkaMetrics.startTimer() : undefined

      const message = await originalReceive.call(this)

      if (!message) {
        return message
      }

      // Record consumed message metric
      if (metrics && timer) {
        try {
          const duration = timer()
          metrics.recordConsumerDuration(message.topic, duration, {
            partition: message.partition,
            groupId,
          })

          if (!shouldIgnoreTopic(message.topic, instrumentation._kafkaConfig.ignoreTopics)) {
            metrics.recordMessagesConsumed(message, { groupId })
          }
        } catch (error) {
          diag.warn('Failed to record consumer metrics:', error)
        }
      }

      if (shouldIgnoreTopic(message.topic, instrumentation._kafkaConfig.ignoreTopics)) {
        return message
      }

      const parentContext = extractTraceContext(message.headers || {}) || context.active()

      // Start timer for process duration - must match span duration per OTEL semantic conventions
      const processTimer = metrics ? KafkaMetrics.startTimer() : undefined

      // Create a "process" span for message processing (not "receive")
      // The receive operation already happened; this span tracks the application's processing
      const span = createConsumerSpan(tracer, message, {
        operationName: KAFKA_OPERATION_NAMES.PROCESS,
        operationType: KAFKA_OPERATION_TYPES.PROCESS,
        parentContext,
        clientId,
        serverAddress,
        serverPort,
      })

      if (span) {
        const spanCtx = trace.setSpan(parentContext, span)

        context.with(spanCtx, () => {
          if (groupId) {
            span.setAttributes({ 'messaging.consumer.group.name': groupId })
          }

          if (instrumentation._kafkaConfig.messageHook) {
            try {
              instrumentation._kafkaConfig.messageHook(span, message)
            } catch (error) {
              diag.warn('Message hook failed:', error)
            }
          }

          setSpanStatus(span)
        })

        span.end()

        // Record process duration - metric value matches span duration per OTEL semantic conventions
        if (metrics && processTimer) {
          metrics.recordProcessDuration(message, processTimer(), { groupId })
        }
      } else if (metrics && processTimer) {
        // Record process duration even if span wasn't created (e.g., tracer unavailable)
        metrics.recordProcessDuration(message, processTimer(), { groupId })
      }

      return message
    }
  }

  public instrumentBatchReceive(
    originalBatchReceive: (size: number, timeoutMs: number) => Promise<Message[]>,
    groupId?: string,
    clientId?: string,
  ): (size: number, timeoutMs: number) => Promise<Message[]> {
    if (!this.isEnabled() || !this._kafkaConfig.enableBatchInstrumentation) {
      return originalBatchReceive
    }

    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const instrumentation = this
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    const tracer = this._kafkaTracer!
    const metrics = this._kafkaMetrics
    const { serverAddress, serverPort } = this._kafkaConfig

    return async function instrumentedBatchReceive(this: KafkaConsumer, size: number, timeoutMs: number) {
      // Start timer for metrics
      const timer = metrics ? KafkaMetrics.startTimer() : undefined

      const messages = await originalBatchReceive.call(this, size, timeoutMs)

      if (!Array.isArray(messages) || messages.length === 0) {
        return messages
      }

      // Record consumed messages metrics
      if (metrics && timer) {
        try {
          const duration = timer()
          const [firstMessage] = messages
          if (firstMessage) {
            metrics.recordConsumerDuration(firstMessage.topic, duration, {
              partition: firstMessage.partition,
              groupId,
            })
          }

          const nonIgnoredMessages = messages.filter((message: Message) =>
            !shouldIgnoreTopic(message.topic, instrumentation._kafkaConfig.ignoreTopics)
          )
          if (nonIgnoredMessages.length > 0) {
            metrics.recordMessagesConsumed(nonIgnoredMessages, { groupId })
          }
        } catch (error) {
          diag.warn('Failed to record batch consumer metrics:', error)
        }
      }

      const instrumentedMessages = messages.filter((message: Message) =>
        !shouldIgnoreTopic(message.topic, instrumentation._kafkaConfig.ignoreTopics)
      )

      if (instrumentedMessages.length === 0) {
        return messages
      }

      const [firstMessage] = instrumentedMessages
      const parentContext = extractTraceContext(firstMessage.headers || {}) || context.active()

      // Start timer for batch process duration - must match span duration per OTEL semantic conventions
      const processTimer = metrics ? KafkaMetrics.startTimer() : undefined

      const batchSpan = createBatchSpan(tracer, instrumentedMessages.length, {
        topic: firstMessage.topic,
        operationName: KAFKA_OPERATION_NAMES.BATCH_PROCESS,
        parentContext,
        clientId,
        serverAddress,
        serverPort,
      })

      // Track whether we've already recorded metrics (to avoid double recording)
      let metricsRecorded = false

      if (batchSpan) {
        if (groupId) {
          batchSpan.setAttributes({ 'messaging.consumer.group.name': groupId })
        }

        try {
          for (const message of instrumentedMessages) {
            const msgParentContext = extractTraceContext(message.headers || {}) || parentContext
            const messageSpan = createConsumerSpan(tracer, message, {
              operationName: KAFKA_OPERATION_NAMES.PROCESS,
              operationType: KAFKA_OPERATION_TYPES.PROCESS,
              parentContext: msgParentContext,
              clientId,
              serverAddress,
              serverPort,
            })

            if (messageSpan) {
              const messageSpanContext = trace.setSpan(msgParentContext || context.active(), messageSpan)
              context.with(messageSpanContext, () => {
                messageSpan.setAttributes({
                  'messaging.batch.message_count': instrumentedMessages.length,
                })

                if (instrumentation._kafkaConfig.messageHook) {
                  try {
                    instrumentation._kafkaConfig.messageHook(messageSpan, message)
                  } catch (error) {
                    diag.warn('Message hook failed:', error)
                  }
                }

                setSpanStatus(messageSpan)
              })
              messageSpan.end()
            }
          }

          setSpanStatus(batchSpan)
        } catch (error) {
          setSpanStatus(batchSpan, error instanceof Error ? error : new Error(String(error)))

          // Record error in batch process metrics - metric value matches span duration
          if (metrics && processTimer && !metricsRecorded) {
            metricsRecorded = true
            metrics.recordBatchProcessDuration(instrumentedMessages, processTimer(), {
              groupId,
              error: error instanceof Error ? error : new Error(String(error)),
            })
          }
        } finally {
          batchSpan.end()

          // Record batch process duration on success - metric value matches span duration
          if (metrics && processTimer && !metricsRecorded) {
            metricsRecorded = true
            metrics.recordBatchProcessDuration(instrumentedMessages, processTimer(), { groupId })
          }
        }
      } else if (metrics && processTimer) {
        // Record process duration even if span wasn't created (e.g., tracer unavailable)
        metrics.recordBatchProcessDuration(instrumentedMessages, processTimer(), { groupId })
      }

      return messages
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
    }
  }
}

// Singleton instance for global use
let globalInstrumentation: KafkaCrabInstrumentation | null = null

export function getKafkaInstrumentation(config?: KafkaOtelInstrumentationConfig): KafkaCrabInstrumentation {
  if (!globalInstrumentation) {
    globalInstrumentation = new KafkaCrabInstrumentation(config)
    globalInstrumentation.enable()
  } else if (config) {
    globalInstrumentation.updateConfig(config)
  }
  return globalInstrumentation
}

export function resetKafkaInstrumentation(): void {
  if (globalInstrumentation) {
    globalInstrumentation.disable()
    globalInstrumentation = null
  }
  // Also reset metrics
  resetKafkaMetrics()
}
