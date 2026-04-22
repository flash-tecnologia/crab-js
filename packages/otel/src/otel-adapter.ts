/**
 * OpenTelemetry Adapter for Kafka Diagnostic Channels
 *
 * This adapter subscribes to kafka-crab diagnostic channels and creates
 * OpenTelemetry spans and metrics. It provides backward-compatible OTEL
 * instrumentation while using the decoupled diagnostics_channel architecture.
 */
/* eslint @typescript-eslint/no-unsafe-type-assertion: off */

import {
  type Attributes,
  type Context,
  context,
  diag,
  type Span,
  SpanKind,
  trace,
  type Tracer,
} from '@opentelemetry/api'

// Import types and channels from kafka-crab-js
import {
  batchProcessEndChannel,
  type BatchProcessEndEvent,
  batchProcessStartChannel,
  type BatchProcessStartEvent,
  batchReceiveEndChannel,
  type BatchReceiveEndEvent,
  batchReceiveStartChannel,
  type BatchReceiveStartEvent,
  consumerProcessEndChannel,
  type ConsumerProcessEndEvent,
  consumerProcessStartChannel,
  type ConsumerProcessStartEvent,
  consumerReceiveEndChannel,
  type ConsumerReceiveEndEvent,
  consumerReceiveStartChannel,
  type ConsumerReceiveStartEvent,
  type Message,
  type ProducerRecord,
  producerSendEndChannel,
  type ProducerSendEndEvent,
  producerSendStartChannel,
  type ProducerSendStartEvent,
  type RecordMetadata,
} from 'kafka-crab-js'

// Import local OTEL modules
import {
  KAFKA_DEFAULTS,
  KAFKA_OPERATION_NAMES,
  KAFKA_OPERATION_TYPES,
  KAFKA_SEMANTIC_CONVENTIONS,
  KAFKA_SPAN_NAMES,
  PACKAGE_INFO,
} from './constants.js'
import { getKafkaMetrics, KafkaMetrics } from './metrics.js'
import type { KafkaMetricsConfig, TracerProvider } from './types.js'
import {
  createBatchSpan,
  createConsumerSpan,
  createProducerSpan,
  extractTraceContext,
  getCapturedHeaderAttributes,
  injectTraceContext,
  normalizeHeadersToBuffer,
  setSpanStatus,
} from './utils.js'

// Symbol key for storing span in event context
const SPAN_KEY = Symbol('otel.span')
const TIMER_KEY = Symbol('otel.timer')
const MESSAGE_SPANS_KEY = Symbol('otel.messageSpans')
const INSTRUMENTED_MESSAGES_KEY = Symbol('otel.instrumentedMessages')

type MessageWithOtelFields = Message & {
  span?: Span
  otelContext?: Context
}

type BatchWithOtelFields = Message[] & {
  span?: Span
  otelContext?: Context
}

/**
 * Configuration options for the OTEL adapter
 */
export interface OtelAdapterConfig {
  /** Custom tracer provider (uses global if not provided) */
  tracerProvider?: TracerProvider
  /** Metrics configuration */
  metrics?: KafkaMetricsConfig
  /** Function to filter topics from instrumentation */
  ignoreTopics?: string[] | ((topic: string) => boolean)
  /** Whether to capture message headers as span attributes */
  captureMessageHeaders?: boolean
  /** Whether to capture message payloads (security sensitive) */
  captureMessagePayload?: boolean
  /** Maximum payload size to capture */
  maxPayloadSize?: number
  /** Custom hook called for each message */
  messageHook?: (span: Span, message: Message) => void
  /** Custom hook called for producer operations */
  producerHook?: (span: Span, record: ProducerRecord, metadata?: RecordMetadata) => void
}

/**
 * OTEL Adapter that subscribes to diagnostic channels
 */
export class OtelAdapter {
  private _tracer: Tracer
  private _metrics: KafkaMetrics | null = null
  private _config: OtelAdapterConfig
  private _enabled = false
  private readonly _handlers = new Map<string, (event: unknown, name: string) => void>()

  constructor(config: OtelAdapterConfig = {}) {
    this._config = config
    const tracerProvider = config.tracerProvider ?? trace
    this._tracer = tracerProvider.getTracer(PACKAGE_INFO.NAME, PACKAGE_INFO.VERSION)
  }

  updateConfig(config: OtelAdapterConfig): void {
    const mergedMetrics =
      config.metrics && typeof config.metrics === 'object' ? { ...this._config.metrics, ...config.metrics } : undefined

    this._config = {
      ...this._config,
      ...config,
      ...(mergedMetrics ? { metrics: mergedMetrics } : {}),
    }

    const tracerProvider = this._config.tracerProvider ?? trace
    this._tracer = tracerProvider.getTracer(PACKAGE_INFO.NAME, PACKAGE_INFO.VERSION)

    const metricsEnabled = this._config.metrics?.enabled === true
    if (metricsEnabled) {
      if (this._metrics) {
        this._metrics.updateConfig(this._config.metrics as KafkaMetricsConfig)
      } else if (this._enabled) {
        this._metrics = getKafkaMetrics(this._config.metrics as KafkaMetricsConfig)
      }
    } else if (this._metrics) {
      this._metrics.dispose()
      this._metrics = null
    }
  }

  /**
   * Enable the OTEL adapter - subscribes to all diagnostic channels
   */
  enable(): void {
    if (this._enabled) {
      return
    }

    // Initialize metrics if configured
    if (this._config.metrics?.enabled === true) {
      this._metrics = getKafkaMetrics(this._config.metrics)
    }

    // Subscribe to producer channels
    this._subscribeProducer()

    // Subscribe to consumer channels
    this._subscribeConsumer()

    // Subscribe to batch channels
    this._subscribeBatch()

    this._enabled = true
    diag.debug('OTEL adapter enabled')
  }

  /**
   * Disable the OTEL adapter - unsubscribes from all channels
   */
  disable(): void {
    if (!this._enabled) {
      return
    }

    // Unsubscribe all handlers
    for (const [channelName, handler] of this._handlers) {
      try {
        // Type-safe unsubscribe based on channel name
        if (channelName === producerSendStartChannel.name) {
          producerSendStartChannel.unsubscribe(handler as never)
        } else if (channelName === producerSendEndChannel.name) {
          producerSendEndChannel.unsubscribe(handler as never)
        } else if (channelName === consumerReceiveStartChannel.name) {
          consumerReceiveStartChannel.unsubscribe(handler as never)
        } else if (channelName === consumerReceiveEndChannel.name) {
          consumerReceiveEndChannel.unsubscribe(handler as never)
        } else if (channelName === consumerProcessStartChannel.name) {
          consumerProcessStartChannel.unsubscribe(handler as never)
        } else if (channelName === consumerProcessEndChannel.name) {
          consumerProcessEndChannel.unsubscribe(handler as never)
        } else if (channelName === batchReceiveStartChannel.name) {
          batchReceiveStartChannel.unsubscribe(handler as never)
        } else if (channelName === batchReceiveEndChannel.name) {
          batchReceiveEndChannel.unsubscribe(handler as never)
        } else if (channelName === batchProcessStartChannel.name) {
          batchProcessStartChannel.unsubscribe(handler as never)
        } else if (channelName === batchProcessEndChannel.name) {
          batchProcessEndChannel.unsubscribe(handler as never)
        }
      } catch (error) {
        diag.warn(`Failed to unsubscribe from ${channelName}:`, error)
      }
    }
    this._handlers.clear()

    // Dispose metrics
    if (this._metrics) {
      this._metrics.dispose()
      this._metrics = null
    }

    this._enabled = false
    diag.debug('OTEL adapter disabled')
  }

  /**
   * Check if the adapter is enabled
   */
  isEnabled(): boolean {
    return this._enabled
  }

  /**
   * Check if metrics are enabled
   */
  isMetricsEnabled(): boolean {
    return this._metrics?.isEnabled() ?? false
  }

  /**
   * Get the tracer instance
   */
  get tracer(): Tracer {
    return this._tracer
  }

  // ---------------------------------------------------------------------------
  // Producer Channel Handlers
  // ---------------------------------------------------------------------------

  private _subscribeProducer(): void {
    const startHandler = (event: ProducerSendStartEvent) => {
      try {
        if (this._shouldIgnoreTopic(event.topic)) {
          return
        }

        const parentContext = context.active()
        const span = createProducerSpan(this._tracer, event.record, {
          operationName: KAFKA_OPERATION_NAMES.SEND,
          parentContext,
          clientId: event.clientId,
          serverAddress: event.serverAddress,
          serverPort: event.serverPort,
          capturePayload: this._config.captureMessagePayload,
          maxPayloadSize: this._config.maxPayloadSize,
        })

        if (!span) {
          return
        }

        // Store span in event for the end handler
        event.context[SPAN_KEY] = span

        // Start timer for metrics
        if (this._metrics) {
          event.context[TIMER_KEY] = KafkaMetrics.startTimer()
        }

        const spanContext = trace.setSpan(parentContext, span)

        // Inject trace context into message headers
        if (event.record.messages) {
          for (const message of event.record.messages) {
            const headers = message.headers ?? {}
            const injected = injectTraceContext(headers, spanContext)
            message.headers = normalizeHeadersToBuffer(injected)
          }
        }

        if (this._config.captureMessageHeaders && event.record.messages?.length === 1) {
          span.setAttributes(getCapturedHeaderAttributes(event.record.messages[0]?.headers))
        }

        // Call producer hook if configured
        if (this._config.producerHook) {
          try {
            context.with(spanContext, () => {
              this._config.producerHook?.(span, event.record)
            })
          } catch (error) {
            diag.warn('Producer hook failed:', error)
          }
        }
      } catch (error) {
        diag.warn('Producer send start handler failed:', error)
      }
    }

    const endHandler = (event: ProducerSendEndEvent) => {
      try {
        const span = event.context[SPAN_KEY] as Span | undefined
        if (!span) {
          return
        }

        // Set partition/offset from metadata
        if (event.metadata?.length) {
          const [meta] = event.metadata
          if (meta?.partition !== undefined) {
            span.setAttribute(KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_DESTINATION_PARTITION_ID, String(meta.partition))
          }
          if (meta?.offset !== undefined) {
            span.setAttribute(KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_KAFKA_OFFSET, meta.offset)
          }
        }

        setSpanStatus(span, event.error)
        span.end()

        // Record metrics
        if (this._metrics) {
          const timer = event.context[TIMER_KEY] as (() => number) | undefined
          if (timer) {
            try {
              const duration = timer()
              this._metrics.recordProducerDuration(event.topic, duration, {
                partition: event.metadata?.[0]?.partition,
                clientId: event.clientId,
                error: event.error,
              })
              this._metrics.recordMessagesSent(event.record, event.metadata, {
                clientId: event.clientId,
                error: event.error,
              })
            } catch (error) {
              diag.warn('Failed to record producer metrics:', error)
            }
          }
        }

        // Call producer hook with metadata
        if (this._config.producerHook && event.metadata?.length) {
          try {
            this._config.producerHook(span, event.record, event.metadata[0])
          } catch (error) {
            diag.warn('Producer hook with metadata failed:', error)
          }
        }
      } catch (error) {
        diag.warn('Producer send end handler failed:', error)
      }
    }

    producerSendStartChannel.subscribe(startHandler)
    producerSendEndChannel.subscribe(endHandler)

    this._handlers.set(producerSendStartChannel.name, startHandler as never)
    this._handlers.set(producerSendEndChannel.name, endHandler as never)
  }

  // ---------------------------------------------------------------------------
  // Consumer Channel Handlers
  // ---------------------------------------------------------------------------

  private _subscribeConsumer(): void {
    const receiveStartHandler = (event: ConsumerReceiveStartEvent) => {
      try {
        // Start timer for receive duration
        if (this._metrics) {
          event.context[TIMER_KEY] = KafkaMetrics.startTimer()
        }
      } catch (error) {
        diag.warn('Consumer receive start handler failed:', error)
      }
    }

    const receiveEndHandler = (event: ConsumerReceiveEndEvent) => {
      try {
        const ignoredTopic = event.message ? this._shouldIgnoreTopic(event.message.topic) : false

        // Emit poll span for successful receives and errors (skip ignored topics).
        if ((event.message && !ignoredTopic) || event.error) {
          const pollTopic = event.message && !ignoredTopic ? event.message.topic : 'kafka'
          const attributes: Attributes = {
            [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_SYSTEM]: KAFKA_DEFAULTS.MESSAGING_SYSTEM,
            [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_OPERATION_NAME]: KAFKA_OPERATION_NAMES.POLL,
            [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_OPERATION_TYPE]: KAFKA_OPERATION_TYPES.RECEIVE,
            ...(event.clientId ? { [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_CLIENT_ID]: event.clientId } : {}),
            ...(event.serverAddress ? { [KAFKA_SEMANTIC_CONVENTIONS.SERVER_ADDRESS]: event.serverAddress } : {}),
            ...(event.serverPort !== undefined ? { [KAFKA_SEMANTIC_CONVENTIONS.SERVER_PORT]: event.serverPort } : {}),
            ...(event.groupId ? { [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_CONSUMER_GROUP_NAME]: event.groupId } : {}),
          }

          if (event.message && !ignoredTopic) {
            attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_DESTINATION_NAME] = event.message.topic
            if (event.message.partition !== undefined) {
              attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_DESTINATION_PARTITION_ID] = String(
                event.message.partition,
              )
            }
          }

          const endTime = event.timestamp
          const startTime = endTime - event.durationMs

          const pollSpan = this._tracer.startSpan(KAFKA_SPAN_NAMES.CONSUMER_POLL(pollTopic), {
            kind: SpanKind.CONSUMER,
            startTime,
            attributes,
          })
          setSpanStatus(pollSpan, event.error)
          pollSpan.end(endTime)
        }

        if (!event.message || ignoredTopic) {
          return
        }

        // Record receive metrics
        if (this._metrics) {
          const timer = event.context[TIMER_KEY] as (() => number) | undefined
          if (timer) {
            try {
              const duration = timer()
              this._metrics.recordConsumerDuration(event.message.topic, duration, {
                partition: event.message.partition,
                groupId: event.groupId,
                clientId: event.clientId,
                error: event.error,
              })
              this._metrics.recordMessagesConsumed(event.message, {
                groupId: event.groupId,
                clientId: event.clientId,
                error: event.error,
              })
            } catch (error) {
              diag.warn('Failed to record consumer metrics:', error)
            }
          }
        }
      } catch (error) {
        diag.warn('Consumer receive end handler failed:', error)
      }
    }

    const processStartHandler = (event: ConsumerProcessStartEvent) => {
      try {
        if (this._shouldIgnoreTopic(event.message.topic)) {
          return
        }

        const parentContext = extractTraceContext(event.message.headers || {})
        const span = createConsumerSpan(this._tracer, event.message, {
          operationName: KAFKA_OPERATION_NAMES.PROCESS,
          operationType: KAFKA_OPERATION_TYPES.PROCESS,
          parentContext,
          clientId: event.clientId,
          serverAddress: event.serverAddress,
          serverPort: event.serverPort,
          capturePayload: this._config.captureMessagePayload,
          maxPayloadSize: this._config.maxPayloadSize,
        })

        if (!span) {
          return
        }

        if (event.groupId) {
          span.setAttributes({ [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_CONSUMER_GROUP_NAME]: event.groupId })
        }

        if (this._config.captureMessageHeaders) {
          span.setAttributes(getCapturedHeaderAttributes(event.message.headers))
        }

        const spanContext = OtelAdapter._attachMessageSpan(event.message, span, parentContext)

        // Store span in event context
        event.context[SPAN_KEY] = span

        // Start timer for process duration
        if (this._metrics) {
          event.context[TIMER_KEY] = KafkaMetrics.startTimer()
        }

        // Call message hook if configured
        if (this._config.messageHook) {
          try {
            context.with(spanContext, () => {
              this._config.messageHook?.(span, event.message)
            })
          } catch (error) {
            diag.warn('Message hook failed:', error)
          }
        }
      } catch (error) {
        diag.warn('Consumer process start handler failed:', error)
      }
    }

    const processEndHandler = (event: ConsumerProcessEndEvent) => {
      try {
        const span = event.context[SPAN_KEY] as Span | undefined
        if (!span) {
          return
        }

        setSpanStatus(span, event.error)
        span.end()

        // Record process duration metrics
        if (this._metrics) {
          const timer = event.context[TIMER_KEY] as (() => number) | undefined
          if (timer) {
            try {
              this._metrics.recordProcessDuration(event.message, timer(), {
                groupId: event.groupId,
                clientId: event.clientId,
                error: event.error,
              })
            } catch (error) {
              diag.warn('Failed to record process duration metrics:', error)
            }
          }
        }
      } catch (error) {
        diag.warn('Consumer process end handler failed:', error)
      }
    }

    consumerReceiveStartChannel.subscribe(receiveStartHandler)
    consumerReceiveEndChannel.subscribe(receiveEndHandler)
    consumerProcessStartChannel.subscribe(processStartHandler)
    consumerProcessEndChannel.subscribe(processEndHandler)

    this._handlers.set(consumerReceiveStartChannel.name, receiveStartHandler as never)
    this._handlers.set(consumerReceiveEndChannel.name, receiveEndHandler as never)
    this._handlers.set(consumerProcessStartChannel.name, processStartHandler as never)
    this._handlers.set(consumerProcessEndChannel.name, processEndHandler as never)
  }

  // ---------------------------------------------------------------------------
  // Batch Channel Handlers
  // ---------------------------------------------------------------------------

  private _subscribeBatch(): void {
    const receiveStartHandler = (event: BatchReceiveStartEvent) => {
      try {
        if (this._metrics) {
          event.context[TIMER_KEY] = KafkaMetrics.startTimer()
        }
      } catch (error) {
        diag.warn('Batch receive start handler failed:', error)
      }
    }

    const receiveEndHandler = (event: BatchReceiveEndEvent) => {
      try {
        const instrumentedMessages = event.messages.filter((message) => !this._shouldIgnoreTopic(message.topic))

        if (instrumentedMessages.length === 0 && !event.error) {
          return
        }

        // Emit poll span for successful receives and errors (skip fully ignored batches).
        if (instrumentedMessages.length > 0 || event.error) {
          const [first] = instrumentedMessages
          const pollTopic = first ? first.topic : 'kafka'
          const attributes: Attributes = {
            [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_SYSTEM]: KAFKA_DEFAULTS.MESSAGING_SYSTEM,
            [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_OPERATION_NAME]: KAFKA_OPERATION_NAMES.POLL,
            [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_OPERATION_TYPE]: KAFKA_OPERATION_TYPES.RECEIVE,
            ...(event.clientId ? { [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_CLIENT_ID]: event.clientId } : {}),
            ...(event.serverAddress ? { [KAFKA_SEMANTIC_CONVENTIONS.SERVER_ADDRESS]: event.serverAddress } : {}),
            ...(event.serverPort !== undefined ? { [KAFKA_SEMANTIC_CONVENTIONS.SERVER_PORT]: event.serverPort } : {}),
            ...(event.groupId ? { [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_CONSUMER_GROUP_NAME]: event.groupId } : {}),
          }

          if (first) {
            attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_DESTINATION_NAME] = first.topic
            if (first.partition !== undefined) {
              attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_DESTINATION_PARTITION_ID] = String(first.partition)
            }
          }

          const endTime = event.timestamp
          const startTime = endTime - event.durationMs
          const pollSpan = this._tracer.startSpan(KAFKA_SPAN_NAMES.CONSUMER_POLL(pollTopic), {
            kind: SpanKind.CONSUMER,
            startTime,
            attributes,
          })
          setSpanStatus(pollSpan, event.error)
          pollSpan.end(endTime)
        }

        if (instrumentedMessages.length === 0) {
          return
        }

        // Record batch receive metrics
        if (this._metrics) {
          const timer = event.context[TIMER_KEY] as (() => number) | undefined
          if (timer) {
            try {
              const duration = timer()
              const [first] = instrumentedMessages
              this._metrics.recordConsumerDuration(first.topic, duration, {
                partition: first.partition,
                groupId: event.groupId,
                clientId: event.clientId,
                error: event.error,
              })
              this._metrics.recordMessagesConsumed(instrumentedMessages, {
                groupId: event.groupId,
                clientId: event.clientId,
                error: event.error,
              })
            } catch (error) {
              diag.warn('Failed to record batch consumer metrics:', error)
            }
          }
        }
      } catch (error) {
        diag.warn('Batch receive end handler failed:', error)
      }
    }

    const processStartHandler = (event: BatchProcessStartEvent) => {
      try {
        const instrumentedMessages = event.messages.filter((message) => !this._shouldIgnoreTopic(message.topic))
        if (instrumentedMessages.length === 0) {
          return
        }

        const [first] = instrumentedMessages
        const headerCarrier = (event.context as { parentHeaders?: Record<string, unknown> }).parentHeaders
        const parentContext = extractTraceContext(
          (headerCarrier || first.headers || {}) as Record<string, Buffer | string | string[] | undefined>,
        )
        // Re-inject parent context into messages to help stream/batch consumers stay on the producer trace
        for (const message of instrumentedMessages) {
          const headers = message.headers ?? {}
          message.headers = normalizeHeadersToBuffer(injectTraceContext(headers, parentContext))
        }

        const batchSpan = createBatchSpan(this._tracer, instrumentedMessages.length, {
          topic: first.topic,
          operationName: KAFKA_OPERATION_NAMES.PROCESS,
          parentContext,
          clientId: event.clientId,
          serverAddress: event.serverAddress,
          serverPort: event.serverPort,
        })

        const messageSpans: Span[] = []
        const messageParentContext = batchSpan ? trace.setSpan(parentContext, batchSpan) : parentContext

        if (batchSpan) {
          if (event.groupId) {
            batchSpan.setAttributes({ [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_CONSUMER_GROUP_NAME]: event.groupId })
          }

          event.context[SPAN_KEY] = batchSpan
          OtelAdapter._attachBatchSpan(instrumentedMessages, batchSpan, parentContext)
        }

        for (const message of instrumentedMessages) {
          try {
            const messageSpan = createConsumerSpan(this._tracer, message, {
              operationName: KAFKA_OPERATION_NAMES.PROCESS,
              operationType: KAFKA_OPERATION_TYPES.PROCESS,
              parentContext: messageParentContext,
              clientId: event.clientId,
              serverAddress: event.serverAddress,
              serverPort: event.serverPort,
              capturePayload: this._config.captureMessagePayload,
              maxPayloadSize: this._config.maxPayloadSize,
            })

            if (messageSpan) {
              messageSpan.setAttributes({
                [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_BATCH_MESSAGE_COUNT]: instrumentedMessages.length,
                ...(event.groupId ? { [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_CONSUMER_GROUP_NAME]: event.groupId } : {}),
              })

              if (this._config.captureMessageHeaders) {
                messageSpan.setAttributes(getCapturedHeaderAttributes(message.headers))
              }

              const messageSpanContext = OtelAdapter._attachMessageSpan(message, messageSpan, messageParentContext)

              if (this._config.messageHook) {
                try {
                  context.with(messageSpanContext, () => {
                    this._config.messageHook?.(messageSpan, message)
                  })
                } catch (error) {
                  diag.warn('Message hook failed:', error)
                }
              }

              messageSpans.push(messageSpan)
            }
          } catch (error) {
            diag.warn('Failed to create message span in batch:', error)
          }
        }

        event.context[MESSAGE_SPANS_KEY] = messageSpans
        event.context[INSTRUMENTED_MESSAGES_KEY] = instrumentedMessages

        if (this._metrics) {
          event.context[TIMER_KEY] = KafkaMetrics.startTimer()
        }
      } catch (error) {
        diag.warn('Batch process start handler failed:', error)
      }
    }

    const processEndHandler = (event: BatchProcessEndEvent) => {
      try {
        const batchSpan = event.context[SPAN_KEY] as Span | undefined
        const messageSpans = event.context[MESSAGE_SPANS_KEY] as Span[] | undefined
        const instrumentedMessages = event.context[INSTRUMENTED_MESSAGES_KEY] as Message[] | undefined

        if (Array.isArray(messageSpans)) {
          for (const messageSpan of messageSpans) {
            try {
              setSpanStatus(messageSpan, event.error)
              messageSpan.end()
            } catch (error) {
              diag.warn('Failed to end message span in batch:', error)
            }
          }
        }

        if (batchSpan) {
          setSpanStatus(batchSpan, event.error)
          batchSpan.end()
        }

        if (this._metrics && instrumentedMessages?.length) {
          const timer = event.context[TIMER_KEY] as (() => number) | undefined
          if (timer) {
            try {
              this._metrics.recordBatchProcessDuration(instrumentedMessages, timer(), {
                groupId: event.groupId,
                clientId: event.clientId,
                error: event.error,
              })
            } catch (error) {
              diag.warn('Failed to record batch process duration metrics:', error)
            }
          }
        }
      } catch (error) {
        diag.warn('Batch process end handler failed:', error)
      }
    }

    batchReceiveStartChannel.subscribe(receiveStartHandler)
    batchReceiveEndChannel.subscribe(receiveEndHandler)
    batchProcessStartChannel.subscribe(processStartHandler)
    batchProcessEndChannel.subscribe(processEndHandler)

    this._handlers.set(batchReceiveStartChannel.name, receiveStartHandler as never)
    this._handlers.set(batchReceiveEndChannel.name, receiveEndHandler as never)
    this._handlers.set(batchProcessStartChannel.name, processStartHandler as never)
    this._handlers.set(batchProcessEndChannel.name, processEndHandler as never)
  }

  // ---------------------------------------------------------------------------
  // Helper Methods
  // ---------------------------------------------------------------------------

  private static _attachMessageSpan(message: Message, span: Span, parentContext: Context): Context {
    const spanContext = trace.setSpan(parentContext, span)
    OtelAdapter._defineHiddenOtelField(message as MessageWithOtelFields, 'span', span)
    OtelAdapter._defineHiddenOtelField(message as MessageWithOtelFields, 'otelContext', spanContext)
    return spanContext
  }

  private static _attachBatchSpan(messages: Message[], span: Span, parentContext: Context): void {
    const spanContext = trace.setSpan(parentContext, span)
    OtelAdapter._defineHiddenOtelField(messages as BatchWithOtelFields, 'span', span)
    OtelAdapter._defineHiddenOtelField(messages as BatchWithOtelFields, 'otelContext', spanContext)
  }

  private static _defineHiddenOtelField(
    target: MessageWithOtelFields | BatchWithOtelFields,
    key: 'span' | 'otelContext',
    value: Span | Context,
  ): void {
    try {
      Object.defineProperty(target, key, {
        value,
        writable: true,
        configurable: true,
        enumerable: false,
      })
      return
    } catch {
      // Fall back to assignment for exotic objects where defineProperty is restricted.
    }

    try {
      target[key] = value as never
    } catch {
      // Ignore decoration failures to preserve backward compatibility.
    }
  }

  private _shouldIgnoreTopic(topic: string): boolean {
    const { ignoreTopics } = this._config
    if (!ignoreTopics) {
      return false
    }
    if (Array.isArray(ignoreTopics)) {
      return ignoreTopics.includes(topic)
    }
    if (typeof ignoreTopics === 'function') {
      try {
        return ignoreTopics(topic)
      } catch {
        return false
      }
    }
    return false
  }
}

// Singleton instance
let globalAdapter: OtelAdapter | null = null

/**
 * Get or create the global OTEL adapter
 */
export function getOtelAdapter(config?: OtelAdapterConfig): OtelAdapter {
  if (!globalAdapter) {
    globalAdapter = new OtelAdapter(config)
    globalAdapter.enable()
  } else if (config) {
    globalAdapter.updateConfig(config)
  }
  return globalAdapter
}

/**
 * Reset the global OTEL adapter
 */
export function resetOtelAdapter(): void {
  if (globalAdapter) {
    globalAdapter.disable()
    globalAdapter = null
  }
}

/**
 * Convenience function to enable OTEL instrumentation
 */
export function enableOtelInstrumentation(config?: OtelAdapterConfig): OtelAdapter {
  return getOtelAdapter(config)
}
