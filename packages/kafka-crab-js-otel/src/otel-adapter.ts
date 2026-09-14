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
  isSpanContextValid,
  ROOT_CONTEXT,
  type Span,
  type SpanContext,
  SpanKind,
  trace,
  type Tracer,
} from '@opentelemetry/api'

import {
  batchProcessEndChannel,
  batchProcessStartChannel,
  batchReceiveEndChannel,
  batchReceiveStartChannel,
  consumerProcessEndChannel,
  consumerProcessStartChannel,
  consumerReceiveEndChannel,
  consumerReceiveStartChannel,
  producerSendEndChannel,
  producerSendStartChannel,
} from './kafka-channels.js'

import type {
  BatchProcessEndEvent,
  BatchProcessStartEvent,
  BatchReceiveEndEvent,
  BatchReceiveStartEvent,
  ConsumerProcessEndEvent,
  ConsumerProcessStartEvent,
  ConsumerReceiveEndEvent,
  ConsumerReceiveStartEvent,
  Message,
  ProducerRecord,
  ProducerSendEndEvent,
  ProducerSendStartEvent,
  RecordMetadata,
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
  getCommonDestination,
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

/** Keep independent message origins instead of assigning the first origin to a whole batch. */
function getBatchOrigins(messages: Message[]) {
  const parents = messages.map((message) => extractTraceContext(message.headers ?? {}))
  const origins = parents.map((parent) => trace.getSpanContext(parent))
  const [firstOrigin] = origins
  const sharedOrigin =
    firstOrigin !== undefined &&
    isSpanContextValid(firstOrigin) &&
    origins.every((origin) => origin?.traceId === firstOrigin.traceId && origin?.spanId === firstOrigin.spanId)
  const uniqueOrigins = new Map<string, SpanContext>()
  for (const origin of origins) {
    if (origin && isSpanContextValid(origin)) {
      uniqueOrigins.set(`${origin.traceId}:${origin.spanId}`, origin)
    }
  }
  return {
    parents,
    sharedOrigin,
    parentContext: sharedOrigin ? parents[0] : ROOT_CONTEXT,
    links: [...uniqueOrigins.values()].map((origin) => ({ context: origin })),
  }
}

function recordReceiveMetrics(metrics: KafkaMetrics | null, event: ConsumerReceiveEndEvent): void {
  const timer = event.context[TIMER_KEY] as (() => number) | undefined
  if (!metrics || !timer) {
    return
  }
  try {
    metrics.recordConsumerDuration(event.message?.topic, timer(), {
      partition: event.message?.partition,
      groupId: event.groupId,
      clientId: event.clientId,
      error: event.error,
    })
    if (event.message) {
      metrics.recordMessagesConsumed(event.message, {
        groupId: event.groupId,
        clientId: event.clientId,
        error: event.error,
      })
    }
  } catch (error) {
    diag.warn('Failed to record consumer metrics:', error)
  }
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
  /** Whether to record header names and count (never header values; default true) */
  captureMessageHeaders?: boolean
  /** Whether to record payload size within maxPayloadSize (never contents; default false) */
  captureMessagePayload?: boolean
  /** Maximum payload size in bytes for size attributes (default 1024) */
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

  public constructor(config: OtelAdapterConfig = {}) {
    this._config = {
      captureMessagePayload: false,
      captureMessageHeaders: true,
      maxPayloadSize: 1024,
      ...config,
    }
    const tracerProvider = config.tracerProvider ?? trace
    this._tracer = tracerProvider.getTracer(PACKAGE_INFO.NAME, PACKAGE_INFO.VERSION)
  }

  public updateConfig(config: OtelAdapterConfig): void {
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
  public enable(): void {
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
  public disable(): void {
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
  public isEnabled(): boolean {
    return this._enabled
  }

  /**
   * Check if metrics are enabled
   */
  public isMetricsEnabled(): boolean {
    return this._metrics?.isEnabled() ?? false
  }

  /**
   * Get the tracer instance
   */
  public get tracer(): Tracer {
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

        // A send spanning partitions has no single partition or offset.
        const destination =
          event.metadata?.length === event.record.messages.length ? getCommonDestination(event.metadata) : {}
        if (destination.partition !== undefined) {
          span.setAttribute(
            KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_DESTINATION_PARTITION_ID,
            String(destination.partition),
          )
        }
        if (event.record.messages.length === 1 && event.metadata?.length === 1) {
          span.setAttribute(KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_KAFKA_OFFSET, event.metadata[0].offset)
        }

        // Call producer hook with metadata
        if (this._config.producerHook && event.metadata?.length) {
          try {
            context.with(trace.setSpan(context.active(), span), () => {
              this._config.producerHook?.(span, event.record, event.metadata?.[0])
            })
          } catch (error) {
            diag.warn('Producer hook with metadata failed:', error)
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
                partition: destination.partition,
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

        if (ignoredTopic || (!event.message && !event.error)) {
          return
        }

        recordReceiveMetrics(this._metrics, event)
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
          const destination = getCommonDestination(instrumentedMessages)
          const pollTopic = destination.topic ?? 'kafka'
          const attributes: Attributes = {
            [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_SYSTEM]: KAFKA_DEFAULTS.MESSAGING_SYSTEM,
            [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_OPERATION_NAME]: KAFKA_OPERATION_NAMES.POLL,
            [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_OPERATION_TYPE]: KAFKA_OPERATION_TYPES.RECEIVE,
            ...(event.clientId ? { [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_CLIENT_ID]: event.clientId } : {}),
            ...(event.serverAddress ? { [KAFKA_SEMANTIC_CONVENTIONS.SERVER_ADDRESS]: event.serverAddress } : {}),
            ...(event.serverPort !== undefined ? { [KAFKA_SEMANTIC_CONVENTIONS.SERVER_PORT]: event.serverPort } : {}),
            ...(event.groupId ? { [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_CONSUMER_GROUP_NAME]: event.groupId } : {}),
          }

          if (destination.topic) {
            attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_DESTINATION_NAME] = destination.topic
          }
          if (destination.partition !== undefined) {
            attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_DESTINATION_PARTITION_ID] = String(destination.partition)
          }
          attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_BATCH_MESSAGE_COUNT] = instrumentedMessages.length

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

        // Record batch receive metrics, including failures with no returned messages.
        if (this._metrics) {
          const timer = event.context[TIMER_KEY] as (() => number) | undefined
          if (timer) {
            try {
              const duration = timer()
              const destination = getCommonDestination(instrumentedMessages)
              this._metrics.recordConsumerDuration(destination.topic, duration, {
                partition: destination.partition,
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

        const { parents, sharedOrigin, parentContext, links } = getBatchOrigins(instrumentedMessages)
        const destination = getCommonDestination(instrumentedMessages)

        const batchSpan = createBatchSpan(this._tracer, instrumentedMessages.length, {
          topic: destination.topic,
          links,
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
          OtelAdapter._attachBatchSpan(event.messages, batchSpan, parentContext)
        }

        for (const [index, message] of instrumentedMessages.entries()) {
          try {
            const processingParent = sharedOrigin ? messageParentContext : parents[index]
            const messageSpan = createConsumerSpan(this._tracer, message, {
              operationName: KAFKA_OPERATION_NAMES.PROCESS,
              operationType: KAFKA_OPERATION_TYPES.PROCESS,
              parentContext: processingParent,
              links: !sharedOrigin && batchSpan ? [{ context: batchSpan.spanContext() }] : undefined,
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

              const messageSpanContext = OtelAdapter._attachMessageSpan(message, messageSpan, processingParent)

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
  const adapter = getOtelAdapter(config)
  adapter.enable()
  return adapter
}
