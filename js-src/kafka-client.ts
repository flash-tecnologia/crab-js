import { context, type Span } from '@opentelemetry/api'
import type { ReadableOptions } from 'node:stream'
import {
  type ConsumerConfiguration,
  KafkaClientConfig,
  type KafkaConfiguration,
  type KafkaConsumer,
  type KafkaProducer,
  type ProducerConfiguration,
} from '../js-binding.js'

import { getKafkaInstrumentation } from './otel/instrumentation.js'
import type {
  InstrumentedMessage,
  InstrumentedMessageBatch,
  KafkaOtelContext,
  KafkaOtelInstrumentationConfig,
} from './otel/types.js'
import { KafkaBatchStreamReadable } from './streams/kafka-batch-stream-readable.js'
import { KafkaStreamReadable } from './streams/kafka-stream-readable.js'
import {
  instrumentBatchReceive,
  instrumentConsumerReceive,
  instrumentProducerSend,
} from './diagnostics/instrumentation.js'
import { getOtelAdapter } from './diagnostics/otel-adapter.js'

export interface StreamConsumerConfiguration extends ConsumerConfiguration {
  batchSize?: number // Default 1 (single mode), > 1 enables batch mode
  batchTimeout?: number // Default 100ms, only used when batchSize > 1
  streamOptions?: ReadableOptions
}

export interface KafkaClientConfiguration extends Omit<KafkaConfiguration, 'clientId'> {
  /**
   * Optional client id; defaults to librdkafka's default (`rdkafka`).
   *
   * See: https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
   */
  clientId?: string
  otel?: KafkaOtelInstrumentationConfig | false // OTEL configuration or false to disable
}

/**
 * KafkaClient class
 */
export class KafkaClient {
  private readonly kafkaClientConfig: KafkaClientConfig
  private readonly kafkaConfiguration: KafkaClientConfiguration & { clientId: string }
  private readonly _otelEnabled: boolean
  private readonly _otelContext: KafkaOtelContext
  private readonly _otelBatchInstrumentationEnabled: boolean
  private readonly _otelDiagnosticConfig: {
    clientId?: string
    serverAddress?: string
    serverPort?: number
  }

  /**
   * Creates a KafkaClient instance
   * @throws {Error} If the configuration is invalid
   */
  constructor(kafkaConfiguration: KafkaClientConfiguration) {
    const resolvedClientId = kafkaConfiguration.clientId ?? 'rdkafka'
    this.kafkaConfiguration = { ...kafkaConfiguration, clientId: resolvedClientId }

    // Extract OTEL configuration
    const { otel, ...kafkaConfig } = this.kafkaConfiguration
    this.kafkaClientConfig = new KafkaClientConfig(kafkaConfig as KafkaConfiguration)

    const resolvedOtelConfig = typeof otel === 'object' ? otel : undefined

    // Initialize OTEL instrumentation
    this._otelEnabled = otel !== false && otel !== null && resolvedOtelConfig?.enabled !== false
    if (!this._otelEnabled) {
      this._otelContext = this._createDisabledOtelContext()
      this._otelBatchInstrumentationEnabled = false
      this._otelDiagnosticConfig = { clientId: this.kafkaConfiguration.clientId }
      return
    }

    const instrumentation = getKafkaInstrumentation(resolvedOtelConfig)
    const normalizedConfig = instrumentation.kafkaConfig
    this._otelBatchInstrumentationEnabled = normalizedConfig.enableBatchInstrumentation !== false

    // Enable OTEL adapter to subscribe to diagnostic channels (normalized config includes defaults)
    getOtelAdapter({
      ignoreTopics: normalizedConfig.ignoreTopics,
      captureMessageHeaders: normalizedConfig.captureMessageHeaders,
      captureMessagePayload: normalizedConfig.captureMessagePayload,
      maxPayloadSize: normalizedConfig.maxPayloadSize,
      messageHook: normalizedConfig.messageHook,
      producerHook: normalizedConfig.producerHook,
      metrics: normalizedConfig.metrics,
    })

    const firstBroker = String(this.kafkaConfiguration.brokers).split(',')[0]?.trim()
    const [brokerHostRaw, brokerPortRaw] = firstBroker ? firstBroker.split(':') : [undefined, undefined]
    const brokerHost = brokerHostRaw?.trim()
    const brokerPort = brokerPortRaw ? Number(brokerPortRaw) : undefined

    const serverAddress = normalizedConfig.serverAddress
      ?? (normalizedConfig.metrics?.serverAddress && String(normalizedConfig.metrics.serverAddress))
      ?? brokerHost
    const serverPort = normalizedConfig.serverPort
      ?? (normalizedConfig.metrics?.serverPort !== undefined ? Number(normalizedConfig.metrics.serverPort) : undefined)
      ?? (Number.isFinite(brokerPort) ? brokerPort : undefined)

    this._otelDiagnosticConfig = {
      clientId: this.kafkaConfiguration.clientId,
      ...(serverAddress ? { serverAddress } : {}),
      ...(serverPort !== undefined ? { serverPort } : {}),
    }

    this._otelContext = instrumentation.createOtelContext()
  }

  /**
   * Get the OpenTelemetry context for this client
   * @returns {KafkaOtelContext} The OTEL context
   */
  get otel(): KafkaOtelContext {
    return this._otelContext
  }

  /**
   * Creates a KafkaProducer instance
   * @param {ProducerConfiguration} [producerConfiguration] - Optional producer configuration
   * @returns {KafkaProducer} A KafkaProducer instance
   */
  createProducer(producerConfiguration?: ProducerConfiguration) {
    const producer = producerConfiguration
      ? this.kafkaClientConfig.createProducer(producerConfiguration)
      : this.kafkaClientConfig.createProducer({})

    // Instrument producer if OTEL is enabled
    if (this._otelEnabled && this._otelContext.enabled) {
      return this._instrumentProducer(producer)
    }

    return producer
  }

  /**
   * Creates a KafkaConsumer instance
   * @param {ConsumerConfiguration} consumerConfiguration - Consumer configuration
   * @returns {KafkaConsumer} A KafkaConsumer instance
   * @throws {Error} If the configuration is invalid
   */
  createConsumer(consumerConfiguration: ConsumerConfiguration) {
    const consumer = this.kafkaClientConfig.createConsumer(consumerConfiguration)

    // Instrument consumer if OTEL is enabled
    if (this._otelEnabled && this._otelContext.enabled) {
      return this._instrumentConsumer(consumer, consumerConfiguration.groupId)
    }

    return consumer
  }

  /**
   * Creates a stream consumer instance
   * @param {StreamConsumerConfiguration} streamConfiguration - Stream consumer configuration including batch mode and stream options
   * @returns {KafkaStreamReadable | KafkaBatchStreamReadable} A stream consumer instance
   * @throws {Error} If the configuration is invalid
   */
  createStreamConsumer(
    streamConfiguration: StreamConsumerConfiguration,
  ): KafkaStreamReadable | KafkaBatchStreamReadable {
    const { batchSize, batchTimeout, streamOptions, ...consumerConfiguration } = streamConfiguration
    const kafkaConsumer = this.kafkaClientConfig.createConsumer(consumerConfiguration)
    const instrumentedConsumer = this._otelEnabled && this._otelContext.enabled
      ? this._instrumentConsumer(kafkaConsumer, consumerConfiguration.groupId)
      : kafkaConsumer
    const opts = streamOptions ?? { objectMode: true }

    // Return appropriate class based on batch configuration
    if (batchSize && batchSize > 1) {
      return new KafkaBatchStreamReadable({ kafkaConsumer: instrumentedConsumer, batchSize, batchTimeout, ...opts })
    }

    return new KafkaStreamReadable({ kafkaConsumer: instrumentedConsumer, ...opts })
  }

  private _instrumentProducer(producer: KafkaProducer) {
    const originalSend = producer.send.bind(producer)

    // Use diagnostic channel-based instrumentation for near-zero overhead when no subscribers
    producer.send = instrumentProducerSend(originalSend, this._otelDiagnosticConfig)

    return producer
  }

  private _instrumentConsumer(consumer: KafkaConsumer, groupId?: string) {
    const originalRecv = consumer.recv.bind(consumer)
    const originalRecvBatch = consumer.recvBatch.bind(consumer)

    // Use diagnostic channel-based instrumentation for near-zero overhead when no subscribers
    consumer.recv = instrumentConsumerReceive(originalRecv, groupId, this._otelDiagnosticConfig)
    if (this._otelBatchInstrumentationEnabled) {
      consumer.recvBatch = instrumentBatchReceive(originalRecvBatch, groupId, this._otelDiagnosticConfig)
    }

    return consumer
  }

  // eslint-disable-next-line class-methods-use-this
  private _createDisabledOtelContext(): KafkaOtelContext {
    // Provide safe no-op implementations so callers can still invoke methods without guarding
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
      endSpan: (span?: Span | null) => {
        if (span && typeof span.end === 'function') {
          span.end()
        }
      },
      endMessageSpan: (message, error) => {
        if (!message) {
          return
        }
        const existingEndSpan = (message as unknown as { endSpan?: (error?: Error) => void }).endSpan
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
      toInstrumentedMessage: message => message as unknown as InstrumentedMessage,
      toInstrumentedBatch: batch => batch as unknown as InstrumentedMessageBatch,
      processMessage: async (message, handler) => {
        let capturedError: Error | undefined
        try {
          return await handler(message)
        } catch (error) {
          capturedError = error instanceof Error ? error : new Error(String(error))
          throw error
        } finally {
          const existingEndSpan = message
            ? (message as unknown as { endSpan?: (error?: Error) => void }).endSpan
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
