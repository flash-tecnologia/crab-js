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
import type { KafkaOtelContext, KafkaOtelInstrumentationConfig } from './otel/types.js'
import { KafkaBatchStreamReadable } from './streams/kafka-batch-stream-readable.js'
import { KafkaStreamReadable } from './streams/kafka-stream-readable.js'

export interface StreamConsumerConfiguration extends ConsumerConfiguration {
  batchSize?: number // Default 1 (single mode), > 1 enables batch mode
  batchTimeout?: number // Default 100ms, only used when batchSize > 1
  streamOptions?: ReadableOptions
}

export interface KafkaClientConfiguration extends KafkaConfiguration {
  otel?: KafkaOtelInstrumentationConfig | false // OTEL configuration or false to disable
}

/**
 * KafkaClient class
 */
export class KafkaClient {
  private readonly kafkaClientConfig: KafkaClientConfig
  private readonly _otelEnabled: boolean
  private readonly _otelContext: KafkaOtelContext

  /**
   * Creates a KafkaClient instance
   * @throws {Error} If the configuration is invalid
   */
  constructor(private readonly kafkaConfiguration: KafkaClientConfiguration) {
    // Extract OTEL configuration
    const { otel, ...kafkaConfig } = kafkaConfiguration
    this.kafkaClientConfig = new KafkaClientConfig(kafkaConfig)

    // Initialize OTEL instrumentation
    this._otelEnabled = otel !== false && otel !== null
    if (this._otelEnabled && typeof otel === 'object') {
      const instrumentation = getKafkaInstrumentation(otel)
      this._otelContext = instrumentation.createOtelContext()
    } else if (this._otelEnabled) {
      const instrumentation = getKafkaInstrumentation()
      this._otelContext = instrumentation.createOtelContext()
    } else {
      this._otelContext = this._createDisabledOtelContext()
    }
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
    const instrumentation = getKafkaInstrumentation()
    const originalSend = producer.send.bind(producer)

    producer.send = instrumentation.instrumentProducerSend(
      originalSend,
      this.kafkaConfiguration.clientId,
    )

    return producer
  }

  private _instrumentConsumer(consumer: KafkaConsumer, groupId?: string) {
    const instrumentation = getKafkaInstrumentation()
    const originalRecv = consumer.recv.bind(consumer)
    const originalRecvBatch = consumer.recvBatch.bind(consumer)

    consumer.recv = instrumentation.instrumentConsumerReceive(
      originalRecv,
      groupId,
      this.kafkaConfiguration.clientId,
    )
    consumer.recvBatch = instrumentation.instrumentBatchReceive(
      originalRecvBatch,
      groupId,
      this.kafkaConfiguration.clientId,
    )

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
    }
  }
}
