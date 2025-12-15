import type { ReadableOptions } from 'node:stream'
import {
  type ConsumerConfiguration,
  KafkaClientConfig,
  type KafkaConfiguration,
  type KafkaConsumer,
  type KafkaProducer,
  type ProducerConfiguration,
} from '../js-binding.js'

import {
  instrumentBatchReceive,
  instrumentConsumerReceive,
  instrumentProducerSend,
} from './diagnostics/instrumentation.js'
import { KafkaBatchStreamReadable } from './streams/kafka-batch-stream-readable.js'
import { KafkaStreamReadable } from './streams/kafka-stream-readable.js'

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
  /**
   * Enable diagnostic channel instrumentation. Defaults to true.
   * Set to false to disable all diagnostic channel events.
   */
  diagnostics?: boolean
}

/**
 * KafkaClient class
 *
 * Core Kafka client that emits events via diagnostic channels.
 * For OpenTelemetry support, install kafka-crab-js-otel and call enableOtelInstrumentation().
 */
export class KafkaClient {
  private readonly kafkaClientConfig: KafkaClientConfig
  private readonly kafkaConfiguration: KafkaClientConfiguration & { clientId: string }
  private readonly _diagnosticsEnabled: boolean
  private readonly _diagnosticsConfig: {
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

    // Extract configuration
    const { diagnostics, ...kafkaConfig } = this.kafkaConfiguration
    this.kafkaClientConfig = new KafkaClientConfig(kafkaConfig as KafkaConfiguration)

    // Diagnostics defaults to true
    this._diagnosticsEnabled = diagnostics !== false

    // Extract broker info for diagnostics
    const firstBroker = String(this.kafkaConfiguration.brokers).split(',')[0]?.trim()
    const [brokerHostRaw, brokerPortRaw] = firstBroker ? firstBroker.split(':') : [undefined, undefined]
    const brokerHost = brokerHostRaw?.trim()
    const brokerPort = brokerPortRaw ? Number(brokerPortRaw) : undefined

    this._diagnosticsConfig = {
      clientId: this.kafkaConfiguration.clientId,
      ...(brokerHost ? { serverAddress: brokerHost } : {}),
      ...(Number.isFinite(brokerPort) ? { serverPort: brokerPort } : {}),
    }
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

    // Instrument producer for diagnostic channels if enabled
    if (this._diagnosticsEnabled) {
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

    // Instrument consumer for diagnostic channels if enabled
    if (this._diagnosticsEnabled) {
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
    const instrumentedConsumer = this._diagnosticsEnabled
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
    producer.send = instrumentProducerSend(originalSend, this._diagnosticsConfig)

    return producer
  }

  private _instrumentConsumer(consumer: KafkaConsumer, groupId?: string) {
    const originalRecv = consumer.recv.bind(consumer)
    const originalRecvBatch = consumer.recvBatch.bind(consumer)

    // Use diagnostic channel-based instrumentation for near-zero overhead when no subscribers
    consumer.recv = instrumentConsumerReceive(originalRecv, groupId, this._diagnosticsConfig)
    consumer.recvBatch = instrumentBatchReceive(originalRecvBatch, groupId, this._diagnosticsConfig)

    return consumer
  }
}
