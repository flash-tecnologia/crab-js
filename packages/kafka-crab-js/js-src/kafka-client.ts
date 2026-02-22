import type { ReadableOptions } from 'node:stream'
import {
  type ConsumerConfiguration,
  KafkaClientConfig,
  type KafkaConfiguration,
  type KafkaConsumer,
  type KafkaProducer,
  type Message,
  type ProducerConfiguration,
} from '../js-binding.js'

import {
  instrumentBatchReadableStream,
  instrumentBatchReceive,
  instrumentConsumerReadableStream,
  instrumentConsumerReceive,
  instrumentProducerSend,
} from './diagnostics/instrumentation.js'
import { KafkaBatchStreamReadable } from './streams/kafka-batch-stream-readable.js'
import { KafkaStreamReadable } from './streams/kafka-stream-readable.js'

const DEFAULT_WEB_STREAM_BATCH_TIMEOUT = 1000
const DEFAULT_WEB_STREAM_SERIAL_PREFETCH_SIZE = 64
const DEFAULT_WEB_STREAM_SERIAL_PREFETCH_TIMEOUT = 1

type KafkaConsumerWithWebStream = KafkaConsumer & {
  recvStream(): ReadableStream<Message>
  recvBatchStream(size: number, timeoutMs: number): ReadableStream<Message[]>
}

export interface StreamConsumerConfiguration extends ConsumerConfiguration {
  batchSize?: number // Default 1 (single mode), > 1 enables batch mode
  batchTimeout?: number // Default 100ms, only used when batchSize > 1
  streamOptions?: ReadableOptions
}

export interface WebStreamConsumerConfiguration extends ConsumerConfiguration {
  batchSize?: number // Default 1 (single mode), > 1 enables batch mode
  batchTimeout?: number // Default 1000ms
  serialPrefetchSize?: number // Default 64, only used in serial mode
  serialPrefetchTimeout?: number // Default 1ms, only used in serial mode
}

export type WebStreamConsumer =
  | {
    mode: 'serial'
    consumer: KafkaConsumer
    stream: ReadableStream<Message>
  }
  | {
    mode: 'batch'
    consumer: KafkaConsumer
    stream: ReadableStream<Message[]>
  }

type SerialBatchSize = 0 | 1 | undefined
type NumericLiteral<BatchSizeValue extends number> = number extends BatchSizeValue ? never : BatchSizeValue
type BatchLiteralSize<BatchSizeValue extends number> = BatchSizeValue extends 0 | 1 ? never
  : NumericLiteral<BatchSizeValue>

type SerialWebStreamConsumer = Extract<WebStreamConsumer, { mode: 'serial' }>
type BatchWebStreamConsumer = Extract<WebStreamConsumer, { mode: 'batch' }>

type SerialWebStreamConsumerConfiguration = Omit<WebStreamConsumerConfiguration, 'batchSize'> & {
  batchSize?: SerialBatchSize
}

type BatchLiteralWebStreamConsumerConfiguration<BatchSizeValue extends number> =
  & Omit<
    WebStreamConsumerConfiguration,
    'batchSize'
  >
  & {
    batchSize: BatchLiteralSize<BatchSizeValue>
  }

type DynamicBatchWebStreamConsumerConfiguration = Omit<WebStreamConsumerConfiguration, 'batchSize'> & {
  batchSize: number
}

function flattenBatchStream(batchStream: ReadableStream<Message[]>): ReadableStream<Message> {
  const reader = batchStream.getReader()
  let currentBatch: Message[] = []
  let currentIndex = 0
  let closed = false

  return new ReadableStream<Message>({
    async pull(controller) {
      if (currentIndex < currentBatch.length) {
        controller.enqueue(currentBatch[currentIndex] as Message)
        currentIndex += 1
        return
      }

      if (closed) {
        controller.close()
        return
      }

      const { value, done } = await reader.read()
      if (done || !value) {
        closed = true
        controller.close()
        return
      }

      if (value.length > 0) {
        currentBatch = value
        currentIndex = 0
      }
    },
    async cancel(reason) {
      closed = true
      currentBatch = []
      currentIndex = 0
      await reader.cancel(reason)
    },
  })
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

  /**
   * Creates a native WebStream consumer.
   * @param {WebStreamConsumerConfiguration} streamConfiguration - Stream consumer configuration
   * @returns {WebStreamConsumer} Native WebStream consumer and raw consumer pair
   */
  createWebStreamConsumer(streamConfiguration: SerialWebStreamConsumerConfiguration): SerialWebStreamConsumer
  createWebStreamConsumer<const BatchSizeValue extends number>(
    streamConfiguration: BatchLiteralWebStreamConsumerConfiguration<BatchSizeValue>,
  ): BatchWebStreamConsumer
  createWebStreamConsumer(
    streamConfiguration: DynamicBatchWebStreamConsumerConfiguration,
  ): WebStreamConsumer
  createWebStreamConsumer(streamConfiguration: WebStreamConsumerConfiguration): WebStreamConsumer {
    const {
      batchSize,
      batchTimeout,
      serialPrefetchSize,
      serialPrefetchTimeout,
      ...consumerConfiguration
    } = streamConfiguration

    const kafkaConsumer = this.kafkaClientConfig.createConsumer(consumerConfiguration)
    const instrumentedConsumer = this._diagnosticsEnabled
      ? this._instrumentConsumer(kafkaConsumer, consumerConfiguration.groupId)
      : kafkaConsumer
    const webStreamConsumer = instrumentedConsumer as KafkaConsumerWithWebStream

    if (batchSize && batchSize > 1) {
      const resolvedBatchTimeout = batchTimeout ?? DEFAULT_WEB_STREAM_BATCH_TIMEOUT
      const stream = webStreamConsumer.recvBatchStream(batchSize, resolvedBatchTimeout)
      const instrumentedStream = this._diagnosticsEnabled
        ? instrumentBatchReadableStream(
          stream,
          consumerConfiguration.groupId,
          batchSize,
          resolvedBatchTimeout,
          this._diagnosticsConfig,
        )
        : stream

      return {
        mode: 'batch',
        consumer: instrumentedConsumer,
        stream: instrumentedStream,
      }
    }

    const resolvedPrefetchSize = serialPrefetchSize && serialPrefetchSize > 1
      ? serialPrefetchSize
      : DEFAULT_WEB_STREAM_SERIAL_PREFETCH_SIZE
    const resolvedPrefetchTimeout = serialPrefetchTimeout ?? DEFAULT_WEB_STREAM_SERIAL_PREFETCH_TIMEOUT
    const prefetchStream = webStreamConsumer.recvBatchStream(resolvedPrefetchSize, resolvedPrefetchTimeout)
    const serialStream = flattenBatchStream(prefetchStream)
    const instrumentedStream = this._diagnosticsEnabled
      ? instrumentConsumerReadableStream(serialStream, consumerConfiguration.groupId, this._diagnosticsConfig)
      : serialStream

    return {
      mode: 'serial',
      consumer: instrumentedConsumer,
      stream: instrumentedStream,
    }
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

type Assert<Condition extends true> = Condition
type IsEqual<LeftType, RightType> = [LeftType] extends [RightType] ? [RightType] extends [LeftType] ? true
  : false
  : false

type _CreateWebStreamConsumer = KafkaClient['createWebStreamConsumer']

type _AssertSerialConfigReturn = Assert<
  _CreateWebStreamConsumer extends (
    config: SerialWebStreamConsumerConfiguration,
  ) => SerialWebStreamConsumer ? true
    : false
>

type _AssertBatchLiteralConfigReturn = Assert<
  _CreateWebStreamConsumer extends <BatchSizeValue extends number>(
    config: BatchLiteralWebStreamConsumerConfiguration<BatchSizeValue>,
  ) => BatchWebStreamConsumer ? true
    : false
>

type _AssertDynamicBatchConfigReturn = Assert<
  _CreateWebStreamConsumer extends (
    config: DynamicBatchWebStreamConsumerConfiguration,
  ) => WebStreamConsumer ? true
    : false
>

type _AssertBatchLiteralFor1024 = Assert<IsEqual<BatchLiteralSize<1024>, 1024>>
type _AssertBatchLiteralForNumber = Assert<IsEqual<BatchLiteralSize<number>, never>>

// @ts-expect-error batch size literal 1 must not be considered a batch literal
type _AssertBatchLiteralRejectsOne = Assert<IsEqual<BatchLiteralSize<1>, 1>>
