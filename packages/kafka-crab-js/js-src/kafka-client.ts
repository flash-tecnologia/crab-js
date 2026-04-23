import type { ReadableOptions } from 'node:stream'
import {
  type CompactMessageBatch,
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
const LEGACY_STREAM_SERIAL_COLLECTOR_SIZE = 1
const LEGACY_STREAM_SERIAL_COLLECTOR_TIMEOUT = 1000

type KafkaConsumerWithWebStream = KafkaConsumer & {
  recvStream(prefetchSize?: number, prefetchTimeoutMs?: number): ReadableStream<Message>
  recvBatchStream(size: number, timeoutMs: number): ReadableStream<Message[]>
  recvBatchStreamCompact(size: number, timeoutMs: number): ReadableStream<CompactMessageBatch>
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
type BatchLiteralSize<BatchSizeValue extends number> = BatchSizeValue extends 0 | 1
  ? never
  : NumericLiteral<BatchSizeValue>

type SerialWebStreamConsumer = Extract<WebStreamConsumer, { mode: 'serial' }>
type BatchWebStreamConsumer = Extract<WebStreamConsumer, { mode: 'batch' }>

type SerialWebStreamConsumerConfiguration = Omit<WebStreamConsumerConfiguration, 'batchSize'> & {
  batchSize?: SerialBatchSize
}

type BatchLiteralWebStreamConsumerConfiguration<BatchSizeValue extends number> = Omit<
  WebStreamConsumerConfiguration,
  'batchSize'
> & {
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
        controller.enqueue(currentBatch[currentIndex]!)
        currentIndex += 1
        return
      }

      if (closed) {
        controller.close()
        return
      }

      while (true) {
        const { value, done } = await reader.read()
        if (done || !value) {
          closed = true
          controller.close()
          return
        }

        if (value.length === 0) {
          continue
        }

        currentBatch = value
        currentIndex = 1
        controller.enqueue(currentBatch[0]!)
        return
      }
    },
    cancel(reason) {
      closed = true
      currentBatch = []
      currentIndex = 0
      // Avoid blocking downstream cancellation on the upstream batch reader.
      void reader.cancel(reason).catch(() => undefined)
    },
  })
}

function expandCompactBatch(batch: CompactMessageBatch): Message[] {
  const {
    payloads,
    keys,
    denseKeys,
    sharedKey,
    keyDictionary,
    keyDictionaryIndexes,
    topic,
    topics,
    partitions,
    offsets,
    sharedHeaderKey,
    sharedHeaderValue,
    sharedHeaderValues,
    denseSharedHeaderValues,
    headerValueDictionary,
    headerValueDictionaryIndexes,
    headers,
  } = batch

  if (
    topic !== undefined &&
    topics === undefined &&
    keyDictionary !== undefined &&
    keyDictionaryIndexes !== undefined &&
    sharedHeaderKey !== undefined &&
    sharedHeaderValue !== undefined &&
    sharedHeaderValues === undefined &&
    denseSharedHeaderValues === undefined &&
    headerValueDictionary === undefined &&
    headerValueDictionaryIndexes === undefined &&
    headers === undefined
  ) {
    return expandCompactBatchSharedTopicWithKeyDictionaryAndSharedHeaderValue(
      payloads,
      keyDictionary,
      keyDictionaryIndexes,
      topic,
      partitions,
      offsets,
      sharedHeaderKey,
      sharedHeaderValue,
    )
  }

  if (
    topic !== undefined &&
    topics === undefined &&
    sharedKey !== undefined &&
    sharedHeaderKey !== undefined &&
    sharedHeaderValue !== undefined &&
    sharedHeaderValues === undefined &&
    denseSharedHeaderValues === undefined &&
    headerValueDictionary === undefined &&
    headerValueDictionaryIndexes === undefined &&
    headers === undefined
  ) {
    return expandCompactBatchSharedTopicWithSharedKeyAndSharedHeaderValue(
      payloads,
      sharedKey,
      topic,
      partitions,
      offsets,
      sharedHeaderKey,
      sharedHeaderValue,
    )
  }

  if (
    topic !== undefined &&
    topics === undefined &&
    denseKeys !== undefined &&
    sharedKey === undefined &&
    keyDictionary === undefined &&
    keyDictionaryIndexes === undefined &&
    sharedHeaderKey !== undefined &&
    sharedHeaderValue === undefined &&
    denseSharedHeaderValues !== undefined &&
    headerValueDictionary === undefined &&
    headerValueDictionaryIndexes === undefined &&
    headers === undefined
  ) {
    return expandCompactBatchSharedTopicWithDenseKeysAndSharedHeaders(
      payloads,
      denseKeys,
      topic,
      partitions,
      offsets,
      sharedHeaderKey,
      denseSharedHeaderValues,
    )
  }

  if (
    topic !== undefined &&
    topics === undefined &&
    denseKeys !== undefined &&
    sharedKey === undefined &&
    keyDictionary === undefined &&
    keyDictionaryIndexes === undefined &&
    sharedHeaderKey === undefined &&
    sharedHeaderValue === undefined &&
    sharedHeaderValues === undefined &&
    denseSharedHeaderValues === undefined &&
    headerValueDictionary === undefined &&
    headerValueDictionaryIndexes === undefined &&
    headers === undefined
  ) {
    return expandCompactBatchSharedTopicWithDenseKeys(payloads, denseKeys, topic, partitions, offsets)
  }

  if (
    topic !== undefined &&
    topics === undefined &&
    keys === undefined &&
    denseKeys === undefined &&
    sharedKey === undefined &&
    keyDictionary === undefined &&
    keyDictionaryIndexes === undefined &&
    sharedHeaderKey !== undefined &&
    sharedHeaderValue === undefined &&
    denseSharedHeaderValues !== undefined &&
    headerValueDictionary === undefined &&
    headerValueDictionaryIndexes === undefined &&
    headers === undefined
  ) {
    return expandCompactBatchSharedTopicWithSharedHeaders(
      payloads,
      topic,
      partitions,
      offsets,
      sharedHeaderKey,
      denseSharedHeaderValues,
    )
  }

  if (
    topic !== undefined &&
    topics === undefined &&
    keys === undefined &&
    denseKeys === undefined &&
    sharedKey === undefined &&
    keyDictionary === undefined &&
    keyDictionaryIndexes === undefined &&
    sharedHeaderKey === undefined &&
    sharedHeaderValue === undefined &&
    sharedHeaderValues === undefined &&
    denseSharedHeaderValues === undefined &&
    headerValueDictionary === undefined &&
    headerValueDictionaryIndexes === undefined &&
    headers === undefined
  ) {
    return expandCompactBatchSharedTopic(payloads, topic, partitions, offsets)
  }

  const messages = new Array<Message>(payloads.length)

  for (let index = 0; index < payloads.length; index += 1) {
    const payload = payloads[index]!
    const key =
      sharedKey ??
      denseKeys?.[index] ??
      (keyDictionaryIndexes?.[index] === undefined ? undefined : keyDictionary?.[keyDictionaryIndexes[index]!]) ??
      keys?.[index]
    const denseSharedHeaderValue = denseSharedHeaderValues?.[index]
    const dictionaryHeaderValue =
      headerValueDictionaryIndexes?.[index] === undefined
        ? undefined
        : headerValueDictionary?.[headerValueDictionaryIndexes[index]!]
    const messageHeaders =
      headers?.[index] ??
      (sharedHeaderKey &&
      (sharedHeaderValue !== undefined ||
        denseSharedHeaderValue !== undefined ||
        dictionaryHeaderValue !== undefined ||
        sharedHeaderValues?.[index] !== undefined)
        ? {
            [sharedHeaderKey]: (sharedHeaderValue ??
              denseSharedHeaderValue ??
              dictionaryHeaderValue ??
              sharedHeaderValues?.[index])!,
          }
        : undefined)
    const messageTopic = (topic ?? topics?.[index])!
    const partition = partitions[index]!
    const offset = offsets[index]!

    if (key === undefined && messageHeaders === undefined) {
      messages[index] = {
        payload,
        topic: messageTopic,
        partition,
        offset,
      }
    } else if (messageHeaders === undefined) {
      messages[index] = {
        payload,
        key,
        topic: messageTopic,
        partition,
        offset,
      }
    } else if (key === undefined) {
      messages[index] = {
        payload,
        headers: messageHeaders,
        topic: messageTopic,
        partition,
        offset,
      }
    } else {
      messages[index] = {
        payload,
        key,
        headers: messageHeaders,
        topic: messageTopic,
        partition,
        offset,
      }
    }
  }

  return messages
}

function expandCompactBatchSharedTopicWithSharedKeyAndSharedHeaderValue(
  payloads: Buffer[],
  sharedKey: Buffer,
  topic: string,
  partitions: number[],
  offsets: number[],
  sharedHeaderKey: string,
  sharedHeaderValue: Buffer,
): Message[] {
  const messages = new Array<Message>(payloads.length)

  for (let index = 0; index < payloads.length; index += 1) {
    messages[index] = {
      payload: payloads[index]!,
      key: sharedKey,
      headers: {
        [sharedHeaderKey]: sharedHeaderValue,
      },
      topic,
      partition: partitions[index]!,
      offset: offsets[index]!,
    }
  }

  return messages
}

function expandCompactBatchSharedTopicWithKeyDictionaryAndSharedHeaderValue(
  payloads: Buffer[],
  keyDictionary: Buffer[],
  keyDictionaryIndexes: number[],
  topic: string,
  partitions: number[],
  offsets: number[],
  sharedHeaderKey: string,
  sharedHeaderValue: Buffer,
): Message[] {
  const messages = new Array<Message>(payloads.length)

  for (let index = 0; index < payloads.length; index += 1) {
    messages[index] = {
      payload: payloads[index]!,
      key: keyDictionary[keyDictionaryIndexes[index]!]!,
      headers: {
        [sharedHeaderKey]: sharedHeaderValue,
      },
      topic,
      partition: partitions[index]!,
      offset: offsets[index]!,
    }
  }

  return messages
}

function expandCompactBatchSharedTopicWithDenseKeysAndSharedHeaders(
  payloads: Buffer[],
  denseKeys: Buffer[],
  topic: string,
  partitions: number[],
  offsets: number[],
  sharedHeaderKey: string,
  denseSharedHeaderValues: Buffer[],
): Message[] {
  const messages = new Array<Message>(payloads.length)

  for (let index = 0; index < payloads.length; index += 1) {
    messages[index] = {
      payload: payloads[index]!,
      key: denseKeys[index]!,
      headers: {
        [sharedHeaderKey]: denseSharedHeaderValues[index]!,
      },
      topic,
      partition: partitions[index]!,
      offset: offsets[index]!,
    }
  }

  return messages
}

function expandCompactBatchSharedTopicWithDenseKeys(
  payloads: Buffer[],
  denseKeys: Buffer[],
  topic: string,
  partitions: number[],
  offsets: number[],
): Message[] {
  const messages = new Array<Message>(payloads.length)

  for (let index = 0; index < payloads.length; index += 1) {
    messages[index] = {
      payload: payloads[index]!,
      key: denseKeys[index]!,
      topic,
      partition: partitions[index]!,
      offset: offsets[index]!,
    }
  }

  return messages
}

function expandCompactBatchSharedTopicWithSharedHeaders(
  payloads: Buffer[],
  topic: string,
  partitions: number[],
  offsets: number[],
  sharedHeaderKey: string,
  denseSharedHeaderValues: Buffer[],
): Message[] {
  const messages = new Array<Message>(payloads.length)

  for (let index = 0; index < payloads.length; index += 1) {
    messages[index] = {
      payload: payloads[index]!,
      headers: {
        [sharedHeaderKey]: denseSharedHeaderValues[index]!,
      },
      topic,
      partition: partitions[index]!,
      offset: offsets[index]!,
    }
  }

  return messages
}

function expandCompactBatchSharedTopic(
  payloads: Buffer[],
  topic: string,
  partitions: number[],
  offsets: number[],
): Message[] {
  const messages = new Array<Message>(payloads.length)

  for (let index = 0; index < payloads.length; index += 1) {
    messages[index] = {
      payload: payloads[index]!,
      topic,
      partition: partitions[index]!,
      offset: offsets[index]!,
    }
  }

  return messages
}

function expandCompactBatchStream(compactStream: ReadableStream<CompactMessageBatch>): ReadableStream<Message[]> {
  const reader = compactStream.getReader()
  let closed = false

  return new ReadableStream<Message[]>({
    async pull(controller) {
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

      controller.enqueue(expandCompactBatch(value))
    },
    async cancel(reason) {
      closed = true
      await reader.cancel(reason)
    },
  })
}

function createMetadataBatchStream(
  consumer: KafkaConsumerWithWebStream,
  batchSize: number,
  batchTimeout: number,
): ReadableStream<Message[]> {
  return expandCompactBatchStream(consumer.recvBatchStreamCompact(batchSize, batchTimeout))
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
    const firstBroker = this.kafkaConfiguration.brokers.split(',')[0]?.trim()
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
    const webStreamConsumer = instrumentedConsumer as KafkaConsumerWithWebStream
    const opts = streamOptions ?? { objectMode: true }

    if (batchSize && batchSize > 1) {
      const resolvedBatchTimeout = batchTimeout ?? DEFAULT_WEB_STREAM_BATCH_TIMEOUT
      const batchStream = createMetadataBatchStream(webStreamConsumer, batchSize, resolvedBatchTimeout)
      const instrumentedBatchStream = this._diagnosticsEnabled
        ? instrumentBatchReadableStream(
            batchStream,
            consumerConfiguration.groupId,
            batchSize,
            resolvedBatchTimeout,
            this._diagnosticsConfig,
          )
        : batchStream

      return new KafkaBatchStreamReadable({
        kafkaConsumer: instrumentedConsumer,
        batchSize,
        batchTimeout: resolvedBatchTimeout,
        sourceStream: instrumentedBatchStream,
        ...opts,
      })
    }

    const serialBatchStream = createMetadataBatchStream(
      webStreamConsumer,
      LEGACY_STREAM_SERIAL_COLLECTOR_SIZE,
      LEGACY_STREAM_SERIAL_COLLECTOR_TIMEOUT,
    )
    const serialStream = flattenBatchStream(serialBatchStream)
    const instrumentedSerialStream = this._diagnosticsEnabled
      ? instrumentConsumerReadableStream(serialStream, consumerConfiguration.groupId, this._diagnosticsConfig)
      : serialStream

    return new KafkaStreamReadable({
      kafkaConsumer: instrumentedConsumer,
      sourceStream: instrumentedSerialStream,
      ...opts,
    })
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
  createWebStreamConsumer(streamConfiguration: DynamicBatchWebStreamConsumerConfiguration): WebStreamConsumer
  createWebStreamConsumer(streamConfiguration: WebStreamConsumerConfiguration): WebStreamConsumer {
    const { batchSize, batchTimeout, serialPrefetchSize, serialPrefetchTimeout, ...consumerConfiguration } =
      streamConfiguration

    const kafkaConsumer = this.kafkaClientConfig.createConsumer(consumerConfiguration)
    const instrumentedConsumer = this._diagnosticsEnabled
      ? this._instrumentConsumer(kafkaConsumer, consumerConfiguration.groupId)
      : kafkaConsumer
    const webStreamConsumer = instrumentedConsumer as KafkaConsumerWithWebStream

    if (batchSize && batchSize > 1) {
      const resolvedBatchTimeout = batchTimeout ?? DEFAULT_WEB_STREAM_BATCH_TIMEOUT
      const stream = createMetadataBatchStream(webStreamConsumer, batchSize, resolvedBatchTimeout)
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

    const resolvedPrefetchSize =
      serialPrefetchSize && serialPrefetchSize > 1 ? serialPrefetchSize : DEFAULT_WEB_STREAM_SERIAL_PREFETCH_SIZE
    const resolvedPrefetchTimeout = serialPrefetchTimeout ?? DEFAULT_WEB_STREAM_SERIAL_PREFETCH_TIMEOUT
    const serialStream = flattenBatchStream(
      createMetadataBatchStream(webStreamConsumer, resolvedPrefetchSize, resolvedPrefetchTimeout),
    )
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
