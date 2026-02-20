export type {
  ConsumerConfiguration,
  KafkaConfiguration,
  KafkaCrabError,
  KafkaEvent,
  KafkaEventPayload,
  Message,
  MessageProducer,
  OffsetModel,
  PartitionOffset,
  ProducerConfiguration,
  ProducerRecord,
  RecordMetadata,
  TopicPartition,
  TopicPartitionConfig,
} from '../js-binding.js'

export {
  CommitMode,
  KafkaClientConfig,
  KafkaConsumer,
  KafkaEventName,
  KafkaProducer,
  PartitionPosition,
  SecurityProtocol,
} from '../js-binding.js'
export { KafkaClient } from './kafka-client.js'
export { BaseKafkaStreamReadable } from './streams/base-kafka-stream-readable.js'
export { KafkaBatchStreamReadable } from './streams/kafka-batch-stream-readable.js'
export { KafkaStreamReadable } from './streams/kafka-stream-readable.js'

export type {
  KafkaClientConfiguration,
  StreamConsumerConfiguration,
  WebStreamConsumer,
  WebStreamConsumerConfiguration,
} from './kafka-client.js'

// Diagnostic Channels exports
export {
  type BaseEvent,
  batchProcessEndChannel,
  type BatchProcessEndEvent,
  batchProcessStartChannel,
  type BatchProcessStartEvent,
  batchReceiveEndChannel,
  type BatchReceiveEndEvent,
  batchReceiveStartChannel,
  type BatchReceiveStartEvent,
  // Channel names and types
  CHANNEL_NAMES,
  type ChannelName,
  // Channel instances
  channels,
  consumerProcessEndChannel,
  type ConsumerProcessEndEvent,
  consumerProcessStartChannel,
  type ConsumerProcessStartEvent,
  consumerReceiveEndChannel,
  type ConsumerReceiveEndEvent,
  consumerReceiveStartChannel,
  type ConsumerReceiveStartEvent,
  // Diagnostic instrumentation
  type DiagnosticInstrumentationConfig,
  instrumentBatchReceive,
  instrumentBatchReadableStream,
  instrumentConsumerReceive,
  instrumentConsumerReadableStream,
  instrumentProducerSend,
  producerSendEndChannel,
  type ProducerSendEndEvent,
  producerSendErrorChannel,
  type ProducerSendErrorEvent,
  producerSendStartChannel,
  type ProducerSendStartEvent,
  type TypedChannel,
} from './diagnostics/index.js'
