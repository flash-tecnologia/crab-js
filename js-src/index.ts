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

export type { KafkaClientConfiguration, StreamConsumerConfiguration } from './kafka-client.js'

// OpenTelemetry exports
export type {
  BatchOtelContext,
  InstrumentedMessage,
  InstrumentedMessageBatch,
  InstrumentedProducerRecord,
  KafkaOtelContext,
  KafkaOtelInstrumentationConfig,
  MessageHookFn,
  ProducerHookFn,
  TopicFilterFn,
} from './otel/types.js'

export {
  getKafkaInstrumentation,
  peekKafkaInstrumentation,
  KafkaCrabInstrumentation,
  resetKafkaInstrumentation,
} from './otel/instrumentation.js'

export { EndSpan, endSpan } from './otel/helpers.js'

export { KAFKA_OPERATION_TYPES, KAFKA_SEMANTIC_CONVENTIONS, PACKAGE_INFO } from './otel/constants.js'

// Diagnostic Channels exports
export {
  // Channel names and types
  CHANNEL_NAMES,
  type ChannelName,
  type TypedChannel,
  type BaseEvent,
  type ProducerSendStartEvent,
  type ProducerSendEndEvent,
  type ProducerSendErrorEvent,
  type ConsumerReceiveStartEvent,
  type ConsumerReceiveEndEvent,
  type ConsumerProcessStartEvent,
  type ConsumerProcessEndEvent,
  type BatchReceiveStartEvent,
  type BatchReceiveEndEvent,
  type BatchProcessStartEvent,
  type BatchProcessEndEvent,
  // Channel instances
  channels,
  producerSendStartChannel,
  producerSendEndChannel,
  producerSendErrorChannel,
  consumerReceiveStartChannel,
  consumerReceiveEndChannel,
  consumerProcessStartChannel,
  consumerProcessEndChannel,
  batchReceiveStartChannel,
  batchReceiveEndChannel,
  batchProcessStartChannel,
  batchProcessEndChannel,
  // OTEL Adapter
  OtelAdapter,
  type OtelAdapterConfig,
  getOtelAdapter,
  resetOtelAdapter,
  enableOtelInstrumentation,
  // Diagnostic instrumentation
  type DiagnosticInstrumentationConfig,
  instrumentProducerSend,
  instrumentConsumerReceive,
  instrumentBatchReceive,
} from './diagnostics/index.js'
