/**
 * Diagnostics module entry point
 *
 * Re-exports all channels and types for external consumption.
 */
export {
  // Channel names
  CHANNEL_NAMES,
  type ChannelName,

  // Event types
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
  type KafkaEvent,

  // Typed channel interface
  type TypedChannel,

  // Channel instances
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
  channels,
} from './channels.js'

export {
  OtelAdapter,
  type OtelAdapterConfig,
  getOtelAdapter,
  resetOtelAdapter,
  enableOtelInstrumentation,
} from './otel-adapter.js'

export {
  type DiagnosticInstrumentationConfig,
  instrumentProducerSend,
  instrumentConsumerReceive,
  instrumentBatchReceive,
} from './instrumentation.js'
