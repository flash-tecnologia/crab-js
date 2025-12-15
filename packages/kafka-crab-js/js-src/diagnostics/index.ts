/**
 * Diagnostics module entry point
 *
 * Re-exports all channels and types for external consumption.
 */
export {
  // Event types
  type BaseEvent,
  batchProcessEndChannel,
  type BatchProcessEndEvent,
  batchProcessStartChannel,
  type BatchProcessStartEvent,
  batchReceiveEndChannel,
  type BatchReceiveEndEvent,
  batchReceiveStartChannel,
  type BatchReceiveStartEvent,
  // Channel names
  CHANNEL_NAMES,
  type ChannelName,
  channels,
  consumerProcessEndChannel,
  type ConsumerProcessEndEvent,
  consumerProcessStartChannel,
  type ConsumerProcessStartEvent,
  consumerReceiveEndChannel,
  type ConsumerReceiveEndEvent,
  consumerReceiveStartChannel,
  type ConsumerReceiveStartEvent,
  type KafkaEvent,
  producerSendEndChannel,
  type ProducerSendEndEvent,
  producerSendErrorChannel,
  type ProducerSendErrorEvent,
  // Channel instances
  producerSendStartChannel,
  type ProducerSendStartEvent,
  // Typed channel interface
  type TypedChannel,
} from './channels.js'

export {
  type DiagnosticInstrumentationConfig,
  instrumentBatchReceive,
  instrumentConsumerReceive,
  instrumentProducerSend,
} from './instrumentation.js'
