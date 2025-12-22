/**
 * Kafka Diagnostic Channels
 *
 * This module provides Node.js diagnostic channels for Kafka operations.
 * External observability tools (like OpenTelemetry adapters) can subscribe
 * to these channels to receive events without coupling to any specific
 * tracing/metrics implementation.
 *
 * @see https://nodejs.org/api/diagnostics_channel.html
 */
import { type Channel, channel } from 'node:diagnostics_channel'
import type { Message, ProducerRecord, RecordMetadata } from '../../js-binding.js'

// -----------------------------------------------------------------------------
// Channel Names
// -----------------------------------------------------------------------------

export const CHANNEL_NAMES = {
  // Producer channels
  PRODUCER_SEND_START: 'kafka-crab:producer:send:start',
  PRODUCER_SEND_END: 'kafka-crab:producer:send:end',
  PRODUCER_SEND_ERROR: 'kafka-crab:producer:send:error',

  // Consumer channels
  CONSUMER_RECEIVE_START: 'kafka-crab:consumer:receive:start',
  CONSUMER_RECEIVE_END: 'kafka-crab:consumer:receive:end',
  CONSUMER_PROCESS_START: 'kafka-crab:consumer:process:start',
  CONSUMER_PROCESS_END: 'kafka-crab:consumer:process:end',

  // Batch channels
  BATCH_RECEIVE_START: 'kafka-crab:batch:receive:start',
  BATCH_RECEIVE_END: 'kafka-crab:batch:receive:end',
  BATCH_PROCESS_START: 'kafka-crab:batch:process:start',
  BATCH_PROCESS_END: 'kafka-crab:batch:process:end',
} as const

export type ChannelName = (typeof CHANNEL_NAMES)[keyof typeof CHANNEL_NAMES]

// -----------------------------------------------------------------------------
// Event Payload Types
// -----------------------------------------------------------------------------

/**
 * Base event interface with common fields
 */
export interface BaseEvent {
  /** Timestamp when the event occurred (ms since epoch) */
  timestamp: number
  /** Client ID if available */
  clientId?: string
  /** Server address if configured */
  serverAddress?: string
  /** Server port if configured */
  serverPort?: number
  /**
   * Context object for adapters to attach data (e.g., spans).
   * Subscribers can mutate this to pass data between start/end events.
   */
  context: Record<PropertyKey, unknown>
}

// Producer Events
export interface ProducerSendStartEvent extends BaseEvent {
  topic: string
  record: ProducerRecord
  messageCount: number
}

export interface ProducerSendEndEvent extends BaseEvent {
  topic: string
  record: ProducerRecord
  metadata?: RecordMetadata[]
  durationMs: number
  error?: Error
}

export interface ProducerSendErrorEvent extends BaseEvent {
  topic: string
  record: ProducerRecord
  durationMs: number
  error: Error
}

// Consumer Events
export interface ConsumerReceiveStartEvent extends BaseEvent {
  groupId?: string
}

export interface ConsumerReceiveEndEvent extends BaseEvent {
  message: Message | null
  groupId?: string
  durationMs: number
  error?: Error
}

export interface ConsumerProcessStartEvent extends BaseEvent {
  message: Message
  groupId?: string
}

export interface ConsumerProcessEndEvent extends BaseEvent {
  message: Message
  groupId?: string
  durationMs: number
  error?: Error
}

// Batch Events
export interface BatchReceiveStartEvent extends BaseEvent {
  groupId?: string
  requestedSize: number
  timeoutMs: number
}

export interface BatchReceiveEndEvent extends BaseEvent {
  messages: Message[]
  groupId?: string
  durationMs: number
  error?: Error
}

export interface BatchProcessStartEvent extends BaseEvent {
  messages: Message[]
  groupId?: string
}

export interface BatchProcessEndEvent extends BaseEvent {
  messages: Message[]
  groupId?: string
  durationMs: number
  error?: Error
}

// Union type for all events
export type KafkaEvent =
  | ProducerSendStartEvent
  | ProducerSendEndEvent
  | ConsumerReceiveStartEvent
  | ConsumerReceiveEndEvent
  | ConsumerProcessStartEvent
  | ConsumerProcessEndEvent
  | BatchReceiveStartEvent
  | BatchReceiveEndEvent
  | BatchProcessStartEvent
  | BatchProcessEndEvent

// -----------------------------------------------------------------------------
// Channel Instances (Typed)
// -----------------------------------------------------------------------------

/**
 * Typed channel wrapper for type-safe publish/subscribe
 */
export interface TypedChannel<TEvent> {
  readonly name: string
  readonly channel: Channel
  hasSubscribers: boolean
  publish(event: TEvent): void
  subscribe(handler: (event: TEvent, name: string | symbol) => void): void
  unsubscribe(handler: (event: TEvent, name: string | symbol) => void): void
}

function createTypedChannel<TEvent>(name: string): TypedChannel<TEvent> {
  const ch = channel(name)
  return {
    name,
    channel: ch,
    get hasSubscribers() {
      return ch.hasSubscribers
    },
    publish(event: TEvent) {
      ch.publish(event)
    },
    subscribe(handler: (event: TEvent, name: string | symbol) => void) {
      ch.subscribe(handler as (message: unknown, name: string | symbol) => void)
    },
    unsubscribe(handler: (event: TEvent, name: string | symbol) => void) {
      ch.unsubscribe(handler as (message: unknown, name: string | symbol) => void)
    },
  }
}

// Producer channels
export const producerSendStartChannel = createTypedChannel<ProducerSendStartEvent>(
  CHANNEL_NAMES.PRODUCER_SEND_START,
)
export const producerSendEndChannel = createTypedChannel<ProducerSendEndEvent>(
  CHANNEL_NAMES.PRODUCER_SEND_END,
)
export const producerSendErrorChannel = createTypedChannel<ProducerSendErrorEvent>(
  CHANNEL_NAMES.PRODUCER_SEND_ERROR,
)

// Consumer channels
export const consumerReceiveStartChannel = createTypedChannel<ConsumerReceiveStartEvent>(
  CHANNEL_NAMES.CONSUMER_RECEIVE_START,
)
export const consumerReceiveEndChannel = createTypedChannel<ConsumerReceiveEndEvent>(
  CHANNEL_NAMES.CONSUMER_RECEIVE_END,
)
export const consumerProcessStartChannel = createTypedChannel<ConsumerProcessStartEvent>(
  CHANNEL_NAMES.CONSUMER_PROCESS_START,
)
export const consumerProcessEndChannel = createTypedChannel<ConsumerProcessEndEvent>(
  CHANNEL_NAMES.CONSUMER_PROCESS_END,
)

// Batch channels
export const batchReceiveStartChannel = createTypedChannel<BatchReceiveStartEvent>(
  CHANNEL_NAMES.BATCH_RECEIVE_START,
)
export const batchReceiveEndChannel = createTypedChannel<BatchReceiveEndEvent>(
  CHANNEL_NAMES.BATCH_RECEIVE_END,
)
export const batchProcessStartChannel = createTypedChannel<BatchProcessStartEvent>(
  CHANNEL_NAMES.BATCH_PROCESS_START,
)
export const batchProcessEndChannel = createTypedChannel<BatchProcessEndEvent>(
  CHANNEL_NAMES.BATCH_PROCESS_END,
)

// All channels exported as a single object for convenience
export const channels = {
  producerSendStart: producerSendStartChannel,
  producerSendEnd: producerSendEndChannel,
  producerSendError: producerSendErrorChannel,
  consumerReceiveStart: consumerReceiveStartChannel,
  consumerReceiveEnd: consumerReceiveEndChannel,
  consumerProcessStart: consumerProcessStartChannel,
  consumerProcessEnd: consumerProcessEndChannel,
  batchReceiveStart: batchReceiveStartChannel,
  batchReceiveEnd: batchReceiveEndChannel,
  batchProcessStart: batchProcessStartChannel,
  batchProcessEnd: batchProcessEndChannel,
} as const
