import { type Channel, channel } from 'node:diagnostics_channel'
import type {
  BatchProcessEndEvent,
  BatchProcessStartEvent,
  BatchReceiveEndEvent,
  BatchReceiveStartEvent,
  ConsumerProcessEndEvent,
  ConsumerProcessStartEvent,
  ConsumerReceiveEndEvent,
  ConsumerReceiveStartEvent,
  ProducerSendEndEvent,
  ProducerSendStartEvent,
  TypedChannel,
} from 'kafka-crab-js'

const CHANNEL_NAMES = {
  BATCH_PROCESS_END: 'kafka-crab:batch:process:end',
  BATCH_PROCESS_START: 'kafka-crab:batch:process:start',
  BATCH_RECEIVE_END: 'kafka-crab:batch:receive:end',
  BATCH_RECEIVE_START: 'kafka-crab:batch:receive:start',
  CONSUMER_PROCESS_END: 'kafka-crab:consumer:process:end',
  CONSUMER_PROCESS_START: 'kafka-crab:consumer:process:start',
  CONSUMER_RECEIVE_END: 'kafka-crab:consumer:receive:end',
  CONSUMER_RECEIVE_START: 'kafka-crab:consumer:receive:start',
  PRODUCER_SEND_END: 'kafka-crab:producer:send:end',
  PRODUCER_SEND_START: 'kafka-crab:producer:send:start',
} as const

function createTypedChannel<TEvent>(name: string): TypedChannel<TEvent> {
  const diagnosticChannel: Channel = channel(name)

  return {
    name,
    channel: diagnosticChannel,
    get hasSubscribers() {
      return diagnosticChannel.hasSubscribers
    },
    publish(event: TEvent) {
      diagnosticChannel.publish(event)
    },
    subscribe(handler: (event: TEvent, name: string | symbol) => void) {
      diagnosticChannel.subscribe(handler as (message: unknown, name: string | symbol) => void)
    },
    unsubscribe(handler: (event: TEvent, name: string | symbol) => void) {
      diagnosticChannel.unsubscribe(handler as (message: unknown, name: string | symbol) => void)
    },
  }
}

export const batchProcessEndChannel = createTypedChannel<BatchProcessEndEvent>(CHANNEL_NAMES.BATCH_PROCESS_END)
export const batchProcessStartChannel = createTypedChannel<BatchProcessStartEvent>(CHANNEL_NAMES.BATCH_PROCESS_START)
export const batchReceiveEndChannel = createTypedChannel<BatchReceiveEndEvent>(CHANNEL_NAMES.BATCH_RECEIVE_END)
export const batchReceiveStartChannel = createTypedChannel<BatchReceiveStartEvent>(CHANNEL_NAMES.BATCH_RECEIVE_START)
export const consumerProcessEndChannel = createTypedChannel<ConsumerProcessEndEvent>(CHANNEL_NAMES.CONSUMER_PROCESS_END)
export const consumerProcessStartChannel = createTypedChannel<ConsumerProcessStartEvent>(
  CHANNEL_NAMES.CONSUMER_PROCESS_START,
)
export const consumerReceiveEndChannel = createTypedChannel<ConsumerReceiveEndEvent>(CHANNEL_NAMES.CONSUMER_RECEIVE_END)
export const consumerReceiveStartChannel = createTypedChannel<ConsumerReceiveStartEvent>(
  CHANNEL_NAMES.CONSUMER_RECEIVE_START,
)
export const producerSendEndChannel = createTypedChannel<ProducerSendEndEvent>(CHANNEL_NAMES.PRODUCER_SEND_END)
export const producerSendStartChannel = createTypedChannel<ProducerSendStartEvent>(CHANNEL_NAMES.PRODUCER_SEND_START)
