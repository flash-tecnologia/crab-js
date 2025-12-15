/**
 * Diagnostic Channel-based Kafka Instrumentation
 *
 * This module provides instrumentation that publishes events to Node.js
 * diagnostic channels instead of directly calling OpenTelemetry APIs.
 * Observability tools can subscribe to these channels to receive events.
 */
import type { KafkaConsumer, KafkaProducer, Message, ProducerRecord, RecordMetadata } from '../../js-binding.js'
import {
  batchProcessEndChannel,
  type BatchProcessEndEvent,
  batchProcessStartChannel,
  type BatchProcessStartEvent,
  batchReceiveEndChannel,
  type BatchReceiveEndEvent,
  batchReceiveStartChannel,
  type BatchReceiveStartEvent,
  consumerProcessEndChannel,
  type ConsumerProcessEndEvent,
  consumerProcessStartChannel,
  type ConsumerProcessStartEvent,
  consumerReceiveEndChannel,
  type ConsumerReceiveEndEvent,
  consumerReceiveStartChannel,
  type ConsumerReceiveStartEvent,
  producerSendEndChannel,
  type ProducerSendEndEvent,
  producerSendErrorChannel,
  type ProducerSendErrorEvent,
  producerSendStartChannel,
  type ProducerSendStartEvent,
} from './channels.js'

/**
 * Configuration for diagnostic channel instrumentation
 */
export interface DiagnosticInstrumentationConfig {
  /** Client ID for attribution */
  clientId?: string
  /** Server address for attribution */
  serverAddress?: string
  /** Server port for attribution */
  serverPort?: number
}

/**
 * Instruments a producer send method to publish diagnostic events
 */
export function instrumentProducerSend(
  originalSend: (record: ProducerRecord) => Promise<RecordMetadata[]>,
  config: DiagnosticInstrumentationConfig = {},
): (this: KafkaProducer, record: ProducerRecord) => Promise<RecordMetadata[]> {
  const { clientId, serverAddress, serverPort } = config

  return async function instrumentedSend(this: KafkaProducer, record: ProducerRecord) {
    // Fast path: if no subscribers, just call original
    if (!producerSendStartChannel.hasSubscribers && !producerSendEndChannel.hasSubscribers) {
      return originalSend.call(this, record)
    }

    const timestamp = Date.now()
    const context: Record<PropertyKey, unknown> = {}

    const startEvent: ProducerSendStartEvent = {
      timestamp,
      topic: record.topic,
      record,
      messageCount: record.messages?.length ?? 0,
      clientId,
      serverAddress,
      serverPort,
      context,
    }

    // Publish start event - subscribers can inject trace headers here
    if (producerSendStartChannel.hasSubscribers) {
      producerSendStartChannel.publish(startEvent)
    }

    let sendError: Error | undefined
    let metadata: RecordMetadata[] | undefined

    try {
      metadata = await originalSend.call(this, record)
      return metadata
    } catch (error) {
      sendError = error instanceof Error ? error : new Error(String(error))
      throw error
    } finally {
      const durationMs = Date.now() - timestamp

      if (sendError && producerSendErrorChannel.hasSubscribers) {
        const errorEvent: ProducerSendErrorEvent = {
          timestamp: Date.now(),
          topic: record.topic,
          record,
          durationMs,
          error: sendError,
          clientId,
          serverAddress,
          serverPort,
          context,
        }
        producerSendErrorChannel.publish(errorEvent)
      }

      // Publish end event
      if (producerSendEndChannel.hasSubscribers) {
        const endEvent: ProducerSendEndEvent = {
          timestamp: Date.now(),
          topic: record.topic,
          record,
          metadata,
          durationMs,
          error: sendError,
          clientId,
          serverAddress,
          serverPort,
          context,
        }
        producerSendEndChannel.publish(endEvent)
      }
    }
  }
}

/**
 * Instruments a consumer receive method to publish diagnostic events
 */
export function instrumentConsumerReceive(
  originalReceive: () => Promise<Message | null>,
  groupId?: string,
  config: DiagnosticInstrumentationConfig = {},
): (this: KafkaConsumer) => Promise<Message | null> {
  const { clientId, serverAddress, serverPort } = config

  return async function instrumentedReceive(this: KafkaConsumer) {
    // Fast path: if no subscribers at all, just call original
    const hasReceiveSubscribers = consumerReceiveStartChannel.hasSubscribers || consumerReceiveEndChannel.hasSubscribers
    const hasProcessSubscribers = consumerProcessStartChannel.hasSubscribers || consumerProcessEndChannel.hasSubscribers

    if (!hasReceiveSubscribers && !hasProcessSubscribers) {
      return originalReceive.call(this)
    }

    const timestamp = Date.now()
    const context: Record<PropertyKey, unknown> = {}

    // Publish receive start
    if (consumerReceiveStartChannel.hasSubscribers) {
      const startEvent: ConsumerReceiveStartEvent = {
        timestamp,
        groupId,
        clientId,
        serverAddress,
        serverPort,
        context,
      }
      consumerReceiveStartChannel.publish(startEvent)
    }

    let receiveError: Error | undefined
    let message: Message | null = null

    try {
      message = await originalReceive.call(this)
      return message
    } catch (error) {
      receiveError = error instanceof Error ? error : new Error(String(error))
      throw error
    } finally {
      // Publish receive end
      if (consumerReceiveEndChannel.hasSubscribers) {
        const endEvent: ConsumerReceiveEndEvent = {
          timestamp: Date.now(),
          message,
          groupId,
          durationMs: Date.now() - timestamp,
          error: receiveError,
          clientId,
          serverAddress,
          serverPort,
          context,
        }
        consumerReceiveEndChannel.publish(endEvent)
      }

      // If message exists and process channels have subscribers, set up process tracking
      if (message && hasProcessSubscribers) {
        const processingMessage = message
        const processContext: Record<PropertyKey, unknown> = {}
        const processStartTime = Date.now()

        // Publish process start
        if (consumerProcessStartChannel.hasSubscribers) {
          const processStartEvent: ConsumerProcessStartEvent = {
            timestamp: processStartTime,
            message,
            groupId,
            clientId,
            serverAddress,
            serverPort,
            context: processContext,
          }
          consumerProcessStartChannel.publish(processStartEvent)
        }

        // Attach endSpan helper to message for user to call when processing completes
        let ended = false
        const endProcessing = (processError?: Error) => {
          if (ended) {
            return
          }
          ended = true

          if (consumerProcessEndChannel.hasSubscribers) {
            const processEndEvent: ConsumerProcessEndEvent = {
              timestamp: Date.now(),
              message: processingMessage,
              groupId,
              durationMs: Date.now() - processStartTime,
              error: processError,
              clientId,
              serverAddress,
              serverPort,
              context: processContext,
            }
            consumerProcessEndChannel.publish(processEndEvent)
          }
        }

        // Add endSpan to message
        Object.defineProperty(message, 'endSpan', {
          value: endProcessing,
          writable: false,
          configurable: true,
          enumerable: false,
        })
      }
    }
  }
}

/**
 * Instruments a consumer batch receive method to publish diagnostic events
 */
export function instrumentBatchReceive(
  originalBatchReceive: (size: number, timeoutMs: number) => Promise<Message[]>,
  groupId?: string,
  config: DiagnosticInstrumentationConfig = {},
): (this: KafkaConsumer, size: number, timeoutMs: number) => Promise<Message[]> {
  const { clientId, serverAddress, serverPort } = config

  return async function instrumentedBatchReceive(
    this: KafkaConsumer,
    size: number,
    timeoutMs: number,
  ) {
    // Fast path: if no subscribers, just call original
    const hasReceiveSubscribers = batchReceiveStartChannel.hasSubscribers || batchReceiveEndChannel.hasSubscribers
    const hasProcessSubscribers = batchProcessStartChannel.hasSubscribers || batchProcessEndChannel.hasSubscribers

    if (!hasReceiveSubscribers && !hasProcessSubscribers) {
      return originalBatchReceive.call(this, size, timeoutMs)
    }

    const timestamp = Date.now()
    const context: Record<PropertyKey, unknown> = {}

    // Publish batch receive start
    if (batchReceiveStartChannel.hasSubscribers) {
      const startEvent: BatchReceiveStartEvent = {
        timestamp,
        groupId,
        requestedSize: size,
        timeoutMs,
        clientId,
        serverAddress,
        serverPort,
        context,
      }
      batchReceiveStartChannel.publish(startEvent)
    }

    let batchError: Error | undefined
    let messages: Message[] = []

    try {
      messages = await originalBatchReceive.call(this, size, timeoutMs)
      return messages
    } catch (error) {
      batchError = error instanceof Error ? error : new Error(String(error))
      throw error
    } finally {
      // Publish batch receive end
      if (batchReceiveEndChannel.hasSubscribers) {
        const endEvent: BatchReceiveEndEvent = {
          timestamp: Date.now(),
          messages,
          groupId,
          durationMs: Date.now() - timestamp,
          error: batchError,
          clientId,
          serverAddress,
          serverPort,
          context,
        }
        batchReceiveEndChannel.publish(endEvent)
      }

      // If messages exist and process channels have subscribers, set up batch process tracking
      if (messages.length > 0 && hasProcessSubscribers) {
        const processContext: Record<PropertyKey, unknown> = {}
        const processStartTime = Date.now()

        // Publish batch process start
        if (batchProcessStartChannel.hasSubscribers) {
          const processStartEvent: BatchProcessStartEvent = {
            timestamp: processStartTime,
            messages,
            groupId,
            clientId,
            serverAddress,
            serverPort,
            context: processContext,
          }
          batchProcessStartChannel.publish(processStartEvent)
        }

        // Attach endSpan helper to batch array
        let ended = false
        const endBatchProcessing = (processError?: Error) => {
          if (ended) {
            return
          }
          ended = true

          if (batchProcessEndChannel.hasSubscribers) {
            const processEndEvent: BatchProcessEndEvent = {
              timestamp: Date.now(),
              messages,
              groupId,
              durationMs: Date.now() - processStartTime,
              error: processError,
              clientId,
              serverAddress,
              serverPort,
              context: processContext,
            }
            batchProcessEndChannel.publish(processEndEvent)
          }
        }

        Object.defineProperty(messages, 'endSpan', {
          value: endBatchProcessing,
          writable: false,
          configurable: true,
          enumerable: false,
        })
      }
    }
  }
}
