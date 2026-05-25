import { type Context, context, trace } from '@opentelemetry/api'
import type { Message } from 'kafka-crab-js'
import { peekKafkaInstrumentation } from './instrumentation.js'
import type { InstrumentedMessage, InstrumentedMessageBatch } from './types.js'
import { extractTraceContext } from './utils.js'

export type EndSpanTarget = Message | Message[] | InstrumentedMessage | InstrumentedMessageBatch | null | undefined

export type MessageContextTarget = Message | InstrumentedMessage | null | undefined
export type BatchContextTarget = Message[] | InstrumentedMessageBatch | null | undefined

function getMessageHeaders(
  message: Message | InstrumentedMessage,
): Record<string, Buffer | string | string[] | undefined> {
  return (message.headers ?? {}) as Record<string, Buffer | string | string[] | undefined>
}

/**
 * Resolve the OpenTelemetry context for a message.
 *
 * Resolution order:
 * 1. pre-decorated `message.otelContext` (from OTEL adapter)
 * 2. `message.span` converted to a context
 * 3. extracted context from message headers
 */
export function getMessageContext(message: MessageContextTarget): Context {
  if (!message) {
    return context.active()
  }

  if ((message as InstrumentedMessage).otelContext) {
    return (message as InstrumentedMessage).otelContext as Context
  }

  const messageSpan = (message as InstrumentedMessage).span
  if (messageSpan) {
    return trace.setSpan(context.active(), messageSpan)
  }

  return extractTraceContext(getMessageHeaders(message))
}

/**
 * Run a callback under the resolved message context.
 */
export function withMessageContext<TResult>(message: MessageContextTarget, fn: () => TResult): TResult {
  return context.with(getMessageContext(message), fn)
}

/**
 * Resolve the OpenTelemetry context for a message batch.
 *
 * Resolution order:
 * 1. pre-decorated `batch.otelContext` (from OTEL adapter)
 * 2. `batch.span` converted to a context
 * 3. first message context
 * 4. active context
 */
export function getBatchContext(batch: BatchContextTarget): Context {
  if (!batch || batch.length === 0) {
    return context.active()
  }

  if ((batch as InstrumentedMessageBatch).otelContext) {
    const batchOtelContext = (batch as InstrumentedMessageBatch).otelContext
    if (batchOtelContext) {
      return batchOtelContext
    }
  }

  const batchSpan = (batch as InstrumentedMessageBatch).span
  if (batchSpan) {
    return trace.setSpan(context.active(), batchSpan)
  }

  return getMessageContext(batch[0])
}

/**
 * Run a callback under the resolved batch context.
 */
export function withBatchContext<TResult>(batch: BatchContextTarget, fn: () => TResult): TResult {
  return context.with(getBatchContext(batch), fn)
}

/**
 * Convenience helper to end consumer processing spans without optional chaining.
 *
 * Works with:
 * - Decorated messages/batches (`message.endSpan()` / `batch.endSpan()`)
 * - Wrapper mode (`otel.decorateMessages=false`) where recv()/recvBatch() return shallow clones with the same helpers
 */
export function endSpan(target: EndSpanTarget, error?: Error): void {
  if (!target) {
    return
  }

  const existingEndSpan = (target as unknown as { endSpan?: (error?: Error) => void }).endSpan
  if (typeof existingEndSpan === 'function') {
    existingEndSpan(error)
    return
  }

  const instrumentation = peekKafkaInstrumentation()
  if (!instrumentation) {
    return
  }

  const otel = instrumentation.createOtelContext()
  if (Array.isArray(target)) {
    otel.endBatchSpan(target, error)
  } else {
    otel.endMessageSpan(target, error)
  }
}

export const EndSpan = endSpan
