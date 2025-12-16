import type { Message } from 'kafka-crab-js'
import { peekKafkaInstrumentation } from './instrumentation.js'
import type { InstrumentedMessage, InstrumentedMessageBatch } from './types.js'

export type EndSpanTarget =
  | Message
  | Message[]
  | InstrumentedMessage
  | InstrumentedMessageBatch
  | null
  | undefined

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
