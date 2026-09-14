import type { RecordMetadata } from '../js-binding.js'

export interface SendFailureError extends Error {
  enqueuedCount?: number
  totalCount?: number
  confirmedCount?: number
  confirmedMessages?: RecordMetadata[]
}

/** Keep in sync with `SEND_FAILURE_PAYLOAD_MARKER` in kafka_producer.rs. */
export const SEND_FAILURE_PAYLOAD_MARKER = '\n--kafka-crab-send-failure--\n'

type SendFailurePayload = {
  enqueuedCount?: number
  totalCount?: number
  confirmedCount?: number
  confirmedMessages?: RecordMetadata[]
}

/**
 * Copies per-send failure metadata off the native error payload.
 * Does not read `getLastDeliveryResults()`: that slot is shared and racy.
 */
export function attachSendFailureDetails(error: unknown): void {
  if (!error || typeof error !== 'object') {
    return
  }

  const sendError = error as SendFailureError
  if (typeof sendError.message !== 'string') {
    return
  }

  const markerIndex = sendError.message.indexOf(SEND_FAILURE_PAYLOAD_MARKER)
  if (markerIndex === -1) {
    return
  }

  const human = sendError.message.slice(0, markerIndex)
  const rawPayload = sendError.message.slice(markerIndex + SEND_FAILURE_PAYLOAD_MARKER.length)

  try {
    const payload = JSON.parse(rawPayload) as SendFailurePayload
    sendError.enqueuedCount = payload.enqueuedCount
    sendError.totalCount = payload.totalCount
    sendError.confirmedCount = payload.confirmedCount
    sendError.confirmedMessages = Array.isArray(payload.confirmedMessages) ? payload.confirmedMessages : []
    sendError.message = human
  } catch {
    // Leave the raw message. Never fall back to the shared delivery slot.
  }
}
