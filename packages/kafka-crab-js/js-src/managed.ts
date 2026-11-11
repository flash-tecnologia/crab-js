/**
 * Managed consumption (RFC-0017, experimental): structured error contract of
 * `recvManaged` and `commitManaged`. The native binding sets these properties
 * on the rejection itself; nothing here parses error messages.
 */

export const MANAGED_ERROR_CODES = {
  /** The operation does not belong to this consumer's mode (managed vs legacy). */
  MODE: 'ERR_MANAGED_MODE',
  /** Another `recvManaged` is already pending on this consumer. */
  RECEIVE_PENDING: 'ERR_MANAGED_RECEIVE_PENDING',
  /** The consumer is disconnecting or disconnected. */
  CLOSED: 'ERR_MANAGED_CLOSED',
  /** Terminal: the control budget overflowed and commit authorization was invalidated. */
  CONTROL_OVERFLOW: 'ERR_MANAGED_CONTROL_OVERFLOW',
  /** Terminal: native control state became incoherent or the client failed fatally. */
  CONTROL_FAILURE: 'ERR_MANAGED_CONTROL_FAILURE',
  /** A consumption error reported by librdkafka. Not terminal; receive again. */
  RECEIVE_FAILED: 'ERR_MANAGED_RECEIVE_FAILED',
  /** The assignment identity was revoked. Never resubmit under a new identity. */
  STALE_ASSIGNMENT: 'ERR_MANAGED_STALE_ASSIGNMENT',
  /** The identity is malformed or was not issued by this consumer. */
  UNKNOWN_ASSIGNMENT: 'ERR_MANAGED_UNKNOWN_ASSIGNMENT',
  INVALID_ARGUMENT: 'ERR_MANAGED_INVALID_ARGUMENT',
  /** The commit failed with a Kafka error (`kafkaCode`). */
  COMMIT_FAILED: 'ERR_MANAGED_COMMIT_FAILED',
  /** The commit was dispatched but no broker result was observed. */
  COMMIT_UNKNOWN: 'ERR_MANAGED_COMMIT_UNKNOWN',
} as const

export type ManagedErrorCode = (typeof MANAGED_ERROR_CODES)[keyof typeof MANAGED_ERROR_CODES]

export interface ManagedConsumptionError extends Error {
  code: ManagedErrorCode
  /** Name of the librdkafka error, e.g. `IllegalGeneration`, when a Kafka error exists. */
  kafkaCode?: string
  /** Numeric librdkafka error code matching `kafkaCode`. */
  kafkaErrorCode?: number
  /**
   * Commit errors only: `true` when the request may still have been applied by
   * the broker (lost response, timeout, close). It is not proof of failure.
   */
  outcomeUnknown?: boolean
}

const MANAGED_ERROR_CODE_SET: ReadonlySet<string> = new Set(Object.values(MANAGED_ERROR_CODES))

export function isManagedConsumptionError(error: unknown): error is ManagedConsumptionError {
  return error instanceof Error && MANAGED_ERROR_CODE_SET.has((error as { code?: string }).code ?? '')
}
