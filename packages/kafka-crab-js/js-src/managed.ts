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
  /** Terminal: retained messages could not be kept within their bound. */
  BACKPRESSURE_FAILURE: 'ERR_MANAGED_BACKPRESSURE_FAILURE',
  /** Terminal: no `recvManaged` was pending for longer than `maxReceiveIdleMs`. */
  RECEIVE_STALLED: 'ERR_MANAGED_RECEIVE_STALLED',
  /** A consumption error reported by librdkafka. Not terminal; receive again. */
  RECEIVE_FAILED: 'ERR_MANAGED_RECEIVE_FAILED',
  /** The assignment identity was revoked. Never resubmit under a new identity. */
  STALE_ASSIGNMENT: 'ERR_MANAGED_STALE_ASSIGNMENT',
  /** The identity is malformed or was not issued by this consumer. */
  UNKNOWN_ASSIGNMENT: 'ERR_MANAGED_UNKNOWN_ASSIGNMENT',
  INVALID_ARGUMENT: 'ERR_MANAGED_INVALID_ARGUMENT',
  /** `nextOffset` is lower than one already authorized under the identity. Nothing was sent. */
  COMMIT_REGRESSION: 'ERR_MANAGED_COMMIT_REGRESSION',
  /** More commits are pending than `maxPendingCommits`. Nothing was sent. */
  COMMIT_BACKLOG: 'ERR_MANAGED_COMMIT_BACKLOG',
  /** The commit's `timeoutMs` elapsed. The request is not cancelled. */
  COMMIT_TIMEOUT: 'ERR_MANAGED_COMMIT_TIMEOUT',
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
   * Commit errors only. `false` only when nothing was handed to the Kafka
   * client (stale or unknown identity, regression, backlog, local dispatch
   * failure). Every error of a request that was sent is `true`: the client can
   * retry or defer a request after an attempt whose fate is unknown, so the
   * final error code does not prove that no attempt was applied.
   */
  outcomeUnknown?: boolean
}

const MANAGED_ERROR_CODE_SET: ReadonlySet<string> = new Set(Object.values(MANAGED_ERROR_CODES))

export function isManagedConsumptionError(error: unknown): error is ManagedConsumptionError {
  return error instanceof Error && MANAGED_ERROR_CODE_SET.has((error as { code?: string }).code ?? '')
}
