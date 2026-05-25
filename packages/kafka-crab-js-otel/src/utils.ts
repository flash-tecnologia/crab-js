import {
  type Attributes,
  type Context,
  context,
  diag,
  propagation,
  ROOT_CONTEXT,
  type Span,
  SpanKind,
  SpanStatusCode,
  trace,
  type Tracer,
} from '@opentelemetry/api'
import type { Message, ProducerRecord } from 'kafka-crab-js'
import {
  KAFKA_DEFAULTS,
  KAFKA_OPERATION_NAMES,
  KAFKA_OPERATION_TYPES,
  KAFKA_SEMANTIC_CONVENTIONS,
} from './constants.js'

// Safely get tracer
export function getTracer(name: string, version?: string) {
  return trace.getTracer(name, version)
}

// Set span status based on error
export function setSpanStatus(span: Span, error?: Error): void {
  if (error) {
    span.setStatus({
      code: SpanStatusCode.ERROR,
      message: error.message,
    })
    span.recordException(error)
  } else {
    span.setStatus({ code: SpanStatusCode.OK })
  }
}

function isDefined<TValue>(value: TValue | null | undefined): value is TValue {
  return value !== undefined && value !== null
}

function setClientAndServerAttributes(
  attributes: Attributes,
  options: {
    clientId?: string
    serverAddress?: string
    serverPort?: number
  },
): void {
  if (options.clientId) {
    attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_CLIENT_ID] = options.clientId
  }

  if (options.serverAddress) {
    attributes[KAFKA_SEMANTIC_CONVENTIONS.SERVER_ADDRESS] = options.serverAddress
  }

  if (isDefined(options.serverPort)) {
    attributes[KAFKA_SEMANTIC_CONVENTIONS.SERVER_PORT] = options.serverPort
  }
}

function getPayloadSize(payload: unknown): number | undefined {
  if (Buffer.isBuffer(payload)) {
    return (payload as Buffer).length
  }

  if (typeof payload === 'string') {
    return Buffer.byteLength(payload, 'utf8')
  }

  if (!isDefined(payload)) {
    return undefined
  }

  try {
    return JSON.stringify(payload).length
  } catch {
    return undefined
  }
}

function setPayloadSizeAttribute(
  attributes: Attributes,
  payload: unknown,
  options: {
    capturePayload?: boolean
    maxPayloadSize?: number
  },
): void {
  if (!options.capturePayload || !isDefined(payload)) {
    return
  }

  const payloadSize = getPayloadSize(payload)
  if (!isDefined(payloadSize)) {
    return
  }

  if (isDefined(options.maxPayloadSize) && payloadSize > options.maxPayloadSize) {
    return
  }

  attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_MESSAGE_BODY_SIZE] = payloadSize
}

// Extract common Kafka attributes from message for consumer spans
// https://opentelemetry.io/docs/specs/semconv/messaging/kafka/
export function getMessageAttributes(
  message: Message,
  operationName: string,
  operationType: string,
  options: {
    clientId?: string
    serverAddress?: string
    serverPort?: number
    capturePayload?: boolean
    maxPayloadSize?: number
  } = {},
): Attributes {
  const { clientId, serverAddress, serverPort, capturePayload, maxPayloadSize } = options

  const attributes: Attributes = {
    // Required attributes - SHOULD be provided at span creation time
    [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_SYSTEM]: KAFKA_DEFAULTS.MESSAGING_SYSTEM,
    [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_OPERATION_NAME]: operationName,
    [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_OPERATION_TYPE]: operationType,
    [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_DESTINATION_NAME]: message.topic,
  }

  setClientAndServerAttributes(attributes, { clientId, serverAddress, serverPort })

  // Recommended: partition ID as string
  if (isDefined(message.partition)) {
    attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_DESTINATION_PARTITION_ID] = String(message.partition)
  }

  // Recommended: offset (for single message operations)
  if (isDefined(message.offset)) {
    attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_KAFKA_OFFSET] = message.offset
  }

  // Recommended: message key (for single message operations)
  if (isDefined(message.key)) {
    attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_KAFKA_MESSAGE_KEY] = Buffer.isBuffer(message.key)
      ? message.key.toString('utf8')
      : String(message.key)
  }

  // Conditionally Required: tombstone detection
  if (!isDefined(message.payload)) {
    attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_KAFKA_TOMBSTONE] = true
  }

  // Opt-In: message body size (for single message operations)
  setPayloadSizeAttribute(attributes, message.payload, { capturePayload, maxPayloadSize })

  return attributes
}

// Extract common Kafka attributes from producer record
// https://opentelemetry.io/docs/specs/semconv/messaging/kafka/
export function getProducerRecordAttributes(
  record: ProducerRecord,
  operationName: string,
  operationType: string,
  options: {
    clientId?: string
    serverAddress?: string
    serverPort?: number
    capturePayload?: boolean
    maxPayloadSize?: number
  } = {},
): Attributes {
  const { clientId, serverAddress, serverPort, capturePayload, maxPayloadSize } = options

  const attributes: Attributes = {
    // Required attributes - SHOULD be provided at span creation time
    [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_SYSTEM]: KAFKA_DEFAULTS.MESSAGING_SYSTEM,
    [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_OPERATION_NAME]: operationName,
    [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_OPERATION_TYPE]: operationType,
    [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_DESTINATION_NAME]: record.topic,
  }

  setClientAndServerAttributes(attributes, { clientId, serverAddress, serverPort })

  // Add batch message count (always for producer spans per semantic conventions)
  if (record.messages.length > 0) {
    attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_BATCH_MESSAGE_COUNT] = record.messages.length
  }

  // For single message, add message-specific attributes
  if (record.messages.length === 1) {
    const [firstMessage] = record.messages

    // Recommended: message key
    if (isDefined(firstMessage.key)) {
      attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_KAFKA_MESSAGE_KEY] = Buffer.isBuffer(firstMessage.key)
        ? firstMessage.key.toString()
        : String(firstMessage.key)
    }

    // Conditionally Required: tombstone detection
    if (!isDefined(firstMessage.payload)) {
      attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_KAFKA_TOMBSTONE] = true
    }

    // Opt-In: message body size
    setPayloadSizeAttribute(attributes, firstMessage.payload, { capturePayload, maxPayloadSize })
  }

  return attributes
}

// Type for header values that can be passed to/from the instrumentation layer
export type HeaderValue = Buffer | string | string[] | undefined

// Inject trace context into Kafka headers
// Returns a new headers object (does not mutate the input) to avoid altering caller-owned objects
// Accepts both Kafka's native Buffer headers and plain objects for flexibility
// Returns original headers unchanged if injection fails to prevent instrumentation from breaking
export function injectTraceContext(
  headers: Record<string, HeaderValue> = {},
  ctx?: Context,
): Record<string, HeaderValue> {
  try {
    const activeContext = ctx || context.active()
    // Clone headers to avoid mutating caller input
    const targetHeaders: Record<string, HeaderValue> = { ...headers }

    // Convert existing headers to string format for OpenTelemetry propagation
    const stringHeaders: Record<string, string | string[] | undefined> = {}
    for (const [key, value] of Object.entries(targetHeaders)) {
      if (value === undefined || value === null) {
        stringHeaders[key] = undefined
      } else if (Buffer.isBuffer(value)) {
        stringHeaders[key] = value.toString('utf8')
      } else if (Array.isArray(value)) {
        stringHeaders[key] = value
      } else {
        stringHeaders[key] = String(value)
      }
    }

    // Inject trace context using OpenTelemetry propagation API
    propagation.inject(activeContext, stringHeaders, {
      set: (carrier: Record<string, string | string[] | undefined>, key: string, value: string) => {
        carrier[key] = value
      },
    })

    // Mutate cloned headers object with injected trace context
    // Keep the same type format as the input (Buffer headers stay as Buffer)
    const inputHasBuffers = Object.values(targetHeaders).some((headerValue) => Buffer.isBuffer(headerValue))
    for (const [key, value] of Object.entries(stringHeaders)) {
      if (value !== undefined) {
        if (inputHasBuffers) {
          // Convert back to Buffer for Kafka native binding compatibility
          const stringValue = Array.isArray(value) ? value.join(',') : value
          targetHeaders[key] = Buffer.from(stringValue, 'utf8')
        } else {
          // Keep as string for user-facing API
          targetHeaders[key] = value
        }
      }
    }

    return targetHeaders
  } catch (error) {
    diag.warn('Failed to inject trace context into headers, returning original headers:', error)
    return headers
  }
}

// Normalize any header value to Buffer format required by Kafka native bindings
// Kafka native API requires headers as Record<string, Buffer>
export function normalizeHeadersToBuffer(headers: Record<string, HeaderValue>): Record<string, Buffer> {
  const bufferHeaders: Record<string, Buffer> = {}

  for (const [key, value] of Object.entries(headers)) {
    if (value !== undefined && value !== null) {
      if (Buffer.isBuffer(value)) {
        bufferHeaders[key] = value
      } else if (Array.isArray(value)) {
        bufferHeaders[key] = Buffer.from(value.join(','), 'utf8')
      } else {
        bufferHeaders[key] = Buffer.from(String(value), 'utf8')
      }
    }
  }

  return bufferHeaders
}

export function getCapturedHeaderAttributes(
  headers: Record<string, unknown> | null | undefined,
  options: {
    maxHeaderKeys?: number
  } = {},
): Attributes {
  if (!headers || typeof headers !== 'object') {
    return {}
  }

  const maxHeaderKeys = options.maxHeaderKeys ?? 20
  const headerNames = Object.keys(headers).toSorted()

  if (headerNames.length === 0) {
    return {}
  }

  return {
    'kafka_crab.message.header_count': headerNames.length,
    'kafka_crab.message.header_names': headerNames.slice(0, maxHeaderKeys),
  }
}

// Extract trace context from Kafka headers (supports both Buffer and string headers)
// Returns root context when traceparent is missing to avoid inheriting unrelated ambient spans
export function extractTraceContext(headers: Record<string, Buffer | string | string[] | undefined> = {}): Context {
  const hasTraceparent = Object.entries(headers).some(([key, value]) => {
    if (key.toLowerCase() !== 'traceparent') {
      return false
    }

    if (Array.isArray(value)) {
      return value.some((headerValue) => String(headerValue).trim().length > 0)
    }

    if (Buffer.isBuffer(value)) {
      return value.toString('utf8').trim().length > 0
    }

    if (value === undefined || value === null) {
      return false
    }

    return String(value).trim().length > 0
  })

  const extractionBase = hasTraceparent ? context.active() : ROOT_CONTEXT

  try {
    return propagation.extract(extractionBase, headers, {
      get: (carrier: Record<string, Buffer | string | string[] | undefined>, key: string) => {
        let value = carrier[key]
        if (value === undefined) {
          const normalizedKey = key.toLowerCase()
          for (const [headerKey, headerValue] of Object.entries(carrier)) {
            if (headerKey.toLowerCase() === normalizedKey) {
              value = headerValue
              break
            }
          }
        }
        const singleValue = Array.isArray(value) ? value[0] : value

        if (Buffer.isBuffer(singleValue)) {
          return singleValue.toString('utf8')
        }

        return singleValue
      },
      keys: (carrier: Record<string, Buffer | string | string[] | undefined>) => Object.keys(carrier),
    })
  } catch (error) {
    diag.warn(
      `Failed to extract trace context from headers, using ${hasTraceparent ? 'active' : 'root'} context:`,
      error,
    )
    return extractionBase
  }
}

// Check if topic should be ignored based on filter configuration
export function shouldIgnoreTopic(topic: string, ignoreTopics?: string[] | ((topic: string) => boolean)): boolean {
  if (!ignoreTopics) {
    return false
  }

  if (Array.isArray(ignoreTopics)) {
    return ignoreTopics.includes(topic)
  }

  if (typeof ignoreTopics === 'function') {
    try {
      return ignoreTopics(topic)
    } catch {
      // If filter function throws, don't ignore the topic
      return false
    }
  }

  return false
}

// Options for creating producer spans
export interface ProducerSpanOptions {
  operationName?: string
  parentContext?: Context
  clientId?: string
  serverAddress?: string
  serverPort?: number
  capturePayload?: boolean
  maxPayloadSize?: number
}

// Create span for producer send operation
// Span name follows: "<operation> <destination>" per semantic conventions
// https://opentelemetry.io/docs/specs/semconv/messaging/kafka/
export function createProducerSpan(
  tracer: Tracer,
  record: ProducerRecord,
  options: ProducerSpanOptions = {},
): Span | null {
  if (!tracer) {
    return null
  }
  const { operationName = KAFKA_OPERATION_NAMES.SEND, parentContext, clientId, serverAddress, serverPort } = options

  const parent = parentContext ?? context.active()

  // Span name: "<operation> <destination>" (e.g., "send my-topic")
  const spanName = `${operationName} ${record.topic}`
  const attributes = getProducerRecordAttributes(record, operationName, KAFKA_OPERATION_TYPES.SEND, {
    clientId,
    serverAddress,
    serverPort,
    capturePayload: options.capturePayload,
    maxPayloadSize: options.maxPayloadSize,
  })

  const spanOptions = {
    kind: SpanKind.PRODUCER,
    attributes,
  }

  return tracer.startSpan(spanName, spanOptions, parent)
}

// Options for creating consumer spans
export interface ConsumerSpanOptions {
  operationName?: string
  operationType?: string
  parentContext?: Context
  clientId?: string
  serverAddress?: string
  serverPort?: number
  capturePayload?: boolean
  maxPayloadSize?: number
}

// Create span for consumer process operation
// Span name follows: "<operation> <destination>" per semantic conventions
// https://opentelemetry.io/docs/specs/semconv/messaging/kafka/
export function createConsumerSpan(tracer: Tracer, message: Message, options: ConsumerSpanOptions = {}): Span | null {
  if (!tracer) {
    return null
  }

  const {
    operationName = KAFKA_OPERATION_NAMES.PROCESS,
    operationType = KAFKA_OPERATION_TYPES.PROCESS,
    parentContext,
    clientId,
    serverAddress,
    serverPort,
  } = options

  // Span name: "<operation> <destination>" (e.g., "process my-topic")
  const spanName = `${operationName} ${message.topic}`
  const attributes = getMessageAttributes(message, operationName, operationType, {
    clientId,
    serverAddress,
    serverPort,
    capturePayload: options.capturePayload,
    maxPayloadSize: options.maxPayloadSize,
  })

  const spanOptions = {
    kind: SpanKind.CONSUMER,
    attributes,
  }

  // Start span with parent context if provided
  if (parentContext) {
    return tracer.startSpan(spanName, spanOptions, parentContext)
  }

  return tracer.startSpan(spanName, spanOptions)
}

// Options for creating batch spans
export interface BatchSpanOptions {
  topic?: string
  operationName?: string
  parentContext?: Context
  clientId?: string
  serverAddress?: string
  serverPort?: number
}

// Create batch span for batch processing operations
// https://opentelemetry.io/docs/specs/semconv/messaging/kafka/
export function createBatchSpan(tracer: Tracer, batchSize: number, options: BatchSpanOptions = {}): Span | null {
  if (!tracer) {
    return null
  }

  const {
    topic,
    operationName = KAFKA_OPERATION_NAMES.PROCESS,
    parentContext,
    clientId,
    serverAddress,
    serverPort,
  } = options

  // Span name: "<operation> <destination>" (e.g., "process my-topic")
  const spanName = topic ? `${operationName} ${topic}` : `${operationName} kafka`

  const attributes: Attributes = {
    [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_SYSTEM]: KAFKA_DEFAULTS.MESSAGING_SYSTEM,
    [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_OPERATION_NAME]: operationName,
    [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_OPERATION_TYPE]: KAFKA_OPERATION_TYPES.PROCESS,
    [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_BATCH_MESSAGE_COUNT]: batchSize,
  }

  if (topic) {
    attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_DESTINATION_NAME] = topic
  }

  // Recommended: client ID
  if (clientId) {
    attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_CLIENT_ID] = clientId
  }

  // Conditionally Required: server address and port (when available)
  if (serverAddress) {
    attributes[KAFKA_SEMANTIC_CONVENTIONS.SERVER_ADDRESS] = serverAddress
  }
  if (serverPort !== undefined && serverPort !== null) {
    attributes[KAFKA_SEMANTIC_CONVENTIONS.SERVER_PORT] = serverPort
  }

  const spanOptions = {
    kind: SpanKind.CONSUMER,
    attributes,
  }

  if (parentContext) {
    return tracer.startSpan(spanName, spanOptions, parentContext)
  }

  return tracer.startSpan(spanName, spanOptions)
}
