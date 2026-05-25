import type { Attributes, Context, Span, Tracer } from '@opentelemetry/api'
import type { Message, ProducerRecord, RecordMetadata } from 'kafka-crab-js'

// InstrumentationConfig interface (simplified from @opentelemetry/instrumentation)
export interface InstrumentationConfig {
  enabled?: boolean
}

// OpenTelemetry TracerProvider interface
export interface TracerProvider {
  getTracer(name: string, version?: string, options?: unknown): Tracer
}

// OpenTelemetry MeterProvider interface
export interface MeterProvider {
  getMeter(name: string, version?: string, options?: unknown): Meter
}

// OpenTelemetry Meter interface (simplified)
export interface Meter {
  createCounter(name: string, options?: MetricOptions): Counter
  createHistogram(name: string, options?: MetricOptions): Histogram
  createUpDownCounter(name: string, options?: MetricOptions): UpDownCounter
  createObservableGauge(name: string, options?: MetricOptions): ObservableGauge
}

// Metric options interface
export interface MetricOptions {
  description?: string
  unit?: string
  valueType?: ValueType
  advice?: MetricAdvice
}

// Metric advice interface (for histogram boundaries)
export interface MetricAdvice {
  explicitBucketBoundaries?: number[]
}

// Value type enum
export enum ValueType {
  INT = 0,
  DOUBLE = 1,
}

// Counter interface
export interface Counter {
  add(value: number, attributes?: Attributes): void
}

// Histogram interface
export interface Histogram {
  record(value: number, attributes?: Attributes): void
}

// UpDownCounter interface
export interface UpDownCounter {
  add(value: number, attributes?: Attributes): void
}

// ObservableGauge interface
export interface ObservableGauge {
  addCallback(callback: (result: ObservableResult) => void): void
}

// ObservableResult interface
export interface ObservableResult {
  observe(value: number, attributes?: Attributes): void
}

// Metrics configuration options
export interface KafkaMetricsConfig {
  // Whether to enable metrics collection (default: false)
  enabled?: boolean

  // Custom meter provider (if not using global)
  meterProvider?: MeterProvider

  // Server address for broker attribution
  serverAddress?: string

  // Server port for broker attribution
  serverPort?: number

  // Whether to include partition ID in metrics
  includePartitionId?: boolean

  // Custom histogram bucket boundaries for duration metrics (in seconds)
  // Must be in ascending order. If not provided, uses default OTEL recommended buckets.
  // Default: [0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10]
  histogramBuckets?: number[]
}

// Configuration interface for Kafka OTEL instrumentation
export interface KafkaOtelInstrumentationConfig extends InstrumentationConfig {
  // Broker server address for span attributes (conditionally required by semantic conventions)
  serverAddress?: string

  // Broker server port for span attributes (conditionally required by semantic conventions)
  serverPort?: number

  // Function to filter topics from instrumentation
  ignoreTopics?: string[] | ((topic: string) => boolean)

  // Custom hook called for each message/span
  messageHook?: (span: Span, message: Message) => void

  // Custom hook called for producer operations
  producerHook?: (span: Span, record: ProducerRecord, metadata?: RecordMetadata) => void

  // Whether to capture message payloads as span attributes (security sensitive)
  captureMessagePayload?: boolean

  // Maximum size of message payload to capture (in bytes)
  maxPayloadSize?: number

  // Whether to capture message headers as span attributes
  captureMessageHeaders?: boolean

  // Whether to augment received messages/batches with OTEL helper fields (default: true)
  // When false, the native Message objects are not mutated; recv()/recvBatch() return shallow clones augmented with
  // `otelContext`/`span`/`endSpan`.
  decorateMessages?: boolean

  // Whether to enable batch operation instrumentation
  enableBatchInstrumentation?: boolean

  // Metrics configuration
  metrics?: KafkaMetricsConfig
}

// OpenTelemetry context interface exposed by clients
export interface KafkaOtelContext {
  // Whether OTEL is enabled for this client
  enabled: boolean

  // Current active span (if any)
  span: Span | null

  // Tracer instance for creating spans
  tracer: Tracer | null

  // Current OTEL context
  context: Context | null

  // Inject trace context into carrier (e.g., Kafka headers)
  // Optional span parameter allows injecting a specific span's context
  inject: (carrier: Record<string, string | string[] | undefined>, span?: Span) => void

  // Extract trace context from carrier (e.g., Kafka headers)
  extract: (carrier: Record<string, string | string[] | undefined>) => Context

  // Create a child span with proper context
  startSpan: (name: string, attributes?: Attributes) => Span | null

  // End a span with proper status handling
  endSpan: (span: Span | null | undefined, error?: Error) => void

  // End a consumer processing span for a message without mutating the message object
  endMessageSpan: (message: Message | InstrumentedMessage | null | undefined, error?: Error) => void

  // End a batch processing span for a recvBatch() result without mutating the batch array
  endBatchSpan: (batch: Message[] | InstrumentedMessageBatch | null | undefined, error?: Error) => void

  // Convert a Message into an InstrumentedMessage wrapper (does not modify the original message)
  toInstrumentedMessage: (message: Message) => InstrumentedMessage

  // Convert a Message[] batch into an instrumented wrapper batch (does not modify the original batch)
  toInstrumentedBatch: (batch: Message[]) => InstrumentedMessageBatch

  // Wrap message processing and automatically end the consumer span (records errors/metrics as needed)
  processMessage: <TResult>(
    message: Message | InstrumentedMessage,
    handler: (message: Message | InstrumentedMessage) => TResult | Promise<TResult>,
  ) => Promise<TResult>

  // Wrap batch processing and automatically end the batch span (and contained message spans)
  processBatch: <TResult>(
    batch: Message[] | InstrumentedMessageBatch,
    handler: (batch: Message[] | InstrumentedMessageBatch) => TResult | Promise<TResult>,
  ) => Promise<TResult>
}

// Enhanced message interface with OTEL context
export interface InstrumentedMessage extends Message {
  // Extracted OTEL context from message headers
  otelContext?: Context

  // Optional span created for this message
  span?: Span

  // Optional helper to end the span and record processing duration
  endSpan?: (error?: Error) => void
}

// Enhanced batch (Message[]) interface with OTEL helpers
export interface InstrumentedMessageBatch extends Array<InstrumentedMessage> {
  otelContext?: Context
  span?: Span
  endSpan?: (error?: Error) => void
}

// Enhanced producer record with OTEL context injection
export interface InstrumentedProducerRecord extends ProducerRecord {
  // Headers will be automatically injected with trace context
  headers?: Record<string, string | string[] | undefined>

  // Optional span for tracking this record
  span?: Span
}

// Batch processing context
export interface BatchOtelContext {
  // Parent span for the entire batch
  batchSpan: Span

  // Individual message spans within the batch
  messageSpans: Span[]

  // Batch size for telemetry
  batchSize: number

  // Processing start time
  startTime: number
}

// Hook function signatures
export type MessageHookFn = (span: Span, message: Message) => void
export type ProducerHookFn = (span: Span, record: ProducerRecord, metadata?: RecordMetadata) => void
export type TopicFilterFn = (topic: string) => boolean

// Metrics hook function signature (called after recording metrics)
export type MetricsHookFn = (metricName: string, value: number, attributes: Attributes) => void

// Configuration defaults - simplified type definition
export interface DefaultOtelConfig {
  captureMessagePayload: boolean
  maxPayloadSize: number
  captureMessageHeaders: boolean
  decorateMessages: boolean
  enableBatchInstrumentation: boolean
  metrics: {
    enabled: boolean
    includePartitionId: boolean
  }
}

export const DEFAULT_OTEL_CONFIG: DefaultOtelConfig = {
  captureMessagePayload: false,
  maxPayloadSize: 1024,
  captureMessageHeaders: true,
  decorateMessages: true,
  enableBatchInstrumentation: true,
  metrics: {
    enabled: false,
    includePartitionId: true,
  },
}
