import { type Attributes, diag, metrics as otelMetrics } from '@opentelemetry/api'
import type { Message, ProducerRecord, RecordMetadata } from 'kafka-crab-js'
import {
  ERROR_TYPES,
  KAFKA_DEFAULTS,
  KAFKA_METRIC_DESCRIPTIONS,
  KAFKA_METRIC_UNITS,
  KAFKA_METRICS,
  KAFKA_OPERATION_NAMES,
  KAFKA_OPERATION_TYPES,
  KAFKA_SEMANTIC_CONVENTIONS,
  MESSAGING_DURATION_BUCKETS,
  PACKAGE_INFO,
} from './constants.js'
import type { Counter, Histogram, KafkaMetricsConfig, Meter } from './types.js'

const ERROR_MESSAGE_PATTERNS = [
  { type: 'KAFKA_TIMEOUT', terms: ['TIMEOUT'] },
  { type: 'KAFKA_NETWORK_ERROR', terms: ['NETWORK', 'CONNECTION'] },
  { type: 'KAFKA_AUTHORIZATION_ERROR', terms: ['AUTHORIZATION', 'AUTH'] },
  { type: 'KAFKA_STORAGE_ERROR', terms: ['STORAGE', 'DISK'] },
  { type: 'KAFKA_REBALANCE_ERROR', terms: ['REBALANCE'] },
  { type: 'KAFKA_OFFSET_ERROR', terms: ['OFFSET'] },
  { type: 'KAFKA_SERIALIZATION_ERROR', terms: ['SERIALIZATION', 'DESERIALIZ'] },
] as const

/**
 * Get error type for metrics attribution
 * Uses low-cardinality error types as per semantic conventions
 */
function getErrorType(error: Error | unknown): string {
  // Check if it's a KafkaCrabError with a specific code
  if (error && typeof error === 'object' && 'code' in error && typeof (error as { code: unknown }).code === 'number') {
    // Map known codes to error types if we had a mapping
    // For now, we can try to infer from message or just return KAFKA_ERROR_<code>
    return `KAFKA_ERROR_${(error as { code: number }).code}`
  }

  const err = error as Error | null
  // Check for Kafka-specific error codes in the message
  if (err?.message) {
    const message = err.message.toUpperCase()
    const matchedPattern = ERROR_MESSAGE_PATTERNS.find(({ terms }) => terms.some((term) => message.includes(term)))
    if (matchedPattern) {
      return matchedPattern.type
    }
  }

  // Use error constructor name if available
  if (err && err.name && err.name !== 'Error') {
    return err.name
  }

  // Fallback to generic error type
  return ERROR_TYPES.OTHER
}

/**
 * Create a timer for measuring operation duration
 */
function createTimer(): () => number {
  const start = performance.now()
  return () => (performance.now() - start) / 1000 // Convert to seconds
}

/**
 * Manages OpenTelemetry metrics for Kafka operations
 * Implements semantic conventions from:
 * https://opentelemetry.io/docs/specs/semconv/messaging/messaging-metrics/
 */
export class KafkaMetrics {
  private _meter: Meter | null = null
  private _enabled = false
  private _config: KafkaMetricsConfig

  // Metric instruments
  private _operationDuration: Histogram | null = null
  private _sentMessages: Counter | null = null
  private _consumedMessages: Counter | null = null
  private _processDuration: Histogram | null = null

  public constructor(config: KafkaMetricsConfig = {}) {
    // Validate histogram buckets if provided
    if (config.histogramBuckets) {
      KafkaMetrics._validateHistogramBuckets(config.histogramBuckets)
    }

    this._config = {
      enabled: false,
      includePartitionId: true,
      ...config,
    }
  }

  /**
   * Enable metrics collection
   */
  public enable(): void {
    if (this._enabled) {
      return
    }

    try {
      // Get meter from provider or global
      this._meter = this._config.meterProvider
        ? this._config.meterProvider.getMeter(PACKAGE_INFO.NAME, PACKAGE_INFO.VERSION)
        : otelMetrics.getMeter(PACKAGE_INFO.NAME, PACKAGE_INFO.VERSION)

      // Create metric instruments
      this._createInstruments()
      this._enabled = true
      diag.debug('Kafka metrics enabled')
    } catch (error) {
      diag.warn('Failed to enable Kafka metrics:', error)
      this._enabled = false
    }
  }

  /**
   * Disable metrics collection
   */
  public disable(): void {
    this._meter = null
    this._operationDuration = null
    this._sentMessages = null
    this._consumedMessages = null
    this._processDuration = null
    this._enabled = false
    diag.debug('Kafka metrics disabled')
  }

  /**
   * Dispose of metrics resources and clean up references
   * This method should be called when the metrics instance is no longer needed
   * to prevent memory leaks in long-running applications
   */
  public dispose(): void {
    // Disable metrics collection
    this.disable()

    // Clear configuration references
    this._config = {
      enabled: false,
      includePartitionId: false,
    }

    diag.debug('Kafka metrics disposed')
  }

  /**
   * Check if metrics are enabled
   */
  public isEnabled(): boolean {
    return this._enabled && this._meter !== null
  }

  /**
   * Update configuration
   */
  public updateConfig(config: Partial<KafkaMetricsConfig>): void {
    // Validate histogram buckets if provided
    if (config.histogramBuckets) {
      KafkaMetrics._validateHistogramBuckets(config.histogramBuckets)
    }

    const bucketsChanged =
      Array.isArray(config.histogramBuckets) &&
      (this._config.histogramBuckets?.length !== config.histogramBuckets.length ||
        this._config.histogramBuckets?.some((bucket, index) => bucket !== config.histogramBuckets?.[index]))

    this._config = { ...this._config, ...config }

    // Handle enable/disable toggle
    if (this._config.enabled && !this._enabled) {
      this.enable()
    } else if (this._config.enabled === false && this._enabled) {
      this.disable()
    } else if (this._enabled && bucketsChanged) {
      // Recreate instruments to apply new bucket boundaries
      this._createInstruments()
    }
  }

  /**
   * Record producer send operation duration
   */
  public recordProducerDuration(
    topic: string,
    durationSeconds: number,
    options: {
      partition?: number
      error?: Error
      clientId?: string
    } = {},
  ): void {
    if (!this._enabled || !this._operationDuration) {
      return
    }

    const attributes = this._buildProducerAttributes(
      topic,
      KAFKA_OPERATION_NAMES.SEND,
      KAFKA_OPERATION_TYPES.SEND,
      options,
    )
    this._operationDuration.record(durationSeconds, attributes)
  }

  /**
   * Record messages sent by producer
   */
  public recordMessagesSent(
    record: ProducerRecord,
    metadata?: RecordMetadata[],
    options: {
      error?: Error
      clientId?: string
    } = {},
  ): void {
    if (!this._enabled || !this._sentMessages) {
      return
    }

    const messageCount = record.messages?.length ?? 0
    if (messageCount === 0) {
      return
    }

    const attributes = this._buildProducerAttributes(
      record.topic,
      KAFKA_OPERATION_NAMES.SEND,
      KAFKA_OPERATION_TYPES.SEND,
      {
        partition: metadata?.[0]?.partition,
        error: options.error,
        clientId: options.clientId,
      },
    )

    this._sentMessages.add(messageCount, attributes)
  }

  /**
   * Record consumer receive operation duration
   */
  public recordConsumerDuration(
    topic: string,
    durationSeconds: number,
    options: {
      partition?: number
      groupId?: string
      error?: Error
      clientId?: string
    } = {},
  ): void {
    if (!this._enabled || !this._operationDuration) {
      return
    }

    const attributes = this._buildConsumerAttributes(
      topic,
      KAFKA_OPERATION_NAMES.POLL,
      KAFKA_OPERATION_TYPES.RECEIVE,
      options,
    )
    this._operationDuration.record(durationSeconds, attributes)
  }

  /**
   * Record messages consumed
   */
  public recordMessagesConsumed(
    messages: Message | Message[],
    options: {
      groupId?: string
      error?: Error
      clientId?: string
    } = {},
  ): void {
    if (!this._enabled || !this._consumedMessages) {
      return
    }

    const messageArray = Array.isArray(messages) ? messages : [messages]
    const validMessages = messageArray.filter((message) => message && message.topic)
    if (validMessages.length === 0) {
      return
    }

    // Group messages by topic for proper attribution
    const messagesByTopic = new Map<string, Message[]>()
    for (const message of validMessages) {
      const existing = messagesByTopic.get(message.topic) ?? []
      existing.push(message)
      messagesByTopic.set(message.topic, existing)
    }

    for (const [topic, topicMessages] of messagesByTopic) {
      const [firstMessage] = topicMessages
      const attributes = this._buildConsumerAttributes(
        topic,
        KAFKA_OPERATION_NAMES.POLL,
        KAFKA_OPERATION_TYPES.RECEIVE,
        {
          partition: firstMessage?.partition,
          groupId: options.groupId,
          error: options.error,
          clientId: options.clientId,
        },
      )
      this._consumedMessages.add(topicMessages.length, attributes)
    }
  }

  /**
   * Record message processing duration
   */
  public recordProcessDuration(
    message: Message,
    durationSeconds: number,
    options: {
      groupId?: string
      error?: Error
      clientId?: string
    } = {},
  ): void {
    if (!this._enabled || !this._processDuration) {
      return
    }

    const attributes = this._buildConsumerAttributes(
      message.topic,
      KAFKA_OPERATION_NAMES.PROCESS,
      KAFKA_OPERATION_TYPES.PROCESS,
      {
        partition: message.partition,
        groupId: options.groupId,
        error: options.error,
        clientId: options.clientId,
      },
    )

    this._processDuration.record(durationSeconds, attributes)
  }

  /**
   * Record batch processing duration
   */
  public recordBatchProcessDuration(
    messages: Message[],
    durationSeconds: number,
    options: {
      groupId?: string
      error?: Error
      clientId?: string
    } = {},
  ): void {
    if (!this._enabled || !this._processDuration || messages.length === 0) {
      return
    }

    const [firstMessage] = messages
    if (!firstMessage) {
      return
    }

    const attributes = this._buildConsumerAttributes(
      firstMessage.topic,
      KAFKA_OPERATION_NAMES.PROCESS,
      KAFKA_OPERATION_TYPES.PROCESS,
      {
        partition: firstMessage.partition,
        groupId: options.groupId,
        error: options.error,
        clientId: options.clientId,
      },
    )

    // Record duration for the batch operation
    this._processDuration.record(durationSeconds, attributes)
  }

  /**
   * Create a timer for measuring operation duration
   */
  public static startTimer(): () => number {
    return createTimer()
  }

  /**
   * Validate histogram bucket boundaries
   * Ensures buckets are in ascending order and contain valid numbers
   */
  private static _validateHistogramBuckets(buckets: number[]): void {
    if (!Array.isArray(buckets) || buckets.length === 0) {
      throw new Error('histogramBuckets must be a non-empty array')
    }

    for (let index = 0; index < buckets.length; index++) {
      const bucket = buckets[index]

      // Check if bucket is a valid number
      if (typeof bucket !== 'number' || !Number.isFinite(bucket)) {
        throw new Error(`histogramBuckets[${index}] must be a finite number, got: ${bucket}`)
      }

      // Check if bucket is positive
      if (bucket <= 0) {
        throw new Error(`histogramBuckets[${index}] must be positive, got: ${bucket}`)
      }

      // Check ascending order
      if (index > 0 && bucket <= buckets[index - 1]) {
        throw new Error(
          `histogramBuckets must be in strictly ascending order. ` +
            `Found ${bucket} at index ${index}, but previous value was ${buckets[index - 1]}`,
        )
      }
    }
  }

  /**
   * Create all metric instruments
   */
  private _createInstruments(): void {
    if (!this._meter) {
      return
    }

    // Use custom histogram buckets if provided, otherwise use defaults
    const buckets = this._config.histogramBuckets || MESSAGING_DURATION_BUCKETS

    // Messaging.client.operation.duration - Histogram
    this._operationDuration = this._meter.createHistogram(KAFKA_METRICS.CLIENT_OPERATION_DURATION, {
      description: KAFKA_METRIC_DESCRIPTIONS[KAFKA_METRICS.CLIENT_OPERATION_DURATION],
      unit: KAFKA_METRIC_UNITS.SECONDS,
      advice: {
        explicitBucketBoundaries: buckets,
      },
    })

    // Messaging.client.sent.messages - Counter
    this._sentMessages = this._meter.createCounter(KAFKA_METRICS.CLIENT_SENT_MESSAGES, {
      description: KAFKA_METRIC_DESCRIPTIONS[KAFKA_METRICS.CLIENT_SENT_MESSAGES],
      unit: KAFKA_METRIC_UNITS.MESSAGES,
    })

    // Messaging.client.consumed.messages - Counter
    this._consumedMessages = this._meter.createCounter(KAFKA_METRICS.CLIENT_CONSUMED_MESSAGES, {
      description: KAFKA_METRIC_DESCRIPTIONS[KAFKA_METRICS.CLIENT_CONSUMED_MESSAGES],
      unit: KAFKA_METRIC_UNITS.MESSAGES,
    })

    // Messaging.process.duration - Histogram
    this._processDuration = this._meter.createHistogram(KAFKA_METRICS.PROCESS_DURATION, {
      description: KAFKA_METRIC_DESCRIPTIONS[KAFKA_METRICS.PROCESS_DURATION],
      unit: KAFKA_METRIC_UNITS.SECONDS,
      advice: {
        explicitBucketBoundaries: buckets,
      },
    })
  }

  /**
   * Build common attributes for producer metrics
   * https://opentelemetry.io/docs/specs/semconv/messaging/messaging-metrics/
   */
  private _buildProducerAttributes(
    topic: string,
    operationName: string,
    operationType: string,
    options: {
      partition?: number
      error?: Error
      clientId?: string
    } = {},
  ): Attributes {
    const attributes: Attributes = {
      // Required attributes
      [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_SYSTEM]: KAFKA_DEFAULTS.MESSAGING_SYSTEM,
      [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_OPERATION_NAME]: operationName,
      // Conditionally Required attributes
      [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_OPERATION_TYPE]: operationType,
      [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_DESTINATION_NAME]: topic,
    }

    // Add server info if configured
    if (this._config.serverAddress) {
      attributes[KAFKA_SEMANTIC_CONVENTIONS.SERVER_ADDRESS] = this._config.serverAddress
    }
    if (this._config.serverPort) {
      attributes[KAFKA_SEMANTIC_CONVENTIONS.SERVER_PORT] = this._config.serverPort
    }

    // Add partition if available and configured
    if (this._config.includePartitionId && options.partition !== undefined) {
      attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_DESTINATION_PARTITION_ID] = String(options.partition)
    }

    // Add client ID if available
    if (options.clientId) {
      attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_CLIENT_ID] = options.clientId
    }

    // Add error type if operation failed
    if (options.error) {
      attributes[KAFKA_SEMANTIC_CONVENTIONS.ERROR_TYPE] = getErrorType(options.error)
    }

    return attributes
  }

  /**
   * Build common attributes for consumer metrics
   * https://opentelemetry.io/docs/specs/semconv/messaging/messaging-metrics/
   */
  private _buildConsumerAttributes(
    topic: string,
    operationName: string,
    operationType: string,
    options: {
      partition?: number
      groupId?: string
      error?: Error
      clientId?: string
    } = {},
  ): Attributes {
    const attributes: Attributes = {
      // Required attributes
      [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_SYSTEM]: KAFKA_DEFAULTS.MESSAGING_SYSTEM,
      [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_OPERATION_NAME]: operationName,
      // Conditionally Required attributes
      [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_OPERATION_TYPE]: operationType,
      [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_DESTINATION_NAME]: topic,
    }

    // Add server info if configured
    if (this._config.serverAddress) {
      attributes[KAFKA_SEMANTIC_CONVENTIONS.SERVER_ADDRESS] = this._config.serverAddress
    }
    if (this._config.serverPort) {
      attributes[KAFKA_SEMANTIC_CONVENTIONS.SERVER_PORT] = this._config.serverPort
    }

    // Add consumer group if available
    if (options.groupId) {
      attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_CONSUMER_GROUP_NAME] = options.groupId
    }

    // Add partition if available and configured
    if (this._config.includePartitionId && options.partition !== undefined) {
      attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_DESTINATION_PARTITION_ID] = String(options.partition)
    }

    // Add client ID if available
    if (options.clientId) {
      attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_CLIENT_ID] = options.clientId
    }

    // Add error type if operation failed
    if (options.error) {
      attributes[KAFKA_SEMANTIC_CONVENTIONS.ERROR_TYPE] = getErrorType(options.error)
    }

    return attributes
  }
}

// Singleton instance for global use
let globalMetrics: KafkaMetrics | null = null

/**
 * Get or create the global KafkaMetrics instance
 */
export function getKafkaMetrics(config?: KafkaMetricsConfig): KafkaMetrics {
  if (!globalMetrics) {
    globalMetrics = new KafkaMetrics(config)
    if (config?.enabled === true) {
      globalMetrics.enable()
    }
  } else if (config) {
    globalMetrics.updateConfig(config)
  }
  return globalMetrics
}

/**
 * Reset the global KafkaMetrics instance
 * Properly disposes of the current instance to prevent memory leaks
 */
export function resetKafkaMetrics(): void {
  if (globalMetrics) {
    globalMetrics.dispose()
    globalMetrics = null
  }
}
