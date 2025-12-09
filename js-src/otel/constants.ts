// OpenTelemetry semantic conventions for Kafka messaging
// https://opentelemetry.io/docs/specs/semconv/messaging/kafka/
export const KAFKA_SEMANTIC_CONVENTIONS = {
  // Messaging system attributes (https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/)
  MESSAGING_SYSTEM: 'messaging.system',
  MESSAGING_DESTINATION_NAME: 'messaging.destination.name',
  MESSAGING_OPERATION_NAME: 'messaging.operation.name',
  MESSAGING_OPERATION_TYPE: 'messaging.operation.type',
  MESSAGING_CLIENT_ID: 'messaging.client.id',
  MESSAGING_MESSAGE_ID: 'messaging.message.id',
  MESSAGING_MESSAGE_BODY_SIZE: 'messaging.message.body.size',
  MESSAGING_BATCH_MESSAGE_COUNT: 'messaging.batch.message_count',

  // Kafka-specific attributes
  MESSAGING_KAFKA_OFFSET: 'messaging.kafka.offset',
  MESSAGING_KAFKA_MESSAGE_KEY: 'messaging.kafka.message.key',
  MESSAGING_KAFKA_TOMBSTONE: 'messaging.kafka.message.tombstone',
  // Alias for partition (maps to messaging.destination.partition.id)
  MESSAGING_KAFKA_PARTITION: 'messaging.destination.partition.id',

  // Consumer-specific attributes
  MESSAGING_CONSUMER_GROUP_NAME: 'messaging.consumer.group.name',

  // Destination attributes
  MESSAGING_DESTINATION_PARTITION_ID: 'messaging.destination.partition.id',
  MESSAGING_DESTINATION_SUBSCRIPTION_NAME: 'messaging.destination.subscription.name',
  MESSAGING_DESTINATION_TEMPLATE: 'messaging.destination.template',

  // Server attributes
  SERVER_ADDRESS: 'server.address',
  SERVER_PORT: 'server.port',

  // Error attributes
  ERROR_TYPE: 'error.type',
} as const

// Metric names (https://opentelemetry.io/docs/specs/semconv/messaging/messaging-metrics/)
export const KAFKA_METRICS = {
  // Common metrics
  CLIENT_OPERATION_DURATION: 'messaging.client.operation.duration',

  // Producer metrics
  CLIENT_SENT_MESSAGES: 'messaging.client.sent.messages',

  // Consumer metrics
  CLIENT_CONSUMED_MESSAGES: 'messaging.client.consumed.messages',
  PROCESS_DURATION: 'messaging.process.duration',
} as const

// Metric descriptions
export const KAFKA_METRIC_DESCRIPTIONS = {
  [KAFKA_METRICS.CLIENT_OPERATION_DURATION]:
    'Duration of messaging operation initiated by a producer or consumer client.',
  [KAFKA_METRICS.CLIENT_SENT_MESSAGES]: 'Number of messages producer attempted to send to the broker.',
  [KAFKA_METRICS.CLIENT_CONSUMED_MESSAGES]: 'Number of messages that were delivered to the application.',
  [KAFKA_METRICS.PROCESS_DURATION]: 'Duration of processing operation.',
} as const

// Metric units (UCUM)
export const KAFKA_METRIC_UNITS = {
  SECONDS: 's',
  MESSAGES: '{message}',
} as const

// Histogram bucket boundaries as per OpenTelemetry semantic conventions
// https://opentelemetry.io/docs/specs/semconv/messaging/messaging-metrics/
export const MESSAGING_DURATION_BUCKETS = [0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10]

// Operation types (messaging.operation.type)
// https://opentelemetry.io/docs/specs/semconv/messaging/kafka/
export const KAFKA_OPERATION_TYPES = {
  /** A message is created */
  CREATE: 'create',
  /** One or more messages are provided for sending to an intermediary */
  SEND: 'send',
  /** One or more messages are requested by a consumer (pull-based) */
  RECEIVE: 'receive',
  /** One or more messages are processed by a consumer */
  PROCESS: 'process',
  /** One or more messages are settled (e.g., commit) */
  SETTLE: 'settle',
} as const

// Operation names (messaging.operation.name) - system-specific names
export const KAFKA_OPERATION_NAMES = {
  SEND: 'send',
  POLL: 'poll',
  RECEIVE: 'receive',
  PROCESS: 'process',
  BATCH_PROCESS: 'batch_process',
  COMMIT: 'commit',
  ACK: 'ack',
  NACK: 'nack',
} as const

// Span names follow the pattern: "<operation> <destination>"
// https://opentelemetry.io/docs/specs/semconv/messaging/kafka/
export const KAFKA_SPAN_NAMES = {
  PRODUCER_SEND: (topic: string) => `send ${topic}`,
  CONSUMER_POLL: (topic: string) => `poll ${topic}`,
  CONSUMER_RECEIVE: (topic: string) => `receive ${topic}`,
  CONSUMER_PROCESS: (topic: string) => `process ${topic}`,
  CONSUMER_COMMIT: (topic: string) => `commit ${topic}`,
} as const

// Default values
export const KAFKA_DEFAULTS = {
  MESSAGING_SYSTEM: 'kafka',
} as const

// Well-known error types
export const ERROR_TYPES = {
  OTHER: '_OTHER',
} as const

// Package information
// Version must be kept in sync with package.json (verified by tests)
export const PACKAGE_INFO = {
  NAME: 'kafka-crab-js',
  VERSION: '2.1.0',
} as const 
