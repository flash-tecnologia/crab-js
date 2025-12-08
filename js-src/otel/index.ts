// Main OTEL module entry point
export type {
  BatchOtelContext,
  InstrumentedMessage,
  InstrumentedProducerRecord,
  KafkaMetricsConfig,
  KafkaOtelContext,
  KafkaOtelInstrumentationConfig,
  MessageHookFn,
  MetricsHookFn,
  ProducerHookFn,
  TopicFilterFn,
} from './types.js'

export { getKafkaInstrumentation, KafkaCrabInstrumentation, resetKafkaInstrumentation } from './instrumentation.js'

export { getKafkaMetrics, KafkaMetrics, resetKafkaMetrics } from './metrics.js'

export {
  ERROR_TYPES,
  KAFKA_DEFAULTS,
  KAFKA_METRIC_DESCRIPTIONS,
  KAFKA_METRIC_UNITS,
  KAFKA_METRICS,
  KAFKA_OPERATION_NAMES,
  KAFKA_OPERATION_TYPES,
  KAFKA_SEMANTIC_CONVENTIONS,
  KAFKA_SPAN_NAMES,
  MESSAGING_DURATION_BUCKETS,
  PACKAGE_INFO,
} from './constants.js'

export {
  createBatchSpan,
  createConsumerSpan,
  createProducerSpan,
  extractTraceContext,
  getTracer,
  injectTraceContext,
  setSpanStatus,
  shouldIgnoreTopic,
} from './utils.js'
