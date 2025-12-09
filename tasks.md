# TODO - OpenTelemetry Instrumentation

## Completed ✅

### OpenTelemetry Metrics Implementation (Semantic Conventions Compliant)

#### `messaging.client.operation.duration` (Histogram) - Required
- [x] Added histogram metric for producer/consumer operation duration
- [x] Ensured metric value matches corresponding span duration (timer starts at span creation, metric recorded after span.end())
- [x] Unit: `s` (seconds, UCUM)
- [x] Bucket boundaries per spec: `[0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10]`
- [x] This metric SHOULD NOT be used to report processing duration (use `messaging.process.duration` instead)

#### `messaging.client.sent.messages` (Counter) - Required
- [x] Added counter metric for producer message count
- [x] Recorded after messages are sent to broker (not before)
- [x] Unit: `{message}` (UCUM)
- [x] Includes all required attributes: `messaging.system`, `messaging.operation.name`
- [x] Includes conditionally required attributes: `messaging.destination.name`, `error.type`
- [x] Includes recommended attributes: `messaging.destination.partition.id`, `server.address`, `server.port`

#### `messaging.client.consumed.messages` (Counter) - Required
- [x] Added counter metric for consumer message count
- [x] Recorded once per message delivery at receive time, not during processing (per spec requirement)
- [x] Unit: `{message}` (UCUM)
- [x] Includes all required attributes: `messaging.system`, `messaging.operation.name`
- [x] Includes conditionally required attributes: `messaging.destination.name`, `messaging.consumer.group.name`, `error.type`
- [x] Includes recommended attributes: `messaging.destination.partition.id`, `server.address`, `server.port`

#### `messaging.process.duration` (Histogram) - Required for push-based, Recommended for pull-based
- [x] Added histogram metric for message processing duration
- [x] Ensured metric value matches corresponding span duration (timer starts at span creation, metric recorded after span.end())
- [x] Unit: `s` (seconds, UCUM)
- [x] Bucket boundaries per spec: `[0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10]`
- [x] This metric MUST be reported for operations with `messaging.operation.type` = `process`

#### Metrics Infrastructure
- [x] Created `KafkaMetrics` class in `js-src/otel/metrics.ts`
- [x] Added `KafkaMetricsConfig` interface for metrics configuration
- [x] Integrated metrics recording into `instrumentProducerSend()`
- [x] Integrated metrics recording into `instrumentConsumerReceive()`
- [x] Integrated metrics recording into `instrumentBatchReceive()`
- [x] Added Kafka-specific error type classification for low-cardinality `error.type` attribute
- [x] Added singleton pattern via `getKafkaMetrics()` and `resetKafkaMetrics()`

### Required Metric Attributes (per Semantic Conventions)
- [x] `messaging.system` = `"kafka"` (Required)
- [x] `messaging.operation.name` (Required) - system-specific names: `send`, `receive`, `process`
- [x] `messaging.operation.type` (Conditionally Required) - `send`, `receive`, `process`
- [x] `messaging.destination.name` (Conditionally Required) - topic name
- [x] `messaging.consumer.group.name` (Conditionally Required for consumer operations)
- [x] `messaging.destination.partition.id` (Recommended) - as string per spec
- [x] `server.address` / `server.port` (Conditionally Required when available)
- [x] `error.type` (Conditionally Required - only on error, low cardinality)

### Kafka Span Implementation (Semantic Conventions Compliant)
- [x] Ensure spans tracing is Compliance with OpenTelemetry Semantic Conventions for Messaging Systems. See https://opentelemetry.io/docs/specs/semconv/messaging/kafka/#span-attributes
- [x] Added `server.address` and `server.port` attributes (Conditionally Required)
- [x] Added `messaging.client.id` attribute to all spans (Recommended)
- [x] Fixed attribute setting to happen at span creation time for sampling decisions
- [x] Ensured correct operation types for consumer operations (process vs receive)
- [x] Refactored span creation functions to use options objects for better maintainability

### Kafka Semantic Conventions Compliance (https://opentelemetry.io/docs/specs/semconv/messaging/kafka/)
- [x] Fixed span name format to `"<operation> <destination>"` (e.g., `"send my-topic"`)
- [x] Added required `messaging.operation.type` attribute (`send`, `receive`, `process`, `settle`)
- [x] Added `messaging.operation.name` attribute for system-specific names (`send`, `poll`, `process`, `commit`)
- [x] Fixed partition attribute from deprecated `messaging.kafka.partition` to `messaging.destination.partition.id` (as string)
- [x] Removed deprecated `messaging.destination.kind` attribute
- [x] Added tombstone detection with `messaging.kafka.message.tombstone` attribute
- [x] Updated `KAFKA_OPERATION_TYPES` constants to match spec values
- [x] Added `KAFKA_OPERATION_NAMES` constants for system-specific operation names
- [x] Updated `KAFKA_SPAN_NAMES` to use correct format
- [x] Updated `createProducerSpan()` to set correct attributes at span creation time
- [x] Updated `createConsumerSpan()` to set correct attributes at span creation time
- [x] Updated `createBatchSpan()` to set correct attributes at span creation time

### Span Attributes (per Kafka Semantic Conventions)
- [x] `messaging.system` provided at span creation time
- [x] `messaging.operation.name` provided at span creation time
- [x] `messaging.operation.type` provided at span creation time
- [x] `messaging.destination.name` provided at span creation time
- [x] `messaging.destination.partition.id` (Recommended)
- [x] `messaging.kafka.offset` (Recommended for single message operations)
- [x] `messaging.kafka.message.key` (Recommended for single message operations)
- [x] `messaging.kafka.message.tombstone` (Conditionally Required when true)
- [x] `messaging.batch.message_count` (Conditionally Required for batch operations)
- [x] `messaging.consumer.group.name` (Recommended for consumer operations)
- [x] `messaging.client.id` (Recommended)
- [x] `messaging.message.body.size` (Opt-In)

### Code Quality & Reliability Improvements
- [x] Removed type assertions in `injectTraceContext` - replaced with proper runtime type handling
- [x] Added histogram bucket validation for custom metrics configuration
- [x] Fixed memory leak with span timers - timers now properly cleaned up on early returns
- [x] Added disposal pattern for `KafkaMetrics` class with `dispose()` method
- [x] Added error handling in `extractTraceContext` - prevents instrumentation failures
- [x] Added error handling in `injectTraceContext` - prevents instrumentation failures
- [x] Verified hook error recovery - all hooks properly wrapped in try-catch blocks
- [x] Added hook cleanup in `resetInstrumentation` - prevents memory leaks

## Future Improvements

### 🐛 Bug Fixes (Priority: High) ✅ COMPLETED

#### Version Hardcoded (`constants.ts:121`) ✅
- [x] Fix `PACKAGE_INFO.VERSION` hardcoded as `'2.1.0'` - can get out of sync with `package.json`
- [x] Import version from `package.json` using `createRequire` for ESM compatibility

#### Producer Hook Called Twice (`instrumentation.ts:218-271`) ✅
- [x] Review producer hook being called twice (before send and after with metadata)
- [x] Added documentation explaining the intentional behavior (before for inspection, after for results)

#### Incorrect Context in Producer Send (`instrumentation.ts:229`) ✅
- [x] Fix `context.with(callerContext, ...)` should use `spanContext` instead
- [x] Producer span is now active during the send operation

#### Empty Span Name in Consumer Receive (`instrumentation.ts:320`) ✅
- [x] Fix span starting with empty topic name `''` resulting in malformed span name `"receive "`
- [x] Changed to use `'kafka'` placeholder until topic is known

#### Process Timer Not Released on Error (`instrumentation.ts:410-434`) ✅
- [x] Ensure `processTimer` closure is released even when errors occur inside context.with callback
- [x] Wrapped in try/catch/finally to guarantee cleanup

#### Redundant Fallback in extractTraceContext (`utils.ts:391`) ✅
- [x] Remove redundant `|| context.active()` since `extractTraceContext` already returns it on failure

### ⚠️ Inconsistencies (Priority: Medium) ✅ COMPLETED

#### Inconsistent Attribute Key Usage (`instrumentation.ts`) ✅
- [x] Replace hardcoded strings with `KAFKA_SEMANTIC_CONVENTIONS` constants
  - Line 564: `'messaging.consumer.group.name'` → using constant
  - Line 583: `'messaging.batch.message_count'` → using constant

#### Inject Function Signature Mismatch (`types.ts` vs `instrumentation.ts`) ✅
- [x] Update `KafkaOtelContext.inject` type to include optional `Span` parameter

#### Duplicated JSDoc Comment (`metrics.ts:17-24`) ✅
- [x] Remove duplicate `getErrorType` JSDoc comment

#### DEFAULT_OTEL_CONFIG Type Complexity (`types.ts:202-225`) ✅
- [x] Simplify overly complex nested `Required` and `Pick` types
- [x] Created clean `DefaultOtelConfig` interface

### 💡 Improvements (Priority: Low)

#### Missing clientId in Consumer Metrics (`metrics.ts`)
- [ ] Add `clientId` parameter to `_buildConsumerAttributes` method (producer has it)

#### No Config Validation on updateConfig (`instrumentation.ts:54-61`)
- [ ] Validate config values (e.g., `maxPayloadSize` should be positive)

#### Performance: shouldIgnoreTopic Called Multiple Times (`instrumentation.ts`)
- [x] Cache `shouldIgnoreTopic` results in `instrumentBatchReceive` (lines 509-510, 526-528)

#### Performance: injectTraceContext Creates Intermediate Arrays (`utils.ts:216`)
- [ ] Optimize `Object.values(headers).some(...)` to avoid intermediate array creation

#### Re-enable Disabled Instrumentation (`instrumentation.ts:646-648`)
- [ ] Handle case where disabled instrumentation is updated with `enabled: true`

#### Incomplete Batch Error Handling (`instrumentation.ts`)
- [ ] Handle case where `createConsumerSpan` fails for one message in batch loop

#### Redundant Array Filtering (`instrumentation.ts`)
- [x] Consolidate `instrumentedMessages` and `nonIgnoredMessages` filters in `instrumentBatchReceive` to avoid iterating twice


#### Consistent Partition Attribute (`instrumentation.ts:238`)
- [ ] Pick one: `messaging.destination.partition.id` vs `MESSAGING_KAFKA_PARTITION` alias

#### Consider WeakRef for Singletons (`instrumentation.ts`, `metrics.ts`)
- [ ] Evaluate using WeakRef for `globalInstrumentation` and `globalMetrics`

---

### Potential Enhancements
- [ ] Add `poll` span for consumer poll/receive operations (separate from `process` span)
- [ ] Add `commit` span for offset commit operations with `messaging.operation.type` = `settle`
- [ ] Add support for `OTEL_SEMCONV_STABILITY_OPT_IN` environment variable
- [ ] Add `peer.service` attribute for producer spans
- [ ] Add span links between producer and consumer spans
- [ ] Add support for custom MeterProvider injection
- [ ] Add observable gauges for consumer lag metrics
- [ ] Add metrics for rebalance events

### Documentation
- [x] Add OpenTelemetry setup examples to README
  - Created `example/otel-tracing-example.mjs` - Comprehensive tracing example
  - Created `example/otel-metrics-example.mjs` - Comprehensive metrics example
  - Created `example/README.md` - Documentation for all examples
- [x] Document all available metrics and their attributes
  - Documented in example/README.md and otel-metrics-example.mjs
- [ ] Add Grafana/Prometheus dashboard examples
- [ ] Document integration with popular observability platforms

## References

- [OpenTelemetry Messaging Metrics](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-metrics/)
- [OpenTelemetry Kafka Semantic Conventions](https://opentelemetry.io/docs/specs/semconv/messaging/kafka/)
- [OpenTelemetry Messaging Spans](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/)
