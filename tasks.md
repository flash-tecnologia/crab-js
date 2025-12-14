# Tasks - OpenTelemetry Instrumentation

## Plan

- [x] 1. Align spans with OTEL messaging + Kafka semantic conventions (`send|poll|process <topic>`).
- [x] 2. Make payload/body size opt-in (`captureMessagePayload=false` omits `messaging.message.body.size`) + add negative test.
- [x] 3. Document consumer span lifecycle + add ergonomic helpers (`endSpan`, `processMessage`, `processBatch`).
- [x] 4. Remove WeakMap tracking; when OTEL is enabled, attach `otelContext`/`span`/`endSpan` to returned messages/batches.
- [x] 5. Make `clientId` optional on `KafkaClient` (default: `rdkafka`).
- [x] 6. Add Grafana validation example (produce 100, consume 50 fast + 50 delayed) + validate via Grafana HTTP API.
- [x] 7. Stabilize OTEL integration tests (skip when Kafka unavailable + add negative “endSpan not called” check).

## References

- [OpenTelemetry Messaging Metrics](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-metrics/)
- [OpenTelemetry Kafka Semantic Conventions](https://opentelemetry.io/docs/specs/semconv/messaging/kafka/)
- [OpenTelemetry Messaging Spans](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/)
