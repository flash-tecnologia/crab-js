# Kafka API reference

The maintained reference is now [alongside kafka-crab-js](../../packages/kafka-crab-js/docs/api.md).
Use the [quick start](../../packages/kafka-crab-js/README.md#quick-start) for complete
examples. This page keeps the main wiki entry points available without duplicating
signatures or defaults.

## KafkaClient

[Configuration and factories](../../packages/kafka-crab-js/docs/api.md#kafkaclient),
including [TLS/SASL](../../packages/kafka-crab-js/docs/api.md#secure-connections).

## KafkaProducer

[Sending, flushing, and delivery results](../../packages/kafka-crab-js/docs/api.md#kafkaproducer),
including partial failures and tombstones. The producer has no `disconnect()` method;
await sends and manual flushes before shutdown.

## KafkaConsumer

[Subscription, assignment, receiving, commits, and shutdown](../../packages/kafka-crab-js/docs/api.md#kafkaconsumer).

## KafkaStreamReadable

[Node.js streams](../../packages/kafka-crab-js/docs/api.md#nodejs-streams) and
[Web Streams](../../packages/kafka-crab-js/docs/api.md#web-streams).

## KafkaBatchStreamReadable

[Node batch stream behavior](../../packages/kafka-crab-js/docs/api.md#nodejs-streams).
Node streams emit individual messages even with internal batching. For arrays,
use the [Web batch API](../../packages/kafka-crab-js/docs/api.md#web-streams).

## Types

[Message, metadata, offset, and configuration types](../../packages/kafka-crab-js/docs/api.md#types).

## OpenTelemetry (kafka-crab-js-otel)

[Instrumentation and SDK setup](../../packages/kafka-crab-js-otel/README.md).

## librdkafka Configuration

[Client configuration](../../packages/kafka-crab-js/docs/api.md#kafkaclient) and
[production profiles](../../packages/kafka-crab-js/README.md#production-recommendations).
