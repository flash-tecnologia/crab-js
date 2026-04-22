# kafka-crab-js Examples

This directory contains example code demonstrating various features of kafka-crab-js.

## Basic Examples

### simple.mjs

Basic producer and consumer example showing fundamental Kafka operations.

KAFKA_AVAILABLE=true node simple.mjs

````

### stream-sample.mjs
Demonstrates using Kafka consumers with Node.js streams.

```bash
KAFKA_AVAILABLE=true node stream-sample.mjs
````

### events.mjs

Shows how to handle Kafka consumer events (rebalance, errors, etc.).

```bash
KAFKA_AVAILABLE=true node events.mjs
```

### kafka-consumer-with-retry.mjs

Example of implementing retry logic for failed message processing.

```bash
KAFKA_AVAILABLE=true node kafka-consumer-with-retry.mjs
```

### batch-usage-examples.mjs

Demonstrates batch processing for higher throughput.

```bash
KAFKA_AVAILABLE=true node batch-usage-examples.mjs
```

## OpenTelemetry Examples

### otel-tracing-example.mjs

**Comprehensive OpenTelemetry tracing example** showing:

- Automatic instrumentation for Kafka operations
- Trace context propagation between producer and consumer
- Custom span creation and attributes
- Integration with Jaeger/OTLP backends
- Producer and consumer hooks
- Manual OTEL context usage

**Prerequisites:**

```bash
# Start Kafka
docker-compose up -d

# (Optional) Start Jaeger for trace visualization
docker run -d \
  --name jaeger \
  -p 16686:16686 \
  -p 4318:4318 \
  jaegertracing/all-in-one:latest
```

**Run with console exporter (default):**

```bash
KAFKA_AVAILABLE=true node otel-tracing-example.mjs
```

**Run with OTLP exporter (Jaeger):**

```bash
KAFKA_AVAILABLE=true \
OTEL_EXPORTER_TYPE=otlp \
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 \
node otel-tracing-example.mjs
```

Then open http://localhost:16686 to view traces in Jaeger UI.

### otel-metrics-example.mjs

**Comprehensive OpenTelemetry metrics example** showing:

- Automatic metrics collection for Kafka operations
- Custom histogram bucket configuration
- Producer and consumer metrics
- Batch processing metrics
- Integration with Prometheus/OTLP backends

**Metrics collected:**

- `messaging.client.operation.duration` - Producer/consumer operation latency
- `messaging.client.sent.messages` - Number of messages sent
- `messaging.client.consumed.messages` - Number of messages consumed
- `messaging.process.duration` - Message processing latency

**Run:**

```bash
KAFKA_AVAILABLE=true node otel-metrics-example.mjs
```

**With Prometheus (optional):**

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'kafka-crab'
    scrape_interval: 10s
    static_configs:
      - targets: ['localhost:9464']
```

```bash
# Start Prometheus
docker run -d \
  -p 9090:9090 \
  -v $(pwd)/prometheus.yml:/etc/prometheus/prometheus.yml \
  prom/prometheus
```

## Environment Variables

- `KAFKA_AVAILABLE` - Set to `true` to run examples (prevents running without Kafka)
- `KAFKA_BROKERS` - Kafka broker addresses (default: `localhost:9092`)
- `KAFKA_TOPIC` - Topic name to use (default: auto-generated)
- `OTEL_EXPORTER_TYPE` - Trace exporter type: `console` or `otlp` (default: `console`)
- `OTEL_EXPORTER_OTLP_ENDPOINT` - OTLP endpoint URL (default: `http://localhost:4318`)

## OpenTelemetry Configuration Options

### Complete OTEL Configuration Example

```javascript
import { KafkaClient } from 'kafka-crab-js'
import { enableOtelInstrumentation } from 'kafka-crab-js-otel'

// Enable the OTEL adapter once at startup (subscribes to diagnostics channels)
enableOtelInstrumentation({
  // Tracing configuration
  captureMessagePayload: true, // Include payload in spans (default: false)
  maxPayloadSize: 1024, // Max payload size in bytes (default: 1024)
  captureMessageHeaders: true, // Include headers in spans (default: true)
  enableBatchInstrumentation: true, // Enable batch spans (default: true)

  // Topic filtering
  ignoreTopics: ['__consumer_offsets'], // Array or function to ignore topics

  // Metrics configuration
  metrics: {
    enabled: true, // Enable metrics (default: false in examples)
    meterProvider: customMeterProvider, // Optional custom meter provider
    includePartitionId: true, // Include partition in labels (default: true)
    serverAddress: 'localhost', // Broker address for metrics
    serverPort: 9092, // Broker port for metrics
    histogramBuckets: [0.001, 0.01, 0.1, 1, 10], // Optional custom buckets (seconds)
  },

  // Custom hooks for advanced use cases
  producerHook: (span, record, metadata) => {
    span.setAttribute('custom.key', 'value')
    if (metadata) span.setAttribute('custom.partition', metadata.partition)
  },
  messageHook: (span, message) => {
    span.setAttribute('custom.key', 'value')
    if (message.headers?.['user-id']) {
      span.setAttribute('user.id', message.headers['user-id'].toString())
    }
  },
})

const kafkaClient = new KafkaClient({
  brokers: 'localhost:9092',
  clientId: 'my-client',
  diagnostics: true, // keep diagnostics on so the OTEL adapter receives events
})
```

Tip: When consuming via streams or batches, call `message.endSpan()` (or `batch.endSpan()` when provided) after processing to close the tracing span. The stream examples already do this for you.

### Semantic Conventions Compliance

kafka-crab-js fully implements [OpenTelemetry Semantic Conventions for Messaging Systems](https://opentelemetry.io/docs/specs/semconv/messaging/kafka/):

**Span Attributes:**

- `messaging.system` = `"kafka"`
- `messaging.operation.name` - `send`, `receive`, `process`
- `messaging.operation.type` - `send`, `receive`, `process`
- `messaging.destination.name` - Topic name
- `messaging.destination.partition.id` - Partition number
- `messaging.kafka.offset` - Message offset
- `messaging.kafka.message.key` - Message key
- `messaging.client.id` - Client ID
- `messaging.consumer.group.name` - Consumer group
- `server.address` / `server.port` - Broker info

**Metric Attributes:**

- All span attributes plus:
- `error.type` - Error classification (only on error)

## Testing

All examples require a running Kafka broker. Use the included Docker Compose:

```bash
# Start Kafka
docker-compose up -d

# Run examples
KAFKA_AVAILABLE=true node simple.mjs

# Stop Kafka
docker-compose down
```

## Tips

1. **Start with simple.mjs** to understand basic operations
2. **Use otel-tracing-example.mjs** to see distributed tracing in action
3. **Use otel-metrics-example.mjs** to monitor performance
4. **Combine tracing and metrics** for full observability
5. **Check Jaeger UI** (http://localhost:16686) to visualize trace propagation

## Troubleshooting

**"Connection refused" errors:**

- Ensure Kafka is running: `docker-compose ps`
- Check broker address: `KAFKA_BROKERS=localhost:9092`

**No traces in Jaeger:**

- Verify Jaeger is running: `docker ps | grep jaeger`
- Check OTLP endpoint: `OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318`
- Ensure `OTEL_EXPORTER_TYPE=otlp` is set

**Metrics not appearing:**

- Wait for export interval (10 seconds by default)
- Check console output for metric exports
- Verify MeterProvider configuration

## Resources

- [OpenTelemetry JavaScript SDK](https://opentelemetry.io/docs/instrumentation/js/)
- [Kafka Semantic Conventions](https://opentelemetry.io/docs/specs/semconv/messaging/kafka/)
- [Jaeger Documentation](https://www.jaegertracing.io/docs/)
- [Prometheus Documentation](https://prometheus.io/docs/)
