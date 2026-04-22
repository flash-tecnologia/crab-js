# Benchmark Suite

This directory contains benchmark tests for kafka-crab-js performance comparison with other Kafka clients.

## Setup

1. Install benchmark dependencies:

```bash
vp install
```

2. Make sure you have Kafka running (use the integration test setup):

```bash
cd ../__test__/integration
docker-compose up -d
# or
podman-compose up -d
```

3. Prepare the benchmark data:

```bash
vp run setup:consumer
```

## Running Benchmarks

```bash
vp run benchmark
```

## Dependencies

The benchmark suite uses separate dependencies to avoid installing heavy native modules in CI/CD:

- `@platformatic/rdkafka`: Platformatic's Kafka client
- `node-rdkafka`: Native librdkafka bindings
- `kafkajs`: Pure JavaScript Kafka client
- `cronometro`: Benchmarking utilities
- `nanoid`: ID generation for test data
