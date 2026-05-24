import { readPositiveInteger } from './env.js'

const defaultBrokers = ['localhost:9092']

function parseBrokers(input: string | undefined): string[] {
  if (!input) {
    return defaultBrokers
  }

  const values = input
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean)

  return values.length > 0 ? values : defaultBrokers
}

export const brokers = parseBrokers(process.env.KAFKA_BROKERS)
export const topic = process.env.BENCHMARK_TOPIC?.trim() || 'benchmarks'
export const partitionCount = readPositiveInteger('BENCHMARK_PARTITIONS', 3)

// This is needed by KafkaJS
process.env.KAFKAJS_NO_PARTITIONER_WARNING = '3'
