export const topic = 'benchmarks'
const defaultBrokers = ['localhost:9092', 'localhost:9093', 'localhost:9094']

function parseBrokers(input: string | undefined): string[] {
  if (!input) {
    return defaultBrokers
  }

  const values = input
    .split(',')
    .map(value => value.trim())
    .filter(Boolean)

  return values.length > 0 ? values : defaultBrokers
}

export const brokers = parseBrokers(process.env.KAFKA_BROKERS)

// This is needed by KafkaJS
process.env.KAFKAJS_NO_PARTITIONER_WARNING = '3'
