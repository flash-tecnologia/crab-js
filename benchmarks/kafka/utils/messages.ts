import type { MessageProducer } from 'kafka-crab-js'
import { Buffer } from 'node:buffer'

const benchmarkMessageDate = '2024-01-01T00:00:00.000Z'
const benchmarkHeaderKey = 'benchmark-header'
const benchmarkHeaderValue = Buffer.from('benchmark-header-value')

export function createBenchmarkPartitionKeys(partitionCount: number): Buffer[] {
  return Array.from({ length: Math.max(1, partitionCount) }, (_, partition) => Buffer.from(`partition-${partition}`))
}

export function createBenchmarkMessagePayload(index: number): Buffer {
  return Buffer.from(
    JSON.stringify({
      message: `message index ${index}`,
      index,
      date: benchmarkMessageDate,
    }),
  )
}

export function createBenchmarkMessage(index: number, partitionKeys: readonly Buffer[]): MessageProducer {
  const key = partitionKeys[index % partitionKeys.length] ?? Buffer.from('partition-0')

  return {
    payload: createBenchmarkMessagePayload(index),
    key,
    headers: {
      [benchmarkHeaderKey]: benchmarkHeaderValue,
    },
  }
}
