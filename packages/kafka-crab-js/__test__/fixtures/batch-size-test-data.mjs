import { nanoid } from 'nanoid'
import { Buffer } from 'node:buffer'

// Batch size test configurations
export const BATCH_SIZE_LIMITS = {
  MIN: 1,
  MAX: 10,
  DEFAULT: 10,
  OUT_OF_RANGE_LOW: 0,
  OUT_OF_RANGE_HIGH: 25,
  EXTREME_HIGH: 1000,
}

// Test scenarios for batch size validation
export const BATCH_SIZE_TEST_SCENARIOS = [
  {
    name: 'minimum_valid',
    batchSize: BATCH_SIZE_LIMITS.MIN,
    expected: BATCH_SIZE_LIMITS.MIN,
    shouldWarn: false,
    description: 'Test minimum valid batch size',
  },
  {
    name: 'maximum_valid',
    batchSize: BATCH_SIZE_LIMITS.MAX,
    expected: BATCH_SIZE_LIMITS.MAX,
    shouldWarn: false,
    description: 'Test maximum valid batch size',
  },
  {
    name: 'default_size',
    batchSize: BATCH_SIZE_LIMITS.DEFAULT,
    expected: BATCH_SIZE_LIMITS.DEFAULT,
    shouldWarn: false,
    description: 'Test default batch size',
  },
  {
    name: 'too_low',
    batchSize: BATCH_SIZE_LIMITS.OUT_OF_RANGE_LOW, // 0 - will trigger warning
    expected: BATCH_SIZE_LIMITS.DEFAULT, // Should fallback to default
    shouldWarn: true,
    description: 'Test batch size too low (should fallback to configured max) - WARNING EXPECTED',
  },
  {
    name: 'moderately_high',
    batchSize: BATCH_SIZE_LIMITS.OUT_OF_RANGE_HIGH,
    expected: BATCH_SIZE_LIMITS.MAX, // Should be clamped to max
    shouldWarn: true,
    description: 'Test batch size moderately above limit',
  },
  {
    name: 'extremely_high',
    batchSize: BATCH_SIZE_LIMITS.EXTREME_HIGH,
    expected: BATCH_SIZE_LIMITS.MAX, // Should be clamped to max
    shouldWarn: true,
    description: 'Test extremely high batch size',
  },
]

// Generate test messages for batch testing
export function createBatchTestMessages(count, testId = nanoid(6), messageSize = 'small') {
  const payloadSizes = {
    small: 100,
    medium: 1000,
    large: 10000,
  }

  const payloadSize = payloadSizes[messageSize] || payloadSizes.small
  const padding = 'x'.repeat(payloadSize)

  return Array.from({ length: count }, (_, i) => ({
    key: Buffer.from(`batch-key-${testId}-${i}`),
    headers: {
      'batch-test-id': Buffer.from(testId),
      'message-index': Buffer.from(i.toString()),
      'batch-size': Buffer.from(count.toString()),
      'message-size': Buffer.from(messageSize),
    },
    payload: Buffer.from(JSON.stringify({
      _id: i,
      testId,
      batchIndex: i,
      totalBatchSize: count,
      messageSize,
      timestamp: Date.now(),
      padding,
    })),
  }))
}

// Batch timeout test configurations
export const BATCH_TIMEOUT_SCENARIOS = [
  {
    name: 'very_short',
    timeoutMs: 50,
    expected: 50,
    shouldWarn: false,
    description: 'Very short timeout',
  },
  {
    name: 'default',
    timeoutMs: 100,
    expected: 100,
    shouldWarn: false,
    description: 'Default timeout',
  },
  {
    name: 'medium',
    timeoutMs: 1000,
    expected: 1000,
    shouldWarn: false,
    description: 'Medium timeout',
  },
  {
    name: 'maximum_valid',
    timeoutMs: 30000,
    expected: 30000,
    shouldWarn: false,
    description: 'Maximum valid timeout',
  },
  {
    name: 'too_low',
    timeoutMs: 0,
    expected: 100, // Should fallback to default
    shouldWarn: true,
    description: 'Timeout too low',
  },
  {
    name: 'too_high',
    timeoutMs: 50000,
    expected: 100, // Should fallback to default
    shouldWarn: true,
    description: 'Timeout too high',
  },
  {
    name: 'negative',
    timeoutMs: -100,
    expected: 100, // Should fallback to default
    shouldWarn: true,
    description: 'Negative timeout',
  },
]

// Performance test data
export const PERFORMANCE_TEST_CONFIGS = [
  {
    name: 'small_batch_small_messages',
    batchSize: 5,
    messageCount: 50,
    messageSize: 'small',
    description: 'Small batch with small messages',
  },
  {
    name: 'max_batch_small_messages',
    batchSize: 10,
    messageCount: 100,
    messageSize: 'small',
    description: 'Maximum batch size with small messages',
  },
  {
    name: 'max_batch_large_messages',
    batchSize: 10,
    messageCount: 50,
    messageSize: 'large',
    description: 'Maximum batch size with large messages',
  },
]

// Expected warning messages for validation
export const EXPECTED_WARNING_PATTERNS = [
  /size \d+ out of range \[1-\d+\], using \d+/, // Covers various maxBatchMessages values
  /batch_timeout_ms \d+ out of range \[1-30000\], using 100/,
]
