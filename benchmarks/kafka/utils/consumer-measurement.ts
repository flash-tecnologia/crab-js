import type { RunMeasurement } from './results.js'

export interface RunMeasurementHooks {
  onMeasureStart?: () => void
  onMeasureFinish?: () => void
  onMessage?: () => void
}

export interface ConsumerMeasurementOptions {
  iterations: number
  window: 'steady' | 'first-message'
  warmupMessages: number
}

export function createConsumerRunState(
  options: ConsumerMeasurementOptions,
  hooks: RunMeasurementHooks = {},
  now: () => bigint = process.hrtime.bigint,
) {
  return {
    options,
    hooks,
    now,
    createdAt: now(),
    firstDeliveryAt: undefined as bigint | undefined,
    startedAt: undefined as bigint | undefined,
    finishedAt: undefined as bigint | undefined,
    firstDeliveryMessages: 0,
    warmupMessages: 0,
    receivedMessages: 0,
    deliveries: 0,
    minDeliveryMessages: Infinity,
    maxDeliveryMessages: 0,
    measured: 0,
    finished: false,
  }
}

export type RunState = ReturnType<typeof createConsumerRunState>

function start(state: RunState) {
  // Hooks (CPU/GC/sampler setup) are outside the throughput clock.
  state.hooks.onMeasureStart?.()
  state.startedAt = state.now()
}

// Called once per delivery, not once per element inside a batch. Warmup consumes
// Whole deliveries so no already-converted prefix enters the measured count.
export function beginDelivery(state: RunState, count: number): boolean {
  if (state.finished || count <= 0) return false
  if (state.firstDeliveryAt === undefined) {
    state.firstDeliveryAt = state.now()
    state.firstDeliveryMessages = count
  }
  state.receivedMessages += count
  state.deliveries++
  state.minDeliveryMessages = Math.min(state.minDeliveryMessages, count)
  state.maxDeliveryMessages = Math.max(state.maxDeliveryMessages, count)
  if (state.startedAt === undefined) {
    if (state.options.window === 'steady') {
      state.warmupMessages += count
      if (state.warmupMessages >= state.options.warmupMessages) start(state)
      return false
    }
    start(state)
  }
  return true
}

export function observeMeasuredMessage(state: RunState): boolean {
  if (state.finished) return true
  state.hooks.onMessage?.()
  state.measured++
  return finishIfComplete(state)
}

function finishIfComplete(state: RunState): boolean {
  if (state.measured < state.options.iterations) return false
  state.finishedAt = state.now()
  state.finished = true
  state.hooks.onMeasureFinish?.()
  return true
}

export function observeMessage(state: RunState): boolean {
  if (state.finished) return true
  return beginDelivery(state, 1) && observeMeasuredMessage(state)
}

export function observeMessageCount(state: RunState, count: number): boolean {
  if (state.finished) return true
  if (!beginDelivery(state, count)) return false
  state.measured += Math.min(count, state.options.iterations - state.measured)
  return finishIfComplete(state)
}

export function finishRun(state: RunState): RunMeasurement {
  if (
    state.startedAt === undefined ||
    state.finishedAt === undefined ||
    state.firstDeliveryAt === undefined ||
    !state.finished
  ) {
    throw new Error(`Benchmark ended before ${state.options.iterations} measured messages were consumed`)
  }
  return {
    messages: state.measured,
    elapsedNs: Math.max(1, Number(state.finishedAt - state.startedAt)),
    consumer: {
      window: state.options.window,
      requestedWarmupMessages: state.options.warmupMessages,
      warmupMessages: state.warmupMessages,
      firstDeliveryMs: Number(state.firstDeliveryAt - state.createdAt) / 1e6,
      firstDeliveryMessages: state.firstDeliveryMessages,
      receivedMessages: state.receivedMessages,
      deliveries: state.deliveries,
      minDeliveryMessages: state.minDeliveryMessages,
      maxDeliveryMessages: state.maxDeliveryMessages,
    },
  }
}
