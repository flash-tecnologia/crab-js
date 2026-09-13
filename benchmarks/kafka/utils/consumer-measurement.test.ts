import assert from 'node:assert/strict'
import { test } from 'node:test'
import {
  beginDelivery,
  createConsumerRunState,
  finishRun,
  observeMeasuredMessage,
  observeMessage,
  observeMessageCount,
} from './consumer-measurement.js'

await test('steady excludes the whole initial delivery and includes the next receive wait', () => {
  let time = 0n
  const state = createConsumerRunState({ iterations: 5, window: 'steady', warmupMessages: 0 }, {}, () => time)
  time = 100_000_000n
  assert.equal(observeMessageCount(state, 4), false)
  time = 110_000_000n
  assert.equal(observeMessageCount(state, 3), false)
  time = 120_000_000n
  assert.equal(observeMessageCount(state, 4), true)
  const result = finishRun(state)
  assert.equal(result.messages, 5)
  assert.equal(result.elapsedNs, 20_000_000)
  assert.equal(result.consumer?.firstDeliveryMs, 100)
  assert.equal(result.consumer?.warmupMessages, 4)
  assert.equal(result.consumer?.receivedMessages, 11)
  assert.equal(result.consumer?.deliveries, 3)
})

await test('warmup crosses delivery boundaries without counting a partially warmed batch', () => {
  let time = 0n
  let starts = 0
  let finishes = 0
  const state = createConsumerRunState(
    { iterations: 2, window: 'steady', warmupMessages: 5 },
    {
      onMeasureStart() {
        starts++
      },
      onMeasureFinish() {
        finishes++
      },
    },
    () => time,
  )
  assert.equal(observeMessageCount(state, 3), false)
  assert.equal(starts, 0)
  time = 10n
  assert.equal(observeMessageCount(state, 3), false)
  assert.equal(starts, 1)
  time = 20n
  assert.equal(observeMessageCount(state, 2), true)
  assert.equal(observeMessageCount(state, 2), true)
  assert.equal(finishes, 1)
  assert.equal(finishRun(state).consumer?.warmupMessages, 6)
  assert.equal(finishRun(state).elapsedNs, 10)
})

await test('first-message retains the historical count of the initial batch', () => {
  let time = 0n
  const state = createConsumerRunState({ iterations: 5, window: 'first-message', warmupMessages: 0 }, {}, () => time)
  time = 100n
  assert.equal(observeMessageCount(state, 4), false)
  time = 120n
  assert.equal(observeMessageCount(state, 4), true)
  assert.equal(finishRun(state).elapsedNs, 20)
  assert.equal(finishRun(state).consumer?.warmupMessages, 0)
})

await test('serial timing hooks observe only measured messages', () => {
  let observed = 0
  let time = 0n
  const state = createConsumerRunState(
    { iterations: 2, window: 'steady', warmupMessages: 2 },
    {
      onMessage() {
        observed++
      },
    },
    () => time,
  )
  assert.equal(observeMessage(state), false)
  assert.equal(observeMessage(state), false)
  assert.equal(observed, 0)
  time = 10n
  assert.equal(observeMessage(state), false)
  time = 20n
  assert.equal(observeMessage(state), true)
  assert.equal(observed, 2)
  assert.equal(finishRun(state).messages, 2)
})

await test('batch element callbacks respect the measured target and empty deliveries do not start a window', () => {
  const state = createConsumerRunState({ iterations: 2, window: 'steady', warmupMessages: 0 })
  assert.equal(beginDelivery(state, 0), false)
  assert.equal(state.firstDeliveryAt, undefined)
  assert.equal(beginDelivery(state, 10), false)
  assert.equal(beginDelivery(state, 3), true)
  assert.equal(observeMeasuredMessage(state), false)
  assert.equal(observeMeasuredMessage(state), true)
  assert.equal(finishRun(state).messages, 2)
  assert.equal(finishRun(state).consumer?.firstDeliveryMessages, 10)
})

await test('exhaustion during warmup or a partial measured window is an error', () => {
  const state = createConsumerRunState({ iterations: 2, window: 'steady', warmupMessages: 5 })
  observeMessageCount(state, 3)
  assert.throws(() => finishRun(state), /before 2 measured messages/)
  observeMessageCount(state, 3)
  observeMessageCount(state, 1)
  assert.throws(() => finishRun(state), /before 2 measured messages/)
})
