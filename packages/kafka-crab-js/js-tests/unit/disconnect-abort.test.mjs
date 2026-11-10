import { equal, rejects } from 'node:assert/strict'
import { getEventListeners } from 'node:events'
import { test } from 'vite-plus/test'

import { KafkaClient } from '../../js-src/index.js'

const createConsumer = () =>
  new KafkaClient({ brokers: '127.0.0.1:1', clientId: 'disconnect-abort-test', logLevel: 'error' }).createConsumer({
    groupId: `disconnect-abort-${crypto.randomUUID()}`,
    enableAutoCommit: false,
    configuration: { 'session.timeout.ms': '1000', 'heartbeat.interval.ms': '300' },
  })

// A commit without an available coordinator keeps close pending until its session deadline.
const withPendingCommit = async () => {
  const consumer = createConsumer()
  consumer.onEvents(() => undefined)
  await consumer.commit('topic', 0, 1, 'Async')
  return consumer
}

test('AbortSignal.timeout limits the wait while a subsequent caller awaits native close', async () => {
  const consumer = await withPendingCommit()
  try {
    const signal = AbortSignal.timeout(10)
    await rejects(consumer.disconnect({ signal }), (error) => error === signal.reason)
    await consumer.disconnect()
    equal(consumer.assignment().length, 0)
  } finally {
    await consumer.disconnect()
  }
})

test('aborting one caller preserves its reason, other listeners and concurrent close', async () => {
  const consumer = await withPendingCommit()
  const controller = new AbortController()
  const reason = new Error('shutdown deadline')
  let existingListenerCalls = 0
  controller.signal.onabort = () => {
    existingListenerCalls++
  }
  controller.signal.addEventListener('abort', (event) => {
    event.stopImmediatePropagation()
  })
  // Receive interruption is covered with a live broker in partition-lifecycle.test.mjs.
  // This unavailable broker can legitimately reject recv() before shutdown starts.
  try {
    const aborted = consumer.disconnect({ signal: controller.signal })
    const closed = consumer.disconnect()
    controller.abort(reason)
    await rejects(aborted, (error) => error === reason)
    equal(existingListenerCalls, 1)
    await closed
    equal(consumer.assignment().length, 0)
  } finally {
    await consumer.disconnect()
  }
})

test('an already aborted signal rejects its wait and still starts native close', async () => {
  const consumer = createConsumer()
  const reason = new Error('already aborted')
  try {
    await rejects(consumer.disconnect({ signal: AbortSignal.abort(reason) }), (error) => error === reason)
    await consumer.disconnect()
    equal(consumer.assignment().length, 0)
  } finally {
    await consumer.disconnect()
  }
})

test('successful close removes the abort listener and remains safe to call again', async () => {
  const consumer = createConsumer()
  const controller = new AbortController()
  const before = getEventListeners(controller.signal, 'abort').length
  await consumer.disconnect({ signal: controller.signal })
  equal(getEventListeners(controller.signal, 'abort').length, before)
  controller.abort()
  await consumer.disconnect()
  await consumer.disconnect({})
})

test('invalid signals reject without abandoning native cleanup', async () => {
  const consumer = createConsumer()
  try {
    await rejects(consumer.disconnect({ signal: {} }), TypeError)
    await consumer.disconnect()
  } finally {
    await consumer.disconnect()
  }
})
