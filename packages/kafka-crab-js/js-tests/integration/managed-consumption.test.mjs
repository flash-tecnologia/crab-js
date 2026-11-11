// RFC-0017 managed consumption against a real broker.
// Run: KAFKA_BROKERS=127.0.0.1:29092 node --test js-tests/integration/managed-consumption.test.mjs
// A missing broker fails these tests; nothing here is skipped.
import { deepEqual, equal, notEqual, ok, rejects } from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import test from 'node:test'
import { setTimeout as sleep } from 'node:timers/promises'

import { KafkaClient, MANAGED_CONSUMPTION_VERSION, MANAGED_ERROR_CODES } from '../../dist/index.js'

const brokers = process.env.KAFKA_BROKERS ?? '127.0.0.1:9092'

const hasCode = (code) => (error) => {
  equal(error?.code, code, `expected ${code}, got ${error?.code}: ${error?.message}`)
  return true
}

async function until(condition, description, timeoutMs = 30_000) {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    const value = await condition()
    if (value) {
      return value
    }
    await sleep(50)
  }
  throw new Error(`Timed out waiting for ${description}`)
}

async function environment(name, numPartitions) {
  const topic = `managed-${name}-${randomUUID()}`
  const client = new KafkaClient({ brokers, clientId: topic, logLevel: 'error' })
  const admin = client.createAdmin()
  const producer = client.createProducer({ autoFlush: true })
  const [created] = await admin.createTopics({ topics: [{ topic, numPartitions, replicationFactor: 1 }] })
  ok(!created.error, created.error?.message)
  let nextId = 0
  const produced = new Set()
  const env = {
    topic,
    client,
    admin,
    produced,
    produce: async (count) => {
      const messages = Array.from({ length: count }, () => {
        const id = nextId++
        produced.add(id)
        return {
          partition: id % numPartitions,
          key: Buffer.from(`key-${id}`),
          headers: { id: Buffer.from(String(id)) },
          payload: Buffer.from(JSON.stringify({ id })),
        }
      })
      await producer.send({ topic, messages })
    },
    /** Keeps traffic flowing until the returned function is awaited. */
    traffic: (batch = 10, intervalMs = 20) => {
      let running = true
      const loop = (async () => {
        while (running) {
          await env.produce(batch)
          await sleep(intervalMs)
        }
      })()
      return async () => {
        running = false
        await loop
      }
    },
    consumer: (strategy, managedOptions) =>
      client.createConsumer({
        groupId: topic,
        managed: true,
        managedOptions,
        configuration: {
          'auto.offset.reset': 'earliest',
          'session.timeout.ms': '6000',
          'heartbeat.interval.ms': '300',
          ...(strategy ? { 'partition.assignment.strategy': strategy } : {}),
        },
      }),
    committed: async () => {
      const [offsets] = await admin.fetchOffsets({ groupId: topic, topics: [topic] })
      return new Map(offsets.partitions.map(({ partition, offset }) => [partition, offset]))
    },
    highWatermarks: async () =>
      new Map((await admin.fetchTopicOffsets(topic)).map(({ partition, high }) => [partition, high])),
    legacyConsumer: (strategy) =>
      client.createConsumer({
        groupId: topic,
        enableAutoCommit: false,
        configuration: {
          'auto.offset.reset': 'earliest',
          'session.timeout.ms': '6000',
          'heartbeat.interval.ms': '300',
          'partition.assignment.strategy': strategy,
        },
      }),
    close: async () => {
      await producer.flush(1000)
      await admin.close()
    },
  }
  return env
}

/** Drives one managed consumer the way a runtime would and records what it observed. */
class Member {
  constructor(consumer, topic) {
    this.consumer = consumer
    this.topic = topic
    this.owned = new Map()
    this.revoked = new Map()
    this.transitions = []
    this.received = new Set()
    this.duplicates = 0
    this.violations = []
    this.staleCommits = 0
    this.brokerCommitErrors = []
    this.next = new Map()
    this.streams = new Map()
    this.firstOffsets = new Map()
    this.stopped = false
  }

  start() {
    this.loop = (async () => {
      while (!this.stopped) {
        const record = await this.consumer.recvManaged(200)
        if (record === null) {
          return
        }
        await this.handle(record)
      }
    })()
    this.loop.catch((error) => {
      this.error = error
    })
    return this
  }

  async handle(record) {
    if (record.kind === 'assign') {
      this.transitions.push(record)
      for (const { partition, assignment, previousAssignment } of record.partitions) {
        if (previousAssignment === undefined) {
          if (this.streams.has(assignment)) {
            this.violations.push(`identity ${assignment} was issued twice`)
          }
          // The messages delivered under one identity form one fetch stream.
          this.streams.set(assignment, { partition, last: undefined })
        } else if (previousAssignment !== assignment || this.owned.get(partition) !== assignment) {
          this.violations.push(`assign of ${partition} does not retain ${this.owned.get(partition)}`)
        }
        this.owned.set(partition, assignment)
      }
    } else if (record.kind === 'revoke') {
      this.transitions.push(record)
      for (const { partition, previousAssignment } of record.partitions) {
        if (this.owned.get(partition) !== previousAssignment) {
          this.violations.push(`revoke of ${partition} names ${previousAssignment}, owned ${this.owned.get(partition)}`)
        }
        this.owned.delete(partition)
        this.next.delete(partition)
        this.revoked.set(partition, [...(this.revoked.get(partition) ?? []), previousAssignment])
      }
    } else if (record.kind === 'message') {
      const { message, assignment } = record
      // The ordering contract as JavaScript sees it: the identity of a message
      // is the one announced by the latest transition delivered before it.
      if (this.owned.get(message.partition) !== assignment) {
        this.violations.push(`message ${message.partition}@${message.offset} carries ${assignment}`)
      }
      // And against the assignment that really owned the offset: an identity
      // belongs to one partition and its offsets are one contiguous stream
      // that starts where that assignment started fetching. A message fetched
      // under an earlier assignment but stamped with this identity would
      // break the sequence.
      const stream = this.streams.get(assignment)
      if (stream?.partition !== message.partition) {
        this.violations.push(`identity ${assignment} does not belong to partition ${message.partition}`)
      } else if (stream.last !== undefined && message.offset !== stream.last + 1) {
        this.violations.push(`${message.partition}@${message.offset} follows ${stream.last} under ${assignment}`)
      } else {
        stream.last = message.offset
      }
      if (!this.firstOffsets.has(message.partition)) {
        this.firstOffsets.set(message.partition, message.offset)
      }
      const { id } = JSON.parse(message.payload.toString())
      if (message.headers?.id?.toString() !== String(id) || message.key?.toString() !== `key-${id}`) {
        this.violations.push(`message ${id} lost its key or headers`)
      }
      if (this.received.has(id)) {
        this.duplicates++
      }
      this.received.add(id)
      this.next.set(message.partition, message.offset + 1)
      try {
        await this.consumer.commitManaged(this.topic, message.partition, assignment, message.offset + 1)
      } catch (error) {
        if (error.code === MANAGED_ERROR_CODES.STALE_ASSIGNMENT) {
          this.staleCommits++
        } else if (error.code === MANAGED_ERROR_CODES.COMMIT_FAILED) {
          this.brokerCommitErrors.push(error.kafkaCode)
        } else {
          throw error
        }
      }
    }
  }

  check() {
    if (this.error) {
      throw this.error
    }
    deepEqual(this.violations, [])
  }

  async stop() {
    this.stopped = true
    this.consumer.cancelManagedReceive()
    await this.loop
    this.check()
  }
}

const missing = (produced, ...members) => [...produced].filter((id) => !members.some((m) => m.received.has(id)))

test('capability is versioned', () => {
  equal(MANAGED_CONSUMPTION_VERSION, 1)
})

test('initial assignment is delivered as control on an idle topic', { timeout: 60_000 }, async () => {
  const env = await environment('idle', 2)
  const consumer = env.consumer()
  try {
    await consumer.subscribe(env.topic)
    const assign = await until(async () => {
      const record = await consumer.recvManaged(500)
      return record?.kind === 'assign' ? record : undefined
    }, 'the first assignment')
    deepEqual(assign.partitions.map(({ partition }) => partition).toSorted(), [0, 1])
    const [first, second] = assign.partitions
    notEqual(first.assignment, second.assignment)
    ok(consumer.isManagedAssignmentActive(env.topic, first.partition, first.assignment))
    // No message exists: an empty result is idle, not end-of-stream.
    deepEqual(await consumer.recvManaged(300), { kind: 'idle', reason: 'timeout' })
    const state = consumer.managedState()
    equal(state.status, 'active')
    equal(state.assignments.length, 2)
    equal(state.bufferedMessages, 0)
    // A commit on an idle partition is confirmed by the broker.
    await consumer.commitManaged(env.topic, first.partition, first.assignment, 0)
    equal((await env.committed()).get(first.partition), '0')
  } finally {
    await consumer.disconnect()
    await env.close()
  }
})

for (const strategy of ['cooperative-sticky', 'range']) {
  test(
    `${strategy}: a member joins and leaves; identities change only on revocation and stale commits are rejected`,
    { timeout: 180_000 },
    async () => {
      const partitions = 4
      const env = await environment(strategy, partitions)
      const a = new Member(env.consumer(strategy), env.topic)
      const b = new Member(env.consumer(strategy), env.topic)
      let stopTraffic = async () => undefined
      try {
        await a.consumer.subscribe(env.topic)
        a.start()
        // Messages keep arriving through both rebalances.
        stopTraffic = env.traffic()
        await until(() => a.owned.size === partitions && a.received.size >= 60, 'the first member to consume')
        a.check()
        const before = new Map(a.owned)
        const transitionsBefore = a.transitions.length

        await b.consumer.subscribe(env.topic)
        b.start()
        await until(
          () => b.owned.size > 0 && a.owned.size + b.owned.size === partitions && a.revoked.size > 0,
          'the second member to take partitions',
        )
        a.check()
        b.check()
        const moved = [...b.owned.keys()]
        ok(moved.length > 0 && moved.length < partitions)
        for (const partition of moved) {
          const old = before.get(partition)
          deepEqual(a.revoked.get(partition), [old])
          // Authorization is gone natively, and a commit under it is refused
          // without being dispatched.
          equal(a.consumer.isManagedAssignmentActive(env.topic, partition, old), false)
          await rejects(
            a.consumer.commitManaged(env.topic, partition, old, 1),
            hasCode(MANAGED_ERROR_CODES.STALE_ASSIGNMENT),
          )
          // The other member's identity is not valid here either.
          await rejects(
            a.consumer.commitManaged(env.topic, partition, b.owned.get(partition), 1),
            hasCode(MANAGED_ERROR_CODES.UNKNOWN_ASSIGNMENT),
          )
        }
        for (const [partition, identity] of a.owned) {
          if (strategy === 'cooperative-sticky') {
            // Retained partitions were never revoked and keep their identity,
            // including across the empty assign that ends the rebalance.
            equal(identity, before.get(partition))
            equal(a.revoked.has(partition), false)
          } else {
            // The eager protocol revokes everything: same partition, new identity.
            notEqual(identity, before.get(partition))
            deepEqual(a.revoked.get(partition), [before.get(partition)])
          }
        }
        ok(a.transitions.length > transitionsBefore)

        const sharedFrom = b.received.size
        await until(() => b.received.size >= sharedFrom + 60, 'the second member to consume')

        // The second member leaves: its partitions return to the first one
        // under identities that differ from every earlier one.
        await b.stop()
        await b.consumer.disconnect()
        await until(() => a.owned.size === partitions, 'the partitions to return')
        a.check()
        for (const partition of moved) {
          const [old] = a.revoked.get(partition)
          const current = a.owned.get(partition)
          notEqual(current, old)
          await rejects(
            a.consumer.commitManaged(env.topic, partition, old, 1),
            hasCode(MANAGED_ERROR_CODES.STALE_ASSIGNMENT),
          )
        }

        await sleep(500)
        await stopTraffic()
        await until(() => missing(env.produced, a, b).length === 0, 'the remaining messages', 60_000)
        // Produced ids against received ids: replays are allowed, gaps are not.
        deepEqual(missing(env.produced, a, b), [])
        ok(env.produced.size >= 300, `${env.produced.size}`)

        // Restart from the broker-confirmed offsets. A commit beyond processed
        // work would leave a gap here; a commit behind it only replays.
        await a.stop()
        const processed = new Map(a.next)
        await env.produce(100)
        await a.consumer.disconnect()
        const committed = await env.committed()
        for (const [partition, next] of processed) {
          ok(Number(committed.get(partition) ?? 0) <= next, `partition ${partition} committed beyond processed work`)
        }
        const c = new Member(env.consumer(strategy), env.topic)
        try {
          await c.consumer.subscribe(env.topic)
          c.start()
          await until(() => missing(env.produced, a, b, c).length === 0, 'the restarted member', 60_000)
          c.check()
          equal(c.firstOffsets.size, partitions)
          for (const [partition, first] of c.firstOffsets) {
            equal(
              first,
              Number(committed.get(partition) ?? 0),
              `partition ${partition} restarts at its committed offset`,
            )
          }
          deepEqual(missing(env.produced, a, b, c), [])
        } finally {
          c.stopped = true
          await c.consumer.disconnect()
        }
        console.log(
          `[${strategy}] produced=${env.produced.size} ` +
            `replays=${[a, b, c].reduce((sum, m) => sum + m.duplicates + m.received.size, 0) - env.produced.size} ` +
            `staleCommits=${a.staleCommits + b.staleCommits} ` +
            `brokerCommitErrors=${JSON.stringify([...a.brokerCommitErrors, ...b.brokerCommitErrors])} ` +
            `transitions(a)=${a.transitions.length}`,
        )
      } finally {
        await stopTraffic()
        a.stopped = true
        b.stopped = true
        await Promise.all([a.consumer.disconnect(), b.consumer.disconnect()])
        await env.close()
      }
    },
  )
}

test(
  'reproduction: the broker accepts a legacy commit for a revoked partition; the managed commit is refused',
  { timeout: 120_000 },
  async () => {
    const strategy = 'cooperative-sticky'
    const env = await environment('reproduction', 4)
    const legacy = env.legacyConsumer(strategy)
    const managed = env.consumer(strategy)
    const other = env.consumer(strategy)
    const partitionsOf = (consumer) => consumer.assignment().map(({ partitionOffset }) => partitionOffset[0].partition)
    try {
      // Legacy contract: the member stays in the group after losing a
      // partition, and nothing ties its commit to the lost assignment.
      await legacy.subscribe(env.topic)
      await until(async () => {
        await legacy.recvBatch(1, 100)
        return legacy.assignment().length === 4
      }, 'the legacy member to own every partition')
      await other.subscribe(env.topic)
      const taken = await until(async () => {
        await legacy.recvBatch(1, 100)
        const owned = partitionsOf(legacy)
        const moved = other.managedState().assignments.map(({ partition }) => partition)
        return owned.length + moved.length === 4 && moved.length > 0 ? moved : undefined
      }, 'the rebalance to move partitions')
      const [lost] = taken
      ok(!partitionsOf(legacy).includes(lost))
      // The previous owner commits an arbitrary offset for a partition it no
      // longer owns, and the broker confirms it.
      await legacy.commit(env.topic, lost, 41, 'Sync')
      equal((await env.committed()).get(lost), '41')
      await legacy.disconnect()

      // Managed contract, same scenario: the identity of the lost assignment
      // cannot authorize a dispatch, so the committed offset does not move.
      await managed.subscribe(env.topic)
      const member = new Member(managed, env.topic).start()
      await until(() => member.revoked.size > 0 || member.owned.size > 0, 'the managed member to join')
      const third = env.consumer(strategy)
      try {
        await other.disconnect()
        await until(() => member.owned.size === 4, 'the managed member to own every partition')
        const before = new Map(member.owned)
        await third.subscribe(env.topic)
        const [revoked] = await until(() => {
          const lostNow = [...before.keys()].filter((p) => member.owned.get(p) !== before.get(p))
          return lostNow.length > 0 ? lostNow : undefined
        }, 'a revocation')
        const snapshot = (await env.committed()).get(revoked)
        await rejects(
          managed.commitManaged(env.topic, revoked, before.get(revoked), 99),
          hasCode(MANAGED_ERROR_CODES.STALE_ASSIGNMENT),
        )
        equal((await env.committed()).get(revoked), snapshot)
        await member.stop()
      } finally {
        await third.disconnect()
      }
    } finally {
      await Promise.all([legacy.disconnect(), managed.disconnect(), other.disconnect()])
      await env.close()
    }
  },
)

test('a managed commit is confirmed by the broker and resumes a later consumer', { timeout: 60_000 }, async () => {
  const env = await environment('commit', 1)
  const first = env.consumer()
  const second = env.consumer()
  const receive = async (consumer, kind) =>
    until(async () => {
      const record = await consumer.recvManaged(500)
      return record?.kind === kind ? record : undefined
    }, `a ${kind} record`)
  try {
    await env.produce(10)
    await first.subscribe(env.topic)
    const [{ assignment }] = (await receive(first, 'assign')).partitions
    for (let offset = 0; offset < 6; offset++) {
      const { message } = await receive(first, 'message')
      equal(message.offset, offset)
    }
    // The promise is the broker result: the offset is visible right after it.
    await first.commitManaged(env.topic, 0, assignment, 6)
    equal((await env.committed()).get(0), '6')
    equal(first.managedState().pendingCommits, 0)
    await first.disconnect()

    await second.subscribe(env.topic)
    await receive(second, 'assign')
    equal((await receive(second, 'message')).message.offset, 6)
  } finally {
    await Promise.all([first.disconnect(), second.disconnect()])
    await env.close()
  }
})

test(
  'control progresses natively while the application consumes nothing and JavaScript is blocked',
  { timeout: 120_000 },
  async () => {
    const env = await environment('backpressure', 2)
    const limit = 16
    const a = env.consumer('range', { maxBufferedMessages: limit })
    const b = env.consumer('range', { maxBufferedMessages: limit })
    const memberA = new Member(a, env.topic)
    const memberB = new Member(b, env.topic)
    try {
      await env.produce(300)
      await a.subscribe(env.topic)
      // The application never calls recvManaged here. The native pump still
      // joins the group, captures the assignment and fills the bounded buffer.
      const paused = await until(() => {
        const state = a.managedState()
        return state.backpressurePaused ? state : undefined
      }, 'retained messages to reach the limit')
      equal(paused.assignments.length, 2)
      await sleep(1000)
      const bounded = a.managedState()
      // Bounded by pause: far below the 300 available, with no growth over time.
      ok(bounded.bufferedMessages >= limit && bounded.bufferedMessages <= limit + 2, `${bounded.bufferedMessages}`)
      equal(bounded.queuedControlRecords, 1)
      const before = bounded.assignments

      // A second member joins while this thread is blocked: no JavaScript of
      // either consumer runs during the rebalance.
      await b.subscribe(env.topic)
      Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 8000)
      const after = a.managedState()
      const joined = b.managedState()
      equal(after.status, 'active')
      equal(after.assignments.length + joined.assignments.length, 2)
      equal(joined.assignments.length, 1)
      for (const { partition, assignment } of before) {
        // Old identities were invalidated natively, before any receive.
        equal(a.isManagedAssignmentActive(env.topic, partition, assignment), false)
      }
      await rejects(
        a.commitManaged(env.topic, before[0].partition, before[0].assignment, 1),
        hasCode(MANAGED_ERROR_CODES.STALE_ASSIGNMENT),
      )
      // Messages buffered under the revoked identities were released.
      ok(after.purgedMessages >= limit, `${after.purgedMessages}`)
      ok(after.bufferedMessages <= limit + 2)

      // Once the application consumes, the transitions arrive first and in
      // order, and nothing produced is missing.
      memberA.start()
      memberB.start()
      await until(() => missing(env.produced, memberA, memberB).length === 0, 'every message', 60_000)
      memberA.check()
      memberB.check()
      deepEqual(
        memberA.transitions.map(({ kind }) => kind),
        ['assign', 'revoke', 'assign'],
      )
      equal(a.managedState().backpressurePaused, false)
      console.log(
        `[backpressure] limit=${limit} peakObserved=${bounded.bufferedMessages} purged=${after.purgedMessages} ` +
          `duplicates=${memberA.duplicates + memberB.duplicates}`,
      )
    } finally {
      memberA.stopped = true
      memberB.stopped = true
      await Promise.all([a.disconnect(), b.disconnect()])
      await env.close()
    }
  },
)

test(
  'control overflow invalidates authorization natively and fails the consumer closed',
  { timeout: 120_000 },
  async () => {
    const env = await environment('overflow', 2)
    const a = env.consumer('range', { maxControlRecords: 1 })
    const b = env.consumer('range')
    try {
      await a.subscribe(env.topic)
      const { assignments } = await until(() => {
        const state = a.managedState()
        return state.assignments.length === 2 ? state : undefined
      }, 'the first assignment')
      equal(a.managedState().queuedControlRecords, 1)

      // The next transition does not fit in the control budget.
      await b.subscribe(env.topic)
      const failed = await until(() => {
        const state = a.managedState()
        return state.status === 'failed' ? state : undefined
      }, 'the control overflow')
      equal(failed.failure, MANAGED_ERROR_CODES.CONTROL_OVERFLOW)
      deepEqual(failed.assignments, [])
      equal(failed.queuedControlRecords, 0)
      for (const { partition, assignment } of assignments) {
        equal(a.isManagedAssignmentActive(env.topic, partition, assignment), false)
      }
      // JavaScript learns about it only now; authorization was already gone.
      await rejects(a.recvManaged(), hasCode(MANAGED_ERROR_CODES.CONTROL_OVERFLOW))
      await rejects(
        a.commitManaged(env.topic, assignments[0].partition, assignments[0].assignment, 0),
        hasCode(MANAGED_ERROR_CODES.CONTROL_OVERFLOW),
      )
      // The failed consumer leaves the group instead of holding partitions,
      // and cannot rejoin it.
      await until(() => b.managedState().assignments.length === 2, 'the healthy member to own every partition')
      await rejects(a.subscribe(env.topic), /ERR_MANAGED_CONTROL_OVERFLOW.*subscribe is not available/)
    } finally {
      await Promise.all([a.disconnect(), b.disconnect()])
      await env.close()
    }
  },
)

test(
  'the receive watchdog releases the partitions of an application that stops receiving',
  { timeout: 60_000 },
  async () => {
    const env = await environment('watchdog', 2)
    const stuck = env.consumer('range', { maxReceiveIdleMs: 1500 })
    const healthy = new Member(env.consumer('range'), env.topic)
    try {
      await stuck.subscribe(env.topic)
      await healthy.consumer.subscribe(env.topic)
      healthy.start()
      const { assignments } = await until(() => {
        const state = stuck.managedState()
        return state.assignments.length > 0 ? state : undefined
      }, 'the stuck member to own partitions')
      // It never calls recvManaged. The native poller would keep it in the
      // group forever; the watchdog fails it instead.
      const failed = await until(() => {
        const state = stuck.managedState()
        return state.status === 'failed' ? state : undefined
      }, 'the watchdog')
      equal(failed.failure, MANAGED_ERROR_CODES.RECEIVE_STALLED)
      equal(stuck.isManagedAssignmentActive(env.topic, assignments[0].partition, assignments[0].assignment), false)
      await rejects(stuck.recvManaged(), hasCode(MANAGED_ERROR_CODES.RECEIVE_STALLED))
      await until(() => healthy.owned.size === 2, 'the healthy member to own every partition')
      healthy.check()
    } finally {
      healthy.stopped = true
      await Promise.all([stuck.disconnect(), healthy.consumer.disconnect()])
      await env.close()
    }
  },
)

test(
  'commits issued continuously across a revocation settle within the contract and lose nothing',
  { timeout: 180_000 },
  async () => {
    const env = await environment('inflight', 2)
    const a = env.consumer('range')
    const b = new Member(env.consumer('range'), env.topic)
    const owned = new Map()
    const position = new Map()
    const first = { received: new Set() }
    const across = []
    const outcomes = {}
    let stopTraffic = async () => undefined
    let running = true
    // Commits are fired without waiting, so one is always in flight.
    const commit = (partition, identity, offset) => {
      a.commitManaged(env.topic, partition, identity, offset)
        .then(
          () => 'confirmed',
          (error) => error.code,
        )
        .then((outcome) => {
          outcomes[outcome] = (outcomes[outcome] ?? 0) + 1
          const dispatched = outcome === 'confirmed' || outcome === MANAGED_ERROR_CODES.COMMIT_FAILED
          if (dispatched && running && !a.isManagedAssignmentActive(env.topic, partition, identity)) {
            // The broker answered a request whose identity had been revoked.
            across.push({ partition, identity, offset, outcome })
          }
        })
    }
    try {
      await a.subscribe(env.topic)
      const loop = (async () => {
        while (running) {
          const record = await a.recvManaged(50)
          if (record === null) {
            return
          }
          if (record.kind === 'assign') {
            for (const { partition, assignment } of record.partitions) {
              owned.set(partition, assignment)
            }
          } else if (record.kind === 'revoke') {
            for (const { partition } of record.partitions) {
              owned.delete(partition)
              position.delete(partition)
            }
          } else if (record.kind === 'message') {
            const { partition, offset, payload } = record.message
            first.received.add(JSON.parse(payload.toString()).id)
            position.set(partition, offset + 1)
            commit(partition, record.assignment, offset + 1)
          }
          if (a.managedState().pendingCommits < 4) {
            for (const [partition, identity] of owned) {
              if (position.has(partition)) {
                commit(partition, identity, position.get(partition))
              }
            }
          }
        }
      })()
      stopTraffic = env.traffic()
      await until(() => owned.size === 2 && first.received.size >= 50, 'the first member to consume')
      const before = new Map(owned)

      await b.consumer.subscribe(env.topic)
      b.start()
      await until(() => b.owned.size === 1 && owned.size === 1, 'the rebalance to finish')
      b.check()
      // Results observed after their identity was revoked are reported, not
      // required: when the broker answers is outside this test's control. The
      // case of a result held across revoke and reassign is deterministic in
      // the Rust suite.
      for (const { partition, identity } of across) {
        // The late result belongs to the old identity only.
        equal(identity, before.get(partition))
        notEqual(owned.get(partition), identity)
      }
      // The current identity commits normally after the late result.
      const [[partition, identity]] = [...owned]
      await until(() => position.has(partition), 'progress under the new identity')
      await a.commitManaged(env.topic, partition, identity, position.get(partition))

      await stopTraffic()
      running = false
      a.cancelManagedReceive()
      await loop
      await until(() => a.managedState().pendingCommits === 0, 'pending commits to settle')
      await b.stop()
      await Promise.all([a.disconnect(), b.consumer.disconnect()])

      // Whatever the late commit did at the broker, a member that restarts
      // from the committed offsets finds every message that was not received.
      await env.produce(40)
      const c = new Member(env.consumer('range'), env.topic)
      try {
        await c.consumer.subscribe(env.topic)
        c.start()
        await until(() => missing(env.produced, first, b, c).length === 0, 'the restarted member', 60_000)
        c.check()
        deepEqual(missing(env.produced, first, b, c), [])
      } finally {
        c.stopped = true
        await c.consumer.disconnect()
      }
      // Every outcome is one the contract allows for a commit issued here.
      const allowed = ['confirmed', 'STALE_ASSIGNMENT', 'COMMIT_FAILED', 'COMMIT_BACKLOG', 'CLOSED'].map(
        (name) => MANAGED_ERROR_CODES[name] ?? name,
      )
      deepEqual(
        Object.keys(outcomes).filter((outcome) => !allowed.includes(outcome)),
        [],
      )
      ok(outcomes.confirmed > 0)
      console.log(
        `[inflight] across=${JSON.stringify(across.map(({ outcome }) => outcome))} outcomes=${JSON.stringify(outcomes)}`,
      )
    } finally {
      running = false
      b.stopped = true
      await stopTraffic()
      await Promise.all([a.disconnect(), b.consumer.disconnect()])
      await env.close()
    }
  },
)

test('disconnect settles a pending recvManaged and an in-flight commit', { timeout: 60_000 }, async () => {
  const env = await environment('disconnect', 1)
  const consumer = env.consumer()
  try {
    await env.produce(3)
    await consumer.subscribe(env.topic)
    const assign = await until(async () => {
      const record = await consumer.recvManaged(500)
      return record?.kind === 'assign' ? record : undefined
    }, 'the first assignment')
    const [{ assignment }] = assign.partitions

    // Nothing is awaited between these calls: the receive is reserved and the
    // commit is dispatched synchronously, so both are pending at disconnect.
    const pending = consumer.recvManaged(60_000)
    const commit = consumer.commitManaged(env.topic, 0, assignment, 3)
    const second = consumer.commitManaged(env.topic, 0, assignment, 3).then(
      () => 'sent',
      (error) => error,
    )
    ok(consumer.managedState().pendingCommits <= 2)
    const closed = consumer.disconnect()
    // A message was available, but the receive had not run: it ends cleanly.
    equal(await pending, null)
    // The request already handed to the client is not cancelled by the
    // disconnect: the close waits for it and the broker confirms it.
    await commit
    // The second one was waiting behind it. It is refused without being sent,
    // unless the broker had already answered the first one before disconnect.
    const waiting = await second
    if (waiting !== 'sent') {
      equal(waiting.code, MANAGED_ERROR_CODES.CLOSED)
      equal(waiting.outcomeUnknown, false)
    }
    await closed
    equal((await env.committed()).get(0), '3')

    const state = consumer.managedState()
    equal(state.status, 'closed')
    equal(state.pendingCommits, 0)
    deepEqual(state.assignments, [])
    await rejects(consumer.recvManaged(), hasCode(MANAGED_ERROR_CODES.CLOSED))
    await rejects(consumer.commitManaged(env.topic, 0, assignment, 3), hasCode(MANAGED_ERROR_CODES.CLOSED))
  } finally {
    await consumer.disconnect()
    await env.close()
  }
})
