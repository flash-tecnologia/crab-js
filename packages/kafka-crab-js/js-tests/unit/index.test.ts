import { equal, ok, throws } from 'node:assert/strict'
import { test } from 'vite-plus/test'

import { KafkaClient, KafkaClientConfig } from '../../js-src/index.js'

const TEST_BROKERS = process.env.KAFKA_BROKERS || 'localhost:29092'
const TEST_CLIENT_ID = process.env.KAFKA_CLIENT_ID || 'kafka-crab-test-client'

function createClient(clientId?: string) {
  return new KafkaClient({
    brokers: TEST_BROKERS,
    ...(clientId ? { clientId } : {}),
  })
}

test('KafkaClientConfig preserves the provided configuration', () => {
  const clientConfig = new KafkaClientConfig({
    brokers: TEST_BROKERS,
    clientId: TEST_CLIENT_ID,
    brokerAddressFamily: 'v4',
    configuration: {
      'message.timeout.ms': '5000',
    },
    logLevel: 'debug',
    securityProtocol: 'Plaintext',
  })

  equal(clientConfig.configuration.brokers, TEST_BROKERS)
  equal(clientConfig.configuration.clientId, TEST_CLIENT_ID)
  equal(clientConfig.configuration.brokerAddressFamily, 'v4')
  equal(clientConfig.configuration.logLevel, 'debug')
  equal(clientConfig.configuration.securityProtocol, 'Plaintext')
  equal(clientConfig.configuration.configuration?.['message.timeout.ms'], '5000')
})

test('KafkaClient propagates an explicit clientId to consumers', () => {
  const client = createClient(TEST_CLIENT_ID)
  const consumer = client.createConsumer({
    groupId: 'explicit-client-id-group',
  })

  equal(consumer.clientId, TEST_CLIENT_ID)
})

test('KafkaClient defaults consumer clientId to rdkafka when omitted', () => {
  const client = createClient()
  const consumer = client.createConsumer({
    groupId: 'default-client-id-group',
  })

  equal(consumer.clientId, 'rdkafka')
})

test('KafkaClient can create a producer', () => {
  const client = createClient()
  const producer = client.createProducer({
    autoFlush: false,
    configuration: {
      'message.timeout.ms': '5000',
    },
    queueTimeout: 1500,
  })

  ok(producer, 'Producer should be created')
  equal(typeof producer.send, 'function', 'Producer should have send method')
  equal(typeof producer.flush, 'function', 'Producer should have flush method')
  equal(typeof producer.inFlightCount, 'function', 'Producer should have inFlightCount method')
  equal(typeof producer.inFlightCount(), 'number', 'Producer should report an in-flight count')
})

test('KafkaClient can create a consumer', () => {
  const client = createClient(TEST_CLIENT_ID)
  const consumer = client.createConsumer({
    configuration: {
      'auto.offset.reset': 'earliest',
    },
    enableAutoCommit: false,
    fetchMetadataTimeout: 5000,
    groupId: 'consumer-test-group',
  })

  ok(consumer, 'Consumer should be created')
  equal(typeof consumer.subscribe, 'function', 'Consumer should have subscribe method')
  equal(typeof consumer.recv, 'function', 'Consumer should have recv method')
  equal(typeof consumer.recvBatch, 'function', 'Consumer should have recvBatch method')
  equal(typeof consumer.onEvents, 'function', 'Consumer should have onEvents method')
  equal(typeof consumer.commit, 'function', 'Consumer should have commit method')

  const config = consumer.getConfig()
  equal(config.groupId, 'consumer-test-group')
  equal(config.enableAutoCommit, false)
  equal(config.fetchMetadataTimeout, 5000)
  equal(config.configuration?.['auto.offset.reset'], 'earliest')
})

test('KafkaConsumer exposes recvStream with configurable prefetch arguments', () => {
  const client = createClient(TEST_CLIENT_ID)
  const consumer = client.createConsumer({
    groupId: 'consumer-stream-test-group',
  })

  const stream = consumer.recvStream(32, 2)

  ok(stream, 'ReadableStream should be created')
  equal(typeof stream.getReader, 'function', 'ReadableStream should expose getReader')
})

test('KafkaClient routes web stream consumers by mode without changing message shapes', () => {
  const client = createClient(TEST_CLIENT_ID)

  const serialConsumer = client.createWebStreamConsumer({
    groupId: 'web-serial-test-group',
    serialPrefetchSize: 32,
    serialPrefetchTimeout: 2,
  })
  equal(serialConsumer.mode, 'serial')
  equal(typeof serialConsumer.stream.getReader, 'function', 'Serial web stream should be readable')

  const batchConsumer = client.createWebStreamConsumer({
    batchSize: 8,
    batchTimeout: 10,
    groupId: 'web-batch-test-group',
  })
  equal(batchConsumer.mode, 'batch')
  equal(typeof batchConsumer.stream.getReader, 'function', 'Batch web stream should be readable')
})

test('KafkaClient throws on invalid configuration', () => {
  throws(
    () => Reflect.construct(KafkaClient, [{ clientId: TEST_CLIENT_ID }]),
    /Error: Missing field `brokers`/,
    'Should throw when brokers is missing',
  )
})

test('createConsumer validates groupId', () => {
  const client = createClient()
  const createConsumer = client.createConsumer.bind(client)

  throws(
    () => Reflect.apply(createConsumer, client, [{}]),
    /Error: Missing field `groupId`/,
    'Should throw when groupId is missing',
  )
})
