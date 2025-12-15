import { equal, ok, throws } from 'node:assert/strict'
import { createRequire } from 'node:module'
import test from 'node:test'

// Use CommonJS require for the kafka client to avoid ESM bundling issues
const require = createRequire(import.meta.url)
const { KafkaClient } = require('../../dist/index.cjs')

// Configuration options
const testConfig = {
  brokers: process.env.KAFKA_BROKERS || 'localhost:29092',
  clientId: process.env.KAFKA_CLIENT_ID || 'kafka-crab-test-client',
}

// Basic client creation test
await test('KafkaClient can be created with configuration', async () => {
  const client = new KafkaClient({
    brokers: testConfig.brokers,
    clientId: testConfig.clientId,
  })

  // Check that configuration was stored correctly
  ok(client.kafkaConfiguration, 'Client should have kafkaConfiguration property')
  equal(client.kafkaConfiguration.brokers, testConfig.brokers, 'Brokers should match')
  equal(client.kafkaConfiguration.clientId, testConfig.clientId, 'ClientId should match')
})

await test('KafkaClient defaults clientId to rdkafka when omitted', async () => {
  const client = new KafkaClient({
    brokers: testConfig.brokers,
  })

  ok(client.kafkaConfiguration, 'Client should have kafkaConfiguration property')
  equal(client.kafkaConfiguration.clientId, 'rdkafka', 'ClientId should default to rdkafka')
})

// Test client with additional configuration
await test('KafkaClient accepts additional configuration options', async () => {
  const client = new KafkaClient({
    brokers: testConfig.brokers,
    clientId: testConfig.clientId,
    // Add some reasonable additional options that your library supports
    autoOffsetReset: 'earliest',
    sessionTimeout: 30000,
  })

  // Make sure the additional configs are present
  equal(client.kafkaConfiguration.brokers, testConfig.brokers)
  equal(client.kafkaConfiguration.autoOffsetReset, 'earliest')
  equal(client.kafkaConfiguration.sessionTimeout, 30000)
})

// Additional configuration options test
await test('KafkaClient accepts securityProtocol and brokerAddressFamily', async () => {
  const client = new KafkaClient({
    brokers: testConfig.brokers,
    clientId: testConfig.clientId,
    securityProtocol: 'Plaintext',
    logLevel: 'debug',
    brokerAddressFamily: 'v4',
  })

  equal(client.kafkaConfiguration.securityProtocol, 'Plaintext')
  equal(client.kafkaConfiguration.brokerAddressFamily, 'v4')
})

// Producer creation test
await test('KafkaClient can create a producer', async () => {
  const client = new KafkaClient({
    brokers: testConfig.brokers,
    clientId: testConfig.clientId,
  })

  const producer = client.createProducer({
    topic: 'test-topic',
    configuration: { 'message.timeout.ms': '5000' },
  })

  ok(producer, 'Producer should be created')
  ok(typeof producer.send === 'function', 'Producer should have send method')
})

// Consumer creation test
await test('KafkaClient can create a consumer', async () => {
  const client = new KafkaClient({
    brokers: testConfig.brokers,
    clientId: testConfig.clientId,
  })

  const consumer = client.createConsumer({
    topic: 'test-topic',
    groupId: 'test-group',
    configuration: {
      'auto.offset.reset': 'earliest',
    },
  })

  ok(consumer, 'Consumer should be created')
  ok(typeof consumer.subscribe === 'function', 'Consumer should have subscribe method')
  ok(typeof consumer.recv === 'function', 'Consumer should have recv method')
  ok(typeof consumer.onEvents === 'function', 'Consumer should have onEvents method')
})

// Error handling tests
await test('KafkaClient throws on invalid configuration', () => {
  throws(
    () => {
      new KafkaClient({
        // Missing required brokers
        clientId: testConfig.clientId,
      })
    },
    /Error: Missing field `brokers`/,
    'Should throw when brokers is missing',
  )
})

// Test consumer configuration validation
await test('createConsumer validates groupId', () => {
  const client = new KafkaClient({
    brokers: testConfig.brokers,
    clientId: testConfig.clientId,
  })

  throws(
    () => {
      client.createConsumer({
        topic: 'test-topic',
        // Missing groupId
      })
    },
    /Error: Missing field `groupId`/,
    'Should throw when groupId is missing',
  )
})
