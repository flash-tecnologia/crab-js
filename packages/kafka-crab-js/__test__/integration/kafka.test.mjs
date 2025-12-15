import { nanoid } from 'nanoid'
import { equal, ok } from 'node:assert/strict'
import { Buffer } from 'node:buffer'
import test from 'node:test'
import { setTimeout as sleep } from 'node:timers/promises'
import { KafkaClient } from '../../dist/index.js'

// Create unique identifiers for this test run
const TEST_ID = nanoid(6)
const TEST_TOPIC = `test-topic-${TEST_ID}`

// Configuration
const config = {
  brokers: process.env.KAFKA_BROKERS || 'localhost:9092',
  clientId: `kafka-crab-test-${TEST_ID}`,
  logLevel: process.env.KAFKA_LOG_LEVEL || 'info',
  brokerAddressFamily: 'v4',
  runIntegrationTests: process.env.RUN_KAFKA_INTEGRATION === 'true',
}

// Prepare test messages
const testMessages = Array.from({ length: 3 }, (_, i) => ({
  key: Buffer.from(`key-${TEST_ID}-${i}`),
  headers: { 'test-header': Buffer.from(`header-value-${i}`) },
  payload: Buffer.from(JSON.stringify({
    _id: i,
    testId: TEST_ID,
    name: `Test Message ${i}`,
    timestamp: Date.now(),
  })),
}))

await test('Kafka Crab JS Integration Tests', async (t) => {
  /** @type {import('../../index.js').KafkaClient} */
  let kafkaClient

  // === SETUP ===
  await t.test('Setup: Create KafkaClient', async () => {
    kafkaClient = new KafkaClient({
      brokers: config.brokers,
      clientId: config.clientId,
      logLevel: config.logLevel,
      brokerAddressFamily: config.brokerAddressFamily, // Use config value
      configuration: {
        'auto.offset.reset': 'earliest',
        // Add common config here if needed across tests, or specific ones below
      },
    })
    ok(kafkaClient, 'KafkaClient should be created')
    equal(kafkaClient.kafkaConfiguration.brokers, config.brokers, 'Brokers should match')
  })

  // === PRODUCER TESTS ===
  // ... (Keep existing Producer test) ...
  await t.test('Producer: Send messages', async () => {
    // Create producer according to your API
    const producer = kafkaClient.createProducer({
      configuration: { 'message.timeout.ms': '5000' },
    })

    ok(producer, 'Producer should be created')

    // Send test messages
    const results = await producer.send({
      topic: TEST_TOPIC,
      messages: testMessages,
    })

    ok(Array.isArray(results), 'Send should return array of results')
    equal(results.length, testMessages.length, 'Should have results for all messages')

    // Verify offsets
    for (const offset of results) {
      // Allow both number and object with offset property based on potential return types
      ok(
        typeof offset === 'number' || (offset && typeof offset.offset === 'number'),
        `Each result should contain an offset (got: ${JSON.stringify(offset)})`,
      )
    }

    console.log(`Successfully produced ${results.length} messages to ${TEST_TOPIC}`)
  })

  // === CONSUMER TESTS - POLLING API ===
  // ... (Keep existing Polling Consumer test) ...
  await t.test('Consumer (Polling): Receive messages', async () => {
    // Create consumer according to your API
    const consumer = kafkaClient.createConsumer({
      // Removed topic here, should be in subscribe
      groupId: `test-group-polling-${TEST_ID}`,
      configuration: {
        'auto.offset.reset': 'earliest',
        // Disable auto commit for polling test if manual commit is implied
        // 'enable.auto.commit': 'false'
      },
    })

    ok(consumer, 'Consumer should be created')

    // Subscribe to the topic
    await consumer.subscribe(TEST_TOPIC)

    // Receive messages
    const receivedMessages = []
    let pollingCount = 0
    const maxPolls = 20 // Maximum number of polling attempts

    console.log(`Polling for ${testMessages.length} messages on ${TEST_TOPIC}...`)

    while (receivedMessages.length < testMessages.length && pollingCount < maxPolls) {
      pollingCount++
      console.log(`Polling attempt ${pollingCount}/${maxPolls}...`)
      // Add a timeout to recv if available, otherwise use sleep
      // Assuming recv waits indefinitely or has its own internal timeout
      const message = await consumer.recv() // Consider adding timeout if API supports it

      if (message) {
        console.log(`Received raw message: Offset ${message.offset}, Partition ${message.partition}`)
        // Convert the payload to a string and parse as JSON to check the testId
        try {
          const payload = JSON.parse(message.payload.toString())

          if (payload.testId === TEST_ID) {
            receivedMessages.push(message)
            console.log(
              `Received message ${receivedMessages.length}/${testMessages.length} matching TEST_ID from ${TEST_TOPIC}`,
            )
          } else {
            console.log(`Received message from different test run (ID: ${payload.testId}), skipping.`)
          }
        } catch (e) {
          console.error('Failed to parse message payload:', message.payload?.toString(), e)
        }
      } else {
        console.log('Polling returned no message.')
        // Only sleep if no message was received to avoid unnecessary delays
        if (receivedMessages.length < testMessages.length) {
          await sleep(500) // Small delay between polls only if needed
        }
      }
    }

    // Cleanup
    await consumer.disconnect()
    console.log('Polling consumer disconnected')

    // Verification
    equal(receivedMessages.length, testMessages.length, `Should receive all ${testMessages.length} test messages`)

    // Verify message contents
    for (const msg of receivedMessages) {
      ok(msg.payload, 'Message should have payload')
      ok(msg.offset !== undefined, 'Message should have offset')
      ok(msg.partition !== undefined, 'Message should have partition')

      // Verify headers if your messages include them
      if (Object.keys(msg.headers || {}).length > 0) {
        ok(msg.headers['test-header'], 'Message should have test header')
        // Check header content if needed
        const expectedHeaderIndex = testMessages.findIndex(tm => tm.key.equals(msg.key))
        if (expectedHeaderIndex !== -1) {
          equal(
            msg.headers['test-header'].toString(),
            `header-value-${expectedHeaderIndex}`,
            'Header value should match',
          )
        }
      }
    }
  })

  // === CONSUMER TESTS - STREAM API ===
  // ... (Keep existing Stream Consumer test) ...
  await t.test('Consumer (Stream): Receive messages via stream', async () => {
    // Create stream consumer according to your API
    const streamConsumer = kafkaClient.createStreamConsumer({
      groupId: `test-group-stream-${TEST_ID}`,
      enableAutoCommit: true,
      configuration: {
        'auto.offset.reset': 'earliest',
        // Optional: Shorter auto-commit interval for streams too
        'auto.commit.interval.ms': '1000',
      },
    })

    ok(streamConsumer, 'Stream consumer should be created')

    // Subscribe using the array format from your examples
    await streamConsumer.subscribe([
      { topic: TEST_TOPIC, allOffsets: { position: 'Beginning' } },
    ])

    // Set up promise to collect messages
    const receivedMessages = []
    const streamMessagesPromise = new Promise((resolve, reject) => {
      const timeoutDuration = 15000 // 15 seconds
      const timeout = setTimeout(() => {
        console.log(`Stream timeout reached after ${timeoutDuration}ms. Received ${receivedMessages.length} messages.`)
        // Resolve even if we don't get all messages, verification will happen later
        resolve()
      }, timeoutDuration)

      streamConsumer.on('data', (message) => {
        try {
          // Log raw received message details
          // console.log(`Stream raw message: Topic ${message.topic}, Partition ${message.partition}, Offset ${message.offset}`);

          const payload = JSON.parse(message.payload.toString())

          if (payload.testId === TEST_ID) {
            receivedMessages.push(message)
            console.log(
              `Stream received message ${receivedMessages.length}/${testMessages.length} matching TEST_ID from ${message.topic}`,
            )

            // If we've received all expected messages, resolve early
            if (receivedMessages.length >= testMessages.length) {
              console.log('Stream received all expected messages.')
              clearTimeout(timeout)
              resolve()
            }
          } else {
            console.log(`Stream received message from different test run (ID: ${payload.testId}), skipping.`)
          }
        } catch (err) {
          console.error('Error processing stream message:', err, message.payload?.toString())
          // Decide if an error here should reject the promise
          // reject(err); // uncomment if processing errors should fail the test immediately
        }
      })

      streamConsumer.on('error', (error) => {
        console.error('Stream consumer error:', error)
        clearTimeout(timeout) // Clear timeout on error
        reject(error) // Fail the test on stream error
      })

      // Add a 'close' handler? Depending on library behavior
      streamConsumer.on('close', () => {
        console.log('Stream consumer closed.')
        clearTimeout(timeout) // Clear timeout if closed prematurely
        // Resolve here if closing is expected after messages or timeout
        resolve()
      })
    })

    // Wait for stream to collect messages or time out
    await streamMessagesPromise

    // Cleanup - Ensure disconnect is always called
    try {
      await streamConsumer.disconnect()
      console.log('Stream consumer disconnected')
    } catch (disconnectError) {
      console.error('Error disconnecting stream consumer:', disconnectError)
    }

    // Verify results after waiting/timeout
    // Use greater than or equal to allow for potential duplicate deliveries in test scenarios
    ok(
      receivedMessages.length >= testMessages.length,
      `Should receive at least ${testMessages.length} test messages via stream (received ${receivedMessages.length})`,
    )
    console.log(`Received ${receivedMessages.length} messages via stream consumer matching TEST_ID.`)

    // Verify message content
    for (const msg of receivedMessages) {
      ok(msg.payload, 'Stream Message should have payload')
      ok(msg.offset !== undefined, 'Stream Message should have offset')
      ok(msg.partition !== undefined, 'Stream Message should have partition')
      ok(msg.topic === TEST_TOPIC, 'Stream Message should have correct topic')
      // Check headers if necessary
      if (Object.keys(msg.headers || {}).length > 0) {
        ok(msg.headers['test-header'], 'Stream Message should have test header')
      }
    }
  })

  // === ERROR HANDLING TESTS ===
  // ... (Keep existing Error Handling test) ...
  await t.test('Error handling: Invalid configuration', async () => {
    let invalidClient
    let producer
    try {
      // Try to connect to a non-existent broker
      invalidClient = new KafkaClient({
        brokers: 'nonexistent-host:9092',
        clientId: `invalid-client-${TEST_ID}`, // Unique client ID
        logLevel: 'info', // Keep logs minimal for error test
        // Short connection timeout to fail faster
        configuration: {
          'socket.timeout.ms': '2000', // Reduced timeout
          'socket.connection.setup.timeout.ms': '2000', // Reduced timeout
          // Potentially reduce metadata timeout as well
          'metadata.request.timeout.ms': '2000',
        },
      })

      producer = invalidClient.createProducer({
        // Add producer-specific timeouts if needed
        configuration: { 'message.timeout.ms': '3000' },
      })

      // Try to send a message (should eventually fail)
      console.log('Attempting to send message with invalid client...')
      await producer.send({
        topic: 'invalid-topic',
        messages: [{ payload: Buffer.from('test') }],
      })

      // If we reach here without an error being thrown, fail the test
      ok(false, 'Should throw error for invalid broker connection/send')
    } catch (error) {
      // We expect an error
      ok(error instanceof Error, 'Should throw an Error instance')
      // Check if the error message indicates a connection or timeout issue
      const errMsg = error.message.toLowerCase()
      ok(
        errMsg.includes('broker') || errMsg.includes('connect') || errMsg.includes('timeout') ||
          errMsg.includes('failed'),
        `Error message should indicate connection/broker/timeout issue (got: ${error.message})`,
      )
      console.log('Expected error occurred for invalid configuration:', error.message)
    } finally {
      // Cleanup resources if they were created, though they might be in an invalid state
      if (producer && typeof producer.disconnect === 'function') {
        try {
          await producer.disconnect()
        } catch (e) {
          console.warn('Error disconnecting invalid producer:', e.message)
        }
      }
      if (invalidClient && typeof invalidClient.disconnect === 'function') { // Assuming client has disconnect
        try {
          await invalidClient.disconnect()
        } catch (e) {
          console.warn('Error disconnecting invalid client:', e.message)
        }
      }
    }
  })

  // === EVENT HANDLING TESTS ===
  // ... (Keep existing general Events test, maybe refine slightly) ...
  await t.test('Events: Consumer general events', async () => {
    // This test verifies the onEvents API receives *some* event
    const consumer = kafkaClient.createConsumer({
      // topic: TEST_TOPIC, // Subscribe below
      groupId: `test-group-events-${TEST_ID}`,
      configuration: {
        'auto.offset.reset': 'earliest',
      },
    })

    let eventReceived = false
    let receivedEventName = ''
    const eventPromise = new Promise((resolve) => {
      consumer.onEvents((_err, event) => {
        if (_err) {
          console.error('Error in onEvents (general):', _err)
          return
        }
        console.log('General consumer event received:', event.name)
        eventReceived = true
        receivedEventName = event.name
        // Resolve on *any* event for this simple test
        resolve()
      })
    })

    // Subscribe to trigger events (like AssignCallback)
    await consumer.subscribe(TEST_TOPIC)

    // Wait briefly for an event (e.g., AssignCallback from subscribe) or poll
    // Sometimes just subscribing is enough to trigger rebalance events
    console.log('Waiting for *any* consumer event after subscribe...')
    // Poll for a message to potentially generate more events (like Fetch)
    // Or just wait using the promise
    try {
      await Promise.race([
        eventPromise,
        sleep(5000).then(() => {
          console.warn('Timeout waiting for any general event.')
        }), // Wait up to 5s
      ])

      // Optional: Try receiving a message AFTER subscribing to potentially trigger Fetch/Commit events
      if (!eventReceived) {
        console.log('No event after subscribe, trying recv...')
        await consumer.recv() // This might trigger Fetch event
        await Promise.race([
          eventPromise, // Check again if recv triggered it
          sleep(5000).then(() => {
            console.warn('Timeout waiting for any general event after recv.')
          }),
        ])
      }
    } catch (e) {
      console.error('Error during general event test:', e)
    } finally {
      // Cleanup
      await consumer.disconnect()
      console.log('General events consumer disconnected')
    }

    // Verify we got *some* event
    ok(
      eventReceived,
      `Should receive at least one consumer event (e.g., AssignCallback, Fetch). Last seen: ${
        receivedEventName || 'None'
      }`,
    )
  })

  // === MULTI-TOPIC STREAM TEST ===
  await t.test('Stream: Process messages from multiple topics', async () => {
    // Create a second test topic
    const SECOND_TOPIC = `${TEST_TOPIC}-second`

    // Send messages to both topics
    const producer = kafkaClient.createProducer()

    // Ensure first topic has messages (sent earlier)
    // Send to second topic
    console.log(`Multi-topic test: Sending message to ${SECOND_TOPIC}`)
    await producer.send({
      topic: SECOND_TOPIC,
      messages: [{
        key: Buffer.from(`key-${TEST_ID}-second-0`), // Unique key
        payload: Buffer.from(JSON.stringify({
          testId: TEST_ID,
          fromTopic: SECOND_TOPIC,
        })),
      }],
    })
    console.log(`Multi-topic test: Message sent to ${SECOND_TOPIC}`)

    // Create stream consumer for multiple topics
    const streamConsumer = kafkaClient.createStreamConsumer({
      groupId: `multi-topic-${TEST_ID}`,
      enableAutoCommit: true,
      configuration: {
        'auto.offset.reset': 'earliest',
        'auto.commit.interval.ms': '1000',
      },
    })

    console.log(`Multi-topic test: Subscribing to ${TEST_TOPIC} and ${SECOND_TOPIC}`)
    await streamConsumer.subscribe([
      { topic: TEST_TOPIC, allOffsets: { position: 'Beginning' } },
      { topic: SECOND_TOPIC, allOffsets: { position: 'Beginning' } },
    ])
    console.log(`Multi-topic test: Subscribed.`)

    const receivedTopics = new Set()
    const receivedMessages = [] // Collect messages for inspection
    const multiTopicComplete = new Promise((resolve, reject) => {
      const timeoutDuration = 15000 // Increased timeout slightly
      const timeout = setTimeout(() => {
        console.log(
          `Multi-topic test: Timeout reached after ${timeoutDuration}ms. Received from topics: ${
            [...receivedTopics].join(', ')
          }`,
        )
        resolve() // Resolve on timeout, verification happens later
      }, timeoutDuration)

      streamConsumer.on('data', (message) => {
        try {
          const payload = JSON.parse(message.payload.toString())
          if (payload.testId === TEST_ID) { // Filter for current test run
            console.log(`Multi-topic test: Received message from topic ${message.topic} (Offset ${message.offset})`)
            receivedTopics.add(message.topic)
            receivedMessages.push(message)
            // Check if we received at least one message from each expected topic
            if (receivedTopics.has(TEST_TOPIC) && receivedTopics.has(SECOND_TOPIC)) {
              console.log('Multi-topic test: Received messages from both topics.')
              clearTimeout(timeout)
              resolve()
            }
          } else {
            console.log(`Multi-topic test: Skipping message from other test run (ID: ${payload.testId})`)
          }
        } catch (e) {
          console.error('Multi-topic test: Error parsing message', e, message.payload?.toString())
        }
      })

      streamConsumer.on('error', (error) => {
        console.error('Multi-topic stream consumer error:', error)
        clearTimeout(timeout)
        reject(error)
      })

      streamConsumer.on('close', () => {
        console.log('Multi-topic stream closed.')
        clearTimeout(timeout)
        resolve() // Resolve if closed
      })
    })

    await multiTopicComplete
    console.log('Multi-topic test: Proceeding to disconnect.')
    await streamConsumer.disconnect()
    console.log('Multi-topic test: Disconnected.')

    console.log(`Multi-topic test: Verification - Received from topics: ${[...receivedTopics].join(', ')}`)
    equal(receivedTopics.size, 2, 'Should receive messages from exactly 2 topics')
    ok(receivedTopics.has(TEST_TOPIC), `Should receive from first topic (${TEST_TOPIC})`)
    ok(receivedTopics.has(SECOND_TOPIC), `Should receive from second topic (${SECOND_TOPIC})`)

    // Optional: Verify specific message content if needed
    ok(receivedMessages.some(m => m.topic === TEST_TOPIC), 'Should have at least one message object from TEST_TOPIC')
    ok(
      receivedMessages.some(m => m.topic === SECOND_TOPIC),
      'Should have at least one message object from SECOND_TOPIC',
    )
  })

  // === HEADERS TEST ===
  // ... (Keep existing Headers test) ...
  await t.test('Messages: Process headers and message format', async () => {
    const HEADER_TOPIC = `${TEST_TOPIC}-headers`
    const producer = kafkaClient.createProducer()

    // Create a message with complex headers
    const correlationId = nanoid()
    const timestampStr = Date.now().toString()
    const nestedJson = JSON.stringify({ nested: 'value', id: TEST_ID }) // Include TEST_ID in header data if useful

    const complexHeaders = {
      'correlation-id': Buffer.from(correlationId),
      'message-type': Buffer.from('test-message'),
      'timestamp': Buffer.from(timestampStr),
      'json-header': Buffer.from(nestedJson),
    }

    console.log(`Headers test: Sending message with headers to ${HEADER_TOPIC}`)
    await producer.send({
      topic: HEADER_TOPIC,
      messages: [{
        key: Buffer.from(`key-header-${TEST_ID}`), // Unique key
        headers: complexHeaders,
        payload: Buffer.from(JSON.stringify({ testId: TEST_ID, withHeaders: true })),
      }],
    })
    console.log(`Headers test: Message sent.`)

    // Consume and verify
    const consumer = kafkaClient.createConsumer({
      // topic: HEADER_TOPIC, // Subscribe below
      groupId: `header-test-${TEST_ID}`,
      configuration: { 'auto.offset.reset': 'earliest' },
    })

    console.log(`Headers test: Subscribing to ${HEADER_TOPIC}`)
    await consumer.subscribe(HEADER_TOPIC)

    console.log(`Headers test: Receiving message...`)
    const message = await consumer.recv() // Assuming one message is enough

    ok(message, 'Should receive message with headers')
    ok(message.headers, 'Message should have a headers object')

    console.log(`Headers test: Received message with headers:`, message.headers)

    // Verify all headers are received and are buffers
    for (const [key, expectedValueBuffer] of Object.entries(complexHeaders)) {
      ok(message.headers[key], `Should have header '${key}'`)
      ok(Buffer.isBuffer(message.headers[key]), `Header '${key}' value should be a Buffer`)
      // Compare buffer content
      equal(
        message.headers[key].compare(expectedValueBuffer),
        0,
        `Header '${key}' content should match expected buffer`,
      )
      console.log(`Header '${key}': OK`)
    }

    // Also verify content by decoding if needed
    equal(
      message.headers['message-type'].toString(),
      'test-message',
      'Header "message-type" content should match string',
    )
    equal(
      message.headers['correlation-id'].toString(),
      correlationId,
      'Header "correlation-id" content should match',
    )
    equal(
      message.headers['timestamp'].toString(),
      timestampStr,
      'Header "timestamp" content should match',
    )
    equal(
      message.headers['json-header'].toString(),
      nestedJson,
      'Header "json-header" content should match',
    )
    // Verify payload testId as well
    try {
      const payload = JSON.parse(message.payload.toString())
      equal(payload.testId, TEST_ID, 'Payload testId should match')
    } catch (e) {
      ok(false, 'Failed to parse payload of headers message' + e.message)
    }

    await consumer.disconnect()
    console.log('Headers test: Consumer disconnected.')
  })
})
