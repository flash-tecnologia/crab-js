/**
 * End-to-end OTEL smoke test for kafka-crab-js with Grafana
 *
 * What it does:
 * - Starts OTEL SDK with OTLP (or console) exporters
 * - Produces a handful of messages
 * - Consumes them (ending spans) so traces/metrics are emitted
 * - Prints quick validation steps for Grafana at http://localhost:3000
 *
 * Run:
 *   KAFKA_AVAILABLE=true node example/otel-grafana-validation.mjs
 *
 * Optional env vars:
 *   KAFKA_BROKERS (default: localhost:9092)
 *   OTEL_EXPORTER_OTLP_ENDPOINT (default: http://localhost:4317)
 *   OTEL_EXPORTER_TYPE=console (to log spans/metrics instead of OTLP)
 *   KAFKA_TOPIC (override topic)
 *
 * Grafana validation (optional; uses Grafana HTTP API at http://localhost:3000):
 *   GRAFANA_URL (default: http://localhost:3000)
 *   GRAFANA_USER / GRAFANA_PASSWORD (default: admin/admin)
 *   GRAFANA_TOKEN (preferred over user/password)
 *   GRAFANA_VALIDATE=true|false (default: true when exporter is not console)
 */

import { KafkaClient } from 'kafka-crab-js'
import { enableOtelInstrumentation, endSpan } from 'kafka-crab-js-otel'
import { nanoid } from 'nanoid'
import { Buffer } from 'node:buffer'

import { OTLPMetricExporter } from '@opentelemetry/exporter-metrics-otlp-grpc'
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-grpc'
import resourcesPkg from '@opentelemetry/resources'
import { ConsoleMetricExporter, PeriodicExportingMetricReader } from '@opentelemetry/sdk-metrics'
import { NodeSDK } from '@opentelemetry/sdk-node'
import { ConsoleSpanExporter } from '@opentelemetry/sdk-trace-node'
import { SEMRESATTRS_SERVICE_NAME } from '@opentelemetry/semantic-conventions'

process.env.NAPI_RS_TOKIO_RUNTIME = '1'

if (process.env.KAFKA_AVAILABLE !== 'true') {
  console.error('Set KAFKA_AVAILABLE=true to run this example (prevents accidental execution without Kafka).')
  process.exit(1)
}

const serviceName = 'kafka-crab-otel-grafana'
const topic = process.env.KAFKA_TOPIC || `otel-grafana-${nanoid()}`
const brokers = process.env.KAFKA_BROKERS || 'localhost:9092'
const endpoint = process.env.OTEL_EXPORTER_OTLP_ENDPOINT || 'http://localhost:4317'
const useConsoleExporter = process.env.OTEL_EXPORTER_TYPE === 'console'
const totalMessages = Number(process.env.MESSAGE_COUNT || '100')
const delayedMessages = Number(process.env.DELAYED_MESSAGES || '50')
const delayMs = Number(process.env.DELAY_MS || '200')
const grafanaUrl = process.env.GRAFANA_URL || 'http://localhost:3000'
const grafanaUser = process.env.GRAFANA_USER || 'admin'
const grafanaPassword = process.env.GRAFANA_PASSWORD || 'admin'
const grafanaToken = process.env.GRAFANA_TOKEN
const validateGrafana = (process.env.GRAFANA_VALIDATE ?? String(!useConsoleExporter)) === 'true'

const traceExporter = useConsoleExporter
  ? new ConsoleSpanExporter()
  : new OTLPTraceExporter({ url: endpoint })

const metricExporter = useConsoleExporter
  ? new ConsoleMetricExporter()
  : new OTLPMetricExporter({ url: endpoint })

const metricReader = new PeriodicExportingMetricReader({
  exporter: metricExporter,
  exportIntervalMillis: 5000,
})

const sdk = new NodeSDK({
  resource: new resourcesPkg.Resource({
    [SEMRESATTRS_SERVICE_NAME]: serviceName,
  }),
  traceExporter,
  metricReader,
})

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms))
}

function grafanaAuthHeader() {
  if (grafanaToken) {
    return `Bearer ${grafanaToken}`
  }
  return `Basic ${Buffer.from(`${grafanaUser}:${grafanaPassword}`, 'utf8').toString('base64')}`
}

async function grafanaGetJson(path) {
  const res = await fetch(new URL(path, grafanaUrl), {
    headers: {
      accept: 'application/json',
      authorization: grafanaAuthHeader(),
    },
  })

  if (!res.ok) {
    const body = await res.text().catch(() => '')
    throw new Error(`Grafana ${res.status} ${res.statusText} for ${path}: ${body}`)
  }

  return res.json()
}

async function grafanaGetText(path) {
  const res = await fetch(new URL(path, grafanaUrl), {
    headers: {
      accept: 'application/json',
      authorization: grafanaAuthHeader(),
    },
  })

  if (!res.ok) {
    const body = await res.text().catch(() => '')
    throw new Error(`Grafana ${res.status} ${res.statusText} for ${path}: ${body}`)
  }

  return res.text()
}

async function getDatasourceUid(type) {
  const datasources = await grafanaGetJson('/api/datasources')
  if (!Array.isArray(datasources)) {
    return null
  }
  const found = datasources.find(ds => typeof ds?.type === 'string' && ds.type.toLowerCase().includes(type))
  return found?.uid || null
}

async function prometheusQuery(promUid, promql) {
  const params = new URLSearchParams({ query: promql })
  return grafanaGetJson(`/api/datasources/proxy/uid/${promUid}/api/v1/query?${params.toString()}`)
}

function sumVectorResultByTopic(result, topicValue) {
  const series = Array.isArray(result?.data?.result) ? result.data.result : []
  let sum = 0
  let matched = 0

  for (const item of series) {
    const labels = item?.metric && typeof item.metric === 'object' ? item.metric : {}
    const labelValues = Object.values(labels)
    const value = Array.isArray(item?.value) ? item.value[1] : undefined
    const numeric = typeof value === 'string' ? Number(value) : Number.NaN
    if (!Number.isFinite(numeric)) {
      continue
    }

    if (topicValue && labelValues.includes(topicValue)) {
      sum += numeric
      matched += 1
    }
  }

  if (matched > 0) {
    return { sum, matched, mode: 'topic-filtered' }
  }

  // Fallback: sum all series (useful when exporters drop topic labels)
  for (const item of series) {
    const value = Array.isArray(item?.value) ? item.value[1] : undefined
    const numeric = typeof value === 'string' ? Number(value) : Number.NaN
    if (!Number.isFinite(numeric)) {
      continue
    }
    sum += numeric
  }

  return { sum, matched: series.length, mode: 'all-series' }
}

async function validateGrafanaData({ topic }) {
  console.log(`🔎 Validating in Grafana via HTTP API (${grafanaUrl})...`)

  const tempoUid = await getDatasourceUid('tempo')
  const promUid = await getDatasourceUid('prometheus')

  if (!tempoUid) {
    console.warn('⚠️ Tempo datasource not found in Grafana; skipping trace validation.')
  }
  if (!promUid) {
    console.warn('⚠️ Prometheus datasource not found in Grafana; skipping metric validation.')
  }

  if (tempoUid) {
    try {
      const tags = encodeURIComponent(`service.name=${serviceName}`)
      const search = await grafanaGetJson(`/api/datasources/proxy/uid/${tempoUid}/api/search?tags=${tags}&limit=20`)
      let traces = []
      if (Array.isArray(search)) {
        traces = search
      } else if (Array.isArray(search?.traces)) {
        traces = search.traces
      }
      const first = traces[0]
      const traceId = first?.traceID || first?.traceId
      console.log(`✅ Tempo search returned ${traces.length} trace(s)`)

      if (traceId) {
        const raw = await grafanaGetText(`/api/datasources/proxy/uid/${tempoUid}/api/traces/${traceId}`)
        const hasSend = raw.includes(`send ${topic}`)
        const hasPoll = raw.includes(`poll ${topic}`)
        const hasProcess = raw.includes(`process ${topic}`)
        console.log(`✅ Trace ${traceId}: send=${hasSend} poll=${hasPoll} process=${hasProcess}`)
      } else {
        console.warn('⚠️ Tempo search did not include a trace id; skipping trace detail check.')
      }
    } catch (error) {
      console.warn('⚠️ Tempo validation failed:', error?.message || error)
    }
  }

  if (promUid) {
    const metricCandidates = [
      'messaging_client_sent_messages_total',
      'messaging_client_sent_messages',
      'messaging_client_consumed_messages_total',
      'messaging_client_consumed_messages',
      'messaging_process_duration_count',
      'messaging_process_duration_sum',
    ]

    for (const metric of metricCandidates) {
      try {
        const res = await prometheusQuery(promUid, metric)
        const { sum, matched, mode } = sumVectorResultByTopic(res, topic)
        if (matched === 0) {
          continue
        }
        console.log(`✅ Prometheus ${metric}: sum=${sum} (series=${matched}, mode=${mode})`)
      } catch {
        // Ignore unknown metric names / query errors and try the next one
      }
    }
  }
}

async function produceAndConsume() {
  console.log('🚀 Starting OTEL SDK...')
  await sdk.start()
  console.log(`✅ OTEL SDK ready → exporter=${useConsoleExporter ? 'console' : endpoint}\n`)

  // Enable OTEL instrumentation with the kafka-crab-js-otel package
  enableOtelInstrumentation({
    serviceName,
    captureMessagePayload: true,
    captureMessageHeaders: true,
    enableBatchInstrumentation: true,
    metrics: {
      enabled: true,
      includePartitionId: true,
      serverAddress: brokers.split(',')[0]?.split(':')[0],
      serverPort: Number(brokers.split(',')[0]?.split(':')[1]) || undefined,
    },
  })

  const clientId = `otel-grafana-client-${nanoid(6)}`
  const kafkaClient = new KafkaClient({
    brokers,
    clientId,
    diagnostics: true, // Enable diagnostics channel (OTEL adapter subscribes to these)
  })

  const producer = kafkaClient.createProducer()

  console.log(`📝 Producing ${totalMessages} messages to topic ${topic}...`)
  for (let i = 0; i < totalMessages; i++) {
    await producer.send({
      topic,
      messages: [{
        key: Buffer.from(`key-${i}`),
        headers: { 'x-otel-smoke': Buffer.from(serviceName) },
        payload: Buffer.from(JSON.stringify({ id: i, msg: `hello-${i}`, ts: Date.now() })),
      }],
    })
  }
  await producer.flush()
  console.log('✅ Messages produced\n')

  const consumer = kafkaClient.createConsumer({
    groupId: `otel-grafana-group-${nanoid(4)}`,
    enableAutoCommit: false,
    configuration: { 'auto.offset.reset': 'earliest' },
  })

  // Some Kafka setups disable auto topic creation; subscribing before the topic exists can fail.
  // Produce first, then subscribe with a short retry loop to wait for topic metadata propagation.
  const subscribeDeadline = Date.now() + 10000
  // eslint-disable-next-line no-constant-condition
  while (true) {
    try {
      await consumer.subscribe([{ topic, allOffsets: { position: 'Beginning' } }])
      break
    } catch (error) {
      if (Date.now() >= subscribeDeadline) {
        throw error
      }
      await new Promise(resolve => setTimeout(resolve, 500))
    }
  }

  console.log('🎯 Consuming the messages (ending spans to flush OTEL data)...')
  let received = 0
  const deadline = Date.now() + 60000
  const fastMessages = Math.max(0, totalMessages - delayedMessages)
  try {
    while (received < totalMessages && Date.now() < deadline) {
      const message = await consumer.recv()
      if (!message) {
        break
      }
      // End processing span to ensure it is exported.
      // First N: as fast as possible; last M: delayed to create a clear latency split.
      if (received >= fastMessages) {
        await sleep(delayMs)
      }
      endSpan(message)
      received += 1
    }
  } finally {
    await consumer.disconnect().catch(() => undefined)
  }
  console.log(`✅ Consumed ${received} messages\n`)

  // Allow periodic metric reader to flush at least once
  await sleep(6000)

  console.log('📊 Validation steps (Grafana at http://localhost:3000):')
  console.log('  1) Open Grafana → Explore → Tempo (traces) → query: service.name="' + serviceName + '"')
  console.log('  2) Look for spans named "send <topic>", "poll <topic>", "process <topic>"')
  console.log('  3) Switch to Prometheus/OTLP metrics data source → query:')
  console.log('     - messaging_client_sent_messages   (messages sent)')
  console.log('     - messaging_client_consumed_messages   (messages consumed)')
  console.log('     - messaging_process_duration_bucket / _sum / _count (processing latency)')
  console.log(
    `  4) Expected: sent=${totalMessages}, consumed=${totalMessages}, with ~${delayedMessages} process spans ~${delayMs}ms`,
  )
  console.log('  5) If using console exporters, spans/metrics were printed above instead of Grafana.')

  if (validateGrafana) {
    await validateGrafanaData({ topic })
  }

  await sdk.shutdown()
  console.log('\n🛑 OTEL SDK shut down; smoke test complete.')
}

produceAndConsume().catch(async (error) => {
  console.error('❌ Smoke test failed:', error)
  await sdk.shutdown().catch(() => undefined)
  process.exit(1)
})
