import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { fileURLToPath } from 'node:url'
import { ok } from 'node:assert/strict'
import test from 'node:test'

const execute = promisify(execFile)
const fixture = fileURLToPath(new URL('../fixtures/production-readiness.mjs', import.meta.url))
const enabled = process.env.RUN_KAFKA_FAULT_TESTS === 'true'
const cases = [
  ['producer-recovery', 'auto'],
  ['producer-recovery', 'manual'],
  ['producer-timeout', 'auto'],
  ['producer-timeout', 'manual'],
  ['rebalance', 'group'],
  ...['serial', 'batch', 'compact'].flatMap((mode) => [
    ['consumer-recovery', mode],
    ['committed-restart', mode],
  ]),
]

for (const [scenario, mode] of cases) {
  test(
    `Production readiness: ${scenario} / ${mode}`,
    {
      skip: enabled ? false : 'Requires RUN_KAFKA_FAULT_TESTS=true and an isolated KAFKA_FAULT_CONTAINER',
      timeout: 85_000,
    },
    async (context) => {
      const { stdout } = await execute(process.execPath, [fixture, scenario, mode], {
        timeout: 80_000,
        killSignal: 'SIGKILL',
        maxBuffer: 4 * 1024 * 1024,
        env: process.env,
      })
      const line = stdout.split('\n').find((entry) => entry.startsWith('READINESS_RESULT '))
      ok(line, 'The child must finish its assertions and exit naturally')
      context.diagnostic(line)
    },
  )
}
