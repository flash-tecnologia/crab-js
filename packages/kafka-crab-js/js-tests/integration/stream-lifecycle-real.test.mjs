import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { fileURLToPath } from 'node:url'
import { ok } from 'node:assert/strict'
import test from 'node:test'

const execute = promisify(execFile)
const fixture = fileURLToPath(new URL('../fixtures/real-kafka-lifecycle.mjs', import.meta.url))

for (const mode of ['serial', 'batch', 'compact']) {
  for (const scenario of ['slow', 'backlog', 'cancel', ...(mode === 'serial' ? ['prefetch'] : ['partial'])]) {
    test(`Real Kafka lifecycle: ${mode} / ${scenario}`, { timeout: 40_000 }, async (context) => {
      // Process deadline also covers native stalls that block JavaScript timers.
      const { stdout } = await execute(process.execPath, [fixture, mode, scenario], {
        timeout: 35_000,
        killSignal: 'SIGKILL',
        maxBuffer: 2 * 1024 * 1024,
        env: process.env,
      })
      const line = stdout.split('\n').find((entry) => entry.startsWith('LIFECYCLE_RESULT '))
      ok(line, 'The child must finish its assertions and report results')
      context.diagnostic(line)
    })
  }
}
