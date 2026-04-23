import { NapiCli } from '@napi-rs/cli'
import { build } from 'vite-plus/pack'

const getTarget = () => {
  const idx = process.argv.findIndex((arg) => arg === '--target')
  return idx !== -1 ? process.argv[idx + 1] : undefined
}

/**
 * Executes a NAPI build task with the provided options.
 * @param {@type import('@napi-rs/cli').NapiCli['build']} options
 * @returns {Promise<NapiBuildResult>}
 */
const napiTask = async (options) => {
  const napi = new NapiCli()
  const result = await napi.build(options)
  await result.task
}

async function execNapibuild() {
  const target = getTarget()
  console.log('target', target)

  const commonConfig = {
    dts: 'js-binding.d.ts',
    constEnum: false,
    platform: true,
    release: true,
    target,
  }

  await napiTask({
    ...commonConfig,
    jsBinding: 'js-binding.js',
    esm: true,
  })

  await napiTask({
    ...commonConfig,
    jsBinding: 'js-binding.cjs',
    noDtsHeader: true,
  })
}

async function main() {
  await execNapibuild()
  await build({
    checks: {
      legacyCjs: false,
    },
    dts: true,
    entry: 'js-src/**/*.ts',
    fixedExtension: false,
    format: ['esm', 'cjs'],
    deps: {
      neverBundle: [/js-binding\.(?:js|cjs)$/],
    },
    platform: 'node',
    report: false,
    sourcemap: true,
    target: 'node24',
  })
}

main().catch((err) => {
  console.error('Build script failed:', err)
  process.exit(1)
})
