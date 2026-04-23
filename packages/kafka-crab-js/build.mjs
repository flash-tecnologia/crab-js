import { dirname } from 'node:path'
import { fileURLToPath } from 'node:url'

import { NapiCli } from '@napi-rs/cli'
import { build as pack } from 'vite-plus/pack'

const cwd = dirname(fileURLToPath(import.meta.url))
const napi = new NapiCli()

const NAPI_BINDINGS = [
  {
    jsBinding: 'js-binding.js',
    esm: true,
  },
  {
    jsBinding: 'js-binding.cjs',
    noDtsHeader: true,
  },
]

const PACK_CONFIG = {
  checks: {
    legacyCjs: false,
  },
  cwd,
  deps: {
    neverBundle: [/js-binding\.(?:js|cjs)$/],
  },
  dts: true,
  entry: 'js-src/**/*.ts',
  fixedExtension: false,
  format: ['esm', 'cjs'],
  platform: 'node',
  report: false,
  sourcemap: true,
  target: 'node24',
}

const getArgValue = (name) => {
  const inlinePrefix = `${name}=`
  const inlineArg = process.argv.find((arg) => arg.startsWith(inlinePrefix))
  if (inlineArg) {
    return inlineArg.slice(inlinePrefix.length)
  }

  const index = process.argv.findIndex((arg) => arg === name)
  return index === -1 ? undefined : process.argv[index + 1]
}

const hasFlag = (...names) => names.some((name) => process.argv.includes(name))

const getCliOptions = () => ({
  crossCompile: hasFlag('-x', '--cross-compile'),
  target: getArgValue('--target'),
})

/**
 * Executes a NAPI build task with the provided options.
 * @param {@type import('@napi-rs/cli').NapiCli['build']} options
 * @returns {Promise<NapiBuildResult>}
 */
const runNapiTask = async (options) => {
  const result = await napi.build(options)
  await result.task
}

const runNapiBuild = async ({ target, crossCompile }) => {
  const commonConfig = {
    constEnum: false,
    crossCompile,
    dts: 'js-binding.d.ts',
    platform: true,
    release: true,
    target,
  }

  for (const bindingConfig of NAPI_BINDINGS) {
    await runNapiTask({
      ...commonConfig,
      ...bindingConfig,
    })
  }
}

async function main() {
  const cliOptions = getCliOptions()

  await runNapiBuild(cliOptions)
  await pack(PACK_CONFIG)
}

main().catch((error) => {
  console.error('Build script failed:', error)
  process.exit(1)
})
