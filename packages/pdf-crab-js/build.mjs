import { spawn } from 'node:child_process'
import { dirname } from 'node:path'
import { fileURLToPath } from 'node:url'

import { NapiCli } from '@napi-rs/cli'

const cwd = dirname(fileURLToPath(import.meta.url))
const napi = new NapiCli()

const NAPI_BINDINGS = [
  {
    jsBinding: 'index.js',
    esm: true,
  },
  {
    jsBinding: 'index.cjs',
    noDtsHeader: true,
  },
]

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
  debug: hasFlag('--debug'),
  target: getArgValue('--target'),
})

const runCommand = (command, args) =>
  new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd,
      shell: process.platform === 'win32',
      stdio: 'inherit',
    })

    child.on('error', reject)
    child.on('exit', (code, signal) => {
      if (code === 0) {
        resolve()
        return
      }

      reject(new Error(`${command} ${args.join(' ')} failed with ${signal ?? code}`))
    })
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

const runNapiBuild = async ({ target, crossCompile, debug }) => {
  const commonConfig = {
    constEnum: false,
    crossCompile,
    dts: 'index.d.ts',
    platform: true,
    release: !debug,
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
  await runCommand('vp', ['fmt', 'index.js', 'index.cjs', 'index.d.ts'])
}

main().catch((error) => {
  console.error('Build script failed:', error)
  process.exit(1)
})
