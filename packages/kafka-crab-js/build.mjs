import { NapiCli } from '@napi-rs/cli'
import * as esbuild from 'esbuild'
import fs from 'node:fs'

const getTarget = () => {
  const idx = process.argv.findIndex(arg => arg === '--target')
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

async function execEsbuild() {
  const packageJson = JSON.parse(fs.readFileSync(new URL('./package.json', import.meta.url), 'utf8'))

  const commonConfig = {
    entryPoints: ['./js-src/index.ts'],
    platform: 'node',
    bundle: true,
    sourcemap: true,
    define: {
      __PACKAGE_NAME__: JSON.stringify(packageJson.name),
      __PACKAGE_VERSION__: JSON.stringify(packageJson.version),
    },
    external: [
      '../js-binding.js',
      '../js-binding.d.ts',
      '../js-binding.cjs',
    ],
    outdir: './dist',
  }

  await esbuild.build({
    ...commonConfig,
    format: 'esm',
  })

  await esbuild.build({
    ...commonConfig,
    format: 'cjs',
    outExtension: { '.js': '.cjs' },
    plugins: [
      {
        name: 'js-binding-path-replacer',
        setup(build) {
          // Replace js-binding.js imports with js-binding.cjs
          build.onResolve({ filter: /\.\.\/js-binding\.js$/ }, _args => {
            return {
              path: '../js-binding.cjs',
              external: true,
            }
          })
        },
      },
    ],
  })
}

async function main() {
  await execNapibuild()
  await execEsbuild()
}

main().catch(err => {
  console.error('Build script failed:', err)
  process.exit(1)
})
