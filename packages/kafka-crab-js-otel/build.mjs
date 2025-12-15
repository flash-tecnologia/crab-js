// @ts-check
import * as esbuild from 'esbuild'
import * as fs from 'fs'
import * as path from 'path'

const pkg = JSON.parse(fs.readFileSync('./package.json', 'utf-8'))

/** @type {esbuild.BuildOptions} */
const commonOptions = {
  entryPoints: ['src/index.ts'],
  bundle: true,
  platform: 'node',
  target: 'node20',
  sourcemap: true,
  external: [
    'kafka-crab-js',
    '@opentelemetry/api',
    '@opentelemetry/core',
    '@opentelemetry/instrumentation',
    '@opentelemetry/semantic-conventions',
  ],
  define: {
    __PACKAGE_NAME__: JSON.stringify(pkg.name),
    __PACKAGE_VERSION__: JSON.stringify(pkg.version),
  },
}

// ESM build
await esbuild.build({
  ...commonOptions,
  outfile: 'dist/index.js',
  format: 'esm',
})

// CJS build
await esbuild.build({
  ...commonOptions,
  outfile: 'dist/index.cjs',
  format: 'cjs',
})

console.log('Build complete')
