import { readFileSync } from 'node:fs'
import { defineConfig } from 'vite-plus'
import type { OxlintConfig } from 'vite-plus/lint'
import { sharedFmtConfig, sharedLintConfig, sharedTestLintRules } from '../../vite.shared.mjs'

const packageInfo: unknown = JSON.parse(readFileSync(new URL('package.json', import.meta.url), 'utf8'))
if (
  !packageInfo ||
  typeof packageInfo !== 'object' ||
  !('name' in packageInfo) ||
  typeof packageInfo.name !== 'string' ||
  !('version' in packageInfo) ||
  typeof packageInfo.version !== 'string'
) {
  throw new Error('Package name and version are required for OTEL instrumentation metadata')
}

const externalDependencies = [
  'kafka-crab-js',
  '@opentelemetry/api',
  '@opentelemetry/core',
  '@opentelemetry/instrumentation',
  '@opentelemetry/semantic-conventions',
]

const otelSourceLintRules: NonNullable<OxlintConfig['rules']> = {
  '@typescript-eslint/no-unsafe-type-assertion': 'off',
  '@typescript-eslint/non-nullable-type-assertion-style': 'off',
  '@typescript-eslint/no-unnecessary-type-assertion': 'off',
  '@typescript-eslint/no-unnecessary-type-conversion': 'off',
  '@typescript-eslint/no-unnecessary-type-arguments': 'off',
  '@typescript-eslint/no-redundant-type-constituents': 'off',
  '@typescript-eslint/prefer-readonly': 'off',
  '@typescript-eslint/use-unknown-in-catch-callback-variable': 'off',
  'eslint/no-underscore-dangle': [
    'error',
    {
      allow: [
        '__PACKAGE_NAME__',
        '__PACKAGE_VERSION__',
        '_attachBatchSpan',
        '_attachMessageSpan',
        '_buildConsumerAttributes',
        '_buildProducerAttributes',
        '_config',
        '_consumedMessages',
        '_createDisabledContext',
        '_createInstruments',
        '_defineHiddenOtelField',
        '_enabled',
        '_handlers',
        '_kafkaConfig',
        '_kafkaTracer',
        '_meter',
        '_metrics',
        '_operationDuration',
        '_processDuration',
        '_sentMessages',
        '_shouldIgnoreTopic',
        '_subscribeBatch',
        '_subscribeConsumer',
        '_subscribeProducer',
        '_tracer',
        '_validateHistogramBuckets',
      ],
    },
  ],
}

const otelTestLintRules: NonNullable<OxlintConfig['rules']> = {
  ...sharedTestLintRules,
  '@typescript-eslint/promise-function-async': 'off',
  '@typescript-eslint/use-unknown-in-catch-callback-variable': 'off',
  'no-void': 'off',
}

const otelLintOverrides = [
  {
    files: ['src/**/*'],
    rules: otelSourceLintRules,
  },
  {
    files: ['tests/**'],
    rules: otelTestLintRules,
  },
]

export default defineConfig({
  fmt: {
    ...sharedFmtConfig,
  },
  pack: {
    checks: {
      legacyCjs: false,
    },
    define: {
      __PACKAGE_NAME__: JSON.stringify(packageInfo.name),
      __PACKAGE_VERSION__: JSON.stringify(packageInfo.version),
    },
    deps: {
      neverBundle: externalDependencies,
    },
    dts: {
      tsconfig: 'tsconfig.build.json',
    },
    entry: 'src/index.ts',
    fixedExtension: false,
    format: ['esm', 'cjs'],
    platform: 'node',
    report: false,
    sourcemap: true,
    target: 'node24',
  },
  lint: {
    ...sharedLintConfig,
    overrides: otelLintOverrides,
  },
})
