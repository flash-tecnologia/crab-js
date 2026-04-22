import { defineConfig } from 'vite-plus'
import { sharedFmtConfig, sharedLintConfig, sharedTestLintRules } from '../../vite-plus.shared.ts'

const fmtConfig = sharedFmtConfig ?? {}
const lintConfig = sharedLintConfig ?? {}

const externalDependencies = [
  'kafka-crab-js',
  '@opentelemetry/api',
  '@opentelemetry/core',
  '@opentelemetry/instrumentation',
  '@opentelemetry/semantic-conventions',
]

export default defineConfig({
  fmt: {
    ...fmtConfig,
  },
  pack: {
    checks: {
      legacyCjs: false,
    },
    define: {
      __PACKAGE_NAME__: JSON.stringify('kafka-crab-js-otel'),
      __PACKAGE_VERSION__: JSON.stringify('1.1.0'),
    },
    deps: {
      neverBundle: externalDependencies,
    },
    dts: true,
    entry: 'src/index.ts',
    fixedExtension: false,
    format: ['esm', 'cjs'],
    platform: 'node',
    report: false,
    sourcemap: true,
    target: 'node24',
  },
  lint: {
    ...lintConfig,
    overrides: [
      {
        files: ['tests/**'],
        rules: sharedTestLintRules,
      },
    ],
  },
  test: {
    environment: 'node',
    exclude: ['tests/integration/**'],
    include: ['tests/unit/**/*.test.ts'],
  },
})
