import { defineConfig } from 'vite-plus'
import { sharedFmtConfig, sharedLintConfig, sharedTestLintRules } from '../../vite-plus.shared.ts'

const fmtConfig = sharedFmtConfig ?? {}
const lintConfig = sharedLintConfig ?? {}

const fmtIgnorePatterns = [
  ...(sharedFmtConfig?.ignorePatterns ?? []),
  'js-binding*.*',
  'index.js',
  'index.d.ts',
  'npm/**',
]

const lintIgnorePatterns = [
  ...(sharedLintConfig?.ignorePatterns ?? []),
  'js-binding*.*',
  'index.js',
  'index.d.ts',
  'npm/**',
]

export default defineConfig({
  fmt: {
    ...fmtConfig,
    ignorePatterns: fmtIgnorePatterns,
  },
  pack: {
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
  },
  lint: {
    ...lintConfig,
    ignorePatterns: lintIgnorePatterns,
    overrides: [
      {
        files: ['**/__test__/**'],
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
