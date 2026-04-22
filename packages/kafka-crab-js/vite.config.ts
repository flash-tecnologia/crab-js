import { defineConfig } from 'vite-plus'
import type { OxlintConfig } from 'vite-plus/lint'
import { sharedFmtConfig, sharedLintConfig, sharedTestLintRules } from '../../vite-plus.shared.ts'

const fmtConfig = sharedFmtConfig ?? {}
const lintConfig = sharedLintConfig ?? {}

const kafkaJsFmtIgnorePatterns = [
  ...(sharedFmtConfig?.ignorePatterns ?? []),
  'js-binding*.*',
  'index.js',
  'index.d.ts',
  'npm/**',
]

const kafkaJsLintIgnorePatterns = [
  ...(sharedLintConfig?.ignorePatterns ?? []),
  'js-binding*.*',
  'index.js',
  'index.d.ts',
  'npm/**',
]

const kafkaJsSourceLintRules: NonNullable<OxlintConfig['rules']> = {
  '@typescript-eslint/no-unsafe-type-assertion': 'off',
  '@typescript-eslint/non-nullable-type-assertion-style': 'off',
  '@typescript-eslint/no-unnecessary-type-assertion': 'off',
  '@typescript-eslint/no-unnecessary-type-arguments': 'off',
  '@typescript-eslint/promise-function-async': 'off',
  '@typescript-eslint/prefer-readonly': 'off',
  '@typescript-eslint/use-unknown-in-catch-callback-variable': 'off',
  complexity: 'off',
  'no-await-in-loop': 'off',
  'no-continue': 'off',
  'max-params': 'off',
  'no-use-before-define': 'off',
  'no-void': 'off',
  'unicorn/no-new-array': 'off',
}

const kafkaJsLintOverrides = [
  {
    files: ['js-src/**/*'],
    rules: kafkaJsSourceLintRules,
  },
  {
    files: ['tests/**', '**/__test__/**'],
    rules: sharedTestLintRules,
  },
]

export default defineConfig({
  fmt: {
    ...fmtConfig,
    ignorePatterns: kafkaJsFmtIgnorePatterns,
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
    ignorePatterns: kafkaJsLintIgnorePatterns,
    overrides: kafkaJsLintOverrides,
  },
  test: {
    environment: 'node',
    exclude: ['tests/integration/**'],
    include: ['tests/unit/**/*.test.ts'],
  },
})
