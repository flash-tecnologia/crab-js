import { defineConfig } from 'vite-plus'
import { sharedFmtConfig, sharedLintConfig, sharedTestLintRules } from './vite-plus.shared.ts'

const fmtConfig = sharedFmtConfig ?? {}
const lintConfig = sharedLintConfig ?? {}

const fmtIgnorePatterns = [
  ...(sharedFmtConfig?.ignorePatterns ?? []),
  'packages/kafka-crab-js/index.js',
  'packages/kafka-crab-js/index.d.ts',
  'packages/kafka-crab-js/js-binding*.*',
]

const lintIgnorePatterns = [
  ...(sharedLintConfig?.ignorePatterns ?? []),
  'packages/kafka-crab-js/js-binding*.*',
  'packages/kafka-crab-js/index.js',
  'packages/kafka-crab-js/index.d.ts',
]

export default defineConfig({
  fmt: {
    ...fmtConfig,
    ignorePatterns: fmtIgnorePatterns,
  },
  lint: {
    ...lintConfig,
    ignorePatterns: lintIgnorePatterns,
    overrides: [
      {
        files: ['**/example/**', '**/__test__/**', '**/benchmark/**', '**/examples/**'],
        rules: sharedTestLintRules,
      },
    ],
  },
})
