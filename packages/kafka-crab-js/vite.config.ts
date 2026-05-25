import { defineConfig } from 'vite-plus'
import type { OxlintConfig } from 'vite-plus/lint'
import { sharedFmtConfig, sharedLintConfig, sharedTestLintRules } from '../../vite.shared.mjs'

const kafkaJsLintIgnorePatterns = [
  ...(sharedLintConfig?.ignorePatterns ?? []),
  'js-binding.*',
  'js-binding.d.ts',
  'index.js',
  'index.d.ts',
  'npm/**',
]

const kafkaJsSourceLintRules: NonNullable<OxlintConfig['rules']> = {
  '@typescript-eslint/no-unsafe-type-assertion': 'off',
  '@typescript-eslint/non-nullable-type-assertion-style': 'off',
  '@typescript-eslint/no-non-null-assertion': 'off',
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
  '@typescript-eslint/no-duplicate-type-constituents': 'allow',
  'eslint/no-underscore-dangle': [
    'error',
    {
      allow: [
        '_destroy',
        '_diagnosticsConfig',
        '_diagnosticsEnabled',
        '_instrumentConsumer',
        '_instrumentProducer',
        '_kafkaConsumer',
        '_read',
      ],
    },
  ],
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
    ...sharedFmtConfig,
  },
  lint: {
    ...sharedLintConfig,
    ignorePatterns: kafkaJsLintIgnorePatterns,
    overrides: kafkaJsLintOverrides,
  },
})
