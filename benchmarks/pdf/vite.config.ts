import { defineConfig } from 'vite-plus'
import type { OxlintConfig } from 'vite-plus/lint'
import { sharedFmtConfig, sharedLintConfig, sharedTestLintRules } from '../../vite.shared.mjs'

const benchmarkLintRules: NonNullable<OxlintConfig['rules']> = {
  ...sharedTestLintRules,
  '@typescript-eslint/no-unnecessary-type-assertion': 'off',
  '@typescript-eslint/no-unsafe-type-assertion': 'off',
  '@typescript-eslint/promise-function-async': 'off',
  complexity: 'off',
  'max-params': 'off',
  'max-statements': 'off',
  'no-nested-ternary': 'off',
  'no-use-before-define': 'off',
  'unicorn/prefer-ternary': 'off',
}

export default defineConfig({
  fmt: {
    ...sharedFmtConfig,
  },
  lint: {
    ...sharedLintConfig,
    overrides: [
      {
        files: ['**/*.ts'],
        rules: benchmarkLintRules,
      },
    ],
  },
})
