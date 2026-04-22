import { defineConfig } from 'vite-plus'
import type { OxlintConfig } from 'vite-plus/lint'
import { sharedFmtConfig, sharedLintConfig, sharedTestLintRules } from '../../vite-plus.shared.ts'

const fmtConfig = sharedFmtConfig ?? {}
const lintConfig = sharedLintConfig ?? {}

const benchmarkLintRules: NonNullable<OxlintConfig['rules']> = {
  ...sharedTestLintRules,
  complexity: 'off',
  '@typescript-eslint/no-unnecessary-type-assertion': 'off',
  '@typescript-eslint/non-nullable-type-assertion-style': 'off',
  '@typescript-eslint/promise-function-async': 'off',
  '@typescript-eslint/unbound-method': 'off',
  '@typescript-eslint/use-unknown-in-catch-callback-variable': 'off',
  'no-constant-condition': 'off',
  'no-use-before-define': 'off',
  'no-void': 'off',
  'unicorn/prefer-ternary': 'off',
}

const benchmarkLintOverrides = [
  {
    files: ['**/*.ts'],
    rules: benchmarkLintRules,
  },
]

export default defineConfig({
  fmt: {
    ...fmtConfig,
  },
  lint: {
    ...lintConfig,
    overrides: benchmarkLintOverrides,
  },
})
