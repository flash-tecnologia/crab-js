import { defineConfig } from 'vite-plus'
import { sharedFmtConfig, sharedLintConfig, sharedTestLintRules } from '../../vite.shared.mjs'

const pdfLintIgnorePatterns = [
  ...(sharedLintConfig?.ignorePatterns ?? []),
  'index.js',
  'index.cjs',
  'index.d.ts',
  'npm/**',
]

export default defineConfig({
  fmt: {
    ...sharedFmtConfig,
  },
  lint: {
    ...sharedLintConfig,
    ignorePatterns: pdfLintIgnorePatterns,
    overrides: [
      {
        files: ['examples/**'],
        rules: {
          'id-length': 'off',
          'no-console': 'off',
        },
      },
      {
        files: ['js-tests/**'],
        rules: sharedTestLintRules,
      },
    ],
  },
})
