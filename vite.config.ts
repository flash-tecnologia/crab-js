import { defineConfig } from 'vite-plus'
import { sharedFmtConfig, sharedLintConfig } from './vite-plus.shared.ts'

const fmtConfig = sharedFmtConfig ?? {}
const lintConfig = sharedLintConfig ?? {}

export default defineConfig({
  fmt: {
    ...fmtConfig,
  },
  lint: {
    ...lintConfig,
  },
})
