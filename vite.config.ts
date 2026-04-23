import { defineConfig } from 'vite-plus'
import { sharedFmtConfig } from './vite-plus.shared.ts'

export default defineConfig({
  fmt: {
    ...sharedFmtConfig,
  },
})
