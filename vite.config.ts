import { defineConfig } from 'vite-plus'
import { sharedFmtConfig } from './vite.shared.mjs'

export default defineConfig({
  fmt: {
    ...sharedFmtConfig,
  },
})
