import { defineConfig } from 'vite-plus'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { sharedFmtConfig, sharedLintConfig } from '../../vite.shared.mjs'

const currentDirectory = dirname(fileURLToPath(import.meta.url))
const workspaceRoot = resolve(currentDirectory, '../..')

export default defineConfig({
  build: {
    rollupOptions: {
      input: resolve(currentDirectory, 'wasm/index.html'),
    },
    target: 'esnext',
  },
  fmt: {
    ...sharedFmtConfig,
  },
  lint: {
    ...sharedLintConfig,
    overrides: [
      {
        files: ['**/*.ts'],
        rules: {
          'id-length': 'off',
          'no-console': 'off',
        },
      },
      {
        files: ['wasm/browser.ts'],
        rules: {
          'unicorn/prefer-node-protocol': 'off',
        },
      },
    ],
  },
  optimizeDeps: {
    exclude: ['html-to-pdf-crab-js', 'html-to-pdf-crab-js/browser.js', 'html-to-pdf-crab-js-wasm32-wasi'],
  },
  resolve: {
    alias: {
      'html-to-pdf-crab-js-wasm32-wasi': resolve(
        currentDirectory,
        '../../packages/html-to-pdf-crab-js/html-to-pdf-crab-js.wasi-browser.js',
      ),
    },
  },
  server: {
    fs: {
      allow: [workspaceRoot],
    },
    headers: {
      'Cross-Origin-Embedder-Policy': 'require-corp',
      'Cross-Origin-Opener-Policy': 'same-origin',
    },
    open: '/wasm/',
  },
})
