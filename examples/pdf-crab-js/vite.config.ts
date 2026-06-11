import { defineConfig } from 'vite-plus'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { sharedFmtConfig, sharedLintConfig } from '../../vite.shared.mjs'

const currentDirectory = path.dirname(fileURLToPath(import.meta.url))
const workspaceRoot = path.resolve(currentDirectory, '../..')

export default defineConfig({
  build: {
    rollupOptions: {
      input: path.resolve(currentDirectory, 'wasm/index.html'),
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
    exclude: ['pdf-crab-js', 'pdf-crab-js/browser.js', 'pdf-crab-js-wasm32-wasi'],
  },
  resolve: {
    alias: {
      'pdf-crab-js-wasm32-wasi': path.resolve(
        currentDirectory,
        '../../packages/pdf-crab-js/pdf-crab-js.wasi-browser.js',
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
