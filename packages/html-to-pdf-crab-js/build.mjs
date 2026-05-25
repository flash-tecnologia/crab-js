import { spawn } from 'node:child_process'
import { readFile, writeFile } from 'node:fs/promises'
import { basename, dirname, extname, relative } from 'node:path'
import { fileURLToPath } from 'node:url'

import { NapiCli } from '@napi-rs/cli'

const cwd = dirname(fileURLToPath(import.meta.url))
const napi = new NapiCli()

const NAPI_BINDINGS = [
  {
    jsBinding: 'index.js',
    esm: true,
  },
  {
    jsBinding: 'index.cjs',
    noDtsHeader: true,
  },
]

const getArgValue = (name) => {
  const inlinePrefix = `${name}=`
  const inlineArg = process.argv.find((arg) => arg.startsWith(inlinePrefix))
  if (inlineArg) {
    return inlineArg.slice(inlinePrefix.length)
  }

  const index = process.argv.findIndex((arg) => arg === name)
  return index === -1 ? undefined : process.argv[index + 1]
}

const hasFlag = (...names) => names.some((name) => process.argv.includes(name))

const getCliOptions = () => ({
  crossCompile: hasFlag('-x', '--cross-compile'),
  debug: hasFlag('--debug'),
  target: getArgValue('--target'),
})

const runCommand = (command, args) =>
  new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd,
      shell: process.platform === 'win32',
      stdio: 'inherit',
    })

    child.on('error', reject)
    child.on('exit', (code, signal) => {
      if (code === 0) {
        resolve()
        return
      }

      reject(new Error(`${command} ${args.join(' ')} failed with ${signal ?? code}`))
    })
  })

const runNapiTask = async (options) => {
  const result = await napi.build(options)
  return result.task
}

const runNapiBuild = async ({ target, crossCompile, debug }) => {
  const commonConfig = {
    constEnum: false,
    crossCompile,
    dts: 'index.d.ts',
    platform: true,
    release: !debug,
    target,
  }
  const outputs = []

  for (const bindingConfig of NAPI_BINDINGS) {
    outputs.push(
      ...(await runNapiTask({
        ...commonConfig,
        ...bindingConfig,
      })),
    )
  }

  return outputs
}

const getFormattableOutputFiles = (outputs) => {
  const formattableExtensions = new Set(['.cjs', '.js', '.mjs', '.ts'])

  return [
    ...new Set(
      outputs
        .filter((output) => output.kind === 'js' || output.kind === 'dts')
        .map((output) => relative(cwd, output.path))
        .filter((path) => formattableExtensions.has(extname(path)) || path.endsWith('.d.ts')),
    ),
  ]
}

const wasmBindgenImports = `const __wasmBindgenImports = {
  __wbindgen_placeholder__: {
    __wbindgen_describe() {},
  },
  __wbindgen_externref_xform__: {
    __wbindgen_externref_table_set_null() {},
    __wbindgen_externref_table_grow() {
      return -1
    },
  },
}

`

const addWasmBindgenImportsHelper = (content) => {
  if (content.includes('const __wasi =')) {
    return content.replace('const __wasi =', `${wasmBindgenImports}const __wasi =`)
  }

  if (content.includes('const emnapiContext =')) {
    return content.replace('const emnapiContext =', `${wasmBindgenImports}const emnapiContext =`)
  }

  if (content.includes('const errorOutputs =')) {
    return content.replace('const errorOutputs =', `${wasmBindgenImports}const errorOutputs =`)
  }

  return `${wasmBindgenImports}${content}`
}

const addWasmBindgenImportsToOverwriteImports = (content) =>
  content.replaceAll(
    /(\s*)overwriteImports\(importObject\) {\n(\s*)importObject\.env = {/g,
    (_, declarationIndent, bodyIndent) => `${declarationIndent}overwriteImports(importObject) {
${bodyIndent}importObject.__wbindgen_placeholder__ = __wasmBindgenImports.__wbindgen_placeholder__
${bodyIndent}importObject.__wbindgen_externref_xform__ = __wasmBindgenImports.__wbindgen_externref_xform__
${bodyIndent}importObject.env = {`,
  )

const useAsyncBrowserWasmInstantiation = (content) =>
  content
    .replace(
      'instantiateNapiModuleSync as __emnapiInstantiateNapiModuleSync',
      'instantiateNapiModule as __emnapiInstantiateNapiModule',
    )
    .replace('__emnapiInstantiateNapiModuleSync(__wasmFile, {', 'await __emnapiInstantiateNapiModule(__wasmFile, {')

const patchBrowserEntry = async (outputs) => {
  const browserOutput = outputs.find((output) => output.kind === 'js' && basename(output.path) === 'browser.js')
  if (!browserOutput) {
    return
  }

  await writeFile(
    browserOutput.path,
    `import { Buffer as __Buffer } from 'buffer'

globalThis.Buffer ??= __Buffer

const __binding = await import('html-to-pdf-crab-js-wasm32-wasi')

export default __binding.default ?? __binding

function toBase64(value) {
  return __Buffer.from(value).toString('base64')
}

function normalizeInput(input) {
  return {
    ...input,
    fonts: input.fonts?.map(toBase64),
    images: input.images?.map((image) => ({
      ...image,
      data: toBase64(image.data),
    })),
  }
}

export function createPdfFromHtml(input) {
  return Promise.resolve(__binding.createPdfFromHtml(normalizeInput(input)))
}
`,
  )
}

const patchWasmBindgenImports = async (outputs) => {
  const wasiOutputNames = new Set([
    'html-to-pdf-crab-js.wasi.cjs',
    'html-to-pdf-crab-js.wasi-browser.js',
    'wasi-worker.mjs',
    'wasi-worker-browser.mjs',
  ])

  for (const output of outputs) {
    if (output.kind !== 'js' || !wasiOutputNames.has(basename(output.path))) {
      continue
    }

    const content = await readFile(output.path, 'utf8')
    if (content.includes('__wasmBindgenImports')) {
      continue
    }

    let patchedContent = addWasmBindgenImportsToOverwriteImports(addWasmBindgenImportsHelper(content))
    if (basename(output.path) === 'html-to-pdf-crab-js.wasi-browser.js') {
      patchedContent = useAsyncBrowserWasmInstantiation(patchedContent)
    }

    await writeFile(output.path, patchedContent)
  }
}

async function main() {
  const cliOptions = getCliOptions()
  const outputs = await runNapiBuild(cliOptions)
  await patchBrowserEntry(outputs)
  await patchWasmBindgenImports(outputs)
  const files = getFormattableOutputFiles(outputs)
  if (files.length > 0) {
    await runCommand('vp', ['fmt', ...files])
  }
}

main().catch((error) => {
  console.error('Build script failed:', error)
  process.exit(1)
})
