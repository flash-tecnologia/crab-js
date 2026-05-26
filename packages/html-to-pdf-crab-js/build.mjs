import { readFile, writeFile } from 'node:fs/promises'

import { NapiCli } from '@napi-rs/cli'

const napi = new NapiCli()

const bindings = [
  { jsBinding: 'index.js', esm: true },
  { jsBinding: 'index.cjs', noDtsHeader: true },
]

const argValue = (name) => {
  const prefix = `${name}=`
  const inline = process.argv.find((arg) => arg.startsWith(prefix))
  if (inline) {
    return inline.slice(prefix.length)
  }

  const index = process.argv.indexOf(name)
  return index === -1 ? undefined : process.argv[index + 1]
}

const hasFlag = (...names) => names.some((name) => process.argv.includes(name))

const useAsyncBrowserWasmInstantiation = async () => {
  const path = 'html-to-pdf-crab-js.wasi-browser.js'
  const content = await readFile(path, 'utf8')

  if (!content.includes('__emnapiInstantiateNapiModuleSync')) {
    return
  }

  await writeFile(
    path,
    content
      .replace(
        'instantiateNapiModuleSync as __emnapiInstantiateNapiModuleSync',
        'instantiateNapiModule as __emnapiInstantiateNapiModule',
      )
      .replace('__emnapiInstantiateNapiModuleSync(__wasmFile, {', 'await __emnapiInstantiateNapiModule(__wasmFile, {'),
  )
}

const build = async () => {
  const commonOptions = {
    constEnum: false,
    crossCompile: hasFlag('-x', '--cross-compile'),
    dts: 'index.d.ts',
    platform: true,
    release: !hasFlag('--debug'),
    target: argValue('--target'),
  }

  for (const binding of bindings) {
    const result = await napi.build({
      ...commonOptions,
      ...binding,
    })
    await result.task
  }

  if (commonOptions.target === 'wasm32-wasip1-threads') {
    await useAsyncBrowserWasmInstantiation()
  }
}

build().catch((error) => {
  console.error('Build failed:', error)
  process.exit(1)
})
