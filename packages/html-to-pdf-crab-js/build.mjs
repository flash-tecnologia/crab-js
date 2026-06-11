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
  const path = new URL('./html-to-pdf-crab-js.wasi-browser.js', import.meta.url)
  let content

  try {
    content = await readFile(path, 'utf8')
  } catch (error) {
    if (error && typeof error === 'object' && 'code' in error && error.code === 'ENOENT') {
      return
    }

    throw error
  }

  if (!content.includes('__emnapiInstantiateNapiModuleSync')) {
    return
  }

  const rewritten = content
    .replaceAll(
      'instantiateNapiModuleSync as __emnapiInstantiateNapiModuleSync',
      'instantiateNapiModule as __emnapiInstantiateNapiModule',
    )
    .replaceAll('__emnapiInstantiateNapiModuleSync(', 'await __emnapiInstantiateNapiModule(')

  if (rewritten.includes('__emnapiInstantiateNapiModuleSync')) {
    throw new Error(`Failed to rewrite ${path} to async WASM instantiation`)
  }

  await writeFile(path, rewritten)
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

  await useAsyncBrowserWasmInstantiation()
}

build().catch((error) => {
  console.error('Build failed:', error)
  process.exit(1)
})
