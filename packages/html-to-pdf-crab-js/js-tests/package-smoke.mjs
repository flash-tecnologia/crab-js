import { equal, ok } from 'node:assert/strict'
import { execFileSync } from 'node:child_process'
import { cp, mkdir, mkdtemp, readFile, readdir, realpath, rm, symlink, writeFile } from 'node:fs/promises'
import { createRequire } from 'node:module'
import { tmpdir } from 'node:os'
import { basename, join, relative, sep } from 'node:path'
import { fileURLToPath } from 'node:url'

import { NapiCli, parseTriple } from '@napi-rs/cli'

const source = fileURLToPath(new URL('..', import.meta.url))
const temporary = await mkdtemp(join(tmpdir(), 'html-to-pdf-crab-package-smoke-'))
const staging = join(temporary, 'packages', 'html-to-pdf-crab-js')
const artifacts = join(temporary, 'artifacts')
const consumer = join(temporary, 'consumer')
const ignored = new Set(['node_modules', 'target', 'npm', '.git'])

function npm(args, cwd) {
  return execFileSync('npm', args, {
    cwd,
    encoding: 'utf8',
    shell: process.platform === 'win32',
    stdio: ['ignore', 'pipe', 'pipe'],
  })
}

function pack(cwd) {
  const result = JSON.parse(
    execFileSync('pnpm', ['pack', '--ignore-scripts', '--json', '--pack-destination', temporary], {
      cwd,
      encoding: 'utf8',
      shell: process.platform === 'win32',
      stdio: ['ignore', 'pipe', 'pipe'],
    }),
  )
  return result.filename
}

function runNode(code, commonjs = false, flavor) {
  const env = { ...process.env }
  for (const name of ['NAPI_RS_NATIVE_LIBRARY_PATH', 'NAPI_RS_FORCE_WASI', 'NAPI_RS_WASI_FLAVOR', 'NODE_PATH']) {
    delete env[name]
  }
  if (flavor) env.NAPI_RS_WASI_FLAVOR = flavor
  return execFileSync(process.execPath, ['--input-type', commonjs ? 'commonjs' : 'module', '-e', code], {
    cwd: consumer,
    encoding: 'utf8',
    env,
    stdio: ['ignore', 'pipe', 'pipe'],
  })
}

let passed = false
try {
  await mkdir(join(temporary, 'packages'))
  // Packing needs the workspace catalogs, but the installed consumer must stand alone.
  await cp(join(source, '../../pnpm-workspace.yaml'), join(temporary, 'pnpm-workspace.yaml'))
  await cp(source, staging, {
    recursive: true,
    filter: (file) =>
      !relative(source, file)
        .split(/[\\/]/)
        .some((part) => ignored.has(part)),
  })
  await mkdir(artifacts)
  const files = await readdir(staging, { withFileTypes: true })
  for (const entry of files) {
    if (entry.isFile() && (entry.name.startsWith('html-to-pdf-crab-js.') || entry.name.startsWith('wasi-worker'))) {
      await cp(join(staging, entry.name), join(artifacts, entry.name))
    }
  }

  const manifestPath = join(staging, 'package.json')
  const manifest = JSON.parse(await readFile(manifestPath, 'utf8'))
  const artifactNames = new Set(files.filter((entry) => entry.isFile()).map((entry) => entry.name))
  const availableTargets = manifest.napi.targets.filter((triple) => {
    const target = parseTriple(triple)
    const extension = target.platform === 'wasi' ? 'wasm' : 'node'
    return artifactNames.has(`${manifest.napi.binaryName}.${target.platformArchABI}.${extension}`)
  })
  if (process.env.CI === 'true') {
    equal(availableTargets.length, manifest.napi.targets.length, 'CI must provide every configured release artifact')
  }
  ok(availableTargets.length > 0, 'Build the native and WASM artifacts before testing the packed distribution')
  // Local builds can contain only the current platform. Restrict this temporary copy only.
  manifest.napi.targets = availableTargets
  await writeFile(manifestPath, JSON.stringify(manifest, null, 2))

  await symlink(join(source, 'node_modules'), join(staging, 'node_modules'), 'junction')
  const napi = new NapiCli()
  await napi.createNpmDirs({ cwd: staging })
  await napi.artifacts({ cwd: staging, outputDir: artifacts, buildOutputDir: staging })
  await napi.prePublish({ cwd: staging, tagStyle: 'npm', ghRelease: false, skipOptionalPublish: true })

  const rootTarball = pack(staging)
  const rootManifest = JSON.parse(await readFile(manifestPath, 'utf8'))
  const overrides = {}
  for (const entry of await readdir(join(staging, 'npm'), { withFileTypes: true })) {
    if (!entry.isDirectory()) continue
    const platform = join(staging, 'npm', entry.name)
    const platformManifest = JSON.parse(await readFile(join(platform, 'package.json'), 'utf8'))
    equal(platformManifest.version, rootManifest.version)
    // Resolve unpublished platform versions locally; runtime dependencies still come from the registry.
    overrides[platformManifest.name] = `file:${pack(platform)}`
  }

  await mkdir(consumer)
  await cp(join(source, '../../examples/html-to-pdf-crab-js/assets/Tuffy.ttf'), join(consumer, 'Tuffy.ttf'))
  await writeFile(
    join(consumer, 'package.json'),
    JSON.stringify({
      name: 'html-to-pdf-crab-isolated-consumer',
      private: true,
      type: 'module',
      dependencies: { 'html-to-pdf-crab-js': `file:${rootTarball}` },
      overrides,
    }),
  )
  npm(['install', '--omit=dev', '--ignore-scripts', '--no-audit', '--no-fund', '--package-lock=false'], consumer)

  const installed = JSON.parse(await readFile(join(consumer, 'node_modules/html-to-pdf-crab-js/package.json'), 'utf8'))
  ok(!JSON.stringify(installed).includes('catalog:'), 'Packed manifests must resolve workspace catalogs')
  ok(!JSON.stringify(installed).includes('workspace:'), 'Packed manifests must resolve workspace dependencies')
  const consumerRequire = createRequire(join(consumer, 'package.json'))
  const installedRoot = await realpath(consumer)
  for (const name of ['@emnapi/core', '@emnapi/runtime', '@napi-rs/wasm-runtime']) {
    ok(installed.dependencies[name], `${name} must be a production dependency`)
    ok(
      consumerRequire.resolve(name).startsWith(`${installedRoot}${sep}`),
      `${name} must come from the consumer installation`,
    )
  }
  // WASM packages must install normally on a native host, without a CPU override or a second install.
  for (const flavor of ['wasm32-wasip1', 'wasm32-wasi']) {
    const platform = consumerRequire(`html-to-pdf-crab-js-${flavor}/package.json`)
    equal(platform.version, installed.version)
    equal(installed.optionalDependencies[platform.name], installed.version)
    ok(!platform.cpu, `${flavor} must not be filtered by the host CPU`)
  }

  const render = `
    const pdf = await createPdfFromHtml({
      html: '<!doctype html><html><body><h1>Packed distribution</h1><p>Portuguese document.</p></body></html>',
      css: 'body { font-family: Tuffy; }',
      fonts: [readFileSync('Tuffy.ttf').toString('base64')],
      systemFonts: false,
      lang: 'pt-BR',
      pdfUa: true,
      title: 'Packed distribution',
    })
    const text = Buffer.from(pdf).toString('latin1')
    equal(text.slice(0, 5), '%PDF-')
    ok(text.trimEnd().endsWith('%%EOF'))
    ok(text.includes('/Lang (pt-BR)'))
    ok(text.includes('/StructTreeRoot'))
  `
  const esm = `
    import { equal, ok } from 'node:assert/strict'
    import { readFileSync } from 'node:fs'
    import { createPdfFromHtml, __napiBindingTarget } from 'html-to-pdf-crab-js'
  `
  runNode(`${esm} equal(__napiBindingTarget, 'native'); ${render}`)
  runNode(
    `const { equal, ok } = require('node:assert/strict')
     const { readFileSync } = require('node:fs')
     const { createPdfFromHtml, __napiBindingTarget } = require('html-to-pdf-crab-js')
     equal(__napiBindingTarget, 'native')
     ;(async () => { ${render} })().catch(error => { console.error(error); process.exitCode = 1 })`,
    true,
  )
  for (const flavor of ['wasm32-wasip1', 'wasm32-wasi']) {
    runNode(`${esm} equal(__napiBindingTarget, '${flavor}'); ${render}`, false, flavor)
  }

  runNode(`
    import { equal, notStrictEqual, ok, strictEqual } from 'node:assert/strict'
    import { readFileSync } from 'node:fs'
    import { createInstance, dispose, instantiate } from 'html-to-pdf-crab-js/workerd'
    const module = await WebAssembly.compile(readFileSync(new URL(import.meta.resolve('html-to-pdf-crab-js/wasm.wasm'))))
    const [first, second] = await Promise.all([instantiate(module), instantiate(module)])
    strictEqual(first, second)
    {
      const { createPdfFromHtml } = first
      ${render}
    }
    await dispose()
    const fresh = await instantiate(module)
    notStrictEqual(fresh, first)
    await dispose()
    const independent = await createInstance(module)
    {
      const { createPdfFromHtml } = independent.exports
      ${render}
    }
    await independent.dispose()
  `)

  const input = {}
  for (const [name, entry] of [
    ['browser', 'html-to-pdf-crab-js/browser'],
    ['threaded', 'html-to-pdf-crab-js/browser/threaded'],
  ]) {
    input[name] = join(consumer, `${name}.html`)
    await writeFile(input[name], `<script type="module" src="/${name}.js"></script>`)
    await writeFile(
      join(consumer, `${name}.js`),
      `import { createPdfFromHtml } from '${entry}'; globalThis.pdfResult = await createPdfFromHtml({ html: '<!doctype html><h1>Packed browser</h1>', lang: 'pt-BR' });`,
    )
  }
  const require = createRequire(import.meta.resolve('vite-plus'))
  const { build } = await import(require.resolve('vite'))
  await build({
    root: consumer,
    configFile: false,
    logLevel: 'error',
    build: { target: 'esnext', rollupOptions: { input } },
  })
  console.log(
    `Packed ${basename(rootTarball)}: native ESM/CommonJS, both WASM flavors, Workerd lifecycle, and both browser builds passed.`,
  )
  passed = true
} catch (error) {
  console.error(`Package smoke failed; isolated installation retained at ${consumer}`)
  if (error?.stderr) console.error(String(error.stderr))
  throw error
} finally {
  if (passed) await rm(temporary, { recursive: true, force: true })
}
