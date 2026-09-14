import { equal, ok } from 'node:assert/strict'
import { execFileSync } from 'node:child_process'
import { cp, mkdir, mkdtemp, readFile, readdir, realpath, rm, symlink, writeFile } from 'node:fs/promises'
import { createRequire } from 'node:module'
import { tmpdir } from 'node:os'
import { basename, join, relative, sep } from 'node:path'
import { fileURLToPath } from 'node:url'

import { NapiCli } from '@napi-rs/cli'

const source = fileURLToPath(new URL('..', import.meta.url))
const temporary = await mkdtemp(join(tmpdir(), 'pdf-crab-package-smoke-'))
const staging = join(temporary, 'package')
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
  const [result] = JSON.parse(npm(['pack', '--ignore-scripts', '--json', '--pack-destination', temporary], cwd))
  return join(temporary, result.filename)
}

function runNode(code, commonjs = false) {
  return execFileSync(process.execPath, ['--input-type', commonjs ? 'commonjs' : 'module', '-e', code], {
    cwd: consumer,
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'pipe'],
  })
}

let passed = false
try {
  await cp(source, staging, {
    recursive: true,
    filter: (file) =>
      !relative(source, file)
        .split(/[\\/]/)
        .some((part) => ignored.has(part)),
  })
  await mkdir(artifacts)
  for (const entry of await readdir(staging, { withFileTypes: true })) {
    if (entry.isFile() && (entry.name.startsWith('pdf-crab-js.') || entry.name.startsWith('wasi-worker'))) {
      await cp(join(staging, entry.name), join(artifacts, entry.name))
    }
  }

  // Build tools are available only to preparation, never to the consumer installation.
  await symlink(join(source, 'node_modules'), join(staging, 'node_modules'), 'junction')
  const napi = new NapiCli()
  await napi.createNpmDirs({ cwd: staging })
  await napi.artifacts({ cwd: staging, outputDir: artifacts, buildOutputDir: staging })
  // Materialize the publication manifest without publishing packages or creating a release.
  await napi.prePublish({ cwd: staging, tagStyle: 'npm', ghRelease: false, skipOptionalPublish: true })

  const rootTarball = pack(staging)
  const overrides = {}
  for (const entry of await readdir(join(staging, 'npm'), { withFileTypes: true })) {
    if (!entry.isDirectory()) continue
    const platform = join(staging, 'npm', entry.name)
    const manifest = JSON.parse(await readFile(join(platform, 'package.json'), 'utf8'))
    overrides[manifest.name] = `file:${pack(platform)}`
  }

  await mkdir(consumer)
  await writeFile(
    join(consumer, 'package.json'),
    JSON.stringify({
      name: 'pdf-crab-isolated-consumer',
      private: true,
      type: 'module',
      dependencies: { 'pdf-crab-js': `file:${rootTarball}` },
      // Resolve unpublished platform versions locally without forcing their installation.
      overrides,
    }),
  )
  npm(['install', '--omit=dev', '--ignore-scripts', '--no-audit', '--no-fund', '--package-lock=false'], consumer)

  const installed = JSON.parse(await readFile(join(consumer, 'node_modules/pdf-crab-js/package.json'), 'utf8'))
  const consumerRequire = createRequire(join(consumer, 'package.json'))
  const installedRoot = await realpath(consumer)
  for (const flavor of ['wasm32-wasip1', 'wasm32-wasi']) {
    const manifest = consumerRequire(`pdf-crab-js-${flavor}/package.json`)
    equal(manifest.version, installed.version)
  }
  for (const name of ['@emnapi/core', '@emnapi/runtime', '@napi-rs/wasm-runtime']) {
    ok(
      consumerRequire.resolve(name).startsWith(`${installedRoot}${sep}`),
      `${name} must come from the consumer installation`,
    )
  }

  const render = `
    const pdf = await new PdfDocument().text('Packed distribution').render().bytes()
    if (Buffer.from(pdf.subarray(0, 5)).toString() !== '%PDF-' || !Buffer.from(pdf).toString().trimEnd().endsWith('%%EOF')) {
      throw new Error('Invalid PDF from packed distribution')
    }
  `
  runNode(`import { PdfDocument } from 'pdf-crab-js'; ${render}`)
  runNode(`const { PdfDocument } = require('pdf-crab-js'); (async () => { ${render} })()`, true)
  runNode(`
    import { equal, notStrictEqual, strictEqual } from 'node:assert/strict'
    import { readFile } from 'node:fs/promises'
    import { createInstance, dispose, instantiate } from 'pdf-crab-js/workerd'
    const module = await WebAssembly.compile(await readFile(new URL(import.meta.resolve('pdf-crab-js/wasm.wasm'))))
    const [first, second] = await Promise.all([instantiate(module), instantiate(module)])
    strictEqual(first, second)
    const pdf = first.createPdf({ pages: [{ height: 120, width: 120 }] })
    equal(Buffer.from(pdf.subarray(0, 5)).toString(), '%PDF-')
    await dispose()
    const fresh = await instantiate(module)
    notStrictEqual(fresh, first)
    await dispose()
    const independent = await createInstance(module)
    await independent.dispose()
  `)

  const input = {}
  for (const [name, entry] of [
    ['browser', 'pdf-crab-js/browser'],
    ['threaded', 'pdf-crab-js/browser/threaded'],
  ]) {
    input[name] = join(consumer, `${name}.html`)
    await writeFile(input[name], `<script type="module" src="/${name}.js"></script>`)
    await writeFile(
      join(consumer, `${name}.js`),
      `import { PdfDocument } from '${entry}'; globalThis.pdfResult = await new PdfDocument().text('Packed browser').render().bytes();`,
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
    `Packed ${basename(rootTarball)}: ESM, CommonJS, Workerd loader lifecycle, and both browser builds passed.`,
  )
  passed = true
} catch (error) {
  console.error(`Package smoke failed; isolated installation retained at ${consumer}`)
  if (error?.stderr) console.error(String(error.stderr))
  throw error
} finally {
  if (passed) await rm(temporary, { recursive: true, force: true })
}
