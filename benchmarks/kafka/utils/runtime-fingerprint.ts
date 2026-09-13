import { createHash } from 'node:crypto'
import { readFileSync, readdirSync, realpathSync, existsSync } from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { cpus, release, totalmem } from 'node:os'

export function captureBenchmarkEnvironment() {
  const root = fileURLToPath(new URL('../', import.meta.url))
  const files = [
    'consumer.ts',
    'tsconfig.json',
    'package.json',
    ...readdirSync(path.join(root, 'utils'))
      .filter((name) => name.endsWith('.ts'))
      .map((name) => `utils/${name}`),
  ]
  return {
    harness: files.toSorted().map((file) => fingerprintFile(path.join(root, file))),
    node: process.version,
    execPath: realpathSync(process.execPath),
    execArgv: process.execArgv,
    platform: process.platform,
    arch: process.arch,
    osRelease: release(),
    cpuModels: [...new Set(cpus().map((cpu) => cpu.model))],
    cpuCount: cpus().length,
    totalMemoryBytes: totalmem(),
    // Deliberate allowlist: never dump the process environment or full report.
    nodeOptions: process.env.NODE_OPTIONS ?? null,
    uvThreadpoolSize: process.env.UV_THREADPOOL_SIZE ?? null,
    tokioWorkerThreads: process.env.TOKIO_WORKER_THREADS ?? null,
    tsxConfig: process.env.TSX_TSCONFIG_PATH ? fingerprintFile(process.env.TSX_TSCONFIG_PATH) : null,
  }
}

export function fingerprintFile(filePath: string) {
  const realPath = realpathSync(filePath)
  const bytes = readFileSync(realPath)
  return { path: realPath, bytes: bytes.length, sha256: createHash('sha256').update(bytes).digest('hex') }
}

// Called only after the measured lifecycle and memory snapshot. Never serialize
// The whole process report includes environment variables and must not be serialized.
export function captureRuntime(packageName: string) {
  const entry = realpathSync(fileURLToPath(import.meta.resolve(packageName)))
  let root = path.dirname(entry)
  while (!existsSync(path.join(root, 'package.json'))) {
    const parent = path.dirname(root)
    if (parent === root) throw new Error(`No package.json for ${entry}`)
    root = parent
  }
  const manifest = JSON.parse(readFileSync(path.join(root, 'package.json'), 'utf8')) as {
    name: string
    version: string
  }
  const report = process.report.getReport() as { sharedObjects?: string[] }
  const nativeBindings = (report.sharedObjects ?? [])
    .filter((filePath) => filePath.endsWith('.node'))
    .map(fingerprintFile)
  if (!nativeBindings.some((file) => file.path.includes('kafka'))) {
    throw new Error('No loaded Kafka native binding found in process report')
  }
  const javascript = [entry]
  const dist = path.join(root, 'dist')
  if (existsSync(dist)) {
    for (const file of readdirSync(dist, { recursive: true, encoding: 'utf8' })) {
      if (/\.(?:c|m)?js$/.test(file)) javascript.push(path.join(dist, file))
    }
  }
  return {
    node: process.version,
    execPath: realpathSync(process.execPath),
    execArgv: process.execArgv,
    tsxConfig: process.env.TSX_TSCONFIG_PATH ? fingerprintFile(process.env.TSX_TSCONFIG_PATH) : undefined,
    platform: process.platform,
    arch: process.arch,
    package: { requested: packageName, name: manifest.name, version: manifest.version, root, entry },
    javascript: [...new Set(javascript)].toSorted().map(fingerprintFile),
    nativeBindings,
  }
}
