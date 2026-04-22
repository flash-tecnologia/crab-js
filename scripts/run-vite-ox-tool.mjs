import { existsSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { spawnSync } from 'node:child_process'
import { createRequire } from 'node:module'
import { pathToFileURL } from 'node:url'

const [tool, ...toolArgs] = process.argv.slice(2)
const require = createRequire(import.meta.url)

if (tool !== 'fmt' && tool !== 'lint') {
  console.error('Usage: node scripts/run-vite-ox-tool.mjs <fmt|lint> [...args]')
  process.exit(1)
}

const cwd = process.cwd()
const viteConfigPath = join(cwd, 'vite.config.ts')

if (!existsSync(viteConfigPath)) {
  console.error(`Missing vite.config.ts in ${cwd}`)
  process.exit(1)
}

const configModule = await import(pathToFileURL(viteConfigPath).href)
const viteConfig = configModule.default ?? {}
const toolConfig = viteConfig[tool] ?? {}

const tempConfigPath = join(
  cwd,
  tool === 'fmt' ? '.vite-plus-temp.oxfmtrc.json' : '.vite-plus-temp.oxlintrc.json',
)
const vitePlusPackagePath = require.resolve('vite-plus/package.json')
const vitePlusDir = dirname(vitePlusPackagePath)
const vitePlusBinSource = readFileSync(join(vitePlusDir, 'dist', 'bin.js'), 'utf8')
const resolverModuleMatch = vitePlusBinSource.match(/from "\.\/(main-[^"]+\.js)"/)

if (!resolverModuleMatch) {
  console.error('Could not resolve Vite+ internal package resolver')
  process.exit(1)
}

const resolverModulePath = join(vitePlusDir, 'dist', resolverModuleMatch[1])
const resolverModule = await import(pathToFileURL(resolverModulePath).href)
const resolvePackage = resolverModule.u

if (typeof resolvePackage !== 'function') {
  console.error('Vite+ internal package resolver is not available')
  process.exit(1)
}

const packageEntry = resolvePackage(tool === 'fmt' ? 'oxfmt' : 'oxlint')
const binaryPath = join(dirname(dirname(packageEntry)), 'bin', tool === 'fmt' ? 'oxfmt' : 'oxlint')

writeFileSync(tempConfigPath, JSON.stringify(toolConfig))

const result = spawnSync(binaryPath, ['-c', tempConfigPath, ...toolArgs], {
  cwd,
  stdio: 'inherit',
})

rmSync(tempConfigPath, { force: true })

if (result.error) {
  console.error(result.error.message)
  process.exit(1)
}

if (result.status !== null) {
  process.exit(result.status)
}

process.exit(1)
