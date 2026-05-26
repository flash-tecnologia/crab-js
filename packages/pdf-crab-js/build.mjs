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
}

build().catch((error) => {
  console.error('Build failed:', error)
  process.exit(1)
})
