declare module '*.wasi-browser.js' {
  export const createPdf: unknown
  export const createPdfAsync: unknown
  const binding: Record<string, unknown>
  export default binding
}
