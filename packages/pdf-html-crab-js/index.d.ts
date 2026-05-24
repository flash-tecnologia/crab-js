export declare function createPdfFromHtmlWithFulgur(input: CreatePdfFromHtmlWithFulgurInput): Promise<Buffer>

export interface CreatePdfFromHtmlWithFulgurInput {
  html: string
  title?: string
  css?: string | Array<string>
  basePath?: string
  systemFonts?: boolean
  page?: FulgurPageInput
  bookmarks?: boolean
  tagged?: boolean
  pdfUa?: boolean
  fonts?: Array<Buffer>
  images?: Array<FulgurImageInput>
}

export interface FulgurImageInput {
  name: string
  data: Buffer
}

export interface FulgurPageCustomSizeInput {
  width: number
  height: number
  unit?: 'mm' | 'pt'
}

export interface FulgurPageInput {
  size?: 'A4' | 'LETTER' | 'A3' | FulgurPageCustomSizeInput
  margin?: number | FulgurPageMarginInput
  landscape?: boolean
}

export interface FulgurPageMarginInput {
  top: number
  right: number
  bottom: number
  left: number
  unit?: 'mm' | 'pt'
}
