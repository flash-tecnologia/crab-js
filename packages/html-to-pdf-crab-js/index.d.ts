export declare function createPdfFromHtml(input: CreatePdfFromHtmlInput): Promise<Buffer>

export interface CreatePdfFromHtmlInput {
  html: string
  title?: string
  css?: string | Array<string>
  basePath?: string
  systemFonts?: boolean
  page?: HtmlPdfPageInput
  bookmarks?: boolean
  tagged?: boolean
  pdfUa?: boolean
  fonts?: Array<Buffer>
  images?: Array<HtmlPdfImageInput>
}

export interface HtmlPdfImageInput {
  name: string
  data: Buffer
}

export interface HtmlPdfPageCustomSizeInput {
  width: number
  height: number
  unit?: 'mm' | 'pt'
}

export interface HtmlPdfPageInput {
  size?: 'A4' | 'LETTER' | 'A3' | HtmlPdfPageCustomSizeInput
  margin?: number | HtmlPdfPageMarginInput
  landscape?: boolean
}

export interface HtmlPdfPageMarginInput {
  top: number
  right: number
  bottom: number
  left: number
  unit?: 'mm' | 'pt'
}
