export declare function createPdf(input: CreatePdfInput): Buffer

export declare function createPdfAsync(input: CreatePdfInput): Promise<Buffer>

export declare function createPdfFromHtml(input: CreatePdfFromHtmlInput): Buffer

export declare function createPdfFromHtmlAsync(input: CreatePdfFromHtmlInput): Promise<Buffer>

export interface CreatePdfFromHtmlInput {
  title?: string
  html?: string
  pageWidth?: number
  pageHeight?: number
  marginTop?: number
  marginRight?: number
  marginBottom?: number
  marginLeft?: number
  showPageNumbers?: boolean
  headerText?: string
  footerText?: string
  skipFirstPage?: boolean
  images?: Record<string, Buffer | string>
  fonts?: Record<string, Buffer | string>
  saveOptions?: PdfSaveOptionsInput
}

export interface CreatePdfInput {
  title?: string
  unit?: 'mm' | 'pt'
  metadata?: PdfMetadataInput
  saveOptions?: PdfSaveOptionsInput
  conformance?: 'pdf1_3' | 'pdfA1B' | 'pdfX1A2001' | 'pdfX3_2002'
  bookmarks?: Array<PdfBookmarkInput>
  layers?: Array<PdfLayerInput>
  pages?: Array<PdfPageInput>
}

export interface ParsedPdf {
  metadata: ParsedPdfMetadata
  pageCount: number
  pageSizes: Array<ParsedPdfPageSize>
  bookmarks: Array<ParsedPdfBookmark>
  warnings: Array<string>
}

export interface ParsedPdfBookmark {
  name: string
  pageIndex: number
}

export interface ParsedPdfMetadata {
  title: string
  author: string
  creator: string
  producer: string
  subject: string
  keywords: Array<string>
  identifier: string
  conformance: string
}

export interface ParsedPdfPageSize {
  width: number
  height: number
}

export declare function parsePdf(input: ParsePdfInput): ParsedPdf

export interface ParsePdfInput {
  pdf: Buffer | string
  failOnError?: boolean
}

export interface PdfAnnotationInput {
  type: 'link'
  x?: number
  y?: number
  width?: number
  height?: number
  url?: string
  pageIndex?: number
  left?: number
  top?: number
  zoom?: number
  color?: string
}

export interface PdfBookmarkInput {
  name: string
  pageIndex: number
}

export interface PdfElementInput {
  type: 'text' | 'line' | 'rect' | 'image' | 'svg' | 'textBox' | 'polygon' | 'path'
  text?: string
  x?: number
  y?: number
  x1?: number
  y1?: number
  x2?: number
  y2?: number
  width?: number
  height?: number
  font?: string
  fontSize?: number
  fill?: string
  stroke?: string
  strokeWidth?: number
  layer?: string
  source?: Buffer | string
  format?: 'png' | 'jpeg' | 'gif' | 'bmp' | 'tiff' | 'webp' | 'ico' | 'pnm' | 'tga' | 'dds' | 'hdr'
  svg?: string
  align?: 'left' | 'center' | 'right' | 'justify'
  lineHeight?: number
  hyphenate?: boolean
  points?: Array<PdfPointInput>
  closed?: boolean
  winding?: 'nonZero' | 'evenOdd'
}

export interface PdfImageOptimizationInput {
  quality?: number
  maxImageSize?: string
  ditherGreyscale?: boolean
  convertToGreyscale?: boolean
  autoOptimize?: boolean
  format?: 'auto' | 'jpeg' | 'jpeg2000' | 'flate' | 'lzw' | 'runLength' | 'none'
}

export interface PdfLayerInput {
  id?: string
  name: string
}

export interface PdfMetadataInput {
  title?: string
  author?: string
  creator?: string
  producer?: string
  subject?: string
  keywords?: Array<string>
  identifier?: string
  trapped?: boolean
}

export interface PdfPageInput {
  width: number
  height: number
  elements?: Array<PdfElementInput>
  annotations?: Array<PdfAnnotationInput>
}

export interface PdfPointInput {
  x: number
  y: number
  bezier?: boolean
}

export interface PdfSaveOptionsInput {
  optimize?: boolean
  subsetFonts?: boolean
  secure?: boolean
  imageOptimization?: PdfImageOptimizationInput
}

export declare function renderPdfPageToSvg(input: RenderPdfPageToSvgInput): string

export declare function renderPdfPageToSvgAsync(input: RenderPdfPageToSvgInput): Promise<string>

export interface RenderPdfPageToSvgInput {
  pdf: Buffer | string
  pageIndex: number
  imageFormats?: Array<'png' | 'jpeg' | 'webp' | 'avif' | 'gif' | 'bmp' | 'tiff' | 'tga' | 'pnm'>
}
