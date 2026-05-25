export declare class PdfDocumentBuilder {
  constructor(input?: PdfDocumentBuilderInput | undefined | null)
  startPage(page: PdfPageSetupInput): void
  appendElements(elements: Array<PdfElementInput>): void
  appendAnnotations(annotations: Array<PdfAnnotationInput>): void
  endPage(): void
  addPage(page: PdfPageInput): void
  addPages(pages: Array<PdfPageInput>): void
  finish(): Buffer
  finishAsync(): Promise<Buffer>
}

export declare function createPdf(input: CreatePdfInput): Buffer

export declare function createPdfAsync(input: CreatePdfInput): Promise<Buffer>

export interface CreatePdfInput {
  title?: string
  unit?: 'mm' | 'pt'
  metadata?: PdfMetadataInput
  pages?: Array<PdfPageInput>
}

export interface PdfAnnotationInput {
  type: 'link'
  x?: number
  y?: number
  width?: number
  height?: number
  url?: string
  color?: string
}

export interface PdfDocumentBuilderInput {
  title?: string
  unit?: 'mm' | 'pt'
  metadata?: PdfMetadataInput
}

export interface PdfElementInput {
  type: 'text' | 'line' | 'rect' | 'textBox' | 'polygon' | 'path'
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
  align?: 'left' | 'center' | 'right' | 'justify'
  lineHeight?: number
  hyphenate?: boolean
  points?: Array<PdfPointInput>
  closed?: boolean
  winding?: 'nonZero' | 'evenOdd'
}

export interface PdfMetadataInput {
  title?: string
  author?: string
  creator?: string
  producer?: string
  subject?: string
  keywords?: Array<string>
  trapped?: boolean
}

export interface PdfPageInput {
  width: number
  height: number
  elements?: Array<PdfElementInput>
  annotations?: Array<PdfAnnotationInput>
}

export interface PdfPageSetupInput {
  width: number
  height: number
}

export interface PdfPointInput {
  x: number
  y: number
  bezier?: boolean
}
