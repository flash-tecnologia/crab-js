use napi::bindgen_prelude::{Buffer, Either};
use napi_derive::napi;
use std::collections::BTreeMap;

pub(super) type BytesOrString = Either<Buffer, String>;

#[napi(object)]
pub struct CreatePdfInput {
  pub title: Option<String>,
  #[napi(ts_type = "'mm' | 'pt'")]
  pub unit: Option<String>,
  pub metadata: Option<PdfMetadataInput>,
  pub save_options: Option<PdfSaveOptionsInput>,
  #[napi(ts_type = "'pdf1_3' | 'pdfA1B' | 'pdfX1A2001' | 'pdfX3_2002'")]
  pub conformance: Option<String>,
  pub bookmarks: Option<Vec<PdfBookmarkInput>>,
  pub layers: Option<Vec<PdfLayerInput>>,
  pub pages: Option<Vec<PdfPageInput>>,
}

#[napi(object)]
pub struct CreatePdfFromHtmlInput {
  pub title: Option<String>,
  pub html: Option<String>,
  pub page_width: Option<f64>,
  pub page_height: Option<f64>,
  pub margin_top: Option<f64>,
  pub margin_right: Option<f64>,
  pub margin_bottom: Option<f64>,
  pub margin_left: Option<f64>,
  pub show_page_numbers: Option<bool>,
  pub header_text: Option<String>,
  pub footer_text: Option<String>,
  pub skip_first_page: Option<bool>,
  #[napi(ts_type = "Record<string, Buffer | string>")]
  pub images: Option<BTreeMap<String, BytesOrString>>,
  #[napi(ts_type = "Record<string, Buffer | string>")]
  pub fonts: Option<BTreeMap<String, BytesOrString>>,
  pub save_options: Option<PdfSaveOptionsInput>,
}

#[napi(object)]
pub struct PdfPageInput {
  pub width: f64,
  pub height: f64,
  pub elements: Option<Vec<PdfElementInput>>,
  pub annotations: Option<Vec<PdfAnnotationInput>>,
}

#[napi(object)]
pub struct PdfElementInput {
  #[napi(ts_type = "'text' | 'line' | 'rect' | 'image' | 'svg' | 'textBox' | 'polygon' | 'path'")]
  pub r#type: String,
  pub text: Option<String>,
  pub x: Option<f64>,
  pub y: Option<f64>,
  pub x1: Option<f64>,
  pub y1: Option<f64>,
  pub x2: Option<f64>,
  pub y2: Option<f64>,
  pub width: Option<f64>,
  pub height: Option<f64>,
  pub font: Option<String>,
  pub font_size: Option<f64>,
  pub fill: Option<String>,
  pub stroke: Option<String>,
  pub stroke_width: Option<f64>,
  pub layer: Option<String>,
  #[napi(ts_type = "Buffer | string")]
  pub source: Option<BytesOrString>,
  #[napi(
    ts_type = "'png' | 'jpeg' | 'gif' | 'bmp' | 'tiff' | 'webp' | 'ico' | 'pnm' | 'tga' | 'dds' | 'hdr'"
  )]
  pub format: Option<String>,
  pub svg: Option<String>,
  #[napi(ts_type = "'left' | 'center' | 'right' | 'justify'")]
  pub align: Option<String>,
  pub line_height: Option<f64>,
  pub hyphenate: Option<bool>,
  pub points: Option<Vec<PdfPointInput>>,
  pub closed: Option<bool>,
  #[napi(ts_type = "'nonZero' | 'evenOdd'")]
  pub winding: Option<String>,
}

#[napi(object)]
pub struct PdfPointInput {
  pub x: f64,
  pub y: f64,
  pub bezier: Option<bool>,
}

#[napi(object)]
pub struct PdfMetadataInput {
  pub title: Option<String>,
  pub author: Option<String>,
  pub creator: Option<String>,
  pub producer: Option<String>,
  pub subject: Option<String>,
  pub keywords: Option<Vec<String>>,
  pub identifier: Option<String>,
  pub trapped: Option<bool>,
}

#[napi(object)]
pub struct PdfSaveOptionsInput {
  pub optimize: Option<bool>,
  pub subset_fonts: Option<bool>,
  pub secure: Option<bool>,
  pub image_optimization: Option<PdfImageOptimizationInput>,
}

#[napi(object)]
pub struct PdfImageOptimizationInput {
  pub quality: Option<f64>,
  pub max_image_size: Option<String>,
  pub dither_greyscale: Option<bool>,
  pub convert_to_greyscale: Option<bool>,
  pub auto_optimize: Option<bool>,
  #[napi(ts_type = "'auto' | 'jpeg' | 'jpeg2000' | 'flate' | 'lzw' | 'runLength' | 'none'")]
  pub format: Option<String>,
}

#[napi(object)]
pub struct PdfBookmarkInput {
  pub name: String,
  pub page_index: u32,
}

#[napi(object)]
pub struct PdfLayerInput {
  pub id: Option<String>,
  pub name: String,
}

#[napi(object)]
pub struct PdfAnnotationInput {
  #[napi(ts_type = "'link'")]
  pub r#type: String,
  pub x: Option<f64>,
  pub y: Option<f64>,
  pub width: Option<f64>,
  pub height: Option<f64>,
  pub url: Option<String>,
  pub page_index: Option<u32>,
  pub left: Option<f64>,
  pub top: Option<f64>,
  pub zoom: Option<f64>,
  pub color: Option<String>,
}

#[napi(object)]
pub struct ParsePdfInput {
  #[napi(ts_type = "Buffer | string")]
  pub pdf: BytesOrString,
  pub fail_on_error: Option<bool>,
}

#[napi(object)]
pub struct RenderPdfPageToSvgInput {
  #[napi(ts_type = "Buffer | string")]
  pub pdf: BytesOrString,
  pub page_index: u32,
  #[napi(
    ts_type = "Array<'png' | 'jpeg' | 'webp' | 'avif' | 'gif' | 'bmp' | 'tiff' | 'tga' | 'pnm'>"
  )]
  pub image_formats: Option<Vec<String>>,
}

#[napi(object)]
pub struct ParsedPdf {
  pub metadata: ParsedPdfMetadata,
  pub page_count: u32,
  pub page_sizes: Vec<ParsedPdfPageSize>,
  pub bookmarks: Vec<ParsedPdfBookmark>,
  pub warnings: Vec<String>,
}

#[napi(object)]
pub struct ParsedPdfMetadata {
  pub title: String,
  pub author: String,
  pub creator: String,
  pub producer: String,
  pub subject: String,
  pub keywords: Vec<String>,
  pub identifier: String,
  pub conformance: String,
}

#[napi(object)]
pub struct ParsedPdfPageSize {
  pub width: f64,
  pub height: f64,
}

#[napi(object)]
pub struct ParsedPdfBookmark {
  pub name: String,
  pub page_index: u32,
}
