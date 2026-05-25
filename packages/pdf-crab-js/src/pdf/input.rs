use napi_derive::napi;

#[napi(object)]
pub struct CreatePdfInput {
  pub title: Option<String>,
  #[napi(ts_type = "'mm' | 'pt'")]
  pub unit: Option<String>,
  pub metadata: Option<PdfMetadataInput>,
  pub pages: Option<Vec<PdfPageInput>>,
}

#[napi(object)]
pub struct PdfDocumentBuilderInput {
  pub title: Option<String>,
  #[napi(ts_type = "'mm' | 'pt'")]
  pub unit: Option<String>,
  pub metadata: Option<PdfMetadataInput>,
}

#[napi(object)]
pub struct PdfPageSetupInput {
  pub width: f64,
  pub height: f64,
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
  #[napi(ts_type = "'text' | 'line' | 'rect' | 'textBox' | 'polygon' | 'path'")]
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
  pub trapped: Option<bool>,
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
  pub color: Option<String>,
}
