use napi::bindgen_prelude::{Buffer, Either};
use napi_derive::napi;

#[napi(object)]
pub struct CreatePdfFromHtmlInput {
  pub html: String,
  pub title: Option<String>,
  #[napi(ts_type = "string | Array<string>")]
  pub css: Option<Either<String, Vec<String>>>,
  pub base_path: Option<String>,
  pub system_fonts: Option<bool>,
  pub page: Option<HtmlPdfPageInput>,
  pub bookmarks: Option<bool>,
  pub tagged: Option<bool>,
  pub pdf_ua: Option<bool>,
  pub fonts: Option<Vec<Buffer>>,
  pub images: Option<Vec<HtmlPdfImageInput>>,
}

#[napi(object)]
pub struct HtmlPdfPageInput {
  #[napi(ts_type = "'A4' | 'LETTER' | 'A3' | HtmlPdfPageCustomSizeInput")]
  pub size: Option<Either<String, HtmlPdfPageCustomSizeInput>>,
  #[napi(ts_type = "number | HtmlPdfPageMarginInput")]
  pub margin: Option<Either<f64, HtmlPdfPageMarginInput>>,
  pub landscape: Option<bool>,
}

#[napi(object)]
pub struct HtmlPdfPageCustomSizeInput {
  pub width: f64,
  pub height: f64,
  #[napi(ts_type = "'mm' | 'pt'")]
  pub unit: Option<String>,
}

#[napi(object)]
pub struct HtmlPdfPageMarginInput {
  pub top: f64,
  pub right: f64,
  pub bottom: f64,
  pub left: f64,
  #[napi(ts_type = "'mm' | 'pt'")]
  pub unit: Option<String>,
}

#[napi(object)]
pub struct HtmlPdfImageInput {
  pub name: String,
  pub data: Buffer,
}
