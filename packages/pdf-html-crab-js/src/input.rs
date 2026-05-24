use napi::bindgen_prelude::{Buffer, Either};
use napi_derive::napi;

#[napi(object)]
pub struct CreatePdfFromHtmlWithFulgurInput {
  pub html: String,
  pub title: Option<String>,
  #[napi(ts_type = "string | Array<string>")]
  pub css: Option<Either<String, Vec<String>>>,
  pub base_path: Option<String>,
  pub system_fonts: Option<bool>,
  pub page: Option<FulgurPageInput>,
  pub bookmarks: Option<bool>,
  pub tagged: Option<bool>,
  pub pdf_ua: Option<bool>,
  pub fonts: Option<Vec<Buffer>>,
  pub images: Option<Vec<FulgurImageInput>>,
}

#[napi(object)]
pub struct FulgurPageInput {
  #[napi(ts_type = "'A4' | 'LETTER' | 'A3' | FulgurPageCustomSizeInput")]
  pub size: Option<Either<String, FulgurPageCustomSizeInput>>,
  #[napi(ts_type = "number | FulgurPageMarginInput")]
  pub margin: Option<Either<f64, FulgurPageMarginInput>>,
  pub landscape: Option<bool>,
}

#[napi(object)]
pub struct FulgurPageCustomSizeInput {
  pub width: f64,
  pub height: f64,
  #[napi(ts_type = "'mm' | 'pt'")]
  pub unit: Option<String>,
}

#[napi(object)]
pub struct FulgurPageMarginInput {
  pub top: f64,
  pub right: f64,
  pub bottom: f64,
  pub left: f64,
  #[napi(ts_type = "'mm' | 'pt'")]
  pub unit: Option<String>,
}

#[napi(object)]
pub struct FulgurImageInput {
  pub name: String,
  pub data: Buffer,
}
