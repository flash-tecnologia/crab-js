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
  #[napi(ts_type = "Array<Buffer>")]
  pub fonts: Option<Either<Vec<Buffer>, Vec<String>>>,
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
  #[napi(ts_type = "Buffer")]
  pub data: Either<Buffer, String>,
}

pub(crate) struct RenderPdfFromHtmlInput {
  pub html: String,
  pub title: Option<String>,
  pub css: Option<Either<String, Vec<String>>>,
  pub base_path: Option<String>,
  pub system_fonts: Option<bool>,
  pub page: Option<HtmlPdfPageInput>,
  pub bookmarks: Option<bool>,
  pub tagged: Option<bool>,
  pub pdf_ua: Option<bool>,
  pub fonts: Option<RenderPdfFontInput>,
  pub images: Option<Vec<RenderPdfImageInput>>,
}

pub(crate) enum RenderPdfFontInput {
  Bytes(Vec<Vec<u8>>),
  Base64(Vec<String>),
}

pub(crate) struct RenderPdfImageInput {
  pub name: String,
  pub data: RenderPdfBinaryInput,
}

pub(crate) enum RenderPdfBinaryInput {
  Bytes(Vec<u8>),
  Base64(String),
}

impl From<CreatePdfFromHtmlInput> for RenderPdfFromHtmlInput {
  fn from(input: CreatePdfFromHtmlInput) -> Self {
    Self {
      html: input.html,
      title: input.title,
      css: input.css,
      base_path: input.base_path,
      system_fonts: input.system_fonts,
      page: input.page,
      bookmarks: input.bookmarks,
      tagged: input.tagged,
      pdf_ua: input.pdf_ua,
      fonts: input.fonts.map(|fonts| match fonts {
        Either::A(fonts) => RenderPdfFontInput::Bytes(fonts.into_iter().map(Vec::from).collect()),
        Either::B(fonts) => RenderPdfFontInput::Base64(fonts),
      }),
      images: input.images.map(|images| {
        images
          .into_iter()
          .map(|image| RenderPdfImageInput {
            name: image.name,
            data: match image.data {
              Either::A(data) => RenderPdfBinaryInput::Bytes(Vec::from(data)),
              Either::B(data) => RenderPdfBinaryInput::Base64(data),
            },
          })
          .collect()
      }),
    }
  }
}
