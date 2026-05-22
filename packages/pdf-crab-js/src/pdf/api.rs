use napi::{
  bindgen_prelude::{AsyncTask, Buffer},
  Env, Result, Task,
};

use super::{
  document::create_pdf_bytes,
  html::create_pdf_from_html_bytes,
  input::{CreatePdfFromHtmlInput, CreatePdfInput, ParsePdfInput, RenderPdfPageToSvgInput},
  parse::parse_pdf_summary,
  render::render_svg,
  validation::invalid_arg,
};

#[napi]
pub fn create_pdf(input: CreatePdfInput) -> Result<Buffer> {
  create_pdf_bytes(input).map(Buffer::from)
}

#[napi(ts_return_type = "Promise<Buffer>")]
pub fn create_pdf_async(input: CreatePdfInput) -> AsyncTask<CreatePdfTask> {
  AsyncTask::new(CreatePdfTask { input: Some(input) })
}

#[napi]
pub fn create_pdf_from_html(input: CreatePdfFromHtmlInput) -> Result<Buffer> {
  create_pdf_from_html_bytes(input).map(Buffer::from)
}

#[napi(ts_return_type = "Promise<Buffer>")]
pub fn create_pdf_from_html_async(
  input: CreatePdfFromHtmlInput,
) -> AsyncTask<CreatePdfFromHtmlTask> {
  AsyncTask::new(CreatePdfFromHtmlTask { input: Some(input) })
}

#[napi]
pub fn parse_pdf(input: ParsePdfInput) -> Result<super::input::ParsedPdf> {
  parse_pdf_summary(input)
}

#[napi]
pub fn render_pdf_page_to_svg(input: RenderPdfPageToSvgInput) -> Result<String> {
  render_svg(input)
}

#[napi(ts_return_type = "Promise<string>")]
pub fn render_pdf_page_to_svg_async(
  input: RenderPdfPageToSvgInput,
) -> AsyncTask<RenderPdfPageToSvgTask> {
  AsyncTask::new(RenderPdfPageToSvgTask { input: Some(input) })
}

pub(super) struct CreatePdfTask {
  input: Option<CreatePdfInput>,
}

impl Task for CreatePdfTask {
  type Output = Vec<u8>;
  type JsValue = Buffer;

  fn compute(&mut self) -> Result<Self::Output> {
    let input = self
      .input
      .take()
      .ok_or_else(|| invalid_arg("createPdfAsync input was already consumed"))?;
    create_pdf_bytes(input)
  }

  fn resolve(&mut self, _env: Env, output: Self::Output) -> Result<Self::JsValue> {
    Ok(Buffer::from(output))
  }
}

pub(super) struct CreatePdfFromHtmlTask {
  input: Option<CreatePdfFromHtmlInput>,
}

impl Task for CreatePdfFromHtmlTask {
  type Output = Vec<u8>;
  type JsValue = Buffer;

  fn compute(&mut self) -> Result<Self::Output> {
    let input = self
      .input
      .take()
      .ok_or_else(|| invalid_arg("createPdfFromHtmlAsync input was already consumed"))?;
    create_pdf_from_html_bytes(input)
  }

  fn resolve(&mut self, _env: Env, output: Self::Output) -> Result<Self::JsValue> {
    Ok(Buffer::from(output))
  }
}

pub(super) struct RenderPdfPageToSvgTask {
  input: Option<RenderPdfPageToSvgInput>,
}

impl Task for RenderPdfPageToSvgTask {
  type Output = String;
  type JsValue = String;

  fn compute(&mut self) -> Result<Self::Output> {
    let input = self
      .input
      .take()
      .ok_or_else(|| invalid_arg("renderPdfPageToSvgAsync input was already consumed"))?;
    render_svg(input)
  }

  fn resolve(&mut self, _env: Env, output: Self::Output) -> Result<Self::JsValue> {
    Ok(output)
  }
}
