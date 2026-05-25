use napi::{bindgen_prelude::Buffer, Result};

#[cfg(not(target_arch = "wasm32"))]
use napi::{bindgen_prelude::AsyncTask, Env, Task};

use crate::{
  input::{CreatePdfFromHtmlInput, RenderPdfFromHtmlInput},
  renderer::create_pdf_from_html_bytes,
};

#[cfg(not(target_arch = "wasm32"))]
use crate::validation::invalid_arg;

#[napi(ts_return_type = "Promise<Buffer>")]
#[cfg(not(target_arch = "wasm32"))]
pub fn create_pdf_from_html(input: CreatePdfFromHtmlInput) -> AsyncTask<CreatePdfFromHtmlTask> {
  AsyncTask::new(CreatePdfFromHtmlTask {
    input: Some(RenderPdfFromHtmlInput::from(input)),
  })
}

#[napi(ts_return_type = "Promise<Buffer>")]
#[cfg(target_arch = "wasm32")]
pub fn create_pdf_from_html(input: CreatePdfFromHtmlInput) -> Result<Buffer> {
  create_pdf_from_html_bytes(RenderPdfFromHtmlInput::from(input)).map(Buffer::from)
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct CreatePdfFromHtmlTask {
  input: Option<RenderPdfFromHtmlInput>,
}

#[cfg(not(target_arch = "wasm32"))]
impl Task for CreatePdfFromHtmlTask {
  type Output = Vec<u8>;
  type JsValue = Buffer;

  fn compute(&mut self) -> Result<Self::Output> {
    let input = self
      .input
      .take()
      .ok_or_else(|| invalid_arg("createPdfFromHtml input was already consumed"))?;
    create_pdf_from_html_bytes(input)
  }

  fn resolve(&mut self, _env: Env, output: Self::Output) -> Result<Self::JsValue> {
    Ok(Buffer::from(output))
  }
}
