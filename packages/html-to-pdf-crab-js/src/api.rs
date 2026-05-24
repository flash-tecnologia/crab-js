use napi::{
  bindgen_prelude::{AsyncTask, Buffer},
  Env, Result, Task,
};

use crate::{
  input::CreatePdfFromHtmlInput, renderer::create_pdf_from_html_bytes, validation::invalid_arg,
};

#[napi(ts_return_type = "Promise<Buffer>")]
pub fn create_pdf_from_html(input: CreatePdfFromHtmlInput) -> AsyncTask<CreatePdfFromHtmlTask> {
  AsyncTask::new(CreatePdfFromHtmlTask { input: Some(input) })
}

pub(crate) struct CreatePdfFromHtmlTask {
  input: Option<CreatePdfFromHtmlInput>,
}

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
