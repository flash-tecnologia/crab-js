use napi::{
  bindgen_prelude::{AsyncTask, Buffer},
  Env, Result, Task,
};

use crate::{
  input::CreatePdfFromHtmlWithFulgurInput, renderer::create_pdf_from_html_with_fulgur_bytes,
  validation::invalid_arg,
};

#[napi(ts_return_type = "Promise<Buffer>")]
pub fn create_pdf_from_html_with_fulgur(
  input: CreatePdfFromHtmlWithFulgurInput,
) -> AsyncTask<CreatePdfFromHtmlWithFulgurTask> {
  AsyncTask::new(CreatePdfFromHtmlWithFulgurTask { input: Some(input) })
}

pub(crate) struct CreatePdfFromHtmlWithFulgurTask {
  input: Option<CreatePdfFromHtmlWithFulgurInput>,
}

impl Task for CreatePdfFromHtmlWithFulgurTask {
  type Output = Vec<u8>;
  type JsValue = Buffer;

  fn compute(&mut self) -> Result<Self::Output> {
    let input = self
      .input
      .take()
      .ok_or_else(|| invalid_arg("createPdfFromHtmlWithFulgur input was already consumed"))?;
    create_pdf_from_html_with_fulgur_bytes(input)
  }

  fn resolve(&mut self, _env: Env, output: Self::Output) -> Result<Self::JsValue> {
    Ok(Buffer::from(output))
  }
}
