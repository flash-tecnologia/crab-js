use napi::{
  bindgen_prelude::{AsyncTask, Buffer},
  Env, Result, Task,
};

use super::{
  document::{create_pdf_bytes, PdfDocumentState},
  input::{
    CreatePdfInput, PdfAnnotationInput, PdfDocumentBuilderInput, PdfElementInput, PdfPageInput,
    PdfPageSetupInput,
  },
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
pub struct PdfDocumentBuilder {
  state: Option<PdfDocumentState>,
}

#[napi]
impl PdfDocumentBuilder {
  #[napi(constructor)]
  pub fn new(input: Option<PdfDocumentBuilderInput>) -> Result<Self> {
    let (title, unit, metadata) = match input {
      Some(input) => (input.title, input.unit, input.metadata),
      None => (None, None, None),
    };

    Ok(Self {
      state: Some(PdfDocumentState::new(title, unit, metadata)?),
    })
  }

  #[napi]
  pub fn start_page(&mut self, page: PdfPageSetupInput) -> Result<()> {
    self
      .state_mut()?
      .start_page(page.width, page.height, "currentPage")
  }

  #[napi]
  pub fn append_elements(&mut self, elements: Vec<PdfElementInput>) -> Result<()> {
    self.state_mut()?.append_elements(elements)
  }

  #[napi]
  pub fn append_annotations(&mut self, annotations: Vec<PdfAnnotationInput>) -> Result<()> {
    self.state_mut()?.append_annotations(annotations)
  }

  #[napi]
  pub fn end_page(&mut self) -> Result<()> {
    self.state_mut()?.end_page()
  }

  #[napi]
  pub fn add_page(&mut self, page: PdfPageInput) -> Result<()> {
    self.state_mut()?.add_page(page, "page")
  }

  #[napi]
  pub fn add_pages(&mut self, pages: Vec<PdfPageInput>) -> Result<()> {
    let state = self.state_mut()?;

    for (index, page) in pages.into_iter().enumerate() {
      state.add_page(page, &format!("pages[{index}]"))?;
    }

    Ok(())
  }

  #[napi]
  pub fn finish(&mut self) -> Result<Buffer> {
    self.take_state()?.finish().map(Buffer::from)
  }

  #[napi(ts_return_type = "Promise<Buffer>")]
  pub fn finish_async(&mut self) -> Result<AsyncTask<FinishPdfDocumentTask>> {
    Ok(AsyncTask::new(FinishPdfDocumentTask {
      state: Some(self.take_state()?),
    }))
  }

  fn state_mut(&mut self) -> Result<&mut PdfDocumentState> {
    self
      .state
      .as_mut()
      .ok_or_else(|| invalid_arg("PdfDocumentBuilder has already finished"))
  }

  fn take_state(&mut self) -> Result<PdfDocumentState> {
    self
      .state
      .take()
      .ok_or_else(|| invalid_arg("PdfDocumentBuilder has already finished"))
  }
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

pub struct FinishPdfDocumentTask {
  state: Option<PdfDocumentState>,
}

impl Task for FinishPdfDocumentTask {
  type Output = Vec<u8>;
  type JsValue = Buffer;

  fn compute(&mut self) -> Result<Self::Output> {
    let state = self
      .state
      .take()
      .ok_or_else(|| invalid_arg("finishAsync input was already consumed"))?;
    state.finish()
  }

  fn resolve(&mut self, _env: Env, output: Self::Output) -> Result<Self::JsValue> {
    Ok(Buffer::from(output))
  }
}
