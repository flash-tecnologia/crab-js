use napi::Result;
use pdf_writer::types::{ActionType, AnnotationType, BorderType, TrappingStatus};
use pdf_writer::{Content, Finish, Pdf, Rect, Ref, Str, TextStr};

use super::{
  color::optional_color,
  elements::append_element,
  font::BuiltinFont,
  input::{CreatePdfInput, PdfAnnotationInput, PdfElementInput, PdfMetadataInput, PdfPageInput},
  unit::Unit,
  validation::{invalid_arg, positive_f32, required, required_f32, required_positive_f32},
};

const DEFAULT_TITLE: &str = "pdf-crab-js";
const CATALOG_REF: i32 = 1;
const PAGES_REF: i32 = 2;
const INFO_REF: i32 = 3;
const FIRST_PAGE_REF: i32 = 18;

pub(super) fn create_pdf_bytes(input: CreatePdfInput) -> Result<Vec<u8>> {
  let pages = input.pages.ok_or_else(|| required("pages"))?;

  if pages.is_empty() {
    return Err(invalid_arg("pages must contain at least one page"));
  }

  let mut document = PdfDocumentState::new(input.title, input.unit, input.metadata)?;

  for (page_index, page) in pages.into_iter().enumerate() {
    document.add_page(page, &format!("pages[{page_index}]"))?;
  }

  document.finish()
}

pub(super) struct PdfDocumentState {
  title: String,
  metadata: Option<PdfMetadataInput>,
  unit: Unit,
  prepared_pages: Vec<PreparedPage>,
  next_ref: i32,
  open_page: Option<OpenPage>,
}

impl PdfDocumentState {
  pub(super) fn new(
    title: Option<String>,
    unit: Option<String>,
    metadata: Option<PdfMetadataInput>,
  ) -> Result<Self> {
    Ok(Self {
      title: title.unwrap_or_else(|| DEFAULT_TITLE.to_string()),
      metadata,
      unit: Unit::from_input(unit)?,
      prepared_pages: Vec::new(),
      next_ref: FIRST_PAGE_REF,
      open_page: None,
    })
  }

  pub(super) fn start_page(&mut self, width: f64, height: f64, path: &str) -> Result<()> {
    if self.open_page.is_some() {
      return Err(invalid_arg(
        "cannot start a new page while another page is open; call endPage() first",
      ));
    }

    let width = self
      .unit
      .coordinate(positive_f32(width, &format!("{path}.width"))?);
    let height = self
      .unit
      .coordinate(positive_f32(height, &format!("{path}.height"))?);
    let reference = take_ref(&mut self.next_ref);
    let content_ref = take_ref(&mut self.next_ref);

    self.open_page = Some(OpenPage {
      path: path.to_string(),
      reference,
      content_ref,
      width,
      height,
      content: Content::new(),
      annotations: Vec::new(),
      element_count: 0,
      annotation_count: 0,
    });

    Ok(())
  }

  pub(super) fn append_elements(&mut self, elements: Vec<PdfElementInput>) -> Result<()> {
    let page = self
      .open_page
      .as_mut()
      .ok_or_else(|| invalid_arg("appendElements requires an open page; call startPage() first"))?;

    for element in elements {
      append_element(
        &mut page.content,
        element,
        self.unit,
        &format!("{}.elements[{}]", page.path, page.element_count),
      )?;
      page.element_count += 1;
    }

    Ok(())
  }

  pub(super) fn append_annotations(&mut self, annotations: Vec<PdfAnnotationInput>) -> Result<()> {
    let (page_ref, page_path, start_index) = {
      let page = self.open_page.as_ref().ok_or_else(|| {
        invalid_arg("appendAnnotations requires an open page; call startPage() first")
      })?;
      (page.reference, page.path.clone(), page.annotation_count)
    };
    let mut prepared_annotations = Vec::with_capacity(annotations.len());

    for (index, annotation) in annotations.into_iter().enumerate() {
      let annotation_ref = take_ref(&mut self.next_ref);
      let annotation = prepare_annotation(
        annotation,
        self.unit,
        page_ref,
        &format!("{page_path}.annotations[{}]", start_index + index),
      )?;

      prepared_annotations.push(PreparedAnnotation {
        reference: annotation_ref,
        annotation,
      });
    }

    let page = self.open_page.as_mut().ok_or_else(|| {
      invalid_arg("appendAnnotations requires an open page; call startPage() first")
    })?;
    page.annotation_count += prepared_annotations.len();
    page.annotations.extend(prepared_annotations);

    Ok(())
  }

  pub(super) fn end_page(&mut self) -> Result<()> {
    let page = self
      .open_page
      .take()
      .ok_or_else(|| invalid_arg("endPage requires an open page; call startPage() first"))?;

    self.prepared_pages.push(PreparedPage {
      reference: page.reference,
      content_ref: page.content_ref,
      width: page.width,
      height: page.height,
      content: page.content.finish().into_vec(),
      annotations: page.annotations,
    });

    Ok(())
  }

  pub(super) fn add_page(&mut self, page: PdfPageInput, path: &str) -> Result<()> {
    self.start_page(page.width, page.height, path)?;

    if let Some(elements) = page.elements {
      self.append_elements(elements)?;
    }
    if let Some(annotations) = page.annotations {
      self.append_annotations(annotations)?;
    }

    self.end_page()
  }

  pub(super) fn finish(self) -> Result<Vec<u8>> {
    if self.open_page.is_some() {
      return Err(invalid_arg(
        "cannot finish while a page is open; call endPage() first",
      ));
    }

    if self.prepared_pages.is_empty() {
      return Err(invalid_arg("pages must contain at least one page"));
    }

    write_pdf_document(self.title, self.metadata, self.prepared_pages)
  }
}

struct OpenPage {
  path: String,
  reference: Ref,
  content_ref: Ref,
  width: f32,
  height: f32,
  content: Content,
  annotations: Vec<PreparedAnnotation>,
  element_count: usize,
  annotation_count: usize,
}

fn write_pdf_document(
  title: String,
  metadata: Option<PdfMetadataInput>,
  prepared_pages: Vec<PreparedPage>,
) -> Result<Vec<u8>> {
  let mut pdf = Pdf::new();
  pdf
    .catalog(Ref::new(CATALOG_REF))
    .pages(Ref::new(PAGES_REF));
  pdf
    .pages(Ref::new(PAGES_REF))
    .kids(prepared_pages.iter().map(|page| page.reference))
    .count(prepared_pages.len() as i32);

  write_document_info(&mut pdf, Ref::new(INFO_REF), &title, metadata);
  write_fonts(&mut pdf);

  for page in &prepared_pages {
    let mut page_writer = pdf.page(page.reference);
    page_writer.parent(Ref::new(PAGES_REF));
    page_writer.media_box(Rect::new(0.0, 0.0, page.width, page.height));
    page_writer.contents(page.content_ref);
    if !page.annotations.is_empty() {
      page_writer.annotations(
        page
          .annotations
          .iter()
          .map(|annotation| annotation.reference),
      );
    }

    let mut resources = page_writer.resources();
    let mut fonts = resources.fonts();
    for font in BuiltinFont::ALL {
      fonts.pair(font.resource_name(), Ref::new(font.ref_number()));
    }
    fonts.finish();
    resources.finish();
    page_writer.finish();

    pdf.stream(page.content_ref, &page.content);

    for annotation in &page.annotations {
      write_annotation(&mut pdf, annotation.reference, &annotation.annotation);
    }
  }

  Ok(pdf.finish())
}

fn take_ref(next_ref: &mut i32) -> Ref {
  let reference = Ref::new(*next_ref);
  *next_ref += 1;
  reference
}

fn write_document_info(
  pdf: &mut Pdf,
  info_ref: Ref,
  title: &str,
  metadata: Option<PdfMetadataInput>,
) {
  let metadata = metadata.unwrap_or(PdfMetadataInput {
    title: None,
    author: None,
    creator: None,
    producer: None,
    subject: None,
    keywords: None,
    trapped: None,
  });
  let mut info = pdf.document_info(info_ref);

  info.title(TextStr(metadata.title.as_deref().unwrap_or(title)));
  if let Some(author) = metadata.author.as_deref() {
    info.author(TextStr(author));
  }
  if let Some(creator) = metadata.creator.as_deref() {
    info.creator(TextStr(creator));
  }
  info.producer(TextStr(
    metadata.producer.as_deref().unwrap_or("pdf-crab-js"),
  ));
  if let Some(subject) = metadata.subject.as_deref() {
    info.subject(TextStr(subject));
  }
  if let Some(keywords) = metadata.keywords {
    info.keywords(TextStr(&keywords.join(", ")));
  }
  if let Some(trapped) = metadata.trapped {
    info.trapped(if trapped {
      TrappingStatus::Trapped
    } else {
      TrappingStatus::NotTrapped
    });
  }
}

fn write_fonts(pdf: &mut Pdf) {
  for font in BuiltinFont::ALL {
    pdf
      .type1_font(Ref::new(font.ref_number()))
      .base_font(font.base_name());
  }
}

struct PreparedPage {
  reference: Ref,
  content_ref: Ref,
  width: f32,
  height: f32,
  content: Vec<u8>,
  annotations: Vec<PreparedAnnotation>,
}

struct PreparedAnnotation {
  reference: Ref,
  annotation: LinkAnnotation,
}

struct LinkAnnotation {
  page_ref: Ref,
  rect: Rect,
  url: String,
  color: Option<(f32, f32, f32)>,
}

fn prepare_annotation(
  annotation: PdfAnnotationInput,
  unit: Unit,
  page_ref: Ref,
  path: &str,
) -> Result<LinkAnnotation> {
  match annotation.r#type.as_str() {
    "link" => prepare_link_annotation(annotation, unit, page_ref, path),
    annotation_type => Err(invalid_arg(format!(
      "{path}.type must be \"link\", received \"{annotation_type}\""
    ))),
  }
}

fn prepare_link_annotation(
  annotation: PdfAnnotationInput,
  unit: Unit,
  page_ref: Ref,
  path: &str,
) -> Result<LinkAnnotation> {
  let x = unit.coordinate(required_f32(annotation.x, &format!("{path}.x"))?);
  let y = unit.coordinate(required_f32(annotation.y, &format!("{path}.y"))?);
  let width = unit.coordinate(required_positive_f32(
    annotation.width,
    &format!("{path}.width"),
  )?);
  let height = unit.coordinate(required_positive_f32(
    annotation.height,
    &format!("{path}.height"),
  )?);
  let url = annotation
    .url
    .ok_or_else(|| required(format!("{path}.url")))?;
  if url.trim().is_empty() {
    return Err(invalid_arg(format!("{path}.url must not be empty")));
  }
  let color = optional_color(annotation.color, &format!("{path}.color"))?
    .map(|color| (color.red, color.green, color.blue));

  Ok(LinkAnnotation {
    page_ref,
    rect: Rect::new(x, y, x + width, y + height),
    url,
    color,
  })
}

fn write_annotation(pdf: &mut Pdf, annotation_ref: Ref, annotation: &LinkAnnotation) {
  let mut writer = pdf.annotation(annotation_ref);
  writer.subtype(AnnotationType::Link);
  writer.rect(annotation.rect);
  writer.page(annotation.page_ref);
  writer.contents(TextStr("Link"));
  if let Some((red, green, blue)) = annotation.color {
    writer.color_rgb(red, green, blue);
  }
  writer
    .action()
    .action_type(ActionType::Uri)
    .uri(Str(annotation.url.as_bytes()));
  writer.border_style().width(0.0).style(BorderType::Solid);
}
