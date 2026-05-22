use napi::Result;
use printpdf::{Actions, Destination, LinkAnnotation, Op, PdfDocument, PdfPage, Rect};

use super::{
  color::optional_color_array,
  elements::{append_element_ops, BuildContext},
  input::{CreatePdfInput, PdfAnnotationInput},
  metadata::{add_bookmarks, add_layers, apply_metadata},
  save_options::parse_save_options,
  unit::Unit,
  validation::{
    invalid_arg, optional_f32, positive_f32, required, required_f32, required_positive_f32, to_f32,
  },
};

const DEFAULT_TITLE: &str = "pdf-crab-js";

pub(super) fn default_title() -> &'static str {
  DEFAULT_TITLE
}

pub(super) fn create_pdf_bytes(input: CreatePdfInput) -> Result<Vec<u8>> {
  let unit = Unit::from_input(input.unit)?;
  let pages = input.pages.ok_or_else(|| required("pages"))?;

  if pages.is_empty() {
    return Err(invalid_arg("pages must contain at least one page"));
  }

  let title = input.title.unwrap_or_else(|| DEFAULT_TITLE.to_string());
  let page_count = pages.len();
  let mut document = PdfDocument::new(&title);
  apply_metadata(
    &mut document,
    Some(title),
    input.metadata,
    input.conformance,
  )?;
  let layers = add_layers(&mut document, input.layers)?;
  let mut pdf_pages = Vec::with_capacity(page_count);

  for (page_index, page) in pages.into_iter().enumerate() {
    let page_path = format!("pages[{page_index}]");
    let width = positive_f32(page.width, &format!("{page_path}.width"))?;
    let height = positive_f32(page.height, &format!("{page_path}.height"))?;
    let mut ops = Vec::new();

    if let Some(elements) = page.elements {
      for (element_index, element) in elements.into_iter().enumerate() {
        let mut context = BuildContext {
          document: &mut document,
          layers: &layers,
          unit,
        };
        append_element_ops(
          &mut ops,
          element,
          &mut context,
          &format!("{page_path}.elements[{element_index}]"),
        )?;
      }
    }

    if let Some(annotations) = page.annotations {
      for (annotation_index, annotation) in annotations.into_iter().enumerate() {
        append_annotation_ops(
          &mut ops,
          annotation,
          unit,
          page_count,
          &format!("{page_path}.annotations[{annotation_index}]"),
        )?;
      }
    }

    pdf_pages.push(PdfPage::new(
      unit.page_size(width),
      unit.page_size(height),
      ops,
    ));
  }

  document.with_pages(pdf_pages);
  add_bookmarks(&mut document, input.bookmarks, page_count)?;

  let save_options = parse_save_options(input.save_options)?;
  let mut warnings = Vec::new();

  Ok(document.save(&save_options, &mut warnings))
}

fn append_annotation_ops(
  ops: &mut Vec<Op>,
  annotation: PdfAnnotationInput,
  unit: Unit,
  page_count: usize,
  path: &str,
) -> Result<()> {
  match annotation.r#type.as_str() {
    "link" => append_link_annotation_ops(ops, annotation, unit, page_count, path),
    annotation_type => Err(invalid_arg(format!(
      "{path}.type must be \"link\", received \"{annotation_type}\""
    ))),
  }
}

fn append_link_annotation_ops(
  ops: &mut Vec<Op>,
  annotation: PdfAnnotationInput,
  unit: Unit,
  page_count: usize,
  path: &str,
) -> Result<()> {
  let x = required_f32(annotation.x, &format!("{path}.x"))?;
  let y = required_f32(annotation.y, &format!("{path}.y"))?;
  let width = required_positive_f32(annotation.width, &format!("{path}.width"))?;
  let height = required_positive_f32(annotation.height, &format!("{path}.height"))?;
  let actions = match (annotation.url, annotation.page_index) {
    (Some(url), None) => {
      if url.trim().is_empty() {
        return Err(invalid_arg(format!("{path}.url must not be empty")));
      }
      Actions::uri(url)
    }
    (None, Some(page_index)) => {
      let page_index = page_index as usize;
      if page_index >= page_count {
        return Err(invalid_arg(format!(
          "{path}.pageIndex must reference an existing page"
        )));
      }
      Actions::go_to(Destination::Xyz {
        page: page_index + 1,
        left: optional_f32(annotation.left, &format!("{path}.left"))?,
        top: optional_f32(annotation.top, &format!("{path}.top"))?,
        zoom: annotation
          .zoom
          .map(|zoom| to_f32(zoom, &format!("{path}.zoom")))
          .transpose()?,
      })
    }
    (Some(_), Some(_)) => {
      return Err(invalid_arg(format!(
        "{path} must define either url or pageIndex, not both"
      )));
    }
    (None, None) => {
      return Err(required(format!("{path}.url")));
    }
  };
  let color = optional_color_array(annotation.color, &format!("{path}.color"))?;

  ops.push(Op::LinkAnnotation {
    link: LinkAnnotation::new(
      Rect {
        x: unit.coordinate(x),
        y: unit.coordinate(y),
        width: unit.coordinate(width),
        height: unit.coordinate(height),
        mode: None,
        winding_order: None,
      },
      actions,
      None,
      color,
      None,
    ),
  });

  Ok(())
}
