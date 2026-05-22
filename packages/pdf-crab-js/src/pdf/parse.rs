use napi::Result;
use printpdf::{PdfDocument, PdfParseOptions};

use super::{
  images::source_to_bytes,
  input::{ParsePdfInput, ParsedPdf, ParsedPdfBookmark, ParsedPdfMetadata, ParsedPdfPageSize},
  validation::invalid_arg,
};

pub(super) fn parse_pdf_document(
  input: ParsePdfInput,
) -> Result<(PdfDocument, Vec<printpdf::PdfWarnMsg>)> {
  let bytes = source_to_bytes(input.pdf, "pdf")?;
  let mut warnings = Vec::new();
  let document = PdfDocument::parse(
    &bytes,
    &PdfParseOptions {
      fail_on_error: input.fail_on_error.unwrap_or(false),
    },
    &mut warnings,
  )
  .map_err(|message| invalid_arg(format!("pdf could not be parsed: {message}")))?;

  Ok((document, warnings))
}

pub(super) fn parse_pdf_summary(input: ParsePdfInput) -> Result<ParsedPdf> {
  let (document, warnings) = parse_pdf_document(input)?;
  let info = &document.metadata.info;
  let page_sizes = document
    .pages
    .iter()
    .map(|page| ParsedPdfPageSize {
      width: f64::from(page.media_box.width.0),
      height: f64::from(page.media_box.height.0),
    })
    .collect::<Vec<_>>();
  let bookmarks = document
    .bookmarks
    .map
    .values()
    .map(|bookmark| ParsedPdfBookmark {
      name: bookmark.name.clone(),
      page_index: bookmark.page.saturating_sub(1) as u32,
    })
    .collect::<Vec<_>>();

  Ok(ParsedPdf {
    metadata: ParsedPdfMetadata {
      title: info.document_title.clone(),
      author: info.author.clone(),
      creator: info.creator.clone(),
      producer: info.producer.clone(),
      subject: info.subject.clone(),
      keywords: info.keywords.clone(),
      identifier: info.identifier.clone(),
      conformance: info.conformance.get_identifier_string(),
    },
    page_count: document.pages.len() as u32,
    page_sizes,
    bookmarks,
    warnings: warnings
      .iter()
      .map(|warning| format!("{warning:?}"))
      .collect(),
  })
}
