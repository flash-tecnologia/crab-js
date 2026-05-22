use napi::Result;
use printpdf::{Base64OrRaw, GeneratePdfOptions, PdfDocument, PdfSaveOptions, PdfWarnMsg};
use std::collections::BTreeMap;

use super::{
  document::default_title,
  images::source_to_base64_or_raw,
  input::CreatePdfFromHtmlInput,
  save_options::parse_save_options,
  validation::{invalid_arg, optional_non_negative_f32, optional_positive_f32, required},
};

pub(super) struct HtmlDocumentOutput {
  pub(super) document: PdfDocument,
  pub(super) save_options: PdfSaveOptions,
}

pub(super) fn create_pdf_from_html_bytes(input: CreatePdfFromHtmlInput) -> Result<Vec<u8>> {
  let HtmlDocumentOutput {
    document,
    save_options,
    ..
  } = create_document_from_html(input)?;
  let mut warnings = Vec::new();

  Ok(document.save(&save_options, &mut warnings))
}

pub(super) fn create_document_from_html(
  input: CreatePdfFromHtmlInput,
) -> Result<HtmlDocumentOutput> {
  let html = input.html.ok_or_else(|| required("html"))?;
  if html.trim().is_empty() {
    return Err(invalid_arg("html must not be empty"));
  }

  let title = input.title.unwrap_or_else(|| default_title().to_string());
  let options = GeneratePdfOptions {
    page_width: optional_positive_f32(input.page_width, "pageWidth")?,
    page_height: optional_positive_f32(input.page_height, "pageHeight")?,
    margin_top: optional_non_negative_f32(input.margin_top, "marginTop")?,
    margin_right: optional_non_negative_f32(input.margin_right, "marginRight")?,
    margin_bottom: optional_non_negative_f32(input.margin_bottom, "marginBottom")?,
    margin_left: optional_non_negative_f32(input.margin_left, "marginLeft")?,
    show_page_numbers: input.show_page_numbers,
    header_text: input.header_text,
    footer_text: input.footer_text,
    skip_first_page: input.skip_first_page,
    ..GeneratePdfOptions::default()
  };
  let images = to_asset_map(input.images);
  let fonts = to_asset_map(input.fonts);
  let save_options = parse_save_options(input.save_options)?;

  let mut warnings = Vec::new();
  let mut document = PdfDocument::from_html(&html, &images, &fonts, &options, &mut warnings)
    .map_err(|message| invalid_arg(format!("html could not be rendered: {message}")))?;

  if document.pages.is_empty() {
    return Err(invalid_arg(format!(
      "html did not render any pages{}",
      format_warnings(&warnings)
    )));
  }

  document.metadata.info.document_title = title;

  Ok(HtmlDocumentOutput {
    document,
    save_options,
  })
}

fn to_asset_map(
  input: Option<BTreeMap<String, super::input::BytesOrString>>,
) -> BTreeMap<String, Base64OrRaw> {
  input
    .unwrap_or_default()
    .into_iter()
    .map(|(name, source)| (name, source_to_base64_or_raw(source)))
    .collect()
}

fn format_warnings(warnings: &[PdfWarnMsg]) -> String {
  if warnings.is_empty() {
    return String::new();
  }

  format!(": {warnings:?}")
}
