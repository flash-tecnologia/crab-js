use napi::Result;
use printpdf::{OutputImageFormat, PdfToSvgOptions};

use super::{input::RenderPdfPageToSvgInput, parse::parse_pdf_document, validation::invalid_arg};

pub(super) fn render_options(image_formats: Option<Vec<String>>) -> Result<PdfToSvgOptions> {
  Ok(PdfToSvgOptions {
    image_formats: image_formats
      .map(|formats| {
        formats
          .into_iter()
          .enumerate()
          .map(|(index, format)| {
            parse_output_image_format(&format, &format!("imageFormats[{index}]"))
          })
          .collect::<Result<Vec<_>>>()
      })
      .transpose()?
      .unwrap_or_else(|| PdfToSvgOptions::default().image_formats),
  })
}

pub(super) fn render_svg(input: RenderPdfPageToSvgInput) -> Result<String> {
  let page_number = input.page_index as usize + 1;
  let options = render_options(input.image_formats)?;
  let (document, mut warnings) = parse_pdf_document(super::input::ParsePdfInput {
    pdf: input.pdf,
    fail_on_error: Some(false),
  })?;

  document
    .page_to_svg(page_number, &options, &mut warnings)
    .ok_or_else(|| invalid_arg(format!("pageIndex {} is out of range", input.page_index)))
}

fn parse_output_image_format(value: &str, path: &str) -> Result<OutputImageFormat> {
  match value {
    "png" => Ok(OutputImageFormat::Png),
    "jpeg" => Ok(OutputImageFormat::Jpeg),
    "webp" => Ok(OutputImageFormat::Webp),
    "avif" => Ok(OutputImageFormat::Avif),
    "gif" => Ok(OutputImageFormat::Gif),
    "bmp" => Ok(OutputImageFormat::Bmp),
    "tiff" => Ok(OutputImageFormat::Tiff),
    "tga" => Ok(OutputImageFormat::Tga),
    "pnm" => Ok(OutputImageFormat::Pnm),
    value => Err(invalid_arg(format!(
      "{path} must be one of \"png\", \"jpeg\", \"webp\", \"avif\", \"gif\", \"bmp\", \"tiff\", \"tga\", or \"pnm\", received \"{value}\""
    ))),
  }
}
