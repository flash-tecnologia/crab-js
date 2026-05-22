use napi::{bindgen_prelude::Either, Result};
use printpdf::{Base64OrRaw, Op, PdfDocument, Pt, RawImage, XObjectTransform};

use super::{
  input::{BytesOrString, PdfElementInput},
  unit::Unit,
  validation::{invalid_arg, required, required_f32, required_positive_f32},
};

const IMAGE_DPI: f32 = 72.0;

pub(super) fn source_to_base64_or_raw(source: BytesOrString) -> Base64OrRaw {
  match source {
    Either::A(buffer) => Base64OrRaw::Raw(buffer.to_vec()),
    Either::B(value) => Base64OrRaw::B64(value),
  }
}

pub(super) fn source_to_bytes(source: BytesOrString, path: &str) -> Result<Vec<u8>> {
  source_to_base64_or_raw(source)
    .decode_bytes()
    .map_err(|message| invalid_arg(format!("{path} could not be decoded: {message}")))
}

pub(super) fn append_image_ops(
  ops: &mut Vec<Op>,
  document: &mut PdfDocument,
  element: PdfElementInput,
  unit: Unit,
  path: &str,
) -> Result<()> {
  validate_image_format(element.format.as_deref(), &format!("{path}.format"))?;

  let source = element
    .source
    .ok_or_else(|| required(format!("{path}.source")))?;
  let x = required_f32(element.x, &format!("{path}.x"))?;
  let y = required_f32(element.y, &format!("{path}.y"))?;
  let width = required_positive_f32(element.width, &format!("{path}.width"))?;
  let height = required_positive_f32(element.height, &format!("{path}.height"))?;
  let bytes = source_to_bytes(source, &format!("{path}.source"))?;

  let mut warnings = Vec::new();
  let image = RawImage::decode_from_bytes(&bytes, &mut warnings).map_err(|message| {
    invalid_arg(format!(
      "{path}.source could not be decoded as an image: {message}"
    ))
  })?;
  let image_id = document.add_image(&image);
  let transform = transform_for_box(
    unit.coordinate(x),
    unit.coordinate(y),
    unit.coordinate(width),
    unit.coordinate(height),
    image.width as f32,
    image.height as f32,
  );

  ops.push(Op::SaveGraphicsState);
  ops.push(Op::UseXobject {
    id: image_id,
    transform,
  });
  ops.push(Op::RestoreGraphicsState);

  Ok(())
}

pub(super) fn transform_for_box(
  x: Pt,
  y: Pt,
  width: Pt,
  height: Pt,
  intrinsic_width: f32,
  intrinsic_height: f32,
) -> XObjectTransform {
  XObjectTransform {
    translate_x: Some(x),
    translate_y: Some(y),
    rotate: None,
    scale_x: Some(width.0 / intrinsic_width.max(1.0)),
    scale_y: Some(height.0 / intrinsic_height.max(1.0)),
    dpi: Some(IMAGE_DPI),
  }
}

fn validate_image_format(format: Option<&str>, path: &str) -> Result<()> {
  let Some(format) = format else {
    return Ok(());
  };

  match format {
    "png" | "jpeg" | "gif" | "bmp" | "tiff" | "webp" | "ico" | "pnm" | "tga" | "dds"
    | "hdr" => Ok(()),
    format => Err(invalid_arg(format!(
      "{path} must be one of \"png\", \"jpeg\", \"gif\", \"bmp\", \"tiff\", \"webp\", \"ico\", \"pnm\", \"tga\", \"dds\", or \"hdr\", received \"{format}\""
    ))),
  }
}
