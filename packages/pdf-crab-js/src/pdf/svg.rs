use napi::Result;
use printpdf::{Op, PdfDocument, Svg};

use super::{
  images::transform_for_box,
  input::PdfElementInput,
  unit::Unit,
  validation::{invalid_arg, required, required_f32, required_positive_f32},
};

pub(super) fn append_svg_ops(
  ops: &mut Vec<Op>,
  document: &mut PdfDocument,
  element: PdfElementInput,
  unit: Unit,
  path: &str,
) -> Result<()> {
  let svg = element.svg.ok_or_else(|| required(format!("{path}.svg")))?;
  if svg.trim().is_empty() {
    return Err(invalid_arg(format!("{path}.svg must not be empty")));
  }

  let x = required_f32(element.x, &format!("{path}.x"))?;
  let y = required_f32(element.y, &format!("{path}.y"))?;
  let width = required_positive_f32(element.width, &format!("{path}.width"))?;
  let height = required_positive_f32(element.height, &format!("{path}.height"))?;

  let mut warnings = Vec::new();
  let xobject = Svg::parse(&svg, &mut warnings)
    .map_err(|message| invalid_arg(format!("{path}.svg could not be parsed: {message}")))?;
  let intrinsic_width = xobject
    .width
    .map(|width| width.into_pt(xobject.dpi.unwrap_or(72.0)).0)
    .unwrap_or(1.0);
  let intrinsic_height = xobject
    .height
    .map(|height| height.into_pt(xobject.dpi.unwrap_or(72.0)).0)
    .unwrap_or(1.0);
  let xobject_id = document.add_xobject(&xobject);
  let transform = transform_for_box(
    unit.coordinate(x),
    unit.coordinate(y),
    unit.coordinate(width),
    unit.coordinate(height),
    intrinsic_width,
    intrinsic_height,
  );

  ops.push(Op::SaveGraphicsState);
  ops.push(Op::UseXobject {
    id: xobject_id,
    transform,
  });
  ops.push(Op::RestoreGraphicsState);

  Ok(())
}
