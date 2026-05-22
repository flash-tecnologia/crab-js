use napi::Result;
use printpdf::{
  LayerInternalId, Line, LinePoint, Op, PaintMode, PdfDocument, PdfFontHandle, Point, Polygon,
  PolygonRing, Pt, Rect, TextItem, WindingOrder,
};
use std::collections::BTreeMap;

use super::{
  color::{black, optional_color},
  font::parse_builtin_font,
  images::append_image_ops,
  input::{PdfElementInput, PdfPointInput},
  svg::append_svg_ops,
  unit::Unit,
  validation::{invalid_arg, optional_positive_f32, required, required_f32, required_positive_f32},
};

const DEFAULT_FONT_SIZE: f32 = 12.0;
const DEFAULT_STROKE_WIDTH: f32 = 1.0;

pub(super) struct BuildContext<'a> {
  pub(super) document: &'a mut PdfDocument,
  pub(super) layers: &'a BTreeMap<String, LayerInternalId>,
  pub(super) unit: Unit,
}

pub(super) fn append_element_ops(
  ops: &mut Vec<Op>,
  element: PdfElementInput,
  context: &mut BuildContext<'_>,
  path: &str,
) -> Result<()> {
  let layer = element.layer.clone();
  let mut element_ops = Vec::new();

  match element.r#type.as_str() {
    "text" => append_text_ops(&mut element_ops, element, context.unit, path)?,
    "line" => append_line_ops(&mut element_ops, element, context.unit, path)?,
    "rect" => append_rect_ops(&mut element_ops, element, context.unit, path)?,
    "image" => append_image_ops(
      &mut element_ops,
      context.document,
      element,
      context.unit,
      path,
    )?,
    "svg" => append_svg_ops(
      &mut element_ops,
      context.document,
      element,
      context.unit,
      path,
    )?,
    "textBox" => append_text_box_ops(&mut element_ops, element, context.unit, path)?,
    "polygon" => append_polygon_ops(&mut element_ops, element, context.unit, path)?,
    "path" => append_path_ops(&mut element_ops, element, context.unit, path)?,
    element_type => Err(invalid_arg(format!(
      "{path}.type must be one of \"text\", \"line\", \"rect\", \"image\", \"svg\", \"textBox\", \"polygon\", or \"path\", received \"{element_type}\""
    )))?,
  }

  if let Some(layer) = layer {
    let layer_id = context.layers.get(&layer).ok_or_else(|| {
      invalid_arg(format!(
        "{path}.layer must reference a layer declared in input.layers"
      ))
    })?;
    ops.push(Op::BeginLayer {
      layer_id: layer_id.clone(),
    });
    ops.extend(element_ops);
    ops.push(Op::EndLayer);
  } else {
    ops.extend(element_ops);
  }

  Ok(())
}

fn append_text_ops(
  ops: &mut Vec<Op>,
  element: PdfElementInput,
  unit: Unit,
  path: &str,
) -> Result<()> {
  let text = element
    .text
    .ok_or_else(|| required(format!("{path}.text")))?;
  let x = required_f32(element.x, &format!("{path}.x"))?;
  let y = required_f32(element.y, &format!("{path}.y"))?;
  let font = parse_builtin_font(element.font, &format!("{path}.font"))?;
  let font_size = optional_positive_f32(element.font_size, &format!("{path}.fontSize"))?
    .unwrap_or(DEFAULT_FONT_SIZE);
  let fill = optional_color(element.fill, &format!("{path}.fill"))?.unwrap_or_else(black);

  ops.extend([
    Op::SaveGraphicsState,
    Op::StartTextSection,
    Op::SetFillColor { col: fill },
    Op::SetTextCursor {
      pos: Point {
        x: unit.coordinate(x),
        y: unit.coordinate(y),
      },
    },
    Op::SetFont {
      font: PdfFontHandle::Builtin(font),
      size: Pt(font_size),
    },
    Op::ShowText {
      items: vec![TextItem::Text(text)],
    },
    Op::EndTextSection,
    Op::RestoreGraphicsState,
  ]);

  Ok(())
}

fn append_line_ops(
  ops: &mut Vec<Op>,
  element: PdfElementInput,
  unit: Unit,
  path: &str,
) -> Result<()> {
  let x1 = required_f32(element.x1, &format!("{path}.x1"))?;
  let y1 = required_f32(element.y1, &format!("{path}.y1"))?;
  let x2 = required_f32(element.x2, &format!("{path}.x2"))?;
  let y2 = required_f32(element.y2, &format!("{path}.y2"))?;
  let stroke = optional_color(element.stroke, &format!("{path}.stroke"))?.unwrap_or_else(black);
  let stroke_width = optional_positive_f32(element.stroke_width, &format!("{path}.strokeWidth"))?
    .unwrap_or(DEFAULT_STROKE_WIDTH);

  ops.extend([
    Op::SaveGraphicsState,
    Op::SetOutlineColor { col: stroke },
    Op::SetOutlineThickness {
      pt: Pt(stroke_width),
    },
    Op::DrawLine {
      line: Line {
        points: vec![
          LinePoint {
            p: Point {
              x: unit.coordinate(x1),
              y: unit.coordinate(y1),
            },
            bezier: false,
          },
          LinePoint {
            p: Point {
              x: unit.coordinate(x2),
              y: unit.coordinate(y2),
            },
            bezier: false,
          },
        ],
        is_closed: false,
      },
    },
    Op::RestoreGraphicsState,
  ]);

  Ok(())
}

fn append_rect_ops(
  ops: &mut Vec<Op>,
  element: PdfElementInput,
  unit: Unit,
  path: &str,
) -> Result<()> {
  let x = required_f32(element.x, &format!("{path}.x"))?;
  let y = required_f32(element.y, &format!("{path}.y"))?;
  let width = required_positive_f32(element.width, &format!("{path}.width"))?;
  let height = required_positive_f32(element.height, &format!("{path}.height"))?;
  let fill = optional_color(element.fill, &format!("{path}.fill"))?;
  let stroke = optional_color(element.stroke, &format!("{path}.stroke"))?;
  let stroke_width = optional_positive_f32(element.stroke_width, &format!("{path}.strokeWidth"))?
    .unwrap_or(DEFAULT_STROKE_WIDTH);
  let paint_mode = match (fill.is_some(), stroke.is_some()) {
    (true, true) => PaintMode::FillStroke,
    (true, false) => PaintMode::Fill,
    (false, true) | (false, false) => PaintMode::Stroke,
  };
  let stroke = stroke.unwrap_or_else(black);

  ops.push(Op::SaveGraphicsState);

  if let Some(fill) = fill {
    ops.push(Op::SetFillColor { col: fill });
  }

  if paint_mode != PaintMode::Fill {
    ops.push(Op::SetOutlineColor { col: stroke });
    ops.push(Op::SetOutlineThickness {
      pt: Pt(stroke_width),
    });
  }

  ops.push(Op::DrawRectangle {
    rectangle: Rect {
      x: unit.coordinate(x),
      y: unit.coordinate(y),
      width: unit.coordinate(width),
      height: unit.coordinate(height),
      mode: Some(paint_mode),
      winding_order: Some(WindingOrder::NonZero),
    },
  });
  ops.push(Op::RestoreGraphicsState);

  Ok(())
}

fn append_text_box_ops(
  ops: &mut Vec<Op>,
  element: PdfElementInput,
  unit: Unit,
  path: &str,
) -> Result<()> {
  let text = element
    .text
    .ok_or_else(|| required(format!("{path}.text")))?;
  let x = required_f32(element.x, &format!("{path}.x"))?;
  let y = required_f32(element.y, &format!("{path}.y"))?;
  let width = required_positive_f32(element.width, &format!("{path}.width"))?;
  let height = element
    .height
    .map(|height| required_positive_f32(Some(height), &format!("{path}.height")))
    .transpose()?;
  let font = parse_builtin_font(element.font, &format!("{path}.font"))?;
  let font_size = optional_positive_f32(element.font_size, &format!("{path}.fontSize"))?
    .unwrap_or(DEFAULT_FONT_SIZE);
  let line_height = optional_positive_f32(element.line_height, &format!("{path}.lineHeight"))?
    .unwrap_or(font_size * 1.2);
  let fill = optional_color(element.fill, &format!("{path}.fill"))?.unwrap_or_else(black);
  let align = parse_align(element.align.as_deref(), &format!("{path}.align"))?;
  let box_width_pt = unit.coordinate(width).0;
  let max_lines = height
    .map(|height| (unit.coordinate(height).0 / line_height).floor().max(1.0) as usize)
    .unwrap_or(usize::MAX);
  let lines = wrap_text(
    &text,
    box_width_pt,
    font_size,
    element.hyphenate.unwrap_or(false),
  );

  ops.push(Op::SaveGraphicsState);
  ops.push(Op::StartTextSection);
  ops.push(Op::SetFillColor { col: fill });
  ops.push(Op::SetFont {
    font: PdfFontHandle::Builtin(font),
    size: Pt(font_size),
  });
  ops.push(Op::SetLineHeight {
    lh: Pt(line_height),
  });

  for (line_index, line) in lines.into_iter().take(max_lines).enumerate() {
    let adjusted_x = match align {
      TextAlign::Left | TextAlign::Justify => unit.coordinate(x),
      TextAlign::Center => {
        Pt(unit.coordinate(x).0 + (box_width_pt - estimate_text_width(&line, font_size)) / 2.0)
      }
      TextAlign::Right => {
        Pt(unit.coordinate(x).0 + box_width_pt - estimate_text_width(&line, font_size))
      }
    };
    let cursor_y = Pt(unit.coordinate(y).0 - line_height * line_index as f32);
    ops.push(Op::SetTextCursor {
      pos: Point {
        x: adjusted_x,
        y: cursor_y,
      },
    });
    ops.push(Op::ShowText {
      items: vec![TextItem::Text(line)],
    });
  }

  ops.push(Op::EndTextSection);
  ops.push(Op::RestoreGraphicsState);

  Ok(())
}

fn append_polygon_ops(
  ops: &mut Vec<Op>,
  element: PdfElementInput,
  unit: Unit,
  path: &str,
) -> Result<()> {
  let points = required_points(element.points, &format!("{path}.points"), unit)?;
  if points.len() < 3 {
    return Err(invalid_arg(format!(
      "{path}.points must contain at least 3 points"
    )));
  }

  let fill = optional_color(element.fill, &format!("{path}.fill"))?;
  let stroke = optional_color(element.stroke, &format!("{path}.stroke"))?;
  let stroke_width = optional_positive_f32(element.stroke_width, &format!("{path}.strokeWidth"))?
    .unwrap_or(DEFAULT_STROKE_WIDTH);
  let paint_mode = paint_mode(fill.is_some(), stroke.is_some());
  let winding_order = parse_winding(element.winding.as_deref(), &format!("{path}.winding"))?;

  append_shape_style_ops(ops, fill, stroke, stroke_width, paint_mode);
  ops.push(Op::DrawPolygon {
    polygon: Polygon {
      rings: vec![PolygonRing { points }],
      mode: paint_mode,
      winding_order,
    },
  });
  ops.push(Op::RestoreGraphicsState);

  Ok(())
}

fn append_path_ops(
  ops: &mut Vec<Op>,
  element: PdfElementInput,
  unit: Unit,
  path: &str,
) -> Result<()> {
  let points = required_points(element.points, &format!("{path}.points"), unit)?;
  if points.len() < 2 {
    return Err(invalid_arg(format!(
      "{path}.points must contain at least 2 points"
    )));
  }

  let fill = optional_color(element.fill, &format!("{path}.fill"))?;
  let stroke = optional_color(element.stroke, &format!("{path}.stroke"))?;
  let stroke_width = optional_positive_f32(element.stroke_width, &format!("{path}.strokeWidth"))?
    .unwrap_or(DEFAULT_STROKE_WIDTH);
  let closed = element.closed.unwrap_or(false);

  if fill.is_some() {
    let paint_mode = paint_mode(true, stroke.is_some());
    let winding_order = parse_winding(element.winding.as_deref(), &format!("{path}.winding"))?;
    append_shape_style_ops(ops, fill, stroke, stroke_width, paint_mode);
    ops.push(Op::DrawPolygon {
      polygon: Polygon {
        rings: vec![PolygonRing { points }],
        mode: paint_mode,
        winding_order,
      },
    });
  } else {
    append_shape_style_ops(ops, None, stroke, stroke_width, PaintMode::Stroke);
    ops.push(Op::DrawLine {
      line: Line {
        points,
        is_closed: closed,
      },
    });
  }

  ops.push(Op::RestoreGraphicsState);

  Ok(())
}

fn append_shape_style_ops(
  ops: &mut Vec<Op>,
  fill: Option<printpdf::Color>,
  stroke: Option<printpdf::Color>,
  stroke_width: f32,
  paint_mode: PaintMode,
) {
  ops.push(Op::SaveGraphicsState);

  if let Some(fill) = fill {
    ops.push(Op::SetFillColor { col: fill });
  }

  if paint_mode != PaintMode::Fill {
    ops.push(Op::SetOutlineColor {
      col: stroke.unwrap_or_else(black),
    });
    ops.push(Op::SetOutlineThickness {
      pt: Pt(stroke_width),
    });
  }
}

fn required_points(
  points: Option<Vec<PdfPointInput>>,
  path: &str,
  unit: Unit,
) -> Result<Vec<LinePoint>> {
  let points = points.ok_or_else(|| required(path))?;
  points
    .into_iter()
    .enumerate()
    .map(|(index, point)| {
      Ok(LinePoint {
        p: Point {
          x: unit.coordinate(required_f32(Some(point.x), &format!("{path}[{index}].x"))?),
          y: unit.coordinate(required_f32(Some(point.y), &format!("{path}[{index}].y"))?),
        },
        bezier: point.bezier.unwrap_or(false),
      })
    })
    .collect()
}

fn paint_mode(has_fill: bool, has_stroke: bool) -> PaintMode {
  match (has_fill, has_stroke) {
    (true, true) => PaintMode::FillStroke,
    (true, false) => PaintMode::Fill,
    (false, true) | (false, false) => PaintMode::Stroke,
  }
}

fn parse_winding(value: Option<&str>, path: &str) -> Result<WindingOrder> {
  match value.unwrap_or("nonZero") {
    "nonZero" => Ok(WindingOrder::NonZero),
    "evenOdd" => Ok(WindingOrder::EvenOdd),
    value => Err(invalid_arg(format!(
      "{path} must be \"nonZero\" or \"evenOdd\", received \"{value}\""
    ))),
  }
}

#[derive(Clone, Copy)]
enum TextAlign {
  Left,
  Center,
  Right,
  Justify,
}

fn parse_align(value: Option<&str>, path: &str) -> Result<TextAlign> {
  match value.unwrap_or("left") {
    "left" => Ok(TextAlign::Left),
    "center" => Ok(TextAlign::Center),
    "right" => Ok(TextAlign::Right),
    "justify" => Ok(TextAlign::Justify),
    value => Err(invalid_arg(format!(
      "{path} must be one of \"left\", \"center\", \"right\", or \"justify\", received \"{value}\""
    ))),
  }
}

fn wrap_text(text: &str, max_width_pt: f32, font_size: f32, hyphenate: bool) -> Vec<String> {
  let max_chars = (max_width_pt / (font_size * 0.5)).floor().max(1.0) as usize;
  let mut lines = Vec::new();

  for paragraph in text.split('\n') {
    let mut line = String::new();
    for word in paragraph.split_whitespace() {
      for part in split_word(word, max_chars, hyphenate) {
        let candidate_len = if line.is_empty() {
          part.chars().count()
        } else {
          line.chars().count() + 1 + part.chars().count()
        };

        if !line.is_empty() && candidate_len > max_chars {
          lines.push(line);
          line = part;
        } else {
          if !line.is_empty() {
            line.push(' ');
          }
          line.push_str(&part);
        }
      }
    }
    if !line.is_empty() {
      lines.push(line);
    }
  }

  if lines.is_empty() {
    lines.push(String::new());
  }

  lines
}

fn split_word(word: &str, max_chars: usize, hyphenate: bool) -> Vec<String> {
  if word.chars().count() <= max_chars {
    return vec![word.to_string()];
  }

  let chunk_size = if hyphenate && max_chars > 1 {
    max_chars - 1
  } else {
    max_chars
  };
  let mut chunks = Vec::new();
  let chars = word.chars().collect::<Vec<_>>();
  for chunk in chars.chunks(chunk_size.max(1)) {
    let mut value = chunk.iter().collect::<String>();
    if hyphenate
      && value.chars().count() == chunk_size
      && chunks.len() * chunk_size + chunk.len() < chars.len()
    {
      value.push('-');
    }
    chunks.push(value);
  }

  chunks
}

fn estimate_text_width(text: &str, font_size: f32) -> f32 {
  text.chars().count() as f32 * font_size * 0.5
}
