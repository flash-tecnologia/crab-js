use napi::Result;
use pdf_writer::{Content, Str};

use super::{
  color::{black, optional_color, RgbColor},
  font::parse_builtin_font,
  input::{PdfElementInput, PdfPointInput},
  unit::Unit,
  validation::{invalid_arg, optional_positive_f32, required, required_f32, required_positive_f32},
};

const DEFAULT_FONT_SIZE: f32 = 12.0;
const DEFAULT_STROKE_WIDTH: f32 = 1.0;

pub(super) fn append_element(
  content: &mut Content,
  element: PdfElementInput,
  unit: Unit,
  path: &str,
) -> Result<()> {
  match element.r#type.as_str() {
    "text" => append_text(content, element, unit, path),
    "line" => append_line(content, element, unit, path),
    "rect" => append_rect(content, element, unit, path),
    "textBox" => append_text_box(content, element, unit, path),
    "polygon" => append_polygon(content, element, unit, path),
    "path" => append_path(content, element, unit, path),
    element_type => Err(invalid_arg(format!(
      "{path}.type must be one of \"text\", \"line\", \"rect\", \"textBox\", \"polygon\", or \"path\", received \"{element_type}\""
    ))),
  }
}

fn append_text(
  content: &mut Content,
  element: PdfElementInput,
  unit: Unit,
  path: &str,
) -> Result<()> {
  let text = element
    .text
    .ok_or_else(|| required(format!("{path}.text")))?;
  let x = unit.coordinate(required_f32(element.x, &format!("{path}.x"))?);
  let y = unit.coordinate(required_f32(element.y, &format!("{path}.y"))?);
  let font = parse_builtin_font(element.font, &format!("{path}.font"))?;
  let font_size = optional_positive_f32(element.font_size, &format!("{path}.fontSize"))?
    .unwrap_or(DEFAULT_FONT_SIZE);
  let fill = optional_color(element.fill, &format!("{path}.fill"))?.unwrap_or_else(black);

  content.save_state();
  set_fill(content, fill);
  content.begin_text();
  content.set_font(font.resource_name(), font_size);
  content.set_text_matrix([1.0, 0.0, 0.0, 1.0, x, y]);
  content.show(Str(text.as_bytes()));
  content.end_text();
  content.restore_state();

  Ok(())
}

fn append_line(
  content: &mut Content,
  element: PdfElementInput,
  unit: Unit,
  path: &str,
) -> Result<()> {
  let x1 = unit.coordinate(required_f32(element.x1, &format!("{path}.x1"))?);
  let y1 = unit.coordinate(required_f32(element.y1, &format!("{path}.y1"))?);
  let x2 = unit.coordinate(required_f32(element.x2, &format!("{path}.x2"))?);
  let y2 = unit.coordinate(required_f32(element.y2, &format!("{path}.y2"))?);
  let stroke = optional_color(element.stroke, &format!("{path}.stroke"))?.unwrap_or_else(black);
  let stroke_width = optional_positive_f32(element.stroke_width, &format!("{path}.strokeWidth"))?
    .unwrap_or(DEFAULT_STROKE_WIDTH);

  content.save_state();
  set_stroke(content, stroke);
  content.set_line_width(stroke_width);
  content.move_to(x1, y1);
  content.line_to(x2, y2);
  content.stroke();
  content.restore_state();

  Ok(())
}

fn append_rect(
  content: &mut Content,
  element: PdfElementInput,
  unit: Unit,
  path: &str,
) -> Result<()> {
  let x = unit.coordinate(required_f32(element.x, &format!("{path}.x"))?);
  let y = unit.coordinate(required_f32(element.y, &format!("{path}.y"))?);
  let width = unit.coordinate(required_positive_f32(
    element.width,
    &format!("{path}.width"),
  )?);
  let height = unit.coordinate(required_positive_f32(
    element.height,
    &format!("{path}.height"),
  )?);
  let fill = optional_color(element.fill, &format!("{path}.fill"))?;
  let stroke = optional_color(element.stroke, &format!("{path}.stroke"))?;
  let stroke_width = optional_positive_f32(element.stroke_width, &format!("{path}.strokeWidth"))?
    .unwrap_or(DEFAULT_STROKE_WIDTH);

  content.save_state();
  if let Some(fill) = fill {
    set_fill(content, fill);
  }
  if let Some(stroke) = stroke.or_else(|| fill.is_none().then(black)) {
    set_stroke(content, stroke);
    content.set_line_width(stroke_width);
  }
  content.rect(x, y, width, height);
  paint_path(
    content,
    fill.is_some(),
    stroke.is_some() || fill.is_none(),
    false,
    Winding::NonZero,
  );
  content.restore_state();

  Ok(())
}

fn append_text_box(
  content: &mut Content,
  element: PdfElementInput,
  unit: Unit,
  path: &str,
) -> Result<()> {
  let text = element
    .text
    .ok_or_else(|| required(format!("{path}.text")))?;
  let x = unit.coordinate(required_f32(element.x, &format!("{path}.x"))?);
  let y = unit.coordinate(required_f32(element.y, &format!("{path}.y"))?);
  let width = unit.coordinate(required_positive_f32(
    element.width,
    &format!("{path}.width"),
  )?);
  let height = element
    .height
    .map(|height| required_positive_f32(Some(height), &format!("{path}.height")))
    .transpose()?
    .map(|height| unit.coordinate(height));
  let font = parse_builtin_font(element.font, &format!("{path}.font"))?;
  let font_size = optional_positive_f32(element.font_size, &format!("{path}.fontSize"))?
    .unwrap_or(DEFAULT_FONT_SIZE);
  let line_height = optional_positive_f32(element.line_height, &format!("{path}.lineHeight"))?
    .unwrap_or(font_size * 1.2);
  let fill = optional_color(element.fill, &format!("{path}.fill"))?.unwrap_or_else(black);
  let align = parse_align(element.align.as_deref(), &format!("{path}.align"))?;
  let max_lines = height
    .map(|height| (height / line_height).floor().max(1.0) as usize)
    .unwrap_or(usize::MAX);
  let lines = wrap_text(&text, width, font_size, element.hyphenate.unwrap_or(false));

  content.save_state();
  set_fill(content, fill);
  content.begin_text();
  content.set_font(font.resource_name(), font_size);
  content.set_leading(line_height);

  for (line_index, line) in lines.into_iter().take(max_lines).enumerate() {
    let adjusted_x = match align {
      TextAlign::Left | TextAlign::Justify => x,
      TextAlign::Center => x + (width - estimate_text_width(&line, font_size)) / 2.0,
      TextAlign::Right => x + width - estimate_text_width(&line, font_size),
    };
    let cursor_y = y - line_height * line_index as f32;
    content.set_text_matrix([1.0, 0.0, 0.0, 1.0, adjusted_x, cursor_y]);
    content.show(Str(line.as_bytes()));
  }

  content.end_text();
  content.restore_state();

  Ok(())
}

fn append_polygon(
  content: &mut Content,
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
  let winding = parse_winding(element.winding.as_deref(), &format!("{path}.winding"))?;

  content.save_state();
  apply_shape_style(content, fill, stroke, stroke_width);
  append_points(content, &points, true);
  paint_path(content, fill.is_some(), stroke.is_some(), true, winding);
  content.restore_state();

  Ok(())
}

fn append_path(
  content: &mut Content,
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
  let winding = parse_winding(element.winding.as_deref(), &format!("{path}.winding"))?;

  content.save_state();
  apply_shape_style(content, fill, stroke, stroke_width);
  append_points(content, &points, closed);
  paint_path(
    content,
    fill.is_some(),
    stroke.is_some() || fill.is_none(),
    closed,
    winding,
  );
  content.restore_state();

  Ok(())
}

fn set_fill(content: &mut Content, color: RgbColor) {
  content.set_fill_rgb(color.red, color.green, color.blue);
}

fn set_stroke(content: &mut Content, color: RgbColor) {
  content.set_stroke_rgb(color.red, color.green, color.blue);
}

fn apply_shape_style(
  content: &mut Content,
  fill: Option<RgbColor>,
  stroke: Option<RgbColor>,
  stroke_width: f32,
) {
  if let Some(fill) = fill {
    set_fill(content, fill);
  }
  set_stroke(content, stroke.unwrap_or_else(black));
  content.set_line_width(stroke_width);
}

fn append_points(content: &mut Content, points: &[PdfPoint], closed: bool) {
  let [first, rest @ ..] = points else {
    return;
  };

  content.move_to(first.x, first.y);
  for point in rest {
    content.line_to(point.x, point.y);
  }
  if closed {
    content.close_path();
  }
}

fn paint_path(
  content: &mut Content,
  has_fill: bool,
  has_stroke: bool,
  closed: bool,
  winding: Winding,
) {
  match (has_fill, has_stroke, closed, winding) {
    (true, true, true, Winding::NonZero) => {
      content.close_fill_nonzero_and_stroke();
    }
    (true, true, true, Winding::EvenOdd) => {
      content.close_fill_even_odd_and_stroke();
    }
    (true, true, false, Winding::NonZero) => {
      content.fill_nonzero_and_stroke();
    }
    (true, true, false, Winding::EvenOdd) => {
      content.fill_even_odd_and_stroke();
    }
    (true, false, _, Winding::NonZero) => {
      content.fill_nonzero();
    }
    (true, false, _, Winding::EvenOdd) => {
      content.fill_even_odd();
    }
    (false, true, true, _) => {
      content.close_and_stroke();
    }
    (false, true, false, _) => {
      content.stroke();
    }
    (false, false, _, _) => {
      content.end_path();
    }
  }
}

#[derive(Clone, Copy)]
struct PdfPoint {
  x: f32,
  y: f32,
}

fn required_points(
  points: Option<Vec<PdfPointInput>>,
  path: &str,
  unit: Unit,
) -> Result<Vec<PdfPoint>> {
  let points = points.ok_or_else(|| required(path))?;
  points
    .into_iter()
    .enumerate()
    .map(|(index, point)| {
      if point.bezier.unwrap_or(false) {
        return Err(invalid_arg(format!(
          "{path}[{index}].bezier is not supported by the pdf-writer phase"
        )));
      }

      Ok(PdfPoint {
        x: unit.coordinate(required_f32(Some(point.x), &format!("{path}[{index}].x"))?),
        y: unit.coordinate(required_f32(Some(point.y), &format!("{path}[{index}].y"))?),
      })
    })
    .collect()
}

#[derive(Clone, Copy)]
enum Winding {
  NonZero,
  EvenOdd,
}

fn parse_winding(value: Option<&str>, path: &str) -> Result<Winding> {
  match value.unwrap_or("nonZero") {
    "nonZero" => Ok(Winding::NonZero),
    "evenOdd" => Ok(Winding::EvenOdd),
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

        if candidate_len > max_chars && !line.is_empty() {
          lines.push(line);
          line = String::new();
        }

        if !line.is_empty() {
          line.push(' ');
        }
        line.push_str(&part);
      }
    }

    if !line.is_empty() {
      lines.push(line);
    }
  }

  lines
}

fn split_word(word: &str, max_chars: usize, hyphenate: bool) -> Vec<String> {
  if !hyphenate || word.chars().count() <= max_chars {
    return vec![word.to_string()];
  }

  let chunk_size = max_chars.saturating_sub(1).max(1);
  let chars = word.chars().collect::<Vec<_>>();
  chars
    .chunks(chunk_size)
    .enumerate()
    .map(|(index, chunk)| {
      let mut part = chunk.iter().collect::<String>();
      if index < chars.len().div_ceil(chunk_size) - 1 {
        part.push('-');
      }
      part
    })
    .collect()
}

fn estimate_text_width(text: &str, font_size: f32) -> f32 {
  text.chars().count() as f32 * font_size * 0.5
}
