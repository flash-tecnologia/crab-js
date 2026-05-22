use napi::Result;
use printpdf::{Color, ColorArray, Rgb};

use super::validation::invalid_arg;

pub(super) fn optional_color(value: Option<String>, path: &str) -> Result<Option<Color>> {
  value.map(|color| parse_color(&color, path)).transpose()
}

pub(super) fn optional_color_array(
  value: Option<String>,
  path: &str,
) -> Result<Option<ColorArray>> {
  value
    .map(|color| {
      let hex = color.strip_prefix('#').unwrap_or(&color);
      if hex.len() != 6 || !hex.chars().all(|char| char.is_ascii_hexdigit()) {
        return Err(invalid_arg(format!("{path} must be a hex color (#RRGGBB)")));
      }

      Ok(ColorArray::Rgb([
        parse_hex_pair(&hex[0..2], path)?,
        parse_hex_pair(&hex[2..4], path)?,
        parse_hex_pair(&hex[4..6], path)?,
      ]))
    })
    .transpose()
}

pub(super) fn black() -> Color {
  Color::Rgb(Rgb::new(0.0, 0.0, 0.0, None))
}

fn parse_color(value: &str, path: &str) -> Result<Color> {
  let hex = value.strip_prefix('#').unwrap_or(value);
  if hex.len() != 6 || !hex.chars().all(|char| char.is_ascii_hexdigit()) {
    return Err(invalid_arg(format!("{path} must be a hex color (#RRGGBB)")));
  }

  let red = parse_hex_pair(&hex[0..2], path)?;
  let green = parse_hex_pair(&hex[2..4], path)?;
  let blue = parse_hex_pair(&hex[4..6], path)?;

  Ok(Color::Rgb(Rgb::new(red, green, blue, None)))
}

fn parse_hex_pair(value: &str, path: &str) -> Result<f32> {
  u8::from_str_radix(value, 16)
    .map(|value| f32::from(value) / 255.0)
    .map_err(|_| invalid_arg(format!("{path} must be a hex color (#RRGGBB)")))
}
