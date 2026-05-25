use napi::Result;

use super::validation::invalid_arg;

#[derive(Clone, Copy)]
pub(super) struct RgbColor {
  pub(super) red: f32,
  pub(super) green: f32,
  pub(super) blue: f32,
}

pub(super) fn optional_color(value: Option<String>, path: &str) -> Result<Option<RgbColor>> {
  value.map(|color| parse_color(&color, path)).transpose()
}

pub(super) fn black() -> RgbColor {
  RgbColor {
    red: 0.0,
    green: 0.0,
    blue: 0.0,
  }
}

fn parse_color(value: &str, path: &str) -> Result<RgbColor> {
  let hex = value.strip_prefix('#').unwrap_or(value);
  if hex.len() != 6 || !hex.chars().all(|char| char.is_ascii_hexdigit()) {
    return Err(invalid_arg(format!("{path} must be a hex color (#RRGGBB)")));
  }

  Ok(RgbColor {
    red: parse_hex_pair(&hex[0..2], path)?,
    green: parse_hex_pair(&hex[2..4], path)?,
    blue: parse_hex_pair(&hex[4..6], path)?,
  })
}

fn parse_hex_pair(value: &str, path: &str) -> Result<f32> {
  u8::from_str_radix(value, 16)
    .map(|value| f32::from(value) / 255.0)
    .map_err(|_| invalid_arg(format!("{path} must be a hex color (#RRGGBB)")))
}
