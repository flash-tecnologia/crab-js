use napi::Result;

use super::validation::invalid_arg;

#[derive(Clone, Copy)]
pub(super) enum Unit {
  Mm,
  Pt,
}

impl Unit {
  pub(super) fn from_input(unit: Option<String>) -> Result<Self> {
    match unit.as_deref().unwrap_or("mm") {
      "mm" => Ok(Self::Mm),
      "pt" => Ok(Self::Pt),
      unit => Err(invalid_arg(format!(
        "unit must be \"mm\" or \"pt\", received \"{unit}\""
      ))),
    }
  }

  pub(super) fn coordinate(self, value: f32) -> f32 {
    match self {
      Self::Mm => value * 72.0 / 25.4,
      Self::Pt => value,
    }
  }
}
