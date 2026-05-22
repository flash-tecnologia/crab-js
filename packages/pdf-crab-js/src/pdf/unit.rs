use napi::Result;
use printpdf::{Mm, Pt};

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

  pub(super) fn page_size(self, value: f32) -> Mm {
    match self {
      Self::Mm => Mm(value),
      Self::Pt => Pt(value).into(),
    }
  }

  pub(super) fn coordinate(self, value: f32) -> Pt {
    match self {
      Self::Mm => Mm(value).into(),
      Self::Pt => Pt(value),
    }
  }
}
