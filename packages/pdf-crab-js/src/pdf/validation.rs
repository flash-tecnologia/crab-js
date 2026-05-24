use napi::{Error, Result, Status};

pub(super) fn required_f32(value: Option<f64>, path: &str) -> Result<f32> {
  to_f32(value.ok_or_else(|| required(path))?, path)
}

pub(super) fn required_positive_f32(value: Option<f64>, path: &str) -> Result<f32> {
  positive_f32(value.ok_or_else(|| required(path))?, path)
}

pub(super) fn optional_positive_f32(value: Option<f64>, path: &str) -> Result<Option<f32>> {
  value.map(|value| positive_f32(value, path)).transpose()
}

pub(super) fn positive_f32(value: f64, path: &str) -> Result<f32> {
  let value = to_f32(value, path)?;
  if value <= 0.0 {
    return Err(invalid_arg(format!("{path} must be greater than 0")));
  }

  Ok(value)
}

pub(super) fn to_f32(value: f64, path: &str) -> Result<f32> {
  if !value.is_finite() || value.abs() > f64::from(f32::MAX) {
    return Err(invalid_arg(format!("{path} must be a finite number")));
  }

  Ok(value as f32)
}

pub(super) fn required(path: impl Into<String>) -> Error {
  Error::new(Status::InvalidArg, format!("{} is required", path.into()))
}

pub(super) fn invalid_arg(message: impl Into<String>) -> Error {
  Error::new(Status::InvalidArg, message.into())
}
