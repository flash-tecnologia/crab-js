use napi::{Error, Result, Status};

pub(crate) fn positive_f32(value: f64, path: &str) -> Result<f32> {
  let value = to_f32(value, path)?;
  if value <= 0.0 {
    return Err(invalid_arg(format!("{path} must be greater than 0")));
  }

  Ok(value)
}

fn to_f32(value: f64, path: &str) -> Result<f32> {
  if !value.is_finite() || value.abs() > f64::from(f32::MAX) {
    return Err(invalid_arg(format!("{path} must be a finite number")));
  }

  Ok(value as f32)
}

pub(crate) fn invalid_arg(message: impl Into<String>) -> Error {
  Error::new(Status::InvalidArg, message.into())
}
