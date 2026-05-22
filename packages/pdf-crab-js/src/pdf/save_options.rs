use napi::Result;
use printpdf::{ImageCompression, ImageOptimizationOptions, PdfSaveOptions};

use super::{input::PdfSaveOptionsInput, validation::invalid_arg};

pub(super) fn parse_save_options(input: Option<PdfSaveOptionsInput>) -> Result<PdfSaveOptions> {
  let Some(input) = input else {
    return Ok(PdfSaveOptions::default());
  };

  let mut options = PdfSaveOptions::default();
  if let Some(optimize) = input.optimize {
    options.optimize = optimize;
  }
  if let Some(subset_fonts) = input.subset_fonts {
    options.subset_fonts = subset_fonts;
  }
  if let Some(secure) = input.secure {
    options.secure = secure;
  }
  if let Some(image_optimization) = input.image_optimization {
    let mut image_options = ImageOptimizationOptions::default();

    if let Some(quality) = image_optimization.quality {
      if !quality.is_finite() || !(0.0..=1.0).contains(&quality) {
        return Err(invalid_arg(
          "saveOptions.imageOptimization.quality must be between 0 and 1",
        ));
      }
      image_options.quality = Some(quality as f32);
    }
    if let Some(max_image_size) = image_optimization.max_image_size {
      image_options.max_image_size = Some(max_image_size);
    }
    if let Some(dither_greyscale) = image_optimization.dither_greyscale {
      image_options.dither_greyscale = Some(dither_greyscale);
    }
    if let Some(convert_to_greyscale) = image_optimization.convert_to_greyscale {
      image_options.convert_to_greyscale = Some(convert_to_greyscale);
    }
    if let Some(auto_optimize) = image_optimization.auto_optimize {
      image_options.auto_optimize = Some(auto_optimize);
    }
    if let Some(format) = image_optimization.format {
      image_options.format = Some(parse_image_compression(
        &format,
        "saveOptions.imageOptimization.format",
      )?);
    }

    options.image_optimization = Some(image_options);
  }

  Ok(options)
}

fn parse_image_compression(value: &str, path: &str) -> Result<ImageCompression> {
  match value {
    "auto" => Ok(ImageCompression::Auto),
    "jpeg" => Ok(ImageCompression::Jpeg),
    "jpeg2000" => Ok(ImageCompression::Jpeg2000),
    "flate" => Ok(ImageCompression::Flate),
    "lzw" => Ok(ImageCompression::Lzw),
    "runLength" => Ok(ImageCompression::RunLength),
    "none" => Ok(ImageCompression::None),
    value => Err(invalid_arg(format!(
      "{path} must be one of \"auto\", \"jpeg\", \"jpeg2000\", \"flate\", \"lzw\", \"runLength\", or \"none\", received \"{value}\""
    ))),
  }
}
