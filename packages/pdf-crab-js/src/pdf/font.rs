use napi::Result;
use printpdf::BuiltinFont;

use super::validation::invalid_arg;

pub(super) fn parse_builtin_font(font: Option<String>, path: &str) -> Result<BuiltinFont> {
  let Some(font) = font else {
    return Ok(BuiltinFont::Helvetica);
  };
  let mut normalized = font.to_ascii_lowercase();
  normalized.retain(|char| !matches!(char, ' ' | '-' | '_'));

  match normalized.as_str() {
    "times" | "timesroman" | "timesnewroman" => Ok(BuiltinFont::TimesRoman),
    "timesbold" | "timesnewromanbold" => Ok(BuiltinFont::TimesBold),
    "timesitalic" | "timesnewromanitalic" => Ok(BuiltinFont::TimesItalic),
    "timesbolditalic" | "timesitalicbold" | "timesnewromanbolditalic" => {
      Ok(BuiltinFont::TimesBoldItalic)
    }
    "helvetica" | "arial" => Ok(BuiltinFont::Helvetica),
    "helveticabold" | "arialbold" => Ok(BuiltinFont::HelveticaBold),
    "helveticaoblique" | "helveticaitalic" | "arialitalic" => Ok(BuiltinFont::HelveticaOblique),
    "helveticaboldoblique" | "helveticabolditalic" | "arialbolditalic" => {
      Ok(BuiltinFont::HelveticaBoldOblique)
    }
    "courier" | "couriernew" => Ok(BuiltinFont::Courier),
    "courieroblique" | "courieritalic" | "couriernewitalic" => Ok(BuiltinFont::CourierOblique),
    "courierbold" | "couriernewbold" => Ok(BuiltinFont::CourierBold),
    "courierboldoblique" | "courierbolditalic" | "couriernewbolditalic" => {
      Ok(BuiltinFont::CourierBoldOblique)
    }
    "symbol" => Ok(BuiltinFont::Symbol),
    "zapfdingbats" => Ok(BuiltinFont::ZapfDingbats),
    _ => Err(invalid_arg(format!(
      "{path} must be one of the built-in PDF fonts"
    ))),
  }
}
