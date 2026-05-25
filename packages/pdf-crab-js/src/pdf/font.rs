use napi::Result;
use pdf_writer::Name;

use super::validation::invalid_arg;

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum BuiltinFont {
  TimesRoman,
  TimesBold,
  TimesItalic,
  TimesBoldItalic,
  Helvetica,
  HelveticaBold,
  HelveticaOblique,
  HelveticaBoldOblique,
  Courier,
  CourierOblique,
  CourierBold,
  CourierBoldOblique,
  Symbol,
  ZapfDingbats,
}

impl BuiltinFont {
  pub(super) const ALL: [Self; 14] = [
    Self::TimesRoman,
    Self::TimesBold,
    Self::TimesItalic,
    Self::TimesBoldItalic,
    Self::Helvetica,
    Self::HelveticaBold,
    Self::HelveticaOblique,
    Self::HelveticaBoldOblique,
    Self::Courier,
    Self::CourierOblique,
    Self::CourierBold,
    Self::CourierBoldOblique,
    Self::Symbol,
    Self::ZapfDingbats,
  ];

  pub(super) fn resource_name(self) -> Name<'static> {
    match self {
      Self::TimesRoman => Name(b"F1"),
      Self::TimesBold => Name(b"F2"),
      Self::TimesItalic => Name(b"F3"),
      Self::TimesBoldItalic => Name(b"F4"),
      Self::Helvetica => Name(b"F5"),
      Self::HelveticaBold => Name(b"F6"),
      Self::HelveticaOblique => Name(b"F7"),
      Self::HelveticaBoldOblique => Name(b"F8"),
      Self::Courier => Name(b"F9"),
      Self::CourierOblique => Name(b"F10"),
      Self::CourierBold => Name(b"F11"),
      Self::CourierBoldOblique => Name(b"F12"),
      Self::Symbol => Name(b"F13"),
      Self::ZapfDingbats => Name(b"F14"),
    }
  }

  pub(super) fn base_name(self) -> Name<'static> {
    match self {
      Self::TimesRoman => Name(b"Times-Roman"),
      Self::TimesBold => Name(b"Times-Bold"),
      Self::TimesItalic => Name(b"Times-Italic"),
      Self::TimesBoldItalic => Name(b"Times-BoldItalic"),
      Self::Helvetica => Name(b"Helvetica"),
      Self::HelveticaBold => Name(b"Helvetica-Bold"),
      Self::HelveticaOblique => Name(b"Helvetica-Oblique"),
      Self::HelveticaBoldOblique => Name(b"Helvetica-BoldOblique"),
      Self::Courier => Name(b"Courier"),
      Self::CourierOblique => Name(b"Courier-Oblique"),
      Self::CourierBold => Name(b"Courier-Bold"),
      Self::CourierBoldOblique => Name(b"Courier-BoldOblique"),
      Self::Symbol => Name(b"Symbol"),
      Self::ZapfDingbats => Name(b"ZapfDingbats"),
    }
  }

  pub(super) fn ref_number(self) -> i32 {
    match self {
      Self::TimesRoman => 4,
      Self::TimesBold => 5,
      Self::TimesItalic => 6,
      Self::TimesBoldItalic => 7,
      Self::Helvetica => 8,
      Self::HelveticaBold => 9,
      Self::HelveticaOblique => 10,
      Self::HelveticaBoldOblique => 11,
      Self::Courier => 12,
      Self::CourierOblique => 13,
      Self::CourierBold => 14,
      Self::CourierBoldOblique => 15,
      Self::Symbol => 16,
      Self::ZapfDingbats => 17,
    }
  }
}

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
