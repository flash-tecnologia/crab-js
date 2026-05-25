use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use fulgur::{AssetBundle, Engine, Margin, PageSize};
use napi::bindgen_prelude::Either;
use napi::{Error, Result, Status};

use crate::{
  input::{
    HtmlPdfPageCustomSizeInput, HtmlPdfPageInput, HtmlPdfPageMarginInput, RenderPdfBinaryInput,
    RenderPdfFontInput, RenderPdfFromHtmlInput, RenderPdfImageInput,
  },
  unit::Unit,
  validation::{invalid_arg, positive_f32},
};

pub(crate) fn create_pdf_from_html_bytes(input: RenderPdfFromHtmlInput) -> Result<Vec<u8>> {
  if input.html.trim().is_empty() {
    return Err(invalid_arg("html must not be empty"));
  }

  let mut builder = Engine::builder();

  if let Some(title) = input.title {
    if title.trim().is_empty() {
      return Err(invalid_arg("title must not be empty when provided"));
    }
    builder = builder.title(title);
  }

  if let Some(page) = input.page {
    builder = apply_page(builder, page)?;
  }

  if let Some(bookmarks) = input.bookmarks {
    builder = builder.bookmarks(bookmarks);
  }
  if let Some(tagged) = input.tagged {
    builder = builder.tagged(tagged);
  }
  if let Some(pdf_ua) = input.pdf_ua {
    builder = builder.pdf_ua(pdf_ua);
  }
  if let Some(base_path) = input.base_path {
    if base_path.trim().is_empty() {
      return Err(invalid_arg("basePath must not be empty when provided"));
    }
    builder = builder.base_path(base_path);
  }

  builder = builder.system_fonts(input.system_fonts.unwrap_or(true));

  if let Some(assets) = build_assets(input.css, input.fonts, input.images)? {
    builder = builder.assets(assets);
  }

  builder.build().render_html(&input.html).map_err(|error| {
    Error::new(
      Status::GenericFailure,
      format!("HTML renderer failed to render HTML: {error}"),
    )
  })
}

fn apply_page(
  mut builder: fulgur::EngineBuilder,
  page: HtmlPdfPageInput,
) -> Result<fulgur::EngineBuilder> {
  if let Some(size) = page.size {
    builder = builder.page_size(parse_page_size(size)?);
  }
  if let Some(margin) = page.margin {
    builder = builder.margin(parse_margin(margin)?);
  }
  if let Some(landscape) = page.landscape {
    builder = builder.landscape(landscape);
  }

  Ok(builder)
}

fn build_assets(
  css: Option<Either<String, Vec<String>>>,
  fonts: Option<RenderPdfFontInput>,
  images: Option<Vec<RenderPdfImageInput>>,
) -> Result<Option<AssetBundle>> {
  let mut assets = AssetBundle::new();
  let mut has_assets = false;

  if let Some(css) = css {
    match css {
      Either::A(css) => {
        assets.add_css(css);
      }
      Either::B(css_items) => {
        for css in css_items {
          assets.add_css(css);
        }
      }
    }
    has_assets = true;
  }

  if let Some(fonts) = fonts {
    match fonts {
      RenderPdfFontInput::Bytes(fonts) => {
        for font in fonts {
          add_font_asset(&mut assets, font)?;
        }
      }
      RenderPdfFontInput::Base64(fonts) => {
        for font in fonts {
          add_font_asset(&mut assets, decode_base64_asset(font, "fonts[]")?)?;
        }
      }
    }
    has_assets = true;
  }

  if let Some(images) = images {
    for image in images {
      if image.name.trim().is_empty() {
        return Err(invalid_arg("images[].name must not be empty"));
      }
      let data = match image.data {
        RenderPdfBinaryInput::Bytes(data) => data,
        RenderPdfBinaryInput::Base64(data) => decode_base64_asset(data, "images[].data")?,
      };
      assets.add_image(image.name, data);
    }
    has_assets = true;
  }

  Ok(has_assets.then_some(assets))
}

fn add_font_asset(assets: &mut AssetBundle, font: Vec<u8>) -> Result<()> {
  assets
    .add_font_bytes(font)
    .map_err(|error| invalid_arg(format!("invalid font asset: {error}")))
}

fn decode_base64_asset(data: String, field: &str) -> Result<Vec<u8>> {
  BASE64_STANDARD
    .decode(data)
    .map_err(|error| invalid_arg(format!("invalid {field} base64 asset: {error}")))
}

fn parse_page_size(size: Either<String, HtmlPdfPageCustomSizeInput>) -> Result<PageSize> {
  match size {
    Either::A(size) => match size.as_str() {
      "A4" => Ok(PageSize::A4),
      "LETTER" => Ok(PageSize::LETTER),
      "A3" => Ok(PageSize::A3),
      size => Err(invalid_arg(format!(
        "page.size must be \"A4\", \"LETTER\", \"A3\", or a custom size object, received \"{size}\""
      ))),
    },
    Either::B(size) => {
      let unit = Unit::from_input(size.unit)?;
      Ok(PageSize {
        width: unit.coordinate(positive_f32(size.width, "page.size.width")?),
        height: unit.coordinate(positive_f32(size.height, "page.size.height")?),
      })
    }
  }
}

fn parse_margin(margin: Either<f64, HtmlPdfPageMarginInput>) -> Result<Margin> {
  match margin {
    Either::A(margin) => {
      let margin = Unit::Mm.coordinate(positive_f32(margin, "page.margin")?);
      Ok(Margin::uniform(margin))
    }
    Either::B(margin) => {
      let unit = Unit::from_input(margin.unit)?;
      Ok(Margin {
        top: unit.coordinate(positive_f32(margin.top, "page.margin.top")?),
        right: unit.coordinate(positive_f32(margin.right, "page.margin.right")?),
        bottom: unit.coordinate(positive_f32(margin.bottom, "page.margin.bottom")?),
        left: unit.coordinate(positive_f32(margin.left, "page.margin.left")?),
      })
    }
  }
}
