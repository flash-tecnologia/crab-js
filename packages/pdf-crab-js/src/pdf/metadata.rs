use napi::Result;
use printpdf::{Layer, LayerInternalId, PdfConformance, PdfDocument};
use std::collections::BTreeMap;

use super::{
  input::{PdfBookmarkInput, PdfLayerInput, PdfMetadataInput},
  validation::invalid_arg,
};

pub(super) fn apply_metadata(
  document: &mut PdfDocument,
  title: Option<String>,
  metadata: Option<PdfMetadataInput>,
  conformance: Option<String>,
) -> Result<()> {
  if let Some(title) = title {
    document.metadata.info.document_title = title;
  }

  if let Some(metadata) = metadata {
    if let Some(title) = metadata.title {
      document.metadata.info.document_title = title;
    }
    if let Some(author) = metadata.author {
      document.metadata.info.author = author;
    }
    if let Some(creator) = metadata.creator {
      document.metadata.info.creator = creator;
    }
    if let Some(producer) = metadata.producer {
      document.metadata.info.producer = producer;
    }
    if let Some(subject) = metadata.subject {
      document.metadata.info.subject = subject;
    }
    if let Some(keywords) = metadata.keywords {
      document.metadata.info.keywords = keywords;
    }
    if let Some(identifier) = metadata.identifier {
      document.metadata.info.identifier = identifier;
    }
    if let Some(trapped) = metadata.trapped {
      document.metadata.info.trapped = trapped;
    }
  }

  if let Some(conformance) = conformance {
    document.metadata.info.conformance = parse_conformance(&conformance, "conformance")?;
  }

  Ok(())
}

pub(super) fn add_bookmarks(
  document: &mut PdfDocument,
  bookmarks: Option<Vec<PdfBookmarkInput>>,
  page_count: usize,
) -> Result<()> {
  let Some(bookmarks) = bookmarks else {
    return Ok(());
  };

  for (index, bookmark) in bookmarks.into_iter().enumerate() {
    let path = format!("bookmarks[{index}]");
    let page_index = bookmark.page_index as usize;
    if page_index >= page_count {
      return Err(invalid_arg(format!(
        "{path}.pageIndex must reference an existing page"
      )));
    }
    if bookmark.name.trim().is_empty() {
      return Err(invalid_arg(format!("{path}.name must not be empty")));
    }

    document.add_bookmark(&bookmark.name, page_index + 1);
  }

  Ok(())
}

pub(super) fn add_layers(
  document: &mut PdfDocument,
  layers: Option<Vec<PdfLayerInput>>,
) -> Result<BTreeMap<String, LayerInternalId>> {
  let mut layer_ids = BTreeMap::new();
  let Some(layers) = layers else {
    return Ok(layer_ids);
  };

  for (index, layer) in layers.into_iter().enumerate() {
    let path = format!("layers[{index}]");
    if layer.name.trim().is_empty() {
      return Err(invalid_arg(format!("{path}.name must not be empty")));
    }

    let key = layer.id.unwrap_or_else(|| layer.name.clone());
    if layer_ids.contains_key(&key) {
      return Err(invalid_arg(format!(
        "{path}.id must be unique when provided, otherwise layer names must be unique"
      )));
    }

    let layer_id = document.add_layer(&Layer::new(&layer.name));
    layer_ids.insert(key, layer_id);
  }

  Ok(layer_ids)
}

fn parse_conformance(value: &str, path: &str) -> Result<PdfConformance> {
  match value {
    "pdf1_3" => Ok(PdfConformance::default()),
    "pdfA1B" => Ok(PdfConformance::A1B_2005_PDF_1_4),
    "pdfX1A2001" => Ok(PdfConformance::X1A_2001_PDF_1_3),
    "pdfX3_2002" => Ok(PdfConformance::X3_2002_PDF_1_3),
    value => Err(invalid_arg(format!(
      "{path} must be one of \"pdf1_3\", \"pdfA1B\", \"pdfX1A2001\", or \"pdfX3_2002\", received \"{value}\""
    ))),
  }
}
