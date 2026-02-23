use std::{
  collections::{hash_map::RandomState, HashMap},
  sync::OnceLock,
};

use napi::{bindgen_prelude::Buffer, Error, Status};
use rdkafka::{
  message::{BorrowedHeaders, BorrowedMessage, Header, Headers, OwnedHeaders},
  Message as RdMessage,
};

use super::producer::model::Message;

pub trait IntoNapiError {
  fn into_napi_error(self, context: &str) -> Error;
}

impl<E: std::fmt::Debug> IntoNapiError for E {
  fn into_napi_error(self, context: &str) -> Error {
    Error::new(Status::GenericFailure, format!("{context}: {self:?}"))
  }
}

pub fn hashmap_to_kafka_headers(map: &HashMap<String, Buffer>) -> OwnedHeaders {
  map.iter().fold(OwnedHeaders::new(), |acc, (key, value)| {
    let value: &[u8] = value.as_ref();
    acc.insert(Header {
      key,
      value: Some(value),
    })
  })
}

#[inline]
fn header_hash_builder() -> RandomState {
  static HEADER_HASH_BUILDER: OnceLock<RandomState> = OnceLock::new();
  HEADER_HASH_BUILDER.get_or_init(RandomState::new).clone()
}

#[inline]
fn borrowed_headers_to_hashmap_buffer(
  headers: &BorrowedHeaders,
) -> Option<HashMap<String, Buffer>> {
  let header_count = headers.count();
  if header_count == 0 {
    return None;
  }

  let mut map: Option<HashMap<String, Buffer>> = None;
  for index in 0..header_count {
    let header = headers.get(index);
    if let Some(value) = header.value {
      let header_map = map.get_or_insert_with(|| {
        HashMap::with_capacity_and_hasher(header_count, header_hash_builder())
      });
      header_map.insert(header.key.to_owned(), value.into());
    }
  }

  map
}

#[inline]
pub fn create_message(message: &BorrowedMessage<'_>, payload: &[u8]) -> Message {
  let topic = message.topic().to_owned();
  let partition = message.partition();
  let offset = message.offset();
  match (message.key(), message.headers()) {
    (None, None) => Message::new(payload.into(), None, None, topic, partition, offset),
    (Some(key), None) => Message::new(payload.into(), Some(key.into()), None, topic, partition, offset),
    (None, Some(headers)) => Message::new(
      payload.into(),
      None,
      borrowed_headers_to_hashmap_buffer(headers),
      topic,
      partition,
      offset,
    ),
    (Some(key), Some(headers)) => Message::new(
      payload.into(),
      Some(key.into()),
      borrowed_headers_to_hashmap_buffer(headers),
      topic,
      partition,
      offset,
    ),
  }
}

#[inline]
pub fn create_message_without_metadata(payload: &[u8]) -> Message {
  Message::new(payload.into(), None, None, String::new(), 0, 0)
}

pub fn convert_config_values_to_strings(
  config: HashMap<String, serde_json::Value>,
) -> HashMap<String, String> {
  config
    .into_iter()
    .map(|(k, v)| {
      let value_str = match v {
        serde_json::Value::String(s) => s,
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        _ => v.to_string(),
      };
      (k, value_str)
    })
    .collect()
}

#[cfg(test)]
mod tests {
  use std::collections::HashMap;

  use napi::bindgen_prelude::Buffer;
  use rdkafka::message::{Header, Headers};

  use crate::kafka::kafka_util::hashmap_to_kafka_headers;

  #[test]
  fn headers_test() {
    let hash_map: HashMap<String, Buffer> = HashMap::from([
      ("key_a".to_owned(), "A".as_bytes().into()),
      ("key_b".to_owned(), "B".as_bytes().into()),
    ]);

    let rd_headers = hashmap_to_kafka_headers(&hash_map);

    assert_eq!(
      rd_headers.get(0),
      Header {
        key: "key_a",
        value: Some("A".as_ref())
      }
    );

    assert_eq!(
      rd_headers.get(1),
      Header {
        key: "key_b",
        value: Some("B".as_ref())
      }
    );
  }
}
