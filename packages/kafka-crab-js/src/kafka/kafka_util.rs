use std::collections::HashMap;

use napi::{bindgen_prelude::Buffer, Error, Status};
use rdkafka::{
  message::{BorrowedHeaders, BorrowedMessage, Header, Headers, OwnedHeaders},
  Message as RdMessage,
};

use super::producer::model::{Message, MessageHeaders};
const SMALL_HEADERS_FAST_PATH_MAX: usize = 2;

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
pub(crate) fn borrowed_headers_to_message_headers(
  headers: &BorrowedHeaders,
) -> Option<MessageHeaders> {
  let header_count = headers.count();
  if header_count == 0 {
    return None;
  }

  if header_count <= SMALL_HEADERS_FAST_PATH_MAX {
    return borrowed_headers_to_message_headers_small(headers, header_count);
  }

  let mut entries: Option<Vec<(String, Buffer)>> = None;
  for index in 0..header_count {
    let header = headers.get(index);
    if let Some(value) = header.value {
      let header_entries = entries.get_or_insert_with(|| Vec::with_capacity(header_count));
      header_entries.push((header.key.to_owned(), value.into()));
    }
  }

  entries.map(MessageHeaders::new)
}

#[inline]
fn borrowed_headers_to_message_headers_small(
  headers: &BorrowedHeaders,
  header_count: usize,
) -> Option<MessageHeaders> {
  debug_assert!(header_count <= SMALL_HEADERS_FAST_PATH_MAX);

  if header_count == 1 {
    let header = headers.get(0);
    return header
      .value
      .map(|value| MessageHeaders::one(header.key.to_owned(), value.into()));
  }

  let first = headers.get(0);
  let second = headers.get(1);

  match (first.value, second.value) {
    (None, None) => None,
    (Some(first_value), None) => Some(MessageHeaders::new(vec![(
      first.key.to_owned(),
      first_value.into(),
    )])),
    (None, Some(second_value)) => Some(MessageHeaders::new(vec![(
      second.key.to_owned(),
      second_value.into(),
    )])),
    (Some(first_value), Some(second_value)) => Some(MessageHeaders::new(vec![
      (first.key.to_owned(), first_value.into()),
      (second.key.to_owned(), second_value.into()),
    ])),
  }
}

#[inline]
pub fn create_message(message: &BorrowedMessage<'_>, payload: &[u8]) -> Message {
  let topic = message.topic().to_owned();
  let partition = message.partition();
  let offset = message.offset();
  match (message.key(), message.headers()) {
    (None, None) => Message::new(payload.into(), None, None, topic, partition, offset),
    (Some(key), None) => Message::new(
      payload.into(),
      Some(key.into()),
      None,
      topic,
      partition,
      offset,
    ),
    (None, Some(headers)) => Message::new(
      payload.into(),
      None,
      borrowed_headers_to_message_headers(headers),
      topic,
      partition,
      offset,
    ),
    (Some(key), Some(headers)) => Message::new(
      payload.into(),
      Some(key.into()),
      borrowed_headers_to_message_headers(headers),
      topic,
      partition,
      offset,
    ),
  }
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
