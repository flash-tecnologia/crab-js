use std::collections::HashMap;

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

pub fn kafka_headers_to_hashmap_buffer(
  headers: Option<&BorrowedHeaders>,
) -> HashMap<String, Buffer> {
  match headers {
    Some(value) => value
      .iter()
      .filter_map(|it| it.value.map(|v| (it.key.to_owned(), v.into())))
      .collect::<HashMap<String, Buffer>>(),
    _ => HashMap::new(),
  }
}

pub fn create_message(message: &BorrowedMessage<'_>, payload: &[u8]) -> Message {
  let key: Option<Buffer> = message.key().map(|bytes| bytes.into());
  let headers = Some(kafka_headers_to_hashmap_buffer(message.headers()));
  let payload_js = Message::new(
    payload.into(),
    key,
    headers,
    message.topic().to_owned(),
    message.partition(),
    message.offset(),
  );
  payload_js
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
