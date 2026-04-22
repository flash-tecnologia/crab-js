use std::collections::HashMap;
use std::ffi::CString;
use std::ptr;

use napi::{
  bindgen_prelude::check_status,
  bindgen_prelude::{Buffer, Object},
  bindgen_prelude::{FromNapiValue, ToNapiValue, TypeName, ValidateNapiValue},
  sys, Result, ValueType,
};

pub enum MessageHeaders {
  One(String, Buffer),
  Many(Vec<(String, Buffer)>),
}

impl MessageHeaders {
  #[inline]
  pub fn new(entries: Vec<(String, Buffer)>) -> Self {
    debug_assert!(!entries.is_empty());

    match entries.len() {
      1 => {
        let mut entries = entries;
        let (key, value) = entries.pop().expect("single entry should exist");
        Self::One(key, value)
      }
      _ => Self::Many(entries),
    }
  }

  #[inline]
  pub fn one(key: String, value: Buffer) -> Self {
    Self::One(key, value)
  }

  #[inline]
  pub fn from_entries(entries: Vec<(String, Buffer)>) -> Option<Self> {
    if entries.is_empty() {
      None
    } else {
      Some(Self::new(entries))
    }
  }
}

impl TypeName for MessageHeaders {
  fn type_name() -> &'static str {
    "Record<string, Buffer>"
  }

  fn value_type() -> ValueType {
    ValueType::Object
  }
}

impl ValidateNapiValue for MessageHeaders {}

impl ToNapiValue for MessageHeaders {
  unsafe fn to_napi_value(raw_env: sys::napi_env, val: Self) -> Result<sys::napi_value> {
    let mut obj = ptr::null_mut();
    check_status!(
      unsafe { sys::napi_create_object(raw_env, &mut obj) },
      "Failed to create headers object",
    )?;

    match val {
      MessageHeaders::One(key, value) => {
        let key = CString::new(key)?;
        check_status!(
          unsafe {
            sys::napi_set_named_property(
              raw_env,
              obj,
              key.as_ptr(),
              Buffer::to_napi_value(raw_env, value)?,
            )
          },
          "Failed to set single header property",
        )?;
      }
      MessageHeaders::Many(entries) => {
        let mut object = Object::from_raw(raw_env, obj);
        for (key, value) in entries {
          object.set(key.as_str(), value)?;
        }
        obj = Object::to_napi_value(raw_env, object)?;
      }
    }

    Ok(obj)
  }
}

impl FromNapiValue for MessageHeaders {
  unsafe fn from_napi_value(env: sys::napi_env, napi_val: sys::napi_value) -> Result<Self> {
    let obj = Object::from_napi_value(env, napi_val)?;
    let keys = Object::keys(&obj)?;
    let mut entries = Vec::with_capacity(keys.len());

    for key in keys {
      if let Some(value) = obj.get::<Buffer>(&key)? {
        entries.push((key, value));
      }
    }

    Ok(Self::new(entries))
  }
}

#[napi(object)]
pub struct Message {
  pub payload: Buffer,
  pub key: Option<Buffer>,
  #[napi(ts_type = "Record<string, Buffer>")]
  pub headers: Option<MessageHeaders>,
  pub topic: String,
  pub partition: i32,
  pub offset: i64,
}

impl Message {
  pub fn new(
    payload: Buffer,
    key: Option<Buffer>,
    headers: Option<MessageHeaders>,
    topic: String,
    partition: i32,
    offset: i64,
  ) -> Self {
    Self {
      payload,
      key,
      headers,
      topic,
      partition,
      offset,
    }
  }
}

#[napi(object, object_from_js = false)]
pub struct CompactMessageBatch {
  pub payloads: Vec<Buffer>,
  #[napi(ts_type = "Array<Buffer | undefined>")]
  pub keys: Option<Vec<Option<Buffer>>>,
  pub dense_keys: Option<Vec<Buffer>>,
  pub shared_key: Option<Buffer>,
  pub key_dictionary: Option<Vec<Buffer>>,
  #[napi(ts_type = "Array<number>")]
  pub key_dictionary_indexes: Option<Vec<u8>>,
  pub topic: Option<String>,
  pub topics: Option<Vec<String>>,
  pub partitions: Vec<i32>,
  pub offsets: Vec<i64>,
  pub shared_header_key: Option<String>,
  pub shared_header_value: Option<Buffer>,
  #[napi(ts_type = "Array<Buffer | undefined>")]
  pub shared_header_values: Option<Vec<Option<Buffer>>>,
  pub dense_shared_header_values: Option<Vec<Buffer>>,
  pub header_value_dictionary: Option<Vec<Buffer>>,
  #[napi(ts_type = "Array<number>")]
  pub header_value_dictionary_indexes: Option<Vec<u8>>,
  #[napi(ts_type = "Array<Record<string, Buffer> | undefined>")]
  pub headers: Option<Vec<Option<MessageHeaders>>>,
}

#[napi(object)]
#[derive(Clone)]
pub struct RecordMetadata {
  pub topic: String,
  pub partition: i32,
  pub offset: i64,
  pub error: Option<KafkaCrabError>,
}

#[napi(object)]
pub struct MessageProducer {
  pub payload: Buffer,
  pub key: Option<Buffer>,
  pub headers: Option<HashMap<String, Buffer>>,
}

#[napi(object)]
pub struct ProducerRecord {
  pub topic: String,
  pub messages: Vec<MessageProducer>,
}

#[napi(object)]
#[derive(Clone)]
pub struct KafkaCrabError {
  pub code: i32,
  pub message: String,
}

#[napi(object)]
#[derive(Clone, Debug)]
/*
 * Configuration for the producer
 * default values are set
 * auto_flush: true
 * queue_timeout: 5000
 */
pub struct ProducerConfiguration {
  pub queue_timeout: Option<i64>,
  pub auto_flush: Option<bool>,
  pub configuration: Option<HashMap<String, serde_json::Value>>,
}
