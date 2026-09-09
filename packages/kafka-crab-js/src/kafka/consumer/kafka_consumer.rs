use futures_util::{stream, StreamExt};
use std::{
  collections::{HashMap, VecDeque},
  mem,
  sync::Arc,
  time::Duration,
};
use tokio::sync::{
  mpsc,
  watch::{self},
};

use napi::{
  bindgen_prelude::{Buffer, Function, ReadableStream},
  threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode},
  Either, Env, Error, JsValue, Result, Status,
};

use rdkafka::{
  consumer::{stream_consumer::StreamConsumer, CommitMode as RdKfafkaCommitMode, Consumer},
  message::{BorrowedHeaders, BorrowedMessage, Headers},
  topic_partition_list::TopicPartitionList as RdTopicPartitionList,
  ClientConfig, Message as RdMessage, Offset,
};

use tracing::{debug, info, warn};

use crate::kafka::{
  consumer::consumer_helper::{
    add_topic_partitions_to_tpl, convert_to_rdkafka_offset, try_create_topic, try_subscribe,
  },
  kafka_client_config::KafkaClientConfig,
  kafka_util::{borrowed_headers_to_message_headers, create_message, IntoNapiError},
  producer::model::{CompactMessageBatch, Message, MessageHeaders},
};

use super::{
  consumer_helper::{convert_tpl_to_array_of_topic_partition, create_stream_consumer},
  context::{KafkaCrabContext, KafkaEvent},
  model::{
    CommitMode, ConsumerConfiguration, OffsetModel, TopicPartition, TopicPartitionConfig,
    DEFAULT_FETCH_METADATA_TIMEOUT,
  },
};

use tokio::select;

pub const DEFAULT_SEEK_TIMEOUT: i64 = 1500;
const MAX_SEEK_TIMEOUT: i64 = 300000; // 5 minutes max
const DEFAULT_BATCH_TIMEOUT: i64 = 1000;
const MAX_BATCH_TIMEOUT_MS: i64 = 60_000;
const MAX_FETCH_METADATA_TIMEOUT_MS: i64 = 300_000; // 5 minutes max
const MAX_BATCH_SIZE: u32 = 16_384;
const SERIAL_STREAM_PREFETCH_SIZE: u32 = 64;
const SERIAL_STREAM_PREFETCH_TIMEOUT_MS: i64 = 1;
const CONSUMER_DISCONNECTED_REASON: &str = "Consumer disconnected";
const BUFFER_DICTIONARY_MAX_UNIQUE_VALUES: usize = 64;
const BUFFER_DICTIONARY_MIN_REPEAT_FACTOR: usize = 4;
const DEFAULT_STREAM_BUFFER_CAPACITY: usize = 4;

type CompactKeyEncoding = (
  Option<Vec<Option<Buffer>>>,
  Option<Buffer>,
  Option<Vec<Buffer>>,
  Option<Vec<u8>>,
);

/// Validates and bounds-checks timeout values
#[inline]
fn validate_timeout(timeout: Option<i64>, default: i64, max: i64, min: i64) -> i64 {
  match timeout {
    Some(t) => {
      if t < min {
        default
      } else if t > max {
        max
      } else {
        t
      }
    }
    None => default,
  }
}

/// Validates timeout for seek operations
#[inline]
fn validate_seek_timeout(timeout: Option<i64>) -> i64 {
  validate_timeout(timeout, DEFAULT_SEEK_TIMEOUT, MAX_SEEK_TIMEOUT, 0)
}

#[inline]
fn normalize_serial_stream_prefetch_size(size: Option<u32>) -> u32 {
  normalize_batch_size(size.unwrap_or(SERIAL_STREAM_PREFETCH_SIZE))
}

#[inline]
fn normalize_serial_stream_prefetch_timeout(timeout_ms: Option<i64>) -> i64 {
  validate_timeout(
    timeout_ms,
    SERIAL_STREAM_PREFETCH_TIMEOUT_MS,
    MAX_BATCH_TIMEOUT_MS,
    1,
  )
}

#[inline]
fn normalize_fetch_metadata_timeout(timeout: Option<i64>) -> Duration {
  let default_ms = DEFAULT_FETCH_METADATA_TIMEOUT.as_millis() as i64;
  let timeout_ms = validate_timeout(timeout, default_ms, MAX_FETCH_METADATA_TIMEOUT_MS, 1);
  Duration::from_millis(timeout_ms as u64)
}

type DisconnectSignal = (watch::Sender<()>, watch::Receiver<()>);

struct BatchCollection {
  messages: Vec<Message>,
  disconnected: bool,
  pending_error: Option<Error>,
}

struct CompactBatchCollection {
  batch: CompactMessageBatch,
  disconnected: bool,
  pending_error: Option<Error>,
}

struct SerialStreamState {
  stream_consumer: Arc<StreamConsumer<KafkaCrabContext>>,
  disconnect_signal: watch::Receiver<()>,
  cancel_signal: watch::Receiver<bool>,
  pending_messages: VecDeque<Message>,
  prefetch_size: u32,
  prefetch_timeout_ms: i64,
  closed: bool,
  pending_error: Option<Error>,
}

enum HeaderProjection<'a> {
  None,
  Single { key: &'a str, value: &'a [u8] },
  Many(MessageHeaders),
}

enum HeaderBatchState {
  None,
  SharedSingle {
    key: String,
    values: Vec<Option<Buffer>>,
  },
  Many(Vec<Option<MessageHeaders>>),
}

struct CompactBatchBuilder {
  payloads: Vec<Buffer>,
  keys: Option<Vec<Option<Buffer>>>,
  topic: Option<String>,
  topics: Option<Vec<String>>,
  partitions: Vec<i32>,
  offsets: Vec<i64>,
  headers: HeaderBatchState,
  tombstones: Option<Vec<bool>>,
  capacity: usize,
}

impl CompactBatchBuilder {
  #[inline]
  fn new(capacity: usize) -> Self {
    Self {
      payloads: Vec::with_capacity(capacity),
      keys: None,
      topic: None,
      topics: None,
      partitions: Vec::with_capacity(capacity),
      offsets: Vec::with_capacity(capacity),
      headers: HeaderBatchState::None,
      tombstones: None,
      capacity,
    }
  }

  #[inline]
  fn push(&mut self, kafka_message: &BorrowedMessage<'_>, payload: Option<&[u8]>) {
    let previous_count = self.payloads.len();
    let is_tombstone = payload.is_none();

    self.payloads.push(Buffer::from(payload.unwrap_or(&[])));
    if is_tombstone {
      if self.tombstones.is_none() {
        let mut tombstones = vec![false; previous_count];
        tombstones.push(true);
        self.tombstones = Some(tombstones);
      } else if let Some(ref mut tombstones) = self.tombstones {
        tombstones.push(true);
      }
    } else if let Some(ref mut tombstones) = self.tombstones {
      tombstones.push(false);
    }

    self.push_key(kafka_message.key(), previous_count);
    self.push_topic(kafka_message.topic(), previous_count);
    self.partitions.push(kafka_message.partition());
    self.offsets.push(kafka_message.offset());
    self.push_headers(kafka_message.headers(), previous_count);
  }

  #[inline]
  fn finish(self) -> CompactMessageBatch {
    let (keys, shared_key, key_dictionary, key_dictionary_indexes) =
      encode_optional_dictionary_buffer(self.keys);
    let (shared_header_key, shared_header_value, shared_header_values, headers) = match self.headers
    {
      HeaderBatchState::None => (None, None, None, None),
      HeaderBatchState::SharedSingle { key, values } => {
        let (values, shared_value) = encode_optional_shared_buffer(Some(values));
        (Some(key), shared_value, values, None)
      }
      HeaderBatchState::Many(headers) => (None, None, None, Some(headers)),
    };

    CompactMessageBatch {
      payloads: self.payloads,
      keys,
      shared_key,
      key_dictionary,
      key_dictionary_indexes,
      topic: self.topic,
      topics: self.topics,
      partitions: self.partitions,
      offsets: self.offsets,
      shared_header_key,
      shared_header_value,
      shared_header_values,
      headers,
      tombstones: self.tombstones,
    }
  }

  #[inline]
  fn push_key(&mut self, key: Option<&[u8]>, previous_count: usize) {
    match (&mut self.keys, key) {
      (Some(keys), Some(key)) => keys.push(Some(key.into())),
      (Some(keys), None) => keys.push(None),
      (None, Some(key)) => {
        let mut keys = Vec::with_capacity(self.capacity);
        keys.resize_with(previous_count, || None);
        keys.push(Some(key.into()));
        self.keys = Some(keys);
      }
      (None, None) => {}
    }
  }

  #[inline]
  fn push_topic(&mut self, topic: &str, previous_count: usize) {
    if let Some(topics) = self.topics.as_mut() {
      topics.push(topic.to_owned());
      return;
    }

    match self.topic.as_ref() {
      Some(shared_topic) if shared_topic != topic => {
        let mut topics = Vec::with_capacity(self.capacity);
        topics.resize(previous_count, shared_topic.clone());
        topics.push(topic.to_owned());
        self.topics = Some(topics);
        self.topic = None;
      }
      Some(_) => {}
      None => {
        self.topic = Some(topic.to_owned());
      }
    }
  }

  #[inline]
  fn push_headers(&mut self, headers_input: Option<&BorrowedHeaders>, previous_count: usize) {
    match &mut self.headers {
      HeaderBatchState::None => match project_headers(headers_input) {
        HeaderProjection::None => {}
        HeaderProjection::Single { key, value } => {
          let mut values = Vec::with_capacity(self.capacity);
          values.resize_with(previous_count, || None);
          values.push(Some(value.into()));
          self.headers = HeaderBatchState::SharedSingle {
            key: key.to_owned(),
            values,
          };
        }
        HeaderProjection::Many(message_headers) => {
          let mut header_batches = Vec::with_capacity(self.capacity);
          header_batches.resize_with(previous_count, || None);
          header_batches.push(Some(message_headers));
          self.headers = HeaderBatchState::Many(header_batches);
        }
      },
      HeaderBatchState::SharedSingle { key, values } => match project_headers(headers_input) {
        HeaderProjection::None => {
          values.push(None);
        }
        HeaderProjection::Single {
          key: header_key,
          value,
        } if key == header_key => {
          values.push(Some(value.into()));
        }
        HeaderProjection::Single {
          key: header_key,
          value,
        } => {
          let mut header_batches = shared_single_to_many(mem::take(values), key);
          header_batches.push(Some(MessageHeaders::one(
            header_key.to_owned(),
            value.into(),
          )));
          self.headers = HeaderBatchState::Many(header_batches);
        }
        HeaderProjection::Many(message_headers) => {
          let mut header_batches = shared_single_to_many(mem::take(values), key);
          header_batches.push(Some(message_headers));
          self.headers = HeaderBatchState::Many(header_batches);
        }
      },
      HeaderBatchState::Many(headers) => match project_headers(headers_input) {
        HeaderProjection::None => headers.push(None),
        HeaderProjection::Single { key, value } => {
          headers.push(Some(MessageHeaders::one(key.to_owned(), value.into())));
        }
        HeaderProjection::Many(message_headers) => {
          headers.push(Some(message_headers));
        }
      },
    }
  }
}

#[inline]
fn project_headers<'a>(headers: Option<&'a BorrowedHeaders>) -> HeaderProjection<'a> {
  let Some(headers) = headers else {
    return HeaderProjection::None;
  };

  if headers.count() == 0 {
    return HeaderProjection::None;
  }

  let mut single_header: Option<(&str, &[u8])> = None;
  for index in 0..headers.count() {
    let header = headers.get(index);
    if let Some(value) = header.value {
      if single_header.is_some() {
        return borrowed_headers_to_message_headers(headers)
          .map(HeaderProjection::Many)
          .unwrap_or(HeaderProjection::None);
      }

      single_header = Some((header.key, value));
    }
  }

  match single_header {
    Some((key, value)) => HeaderProjection::Single { key, value },
    None => HeaderProjection::None,
  }
}

#[inline]
fn shared_single_to_many(
  shared_values: Vec<Option<Buffer>>,
  shared_key: &str,
) -> Vec<Option<MessageHeaders>> {
  let mut headers = Vec::with_capacity(shared_values.len());

  for value in shared_values {
    headers.push(value.map(|value| MessageHeaders::one(shared_key.to_owned(), value)));
  }

  headers
}

#[inline]
fn encode_optional_shared_buffer(
  values: Option<Vec<Option<Buffer>>>,
) -> (Option<Vec<Option<Buffer>>>, Option<Buffer>) {
  let Some(values) = values else {
    return (None, None);
  };

  if values.is_empty() {
    return (None, None);
  }

  let is_shared = match values.first() {
    Some(Some(first_value)) => values.iter().all(|value| {
      value
        .as_ref()
        .is_some_and(|value| value.as_ref() == first_value.as_ref())
    }),
    _ => false,
  };

  if is_shared {
    let shared_value = values
      .into_iter()
      .next()
      .flatten()
      .expect("shared compact buffers must contain at least one value");
    return (None, Some(shared_value));
  }

  (Some(values), None)
}

#[inline]
fn should_dictionary_encode(total: usize, unique: usize) -> bool {
  unique > 1
    && unique <= BUFFER_DICTIONARY_MAX_UNIQUE_VALUES
    && total >= unique * BUFFER_DICTIONARY_MIN_REPEAT_FACTOR
}

#[inline]
fn encode_optional_dictionary_buffer(values: Option<Vec<Option<Buffer>>>) -> CompactKeyEncoding {
  let Some(values) = values else {
    return (None, None, None, None);
  };

  if values.is_empty() {
    return (None, None, None, None);
  }

  if values.iter().any(Option::is_none) {
    return (Some(values), None, None, None);
  }

  let first_value = values[0]
    .as_ref()
    .expect("compact key dictionary buffers must contain at least one value");

  if values.iter().all(|value| {
    value
      .as_ref()
      .is_some_and(|value| value.as_ref() == first_value.as_ref())
  }) {
    let shared_value = values
      .into_iter()
      .next()
      .flatten()
      .expect("shared compact key buffers must contain at least one value");
    return (None, Some(shared_value), None, None);
  }

  let mut dictionary_lookup: HashMap<Vec<u8>, u8> = HashMap::new();
  let mut dictionary_values = Vec::new();
  let mut dictionary_indexes = Vec::with_capacity(values.len());

  for value in values.iter().flatten() {
    if let Some(index) = dictionary_lookup.get(value.as_ref()) {
      dictionary_indexes.push(*index);
      continue;
    }

    if dictionary_values.len() >= BUFFER_DICTIONARY_MAX_UNIQUE_VALUES {
      return (Some(values), None, None, None);
    }

    let index = dictionary_values.len() as u8;
    dictionary_lookup.insert(value.as_ref().to_vec(), index);
    dictionary_values.push(value.as_ref().into());
    dictionary_indexes.push(index);
  }

  if should_dictionary_encode(dictionary_indexes.len(), dictionary_values.len()) {
    return (
      None,
      None,
      Some(dictionary_values),
      Some(dictionary_indexes),
    );
  }

  (Some(values), None, None, None)
}

#[inline]
fn normalize_batch_size(size: u32) -> u32 {
  if size == 0 {
    warn!("size cannot be 0, using 1");
    return 1;
  }

  if size > MAX_BATCH_SIZE {
    warn!(
      "size cannot be greater than {}, clamping to {}",
      MAX_BATCH_SIZE, MAX_BATCH_SIZE
    );
    return MAX_BATCH_SIZE;
  }

  size
}

#[inline]
fn normalize_batch_timeout(timeout_ms: i64) -> i64 {
  if timeout_ms < 1 {
    warn!(
      "timeout_ms must be at least 1ms, using default: {}ms",
      DEFAULT_BATCH_TIMEOUT
    );
    return DEFAULT_BATCH_TIMEOUT;
  }

  if timeout_ms > MAX_BATCH_TIMEOUT_MS {
    warn!(
      "timeout_ms cannot be greater than {}ms, clamping to {}ms",
      MAX_BATCH_TIMEOUT_MS, MAX_BATCH_TIMEOUT_MS
    );
    return MAX_BATCH_TIMEOUT_MS;
  }

  timeout_ms
}

async fn collect_batch_messages(
  stream_consumer: &Arc<StreamConsumer<KafkaCrabContext>>,
  disconnect_signal: &mut watch::Receiver<()>,
  mut cancel_signal: Option<&mut watch::Receiver<bool>>,
  size: u32,
  timeout_ms: i64,
) -> Result<BatchCollection> {
  let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms as u64);
  let mut messages = Vec::with_capacity(size as usize);
  let mut disconnected = false;
  let mut pending_error = None;
  let mut message_stream = stream_consumer.stream();
  let sleep_until_deadline = tokio::time::sleep_until(deadline);
  tokio::pin!(sleep_until_deadline);

  // Phase A: wait for the first message or stop on timeout/disconnect/cancel.
  if let Some(cancel_rx) = cancel_signal.as_deref_mut() {
    select! {
      biased;
      _ = disconnect_signal.changed() => {
        debug!("Disconnect signal received during batch receive");
        disconnected = true;
      }
      _ = cancel_rx.changed() => {
        disconnected = true;
      }
      message = message_stream.next() => {
        match message {
          Some(Ok(kafka_message)) => {
            let payload = kafka_message.payload();
            messages.push(create_message(&kafka_message, payload));
          }
          Some(Err(error)) => {
            return Err(error.into_napi_error("Failed to receive message from consumer"));
          }
          None => {
            disconnected = true;
          }
        }
      }
      _ = &mut sleep_until_deadline => {}
    }
  } else {
    select! {
      biased;
      _ = disconnect_signal.changed() => {
        debug!("Disconnect signal received during batch receive");
        disconnected = true;
      }
      message = message_stream.next() => {
        match message {
          Some(Ok(kafka_message)) => {
            let payload = kafka_message.payload();
            messages.push(create_message(&kafka_message, payload));
          }
          Some(Err(error)) => {
            return Err(error.into_napi_error("Failed to receive message from consumer"));
          }
          None => {
            disconnected = true;
          }
        }
      }
      _ = &mut sleep_until_deadline => {}
    }
  }

  if disconnected || messages.is_empty() {
    return Ok(BatchCollection {
      messages,
      disconnected,
      pending_error,
    });
  }

  // Phase B: fill remaining slots until size/deadline/disconnect/cancel.
  for _ in 1..size {
    if let Some(cancel_rx) = cancel_signal.as_deref_mut() {
      select! {
        biased;
        _ = disconnect_signal.changed() => {
          disconnected = true;
          break;
        }
        _ = cancel_rx.changed() => {
          disconnected = true;
          break;
        }
        message = message_stream.next() => {
          match message {
            Some(Ok(kafka_message)) => {
              let payload = kafka_message.payload();
              messages.push(create_message(&kafka_message, payload));
            }
            Some(Err(error)) => {
              pending_error = Some(error.into_napi_error("Failed to receive message from consumer"));
              break;
            }
            None => {
              disconnected = true;
              break;
            }
          }
        }
        _ = &mut sleep_until_deadline => break,
      }
    } else {
      select! {
        biased;
        _ = disconnect_signal.changed() => {
          disconnected = true;
          break;
        }
        message = message_stream.next() => {
          match message {
            Some(Ok(kafka_message)) => {
              let payload = kafka_message.payload();
              messages.push(create_message(&kafka_message, payload));
            }
            Some(Err(error)) => {
              pending_error = Some(error.into_napi_error("Failed to receive message from consumer"));
              break;
            }
            None => {
              disconnected = true;
              break;
            }
          }
        }
        _ = &mut sleep_until_deadline => break,
      }
    }
  }

  Ok(BatchCollection {
    messages,
    disconnected,
    pending_error,
  })
}

async fn collect_batch_messages_compact(
  stream_consumer: &Arc<StreamConsumer<KafkaCrabContext>>,
  disconnect_signal: &mut watch::Receiver<()>,
  mut cancel_signal: Option<&mut watch::Receiver<bool>>,
  size: u32,
  timeout_ms: i64,
) -> Result<CompactBatchCollection> {
  let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms as u64);
  let mut batch = CompactBatchBuilder::new(size as usize);
  let mut disconnected = false;
  let mut pending_error = None;
  let mut message_stream = stream_consumer.stream();
  let sleep_until_deadline = tokio::time::sleep_until(deadline);
  tokio::pin!(sleep_until_deadline);

  // Phase A: wait for the first message or stop on timeout/disconnect/cancel.
  if let Some(cancel_rx) = cancel_signal.as_deref_mut() {
    select! {
      biased;
      _ = disconnect_signal.changed() => {
        debug!("Disconnect signal received during compact batch receive");
        disconnected = true;
      }
      _ = cancel_rx.changed() => {
        disconnected = true;
      }
      message = message_stream.next() => {
        match message {
          Some(Ok(kafka_message)) => {
            let payload = kafka_message.payload();
            batch.push(&kafka_message, payload);
          }
          Some(Err(error)) => {
            return Err(error.into_napi_error("Failed to receive message from consumer"));
          }
          None => {
            disconnected = true;
          }
        }
      }
      _ = &mut sleep_until_deadline => {}
    }
  } else {
    select! {
      biased;
      _ = disconnect_signal.changed() => {
        debug!("Disconnect signal received during compact batch receive");
        disconnected = true;
      }
      message = message_stream.next() => {
        match message {
          Some(Ok(kafka_message)) => {
            let payload = kafka_message.payload();
            batch.push(&kafka_message, payload);
          }
          Some(Err(error)) => {
            return Err(error.into_napi_error("Failed to receive message from consumer"));
          }
          None => {
            disconnected = true;
          }
        }
      }
      _ = &mut sleep_until_deadline => {}
    }
  }

  if disconnected || batch.payloads.is_empty() {
    return Ok(CompactBatchCollection {
      batch: batch.finish(),
      disconnected,
      pending_error,
    });
  }

  // Phase B: fill remaining slots until size/deadline/disconnect/cancel.
  for _ in 1..size {
    if let Some(cancel_rx) = cancel_signal.as_deref_mut() {
      select! {
        biased;
        _ = disconnect_signal.changed() => {
          disconnected = true;
          break;
        }
        _ = cancel_rx.changed() => {
          disconnected = true;
          break;
        }
        message = message_stream.next() => {
          match message {
            Some(Ok(kafka_message)) => {
              let payload = kafka_message.payload();
              batch.push(&kafka_message, payload);
            }
            Some(Err(error)) => {
              pending_error = Some(error.into_napi_error("Failed to receive message from consumer"));
              break;
            }
            None => {
              disconnected = true;
              break;
            }
          }
        }
        _ = &mut sleep_until_deadline => break,
      }
    } else {
      select! {
        biased;
        _ = disconnect_signal.changed() => {
          disconnected = true;
          break;
        }
        message = message_stream.next() => {
          match message {
            Some(Ok(kafka_message)) => {
              let payload = kafka_message.payload();
              batch.push(&kafka_message, payload);
            }
            Some(Err(error)) => {
              pending_error = Some(error.into_napi_error("Failed to receive message from consumer"));
              break;
            }
            None => {
              disconnected = true;
              break;
            }
          }
        }
        _ = &mut sleep_until_deadline => break,
      }
    }
  }

  Ok(CompactBatchCollection {
    batch: batch.finish(),
    disconnected,
    pending_error,
  })
}

async fn next_serial_stream_item(
  mut state: SerialStreamState,
) -> Option<(Result<Message>, SerialStreamState)> {
  loop {
    if let Some(message) = state.pending_messages.pop_front() {
      return Some((Ok(message), state));
    }

    if let Some(err) = state.pending_error.take() {
      state.closed = true;
      return Some((Err(err), state));
    }

    if state.closed
      || state.disconnect_signal.has_changed().unwrap_or(false)
      || *state.cancel_signal.borrow()
      || state.cancel_signal.has_changed().unwrap_or(false)
    {
      state.closed = true;
      return None;
    }

    match collect_batch_messages(
      &state.stream_consumer,
      &mut state.disconnect_signal,
      Some(&mut state.cancel_signal),
      state.prefetch_size,
      state.prefetch_timeout_ms,
    )
    .await
    {
      Ok(batch) => {
        // Cancel is an interrupt: an in-flight collection is abandoned and its
        // partial batch dropped. Items buffered by earlier collects were pulled
        // before cancellation and are still emitted (see loop top).
        if *state.cancel_signal.borrow() {
          state.closed = true;
          return None;
        }

        if batch.messages.is_empty() {
          if let Some(err) = batch.pending_error {
            state.closed = true;
            return Some((Err(err), state));
          }
          if batch.disconnected {
            state.closed = true;
            return None;
          }

          continue;
        }

        state.pending_messages = VecDeque::from(batch.messages);
        state.pending_error = batch.pending_error;
        if batch.disconnected {
          state.closed = true;
        }
      }
      Err(error) => {
        state.closed = true;
        return Some((Err(error), state));
      }
    }
  }
}

#[napi]
pub struct KafkaConsumer {
  client_config: ClientConfig,
  consumer_config: ConsumerConfiguration,
  stream_consumer: Arc<StreamConsumer<KafkaCrabContext>>,
  fetch_metadata_timeout: Duration,
  disconnect_signal: DisconnectSignal,
  client_id: String,
  pending_error: Arc<std::sync::Mutex<Option<Error>>>,
}

#[napi]
impl KafkaConsumer {
  pub fn new(
    kafka_client: &KafkaClientConfig,
    consumer_configuration: &ConsumerConfiguration,
  ) -> Result<Self> {
    let client_config: &ClientConfig = kafka_client.get_client_config();

    let ConsumerConfiguration {
      configuration,
      fetch_metadata_timeout,
      ..
    } = consumer_configuration;

    if let Some(t) = fetch_metadata_timeout {
      if *t < 0 {
        return Err(Error::new(
          Status::InvalidArg,
          format!("Invalid fetchMetadataTimeout: {t}. Timeout must be non-negative."),
        ));
      }
    }

    let stream_consumer =
      create_stream_consumer(client_config, consumer_configuration, configuration.clone())
        .map_err(|e| e.into_napi_error("Failed to create stream consumer"))?;

    Ok(KafkaConsumer {
      client_config: client_config.clone(),
      consumer_config: consumer_configuration.clone(),
      stream_consumer: Arc::new(stream_consumer),
      fetch_metadata_timeout: normalize_fetch_metadata_timeout(*fetch_metadata_timeout),
      disconnect_signal: watch::channel(()),
      client_id: kafka_client.configuration().client_id,
      pending_error: Arc::new(std::sync::Mutex::new(None)),
    })
  }

  /// Returns the current consumer configuration.
  #[napi]
  pub fn get_config(&self) -> Result<ConsumerConfiguration> {
    Ok(self.consumer_config.clone())
  }

  /// Returns the client ID associated with this consumer
  #[napi(getter)]
  pub fn client_id(&self) -> String {
    self.client_id.clone()
  }

  /// Returns the list of topics and partitions currently subscribed to.
  #[napi]
  pub fn get_subscription(&self) -> Result<Vec<TopicPartition>> {
    match self.stream_consumer.subscription() {
      Ok(v) => Ok(convert_tpl_to_array_of_topic_partition(&v)),
      Err(e) => Err(e.into_napi_error("Failed to get subscription")),
    }
  }

  /// Registers a callback to receive Kafka consumer events (rebalance, errors, etc.).
  /// The callback will be invoked for each event until the consumer is disconnected.
  /// @param callback - Function called with each event
  #[napi(
    async_runtime,
    ts_args_type = "callback: (error: Error | undefined, event: KafkaEvent) => void"
  )]
  pub fn on_events(&self, callback: Arc<ThreadsafeFunction<KafkaEvent>>) -> Result<()> {
    let mut rx = self.stream_consumer.context().event_channel.1.resubscribe();
    let mut disconnect_signal = self.disconnect_signal.1.clone();

    tokio::spawn(async move {
      loop {
        select! {
            event = rx.recv() => {
                match event {
                    Ok(event) => {
                        callback.call(Ok(event), ThreadsafeFunctionCallMode::NonBlocking);
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        debug!("Event channel closed");
                        break;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!("Lagged on event channel; skipped {} events", skipped);
                        continue;
                    }
                }
            }
            _ = disconnect_signal.changed() => {
                debug!("Subscription to consumer events is stopped");
                break;
            }
        }
      }
    });
    Ok(())
  }

  /// Subscribes to one or more Kafka topics.
  /// Can accept either a single topic name string or an array of topic configurations
  /// with advanced options like partition offsets and topic creation settings.
  /// Topics are either all group-subscribed or all manually assigned: mixing both
  /// in one call is rejected. When topic creation fails, the remaining topics are
  /// still created and subscribed; the error is returned afterwards, with the
  /// consumer left subscribed.
  #[napi]
  pub async fn subscribe(
    &self,
    topic_configs: Either<String, Vec<TopicPartitionConfig>>,
  ) -> Result<()> {
    let topics = match topic_configs {
      Either::A(config) => {
        debug!("Subscribing to topic: {:#?}", &config);
        vec![TopicPartitionConfig {
          topic: config,
          all_offsets: None,
          partition_offset: None,
          create_topic: None,
          num_partitions: None,
          replicas: None,
        }]
      }
      Either::B(config) => {
        debug!("Subscribing to topic config: {:#?}", &config);
        config
      }
    };

    if topics.is_empty() {
      return Err(Error::new(
        Status::InvalidArg,
        "subscribe requires at least one topic".to_string(),
      ));
    }

    // Manual assignment and group subscription are mutually exclusive in
    // librdkafka: mixing them silently converts everything to a static
    // assignment, losing rebalance for the subscribed topics.
    let manual_count = topics
      .iter()
      .filter(|t| t.all_offsets.is_some() || t.partition_offset.is_some())
      .count();
    if manual_count > 0 && manual_count < topics.len() {
      return Err(Error::new(
        Status::InvalidArg,
        "subscribe does not mix manual partition assignment with group subscription; use all-manual (all_offsets/partition_offset) or all-subscribe"
          .to_string(),
      ));
    }

    // Attempt creation for every topic so one bad entry neither skips the rest
    // nor prevents subscription; failures are reported after subscribing.
    let mut create_errors: Vec<String> = Vec::new();
    for topic_config in &topics {
      let create_topic = topic_config.create_topic.unwrap_or(false);
      if create_topic {
        debug!("Creating topic if not exists: {:?}", &topic_config.topic);
        if let Err(e) = try_create_topic(
          &vec![topic_config.topic.clone()],
          &self.client_config,
          self.fetch_metadata_timeout,
          topic_config.num_partitions,
          topic_config.replicas,
        )
        .await
        {
          warn!("Failed to create topic {:?}: {:?}", &topic_config.topic, e);
          create_errors.push(format!("{}: {e:?}", topic_config.topic));
        }
      } else {
        debug!(
          "Topic creation disabled for topic: {:?}",
          &topic_config.topic
        );
      }
    }

    let has_manual_assignment = topics
      .iter()
      .any(|t| t.all_offsets.is_some() || t.partition_offset.is_some());

    if has_manual_assignment {
      let mut tpl = RdTopicPartitionList::new();
      for item in &topics {
        add_topic_partitions_to_tpl(
          &mut tpl,
          &item.topic,
          item.partition_offset.as_ref(),
          item.all_offsets.as_ref(),
          &self.stream_consumer,
          self.fetch_metadata_timeout,
        )
        .map_err(|e| e.into_napi_error("Failed to assign topic partitions"))?;
      }

      if tpl.count() == 0 {
        return Err(Error::new(
          Status::InvalidArg,
          "subscribe resolved no partitions; check topic names and offsets".to_string(),
        ));
      }

      self
        .stream_consumer
        .assign(&tpl)
        .map_err(|e| anyhow::anyhow!("Failed to assign topic partitions: {:?}", e))
        .map_err(|e| e.into_napi_error("Failed to assign partitions"))?;
      debug!(
        "Assigned topic partition list with {} elements",
        tpl.count()
      );
    } else {
      let topics_name = topics
        .iter()
        .map(|x| x.topic.clone())
        .collect::<Vec<String>>();

      try_subscribe(&self.stream_consumer, &topics_name)
        .map_err(|e| e.into_napi_error("Failed to subscribe to topics"))?;
    }

    if !create_errors.is_empty() {
      return Err(Error::new(
        Status::GenericFailure,
        format!(
          "Failed to create topic(s) before subscription: {}",
          create_errors.join("; ")
        ),
      ));
    }

    Ok(())
  }

  fn get_partitions(&self) -> Result<RdTopicPartitionList> {
    let partitions = self
      .stream_consumer
      .assignment()
      .map_err(|e| e.into_napi_error("Failed to get partition assignment"))?;
    Ok(partitions)
  }

  /// Pauses message consumption on all assigned partitions.
  /// Messages will be buffered by the broker until resume() is called.
  #[napi]
  pub fn pause(&self) -> Result<()> {
    self
      .stream_consumer
      .pause(&self.get_partitions()?)
      .map_err(|e| e.into_napi_error("Failed to pause consumer"))?;
    Ok(())
  }

  /// Resumes message consumption on all assigned partitions after a pause.
  #[napi]
  pub fn resume(&self) -> Result<()> {
    self
      .stream_consumer
      .resume(&self.get_partitions()?)
      .map_err(|e| e.into_napi_error("Failed to resume consumer"))?;
    Ok(())
  }

  /// Unsubscribes from all currently subscribed topics.
  /// After calling this method, the consumer will no longer receive messages.
  #[napi]
  pub fn unsubscribe(&self) -> Result<()> {
    info!("Unsubscribing from topics");
    self.stream_consumer.unsubscribe();
    Ok(())
  }

  /// Disconnects the consumer from the Kafka broker.
  /// This will unsubscribe from all topics and stop receiving messages.
  /// Any pending recv() or recvBatch() calls will return immediately.
  #[napi]
  pub async fn disconnect(&self) -> Result<()> {
    info!("Disconnecting consumer - This will stop the consumer from receiving messages");

    // First unsubscribe from topics
    self.stream_consumer.unsubscribe();

    // Then send disconnect signal - use non-blocking approach
    // Note: watch channels have a single slot, so send() replaces the current value
    // If there are no receivers, the send will succeed but the value will be ignored
    let tx = self.disconnect_signal.0.clone();
    if tx.send(()).is_err() {
      // If send fails, it usually means no receivers are listening
      // This is not necessarily an error during shutdown
      warn!("Disconnect signal could not be sent - no active receivers");
    }

    Ok(())
  }

  /// Seeks to a specific offset on a topic partition.
  /// This allows repositioning the consumer to read from a specific point.
  /// @param topic - The topic name
  /// @param partition - The partition number
  /// @param offsetModel - The offset to seek to (Beginning, End, Offset, or Stored)
  /// @param timeout - Optional timeout in milliseconds (default: 1500ms, max: 300000ms)
  #[napi]
  pub fn seek(
    &self,
    topic: String,
    partition: i32,
    offset_model: OffsetModel,
    timeout: Option<i64>,
  ) -> Result<()> {
    let offset = convert_to_rdkafka_offset(&offset_model);
    debug!(
      "Seeking to topic: {}, partition: {}, offset: {:?}",
      topic, partition, offset
    );
    self
      .stream_consumer
      .seek(
        &topic,
        partition,
        offset,
        Duration::from_millis(validate_seek_timeout(timeout) as u64),
      )
      .map_err(|e| e.into_napi_error("Failed to seek to offset"))?;
    Ok(())
  }

  /// Returns the current partition assignment for this consumer.
  /// This includes all topic partitions that have been assigned to this consumer
  /// as part of the consumer group rebalancing.
  #[napi]
  pub fn assignment(&self) -> Result<Vec<TopicPartition>> {
    let assignment = self
      .stream_consumer
      .assignment()
      .map_err(|e| e.into_napi_error("Failed to get partition assignment"))?;
    Ok(convert_tpl_to_array_of_topic_partition(&assignment))
  }

  /// Receives a single message from the subscribed topics.
  /// This method will block until a message is available or the consumer is disconnected.
  /// @returns The received message, or null if the consumer was disconnected
  #[napi]
  pub async fn recv(&self) -> Result<Option<Message>> {
    if let Some(err) = self
      .pending_error
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .take()
    {
      return Err(err);
    }
    let mut rx = self.disconnect_signal.1.clone();
    select! {
        message = self.stream_consumer.recv() => {
            message
                .map_err(|e| e.into_napi_error("Failed to receive message from consumer"))
                .map(|message| Some(create_message(&message, message.payload())))
        }
        _ = rx.changed() => {
            debug!("Disconnect signal received and this will stop the consumer from receiving messages");
            Ok(None)
        }
    }
  }

  /// Receives multiple messages in a single call for higher throughput
  ///
  /// This method provides 2-5x better performance than calling recv() multiple times
  /// by batching message retrieval and reducing function call overhead.
  ///
  /// @param size Maximum number of messages to retrieve (1-configured max, default 1000)
  /// @param timeout_ms Timeout in milliseconds
  /// @returns Array of messages (may be fewer than size)
  #[napi]
  pub async fn recv_batch(&self, size: u32, timeout_ms: i64) -> Result<Vec<Message>> {
    if let Some(err) = self
      .pending_error
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .take()
    {
      return Err(err);
    }

    let normalized_size = normalize_batch_size(size);
    let normalized_timeout_ms = normalize_batch_timeout(timeout_ms);

    let mut disconnect_signal = self.disconnect_signal.1.clone();
    let collection = collect_batch_messages(
      &self.stream_consumer,
      &mut disconnect_signal,
      None,
      normalized_size,
      normalized_timeout_ms,
    )
    .await?;

    if let Some(err) = collection.pending_error {
      *self.pending_error.lock().unwrap_or_else(|e| e.into_inner()) = Some(err);
    }

    if collection.disconnected && collection.messages.is_empty() {
      return Err(Error::new(
        Status::GenericFailure,
        CONSUMER_DISCONNECTED_REASON,
      ));
    }

    Ok(collection.messages)
  }

  /// Receives messages as a native Web `ReadableStream`.
  /// Uses a small internal batch prefetch to reduce native boundary crossings.
  #[napi]
  pub fn recv_stream<'env>(
    &self,
    env: Env,
    prefetch_size: Option<u32>,
    prefetch_timeout_ms: Option<i64>,
  ) -> Result<ReadableStream<'env, Message>> {
    let normalized_prefetch_size = normalize_serial_stream_prefetch_size(prefetch_size);
    let normalized_prefetch_timeout_ms =
      normalize_serial_stream_prefetch_timeout(prefetch_timeout_ms);
    let (cancel_sender, cancel_receiver) = watch::channel(false);
    let stream_state = SerialStreamState {
      stream_consumer: self.stream_consumer.clone(),
      disconnect_signal: self.disconnect_signal.1.clone(),
      cancel_signal: cancel_receiver,
      pending_messages: VecDeque::with_capacity(normalized_prefetch_size as usize),
      prefetch_size: normalized_prefetch_size,
      prefetch_timeout_ms: normalized_prefetch_timeout_ms,
      closed: false,
      pending_error: None,
    };

    let inner = stream::unfold(stream_state, next_serial_stream_item).boxed();
    let stream = ReadableStream::new(&env, inner)?;
    Self::wrap_stream_with_cancel(&env, stream, cancel_sender)
  }

  fn wrap_stream_with_cancel<'env, T: napi::bindgen_prelude::ToNapiValue + 'static>(
    env: &Env,
    stream: ReadableStream<'env, T>,
    cancel_sender: watch::Sender<bool>,
  ) -> Result<ReadableStream<'env, T>> {
    let wrap_fn: Function = env.run_script(
      r#"
      (function(stream, onCancel) {
        const reader = stream.getReader();
        return new ReadableStream({
          async pull(controller) {
            try {
              const { done, value } = await reader.read();
              if (done) {
                controller.close();
              } else {
                controller.enqueue(value);
              }
            } catch (err) {
              controller.error(err);
            }
          },
          async cancel(reason) {
            try {
              onCancel();
            } finally {
              await reader.cancel(reason);
            }
          }
        });
      })
      "#,
    )?;

    let cancel_sender = Arc::new(cancel_sender);
    let on_cancel: Function<'_, (), ()> =
      env.create_function_from_closure("onCancel", move |_| {
        let _ = cancel_sender.send(true);
        Ok(())
      })?;

    let mut raw_stream = std::ptr::null_mut();
    let args = [stream.raw(), on_cancel.raw()];
    let mut raw_undefined = std::ptr::null_mut();
    unsafe {
      napi::sys::napi_get_undefined(env.raw(), &mut raw_undefined);
      napi::check_status!(
        napi::sys::napi_call_function(
          env.raw(),
          raw_undefined,
          wrap_fn.raw(),
          2,
          args.as_ptr(),
          &mut raw_stream,
        ),
        "Failed to call wrap_stream function"
      )?;
      napi::bindgen_prelude::FromNapiValue::from_napi_value(env.raw(), raw_stream)
    }
  }

  fn recv_batch_stream_internal<'env>(
    &self,
    env: Env,
    size: u32,
    timeout_ms: i64,
  ) -> Result<ReadableStream<'env, Vec<Message>>> {
    let stream_consumer = self.stream_consumer.clone();
    let mut disconnect_signal = self.disconnect_signal.1.clone();
    let normalized_size = normalize_batch_size(size);
    let normalized_timeout_ms = normalize_batch_timeout(timeout_ms);
    let (sender, receiver) = mpsc::channel::<Result<Vec<Message>>>(DEFAULT_STREAM_BUFFER_CAPACITY);
    let (cancel_sender, mut cancel_receiver) = watch::channel(false);
    let mut task_cancel_rx = cancel_receiver.clone();

    napi::bindgen_prelude::spawn(async move {
      loop {
        if sender.is_closed()
          || disconnect_signal.has_changed().unwrap_or(false)
          || *cancel_receiver.borrow()
        {
          break;
        }

        let mut batch_disconnect_rx = disconnect_signal.clone();
        tokio::select! {
          biased;
          _ = sender.closed() => {
            break;
          }
          _ = cancel_receiver.changed() => {
            break;
          }
          batch_res = collect_batch_messages(
            &stream_consumer,
            &mut batch_disconnect_rx,
            Some(&mut task_cancel_rx),
            normalized_size,
            normalized_timeout_ms,
          ) => {
            // No early break on cancel here: a batch collected racing with
            // cancellation still gets the bounded delivery attempt below.
            match batch_res {
              Ok(batch) => {
                let disconnected = batch.disconnected;
                let pending_err = batch.pending_error;
                let messages = batch.messages;

                if messages.is_empty() {
                  if let Some(err) = pending_err {
                    tokio::select! {
                      biased;
                      _ = sender.closed() => {}
                      _ = disconnect_signal.changed() => {}
                      _ = cancel_receiver.changed() => {}
                      _ = sender.send(Err(err)) => {}
                    }
                    break;
                  }
                  if disconnected || *cancel_receiver.borrow() {
                    break;
                  }
                  continue;
                }

                if *cancel_receiver.borrow() {
                  break;
                }

                let mut send_failed = false;
                let pending_messages = messages.len();
                let send_fut = sender.send(Ok(messages));
                tokio::pin!(send_fut);
                let mut is_disconnected = disconnect_signal.has_changed().unwrap_or(false);
                let mut grace_timer = std::pin::pin!(tokio::time::sleep(Duration::from_millis(1500)));
                let mut grace_active = is_disconnected;

                loop {
                  if *cancel_receiver.borrow() {
                    send_failed = true;
                    break;
                  }

                  tokio::select! {
                    biased;
                    _ = sender.closed() => {
                      send_failed = true;
                      break;
                    }
                    _ = cancel_receiver.changed() => {
                      send_failed = true;
                      break;
                    }
                    _ = disconnect_signal.changed(), if !is_disconnected => {
                      is_disconnected = true;
                      grace_timer.as_mut().reset(tokio::time::Instant::now() + Duration::from_millis(1500));
                      grace_active = true;
                    }
                    _ = &mut grace_timer, if grace_active => {
                      warn!(
                        dropped = pending_messages as u64,
                        "Disconnect drainage grace period expired; dropping collected batch"
                      );
                      send_failed = true;
                      break;
                    }
                    send_res = &mut send_fut => {
                      if send_res.is_err() {
                        send_failed = true;
                      }
                      break;
                    }
                  }
                }

                if send_failed || is_disconnected {
                  break;
                }

                if let Some(err) = pending_err {
                  tokio::select! {
                    biased;
                    _ = sender.closed() => {}
                    _ = disconnect_signal.changed() => {}
                    _ = cancel_receiver.changed() => {}
                    _ = sender.send(Err(err)) => {}
                  }
                  break;
                }
              }
              Err(error) => {
                tokio::select! {
                  biased;
                  _ = sender.closed() => {}
                  _ = disconnect_signal.changed() => {}
                  _ = cancel_receiver.changed() => {}
                  _ = sender.send(Err(error)) => {}
                }
                break;
              }
            }
          }
        }
      }
    });

    let inner = stream::unfold(receiver, |mut receiver| async move {
      receiver.recv().await.map(|item| (item, receiver))
    })
    .boxed();
    let native_stream = ReadableStream::new(&env, inner)?;
    Self::wrap_stream_with_cancel(&env, native_stream, cancel_sender)
  }

  fn recv_batch_stream_compact_internal<'env>(
    &self,
    env: Env,
    size: u32,
    timeout_ms: i64,
  ) -> Result<ReadableStream<'env, CompactMessageBatch>> {
    let stream_consumer = self.stream_consumer.clone();
    let mut disconnect_signal = self.disconnect_signal.1.clone();
    let normalized_size = normalize_batch_size(size);
    let normalized_timeout_ms = normalize_batch_timeout(timeout_ms);
    let (sender, receiver) =
      mpsc::channel::<Result<CompactMessageBatch>>(DEFAULT_STREAM_BUFFER_CAPACITY);
    let (cancel_sender, mut cancel_receiver) = watch::channel(false);
    let mut task_cancel_rx = cancel_receiver.clone();

    napi::bindgen_prelude::spawn(async move {
      loop {
        if sender.is_closed()
          || disconnect_signal.has_changed().unwrap_or(false)
          || *cancel_receiver.borrow()
        {
          break;
        }

        let mut batch_disconnect_rx = disconnect_signal.clone();
        tokio::select! {
          biased;
          _ = sender.closed() => {
            break;
          }
          _ = cancel_receiver.changed() => {
            break;
          }
          batch_res = collect_batch_messages_compact(
            &stream_consumer,
            &mut batch_disconnect_rx,
            Some(&mut task_cancel_rx),
            normalized_size,
            normalized_timeout_ms,
          ) => {
            // Same as above: deliver-then-close, never collect-then-drop.
            match batch_res {
              Ok(batch) => {
                let disconnected = batch.disconnected;
                let pending_err = batch.pending_error;
                let batch_data = batch.batch;

                if batch_data.payloads.is_empty() {
                  if let Some(err) = pending_err {
                    tokio::select! {
                      biased;
                      _ = sender.closed() => {}
                      _ = disconnect_signal.changed() => {}
                      _ = cancel_receiver.changed() => {}
                      _ = sender.send(Err(err)) => {}
                    }
                    break;
                  }
                  if disconnected || *cancel_receiver.borrow() {
                    break;
                  }
                  continue;
                }

                if *cancel_receiver.borrow() {
                  break;
                }

                let mut send_failed = false;
                let pending_messages = batch_data.payloads.len();
                let send_fut = sender.send(Ok(batch_data));
                tokio::pin!(send_fut);
                let mut is_disconnected = disconnect_signal.has_changed().unwrap_or(false);
                let mut grace_timer = std::pin::pin!(tokio::time::sleep(Duration::from_millis(1500)));
                let mut grace_active = is_disconnected;

                loop {
                  if *cancel_receiver.borrow() {
                    send_failed = true;
                    break;
                  }

                  tokio::select! {
                    biased;
                    _ = sender.closed() => {
                      send_failed = true;
                      break;
                    }
                    _ = cancel_receiver.changed() => {
                      send_failed = true;
                      break;
                    }
                    _ = disconnect_signal.changed(), if !is_disconnected => {
                      is_disconnected = true;
                      grace_timer.as_mut().reset(tokio::time::Instant::now() + Duration::from_millis(1500));
                      grace_active = true;
                    }
                    _ = &mut grace_timer, if grace_active => {
                      warn!(
                        dropped = pending_messages as u64,
                        "Disconnect drainage grace period expired; dropping collected batch"
                      );
                      send_failed = true;
                      break;
                    }
                    send_res = &mut send_fut => {
                      if send_res.is_err() {
                        send_failed = true;
                      }
                      break;
                    }
                  }
                }

                if send_failed || is_disconnected {
                  break;
                }

                if let Some(err) = pending_err {
                  tokio::select! {
                    biased;
                    _ = sender.closed() => {}
                    _ = disconnect_signal.changed() => {}
                    _ = cancel_receiver.changed() => {}
                    _ = sender.send(Err(err)) => {}
                  }
                  break;
                }
              }
              Err(error) => {
                tokio::select! {
                  biased;
                  _ = sender.closed() => {}
                  _ = disconnect_signal.changed() => {}
                  _ = cancel_receiver.changed() => {}
                  _ = sender.send(Err(error)) => {}
                }
                break;
              }
            }
          }
        }
      }
    });

    let inner = stream::unfold(receiver, |mut receiver| async move {
      receiver.recv().await.map(|item| (item, receiver))
    })
    .boxed();
    let native_stream = ReadableStream::new(&env, inner)?;
    Self::wrap_stream_with_cancel(&env, native_stream, cancel_sender)
  }

  /// Receives batches of messages as a native Web `ReadableStream`.
  #[napi]
  pub fn recv_batch_stream<'env>(
    &self,
    env: Env,
    size: u32,
    timeout_ms: i64,
  ) -> Result<ReadableStream<'env, Vec<Message>>> {
    self.recv_batch_stream_internal(env, size, timeout_ms)
  }

  /// Receives metadata batches as a compact native Web `ReadableStream`.
  /// Intended for JS-side expansion to preserve the public `Message[]` API with less native marshalling overhead.
  #[napi]
  pub fn recv_batch_stream_compact<'env>(
    &self,
    env: Env,
    size: u32,
    timeout_ms: i64,
  ) -> Result<ReadableStream<'env, CompactMessageBatch>> {
    self.recv_batch_stream_compact_internal(env, size, timeout_ms)
  }

  /// Commits an offset for a specific topic partition.
  /// This marks the offset as processed, so the consumer will not receive
  /// messages before this offset after a restart.
  /// @param topic - The topic name
  /// @param partition - The partition number
  /// @param offset - The offset to commit
  /// @param commit - The commit mode (Sync or Async)
  #[napi]
  pub async fn commit(
    &self,
    topic: String,
    partition: i32,
    offset: i64,
    commit: CommitMode,
  ) -> Result<()> {
    let consumer = self.stream_consumer.clone();

    tokio::task::spawn_blocking(move || {
      let mut tpl = RdTopicPartitionList::new();
      tpl
        .add_partition_offset(&topic, partition, Offset::Offset(offset))
        .map_err(|e| e.into_napi_error("Failed to add partition offset"))?;
      let commit_mode = match commit {
        CommitMode::Sync => RdKfafkaCommitMode::Sync,
        CommitMode::Async => RdKfafkaCommitMode::Async,
      };

      debug!(
        "Committing offset for topic: {}, partition: {}, offset: {} mode {:?}",
        &topic, partition, offset, commit_mode
      );

      let result = consumer
        .commit(&tpl, commit_mode)
        .map_err(|e| e.into_napi_error("Failed to commit offset"));

      debug!("Committing done. Tpl: {:?}", &tpl);

      result
    })
    .await
    .map_err(|e| e.into_napi_error("Failed to join commit task"))??;

    Ok(())
  }

  /// Commits the offset for a message.
  /// This is a convenience method that automatically increments the offset by 1.
  /// The offset committed is `message.offset + 1` since Kafka expects the next offset to be consumed.
  /// @param message - The message to commit
  /// @param commit - The commit mode (Sync or Async)
  #[napi]
  pub async fn commit_message(&self, message: Message, commit: CommitMode) -> Result<()> {
    self
      .commit(
        message.topic.clone(),
        message.partition,
        message.offset + 1,
        commit,
      )
      .await
  }
}
