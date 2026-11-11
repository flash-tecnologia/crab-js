use futures_util::{stream, FutureExt, StreamExt};
use std::{
  collections::VecDeque,
  mem,
  sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, OnceLock,
  },
  time::Duration,
};
use tokio::sync::{
  mpsc,
  watch::{self},
  Mutex,
};

use napi::{
  bindgen_prelude::{Buffer, FnArgs, Function, JsObjectValue, Object, PromiseRaw, ReadableStream},
  threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode},
  Either, Env, Error, JsValue, Result, Status,
};

use rdkafka::{
  bindings as rdsys,
  consumer::{CommitMode as RdKfafkaCommitMode, Consumer},
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
  byte_budget::{ByteBudget, DEFAULT_STREAM_BUFFER_BYTES},
  commit_queue::AsyncCommitQueue,
  consumer_helper::{
    build_consumer_config, convert_tpl_to_array_of_topic_partition, create_stream_consumer,
    managed_config_conflict,
  },
  context::{KafkaEvent, LoggingConsumer, ManagedConsumerState},
  managed::{
    KafkaCode, ManagedEntry, ManagedError, ManagedLimits, ManagedState, ManagedStatus,
    PartitionChange, PartitionKey, ReceiveFailure, Received, DEFAULT_MAX_BUFFERED_BYTES,
    DEFAULT_MAX_BUFFERED_MESSAGES, DEFAULT_MAX_CONTROL_RECORDS, DEFAULT_MAX_PENDING_COMMITS,
  },
  managed_commit::ManagedCommitQueue,
  managed_pump::{apply_flow, run_pump, PumpBackend, PumpEvent},
  model::{
    CommitMode, ConsumerConfiguration, ManagedPartition, ManagedRecord, ManagedStateSnapshot,
    OffsetModel, TopicPartition, TopicPartitionConfig, TopicPartitionId,
    DEFAULT_FETCH_METADATA_TIMEOUT,
  },
};

use tokio::select;

pub const DEFAULT_SEEK_TIMEOUT: i64 = 1500;
type DisconnectObserver<'env> =
  Function<'env, FnArgs<(PromiseRaw<'env, ()>, Option<Object<'env>>)>, PromiseRaw<'env, ()>>;

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

/// Native handoff queue depth (batches) for the Web batch/compact streams.
/// Memory stays governed by the 32 MiB wired-byte budget (`ByteBudget`): the
/// count cap must not bind before the budget for small messages. A 4-batch
/// cap limited 64-message prefetches to ~256 messages of lookahead, which
/// starved the reader on fetch bubbles (measured 614k vs 920k op/sec at
/// 200k messages). 256 batches bank up to ~16k small messages so the fetch
/// pipeline never goes cold.
const DEFAULT_STREAM_BUFFER_CAPACITY: usize = 256;
const DRAIN_GRACE_PERIOD: Duration = Duration::from_millis(1500);

fn message_headers_wired_bytes(headers: &Option<MessageHeaders>) -> usize {
  match headers {
    None => 0,
    Some(MessageHeaders::One(key, value)) => key.len() + value.len(),
    Some(MessageHeaders::Many(entries)) => entries
      .iter()
      .map(|(key, value)| key.len() + value.len())
      .sum(),
  }
}

fn message_wired_bytes(message: &Message) -> usize {
  message.payload.len()
    + message.key.as_ref().map(|key| key.len()).unwrap_or(0)
    + message_headers_wired_bytes(&message.headers)
    + message.topic.len()
    + 24
}

fn messages_wired_bytes(messages: &[Message]) -> usize {
  messages.iter().map(message_wired_bytes).sum()
}

fn compact_wired_bytes(batch: &CompactMessageBatch) -> usize {
  let payloads: usize = batch.payloads.iter().map(|payload| payload.len()).sum();
  let keys: usize = batch
    .keys
    .as_ref()
    .map(|keys| {
      keys
        .iter()
        .map(|key| key.as_ref().map(|value| value.len()).unwrap_or(0))
        .sum()
    })
    .unwrap_or(0);
  let shared_key = batch.shared_key.as_ref().map(|key| key.len()).unwrap_or(0);
  let dictionary: usize = batch
    .key_dictionary
    .as_ref()
    .map(|keys| keys.iter().map(|key| key.len()).sum())
    .unwrap_or(0);
  let topics: usize = batch
    .topics
    .as_ref()
    .map(|topics| topics.iter().map(|topic| topic.len()).sum())
    .unwrap_or_else(|| batch.topic.as_ref().map(|topic| topic.len()).unwrap_or(0));
  // Shared header state is retained once (key) or once per distinct value;
  // per-message headers are retained per message. Dictionary indexes and
  // tombstone flags are one small entry per message.
  let shared_header_key = batch
    .shared_header_key
    .as_ref()
    .map(|key| key.len())
    .unwrap_or(0);
  let shared_header_value = batch
    .shared_header_value
    .as_ref()
    .map(|value| value.len())
    .unwrap_or(0);
  let shared_header_values: usize = batch
    .shared_header_values
    .as_ref()
    .map(|values| {
      values
        .iter()
        .map(|value| value.as_ref().map(|value| value.len()).unwrap_or(0))
        .sum()
    })
    .unwrap_or(0);
  let headers: usize = batch
    .headers
    .as_ref()
    .map(|headers| headers.iter().map(message_headers_wired_bytes).sum())
    .unwrap_or(0);
  let dictionary_indexes = batch
    .key_dictionary_indexes
    .as_ref()
    .map(|indexes| indexes.len())
    .unwrap_or(0);
  let tombstones = batch
    .tombstones
    .as_ref()
    .map(|tombstones| tombstones.len())
    .unwrap_or(0);
  payloads
    + keys
    + shared_key
    + dictionary
    + topics
    + shared_header_key
    + shared_header_value
    + shared_header_values
    + headers
    + dictionary_indexes
    + tombstones
    + batch.partitions.len() * 12
}

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

/// Version of the managed consumption contract (RFC-0017) implemented by this
/// binding: ordered `recvManaged`, assignment-bound `commitManaged` and
/// structured `ERR_MANAGED_*` errors. Bumped on any incompatible change.
/// Experimental.
#[napi]
#[cfg_attr(test, allow(dead_code))]
pub const MANAGED_CONSUMPTION_VERSION: u32 = 1;

const MAX_MANAGED_CONTROL_RECORDS: u32 = 65_536;
const MAX_MANAGED_BUFFERED_MESSAGES: u32 = 1_000_000;
const MAX_MANAGED_PENDING_COMMITS: u32 = 65_536;
const MAX_MANAGED_TIMEOUT_MS: i64 = 86_400_000;
const MANAGED_MODE_ERROR: &str = "ERR_MANAGED_MODE";

fn managed_limit(name: &str, value: Option<u32>, default: usize, max: u32) -> Result<usize> {
  match value {
    None => Ok(default),
    Some(value) if (1..=max).contains(&value) => Ok(value as usize),
    Some(value) => Err(Error::new(
      Status::InvalidArg,
      format!("Invalid managedOptions.{name}: {value}. Expected 1-{max}."),
    )),
  }
}

fn managed_limits(configuration: &ConsumerConfiguration) -> Result<ManagedLimits> {
  let options = configuration.managed_options.as_ref();
  Ok(ManagedLimits {
    max_control_records: managed_limit(
      "maxControlRecords",
      options.and_then(|o| o.max_control_records),
      DEFAULT_MAX_CONTROL_RECORDS,
      MAX_MANAGED_CONTROL_RECORDS,
    )?,
    max_buffered_messages: managed_limit(
      "maxBufferedMessages",
      options.and_then(|o| o.max_buffered_messages),
      DEFAULT_MAX_BUFFERED_MESSAGES,
      MAX_MANAGED_BUFFERED_MESSAGES,
    )?,
    max_buffered_bytes: managed_limit(
      "maxBufferedBytes",
      options.and_then(|o| o.max_buffered_bytes),
      DEFAULT_MAX_BUFFERED_BYTES,
      u32::MAX,
    )?,
    max_pending_commits: managed_limit(
      "maxPendingCommits",
      options.and_then(|o| o.max_pending_commits),
      DEFAULT_MAX_PENDING_COMMITS,
      MAX_MANAGED_PENDING_COMMITS,
    )?,
    max_receive_idle: match options.and_then(|o| o.max_receive_idle_ms) {
      None => None,
      Some(ms) => Some(Duration::from_millis(managed_limit(
        "maxReceiveIdleMs",
        Some(ms),
        0,
        MAX_MANAGED_TIMEOUT_MS as u32,
      )? as u64)),
    },
  })
}

fn managed_timeout(
  name: &str,
  value: Option<i64>,
) -> std::result::Result<Option<Duration>, ManagedError> {
  match value {
    None => Ok(None),
    Some(ms) if (0..=MAX_MANAGED_TIMEOUT_MS).contains(&ms) => {
      Ok(Some(Duration::from_millis(ms as u64)))
    }
    Some(ms) => Err(ManagedError::InvalidArgument(format!(
      "Invalid {name}: {ms}. Expected 0-{MAX_MANAGED_TIMEOUT_MS}."
    ))),
  }
}

fn managed_error_code(error: &ManagedError) -> &'static str {
  match error {
    ManagedError::NotManaged => MANAGED_MODE_ERROR,
    ManagedError::ReceivePending => "ERR_MANAGED_RECEIVE_PENDING",
    ManagedError::Closed => "ERR_MANAGED_CLOSED",
    ManagedError::ControlOverflow => "ERR_MANAGED_CONTROL_OVERFLOW",
    ManagedError::ControlFailure(_) => "ERR_MANAGED_CONTROL_FAILURE",
    ManagedError::BackpressureFailure(_) => "ERR_MANAGED_BACKPRESSURE_FAILURE",
    ManagedError::ReceiveStalled => "ERR_MANAGED_RECEIVE_STALLED",
    ManagedError::ReceiveFailed(_) => "ERR_MANAGED_RECEIVE_FAILED",
    ManagedError::StaleAssignment => "ERR_MANAGED_STALE_ASSIGNMENT",
    ManagedError::UnknownAssignment(_) => "ERR_MANAGED_UNKNOWN_ASSIGNMENT",
    ManagedError::InvalidArgument(_) => "ERR_MANAGED_INVALID_ARGUMENT",
    ManagedError::CommitRegression { .. } => "ERR_MANAGED_COMMIT_REGRESSION",
    ManagedError::CommitBacklog => "ERR_MANAGED_COMMIT_BACKLOG",
    ManagedError::CommitFailed { .. } => "ERR_MANAGED_COMMIT_FAILED",
    ManagedError::CommitUnknown(_) => "ERR_MANAGED_COMMIT_UNKNOWN",
    ManagedError::CommitTimeout => "ERR_MANAGED_COMMIT_TIMEOUT",
  }
}

fn managed_error_message(error: &ManagedError) -> String {
  match error {
    ManagedError::NotManaged => {
      "managed operations require a consumer created with `managed: true`".to_owned()
    }
    ManagedError::ReceivePending => "another recvManaged() is already pending".to_owned(),
    ManagedError::Closed => "managed consumer is closing or disconnected".to_owned(),
    ManagedError::ControlOverflow => {
      "managed control queue overflowed; commit authorization was invalidated".to_owned()
    }
    ManagedError::ControlFailure(reason) => format!("managed control failure: {reason}"),
    ManagedError::BackpressureFailure(reason) => format!("managed backpressure failure: {reason}"),
    ManagedError::ReceiveStalled => {
      "no recvManaged() was pending for longer than maxReceiveIdleMs".to_owned()
    }
    ManagedError::ReceiveFailed(failure) => failure.message.clone(),
    ManagedError::StaleAssignment => {
      "assignment identity is no longer active for this partition".to_owned()
    }
    ManagedError::UnknownAssignment(reason) => reason.clone(),
    ManagedError::InvalidArgument(reason) => reason.clone(),
    ManagedError::CommitRegression { authorized } => format!(
      "nextOffset is lower than {authorized}, already authorized under this assignment identity"
    ),
    ManagedError::CommitBacklog => {
      "too many managed commits are pending (maxPendingCommits)".to_owned()
    }
    ManagedError::CommitFailed { kafka, .. } => format!("managed commit failed: {}", kafka.name),
    ManagedError::CommitUnknown(reason) => format!("managed commit outcome is unknown: {reason}"),
    ManagedError::CommitTimeout => {
      "the commit deadline elapsed; the request was not cancelled and may still be applied"
        .to_owned()
    }
  }
}

/// Whether the broker may have applied the commit. Only an error raised
/// before the request was handed to librdkafka is definitive: librdkafka can
/// retry or defer a request after an attempt whose fate is unknown, so the
/// final code of a dispatched request does not prove that no attempt landed.
fn commit_outcome_unknown(error: &ManagedError) -> Option<bool> {
  match error {
    ManagedError::CommitFailed { dispatched, .. } => Some(*dispatched),
    ManagedError::CommitUnknown(_) | ManagedError::CommitTimeout => Some(true),
    ManagedError::StaleAssignment
    | ManagedError::UnknownAssignment(_)
    | ManagedError::CommitRegression { .. }
    | ManagedError::CommitBacklog => Some(false),
    _ => None,
  }
}

/// Builds the JavaScript error on the JS thread so `code`, `kafkaCode`,
/// `kafkaErrorCode` and `outcomeUnknown` are real properties of the rejection.
fn managed_js_error(env: &Env, error: &ManagedError) -> Error {
  let code = managed_error_code(error);
  let message = format!("[{code}] {}", managed_error_message(error));
  let kafka = match error {
    ManagedError::CommitFailed { kafka, .. } => Some(kafka),
    ManagedError::ReceiveFailed(failure) => failure.kafka.as_ref(),
    _ => None,
  };
  let build = || -> Result<Error> {
    let mut object = env.create_error(Error::new(Status::GenericFailure, message.clone()))?;
    object.set_named_property("code", code)?;
    if let Some(kafka) = kafka {
      object.set_named_property("kafkaCode", kafka.name.as_str())?;
      object.set_named_property("kafkaErrorCode", kafka.code)?;
    }
    if let Some(outcome_unknown) = commit_outcome_unknown(error) {
      object.set_named_property("outcomeUnknown", outcome_unknown)?;
    }
    Ok(Error::from(object.to_unknown()))
  };
  build().unwrap_or_else(|_| Error::new(Status::GenericFailure, message.clone()))
}

fn managed_partitions(partitions: Vec<PartitionChange>) -> Vec<ManagedPartition> {
  partitions
    .into_iter()
    .map(|partition| ManagedPartition {
      topic: partition.topic,
      partition: partition.partition,
      assignment: partition.assignment,
      previous_assignment: partition.previous_assignment,
    })
    .collect()
}

fn tpl_keys(partitions: &RdTopicPartitionList) -> Vec<PartitionKey> {
  partitions
    .elements()
    .iter()
    .map(|partition| (partition.topic().to_owned(), partition.partition()))
    .collect()
}

/// librdkafka as seen by the managed pump.
struct ConsumerPumpBackend {
  consumer: Arc<LoggingConsumer>,
}

impl ConsumerPumpBackend {
  /// Runs a pause or resume and returns the partitions it did not confirm:
  /// all of them on a call error, otherwise those with a per-partition error.
  fn flow(
    &self,
    partitions: &[PartitionKey],
    call: impl FnOnce(&RdTopicPartitionList) -> rdkafka::error::KafkaResult<()>,
  ) -> Vec<PartitionKey> {
    let mut list = RdTopicPartitionList::with_capacity(partitions.len());
    for (topic, partition) in partitions {
      list.add_partition(topic, *partition);
    }
    if call(&list).is_err() {
      return partitions.to_vec();
    }
    list
      .elements()
      .iter()
      .filter(|partition| partition.error().is_err())
      .map(|partition| (partition.topic().to_owned(), partition.partition()))
      .collect()
  }
}

impl PumpBackend for ConsumerPumpBackend {
  type Message = Message;

  async fn recv(&self) -> PumpEvent<Message> {
    match self.consumer.recv().await {
      Ok(received) => {
        let message = create_message(&received, received.payload());
        PumpEvent::Message {
          topic: message.topic.clone(),
          partition: message.partition,
          bytes: message_wired_bytes(&message),
          message,
        }
      }
      Err(error) => PumpEvent::Error {
        fatal: matches!(
          error,
          rdkafka::error::KafkaError::MessageConsumptionFatal(_)
        ),
        failure: ReceiveFailure {
          message: format!("Failed to receive message from consumer: {error}"),
          kafka: error.rdkafka_error_code().map(|code| KafkaCode {
            name: format!("{code:?}"),
            code: code as i32,
          }),
        },
      },
    }
  }

  fn pause(&self, partitions: &[PartitionKey]) -> Vec<PartitionKey> {
    self.flow(partitions, |list| self.consumer.pause(list))
  }

  fn resume(&self, partitions: &[PartitionKey]) -> Vec<PartitionKey> {
    self.flow(partitions, |list| self.consumer.resume(list))
  }

  fn unsubscribe(&self) {
    self.consumer.unsubscribe();
  }
}

/// Everything a managed consumer owns beyond a legacy one (RFC-0017). A
/// legacy consumer holds `None` instead and allocates none of it.
struct ManagedRuntime {
  state: Arc<ManagedConsumerState>,
  backend: Arc<ConsumerPumpBackend>,
  runtime: tokio::runtime::Handle,
  pump_started: AtomicBool,
  /// Created on the first commit and kept for the consumer's lifetime. It is
  /// only reached while the state is active, so a commit racing a disconnect
  /// cannot create a queue on a consumer that is closing.
  commit_queue: std::sync::Mutex<Option<Arc<ManagedCommitQueue>>>,
  flow_lock: Arc<std::sync::Mutex<()>>,
}

impl ManagedRuntime {
  fn dispatch(
    &self,
    id: u64,
    topic: &str,
    partition: i32,
    offset: i64,
  ) -> std::result::Result<(), KafkaCode> {
    let mut slot = self.commit_queue.lock().unwrap_or_else(|e| e.into_inner());
    let queue = match slot.as_ref() {
      Some(queue) => queue,
      None => slot.insert(Arc::new(
        ManagedCommitQueue::new(self.backend.consumer.clone()).map_err(|error| KafkaCode {
          name: format!("{error}"),
          code: rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__FAIL as i32,
        })?,
      )),
    };
    queue.enqueue(id, topic, partition, offset)?;
    queue.ensure_polling(&self.state, &self.runtime);
    Ok(())
  }

  /// Authorizes and dispatches (or queues) the commit synchronously, on the
  /// calling thread, so commits enter librdkafka in call order.
  fn submit_commit(
    &self,
    topic: &str,
    partition: i32,
    assignment: &str,
    next_offset: i64,
  ) -> std::result::Result<
    tokio::sync::oneshot::Receiver<std::result::Result<(), ManagedError>>,
    ManagedError,
  > {
    if topic.is_empty() || partition < 0 || next_offset < 0 {
      return Err(ManagedError::InvalidArgument(
        "commitManaged requires a topic, a non-negative partition and a non-negative next offset"
          .to_owned(),
      ));
    }
    self.state.submit_commit(
      topic,
      partition,
      assignment,
      next_offset,
      &mut |id: u64, topic: &str, partition: i32, offset: i64| {
        self.dispatch(id, topic, partition, offset)
      },
    )
  }

  fn apply_flow(&self) {
    apply_flow(&*self.backend, &self.state, &self.flow_lock);
  }
}

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
  stream_consumer: Arc<LoggingConsumer>,
  disconnect_signal: watch::Receiver<()>,
  cancel_signal: watch::Receiver<bool>,
  pending_messages: VecDeque<Message>,
  pending_bytes: usize,
  prefetch_size: u32,
  prefetch_timeout_ms: i64,
  closed: bool,
  pending_error: Option<Error>,
}

enum KeyBatchState {
  None,
  Uniform(Buffer),
  Dictionary {
    values: Vec<Buffer>,
    indexes: Vec<u8>,
  },
  PerMessage(Vec<Option<Buffer>>),
}

enum HeaderProjection<'a> {
  None,
  Single { key: &'a str, value: &'a [u8] },
  Many(MessageHeaders),
}

enum HeaderBatchState {
  None,
  UniformSingle {
    key: String,
    value: Buffer,
  },
  SharedSingle {
    key: String,
    values: Vec<Option<Buffer>>,
  },
  Many(Vec<Option<MessageHeaders>>),
}

struct CompactBatchBuilder {
  payloads: Vec<Buffer>,
  keys: KeyBatchState,
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
      payloads: Vec::new(),
      keys: KeyBatchState::None,
      topic: None,
      topics: None,
      partitions: Vec::new(),
      offsets: Vec::new(),
      headers: HeaderBatchState::None,
      tombstones: None,
      capacity,
    }
  }

  #[inline]
  fn push(&mut self, kafka_message: &BorrowedMessage<'_>, payload: Option<&[u8]>) {
    let previous_count = self.payloads.len();
    // Empty timeouts never allocate batch storage. Reserve once data arrives.
    if previous_count == 0 {
      self.payloads.reserve(self.capacity);
      self.partitions.reserve(self.capacity);
      self.offsets.reserve(self.capacity);
    }
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
      finish_keys(self.keys, self.capacity);
    let (shared_header_key, shared_header_value, shared_header_values, headers) = match self.headers
    {
      HeaderBatchState::None => (None, None, None, None),
      HeaderBatchState::UniformSingle { key, value } => (Some(key), Some(value), None, None),
      HeaderBatchState::SharedSingle { key, values } => (Some(key), None, Some(values), None),
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
      (KeyBatchState::None, None) => {}
      (KeyBatchState::None, Some(key)) => {
        if previous_count == 0 {
          self.keys = KeyBatchState::Uniform(key.into());
        } else {
          let mut keys = Vec::with_capacity(self.capacity);
          keys.resize_with(previous_count, || None);
          keys.push(Some(key.into()));
          self.keys = KeyBatchState::PerMessage(keys);
        }
      }
      (KeyBatchState::Uniform(shared_key), Some(key)) if shared_key.as_ref() == key => {}
      (KeyBatchState::Uniform(shared_key), Some(key)) => {
        let first_key = mem::replace(shared_key, Buffer::from(&[][..]));
        let mut values = Vec::with_capacity(8);
        values.push(first_key);
        values.push(key.into());
        let mut indexes = Vec::with_capacity(self.capacity);
        indexes.resize(previous_count, 0);
        indexes.push(1);
        self.keys = KeyBatchState::Dictionary { values, indexes };
      }
      (KeyBatchState::Uniform(shared_key), None) => {
        let shared_slice = shared_key.as_ref();
        let mut keys = Vec::with_capacity(self.capacity);
        for _ in 0..previous_count {
          keys.push(Some(Buffer::from(shared_slice)));
        }
        keys.push(None);
        self.keys = KeyBatchState::PerMessage(keys);
      }
      (KeyBatchState::Dictionary { values, indexes }, Some(key)) => {
        if let Some(index) = values.iter().position(|entry| entry.as_ref() == key) {
          indexes.push(index as u8);
          return;
        }
        if values.len() < BUFFER_DICTIONARY_MAX_UNIQUE_VALUES {
          let index = values.len() as u8;
          values.push(key.into());
          indexes.push(index);
          return;
        }
        let mut keys = materialize_dictionary_keys(values, indexes, self.capacity);
        keys.push(Some(key.into()));
        self.keys = KeyBatchState::PerMessage(keys);
      }
      (KeyBatchState::Dictionary { values, indexes }, None) => {
        let mut keys = materialize_dictionary_keys(values, indexes, self.capacity);
        keys.push(None);
        self.keys = KeyBatchState::PerMessage(keys);
      }
      (KeyBatchState::PerMessage(keys), Some(key)) => keys.push(Some(key.into())),
      (KeyBatchState::PerMessage(keys), None) => keys.push(None),
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
          if previous_count == 0 {
            self.headers = HeaderBatchState::UniformSingle {
              key: key.to_owned(),
              value: value.into(),
            };
          } else {
            let mut values = Vec::with_capacity(self.capacity);
            values.resize_with(previous_count, || None);
            values.push(Some(value.into()));
            self.headers = HeaderBatchState::SharedSingle {
              key: key.to_owned(),
              values,
            };
          }
        }
        HeaderProjection::Many(message_headers) => {
          let mut header_batches = Vec::with_capacity(self.capacity);
          header_batches.resize_with(previous_count, || None);
          header_batches.push(Some(message_headers));
          self.headers = HeaderBatchState::Many(header_batches);
        }
      },
      HeaderBatchState::UniformSingle { key, value } => match project_headers(headers_input) {
        HeaderProjection::Single {
          key: header_key,
          value: next_value,
        } if key == header_key && value.as_ref() == next_value => {}
        HeaderProjection::Single {
          key: header_key,
          value: next_value,
        } if key == header_key => {
          let shared_slice = value.as_ref();
          let mut values = Vec::with_capacity(self.capacity);
          for _ in 0..previous_count {
            values.push(Some(Buffer::from(shared_slice)));
          }
          values.push(Some(next_value.into()));
          let key = mem::take(key);
          self.headers = HeaderBatchState::SharedSingle { key, values };
        }
        HeaderProjection::None => {
          let shared_slice = value.as_ref();
          let mut values = Vec::with_capacity(self.capacity);
          for _ in 0..previous_count {
            values.push(Some(Buffer::from(shared_slice)));
          }
          values.push(None);
          let key = mem::take(key);
          self.headers = HeaderBatchState::SharedSingle { key, values };
        }
        HeaderProjection::Single {
          key: header_key,
          value: next_value,
        } => {
          let mut header_batches =
            uniform_single_to_many(key, value.as_ref(), previous_count, self.capacity);
          header_batches.push(Some(MessageHeaders::one(
            header_key.to_owned(),
            next_value.into(),
          )));
          self.headers = HeaderBatchState::Many(header_batches);
        }
        HeaderProjection::Many(message_headers) => {
          let mut header_batches =
            uniform_single_to_many(key, value.as_ref(), previous_count, self.capacity);
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
fn uniform_single_to_many(
  shared_key: &str,
  shared_value: &[u8],
  previous_count: usize,
  capacity: usize,
) -> Vec<Option<MessageHeaders>> {
  let mut headers = Vec::with_capacity(capacity.max(previous_count + 1));

  for _ in 0..previous_count {
    headers.push(Some(MessageHeaders::one(
      shared_key.to_owned(),
      Buffer::from(shared_value),
    )));
  }

  headers
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
fn should_dictionary_encode(total: usize, unique: usize) -> bool {
  unique > 1
    && unique <= BUFFER_DICTIONARY_MAX_UNIQUE_VALUES
    && total >= unique * BUFFER_DICTIONARY_MIN_REPEAT_FACTOR
}

#[inline]
fn materialize_dictionary_keys(
  values: &[Buffer],
  indexes: &[u8],
  capacity: usize,
) -> Vec<Option<Buffer>> {
  let mut keys = Vec::with_capacity(capacity.max(indexes.len()));
  for &index in indexes {
    let value = &values[index as usize];
    keys.push(Some(Buffer::from(value.as_ref())));
  }
  keys
}

#[inline]
fn finish_keys(keys: KeyBatchState, capacity: usize) -> CompactKeyEncoding {
  match keys {
    KeyBatchState::None => (None, None, None, None),
    KeyBatchState::Uniform(shared_key) => (None, Some(shared_key), None, None),
    KeyBatchState::Dictionary { values, indexes } => {
      if should_dictionary_encode(indexes.len(), values.len()) {
        (None, None, Some(values), Some(indexes))
      } else {
        let keys = materialize_dictionary_keys(&values, &indexes, capacity);
        (Some(keys), None, None, None)
      }
    }
    KeyBatchState::PerMessage(keys) => {
      if keys.is_empty() {
        (None, None, None, None)
      } else {
        (Some(keys), None, None, None)
      }
    }
  }
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
  stream_consumer: &Arc<LoggingConsumer>,
  disconnect_signal: &mut watch::Receiver<()>,
  mut cancel_signal: Option<&mut watch::Receiver<bool>>,
  size: u32,
  timeout_ms: i64,
) -> Result<BatchCollection> {
  let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms as u64);
  let mut messages = Vec::new();
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
            messages.reserve(size as usize);
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
            messages.reserve(size as usize);
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
  for slot in 1..size {
    if disconnect_signal.has_changed().unwrap_or(false) {
      let _ = disconnect_signal.borrow_and_update();
      disconnected = true;
      break;
    }
    if let Some(cancel_rx) = cancel_signal.as_deref_mut() {
      if *cancel_rx.borrow() || cancel_rx.has_changed().unwrap_or(false) {
        let _ = cancel_rx.borrow_and_update();
        disconnected = true;
        break;
      }
    }
    if (slot & 63) == 0 && tokio::time::Instant::now() >= deadline {
      break;
    }
    if let Some(message) = message_stream.next().now_or_never() {
      match message {
        Some(Ok(kafka_message)) => {
          let payload = kafka_message.payload();
          messages.push(create_message(&kafka_message, payload));
          continue;
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
  stream_consumer: &Arc<LoggingConsumer>,
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
  for slot in 1..size {
    if disconnect_signal.has_changed().unwrap_or(false) {
      let _ = disconnect_signal.borrow_and_update();
      disconnected = true;
      break;
    }
    if let Some(cancel_rx) = cancel_signal.as_deref_mut() {
      if *cancel_rx.borrow() || cancel_rx.has_changed().unwrap_or(false) {
        let _ = cancel_rx.borrow_and_update();
        disconnected = true;
        break;
      }
    }
    if (slot & 63) == 0 && tokio::time::Instant::now() >= deadline {
      break;
    }
    if let Some(message) = message_stream.next().now_or_never() {
      match message {
        Some(Ok(kafka_message)) => {
          let payload = kafka_message.payload();
          batch.push(&kafka_message, payload);
          continue;
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
      state.pending_bytes = state
        .pending_bytes
        .saturating_sub(message_wired_bytes(&message));
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

        state.pending_bytes = messages_wired_bytes(&batch.messages);
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
  stream_consumer: Arc<LoggingConsumer>,
  fetch_metadata_timeout: Duration,
  disconnect_signal: DisconnectSignal,
  client_id: String,
  pending_error: Arc<std::sync::Mutex<Option<Error>>>,
  event_listeners_active: Arc<AtomicUsize>,
  async_commit_queue: Arc<std::sync::Mutex<Option<Arc<AsyncCommitQueue>>>>,
  disconnect_completion: OnceLock<watch::Receiver<Option<std::result::Result<(), String>>>>,
  /// Serializes terminal lifecycle mutations with subscription/assignment
  /// application. Metadata lookup itself remains outside this lock so
  /// disconnect is not held up by a blocking fetch.
  lifecycle_lock: Arc<Mutex<()>>,
  disconnected: Arc<AtomicBool>,
  /// Managed consumption (RFC-0017). `None` for legacy consumers.
  managed: Option<Arc<ManagedRuntime>>,
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

    let managed = if consumer_configuration.managed.unwrap_or(false) {
      let effective =
        build_consumer_config(client_config, consumer_configuration, configuration.clone());
      if let Some(key) = managed_config_conflict(&effective) {
        return Err(Error::new(
          Status::InvalidArg,
          format!(
            "[{MANAGED_MODE_ERROR}] managed consumption requires {key}=false; automatic offset commit/storage would bypass managed completion"
          ),
        ));
      }
      Some(Arc::new(ManagedState::new(managed_limits(
        consumer_configuration,
      )?)))
    } else if consumer_configuration.managed_options.is_some() {
      return Err(Error::new(
        Status::InvalidArg,
        format!("[{MANAGED_MODE_ERROR}] managedOptions requires managed: true"),
      ));
    } else {
      None
    };

    let stream_consumer = Arc::new(
      create_stream_consumer(
        client_config,
        consumer_configuration,
        configuration.clone(),
        managed.clone(),
      )
      .map_err(|e| e.into_napi_error("Failed to create stream consumer"))?,
    );
    let managed = managed.map(|state| {
      Arc::new(ManagedRuntime {
        state,
        backend: Arc::new(ConsumerPumpBackend {
          consumer: stream_consumer.clone(),
        }),
        runtime: tokio::runtime::Handle::current(),
        pump_started: AtomicBool::new(false),
        commit_queue: std::sync::Mutex::new(None),
        flow_lock: Arc::new(std::sync::Mutex::new(())),
      })
    });

    Ok(KafkaConsumer {
      client_config: client_config.clone(),
      consumer_config: consumer_configuration.clone(),
      stream_consumer,
      fetch_metadata_timeout: normalize_fetch_metadata_timeout(*fetch_metadata_timeout),
      disconnect_signal: watch::channel(()),
      client_id: kafka_client.configuration().client_id,
      pending_error: Arc::new(std::sync::Mutex::new(None)),
      event_listeners_active: Arc::new(AtomicUsize::new(0)),
      async_commit_queue: Arc::new(std::sync::Mutex::new(None)),
      disconnect_completion: OnceLock::new(),
      lifecycle_lock: Arc::new(Mutex::new(())),
      disconnected: Arc::new(AtomicBool::new(false)),
      managed,
    })
  }

  /// Legacy receive, commit and repositioning methods would bypass the
  /// managed ordering and authorization, so a managed consumer rejects them.
  fn ensure_legacy(&self, operation: &str) -> Result<()> {
    if self.managed.is_some() {
      return Err(Error::new(
        Status::InvalidArg,
        format!(
          "[{MANAGED_MODE_ERROR}] {operation} is not available on a managed consumer; use recvManaged/commitManaged"
        ),
      ));
    }
    Ok(())
  }

  fn managed_state_or_throw(&self, env: &Env) -> Result<&Arc<ManagedConsumerState>> {
    self
      .managed
      .as_ref()
      .map(|managed| &managed.state)
      .ok_or_else(|| managed_js_error(env, &ManagedError::NotManaged))
  }

  /// Starts the single native poller of a managed consumer (see `run_pump`).
  fn start_managed_pump(&self) {
    let Some(managed) = &self.managed else {
      return;
    };
    if managed.pump_started.swap(true, Ordering::AcqRel) {
      return;
    }
    // Dropping the JS consumer drops the sender, which also ends the pump.
    napi::bindgen_prelude::spawn(run_pump(
      managed.state.clone(),
      managed.backend.clone(),
      managed.flow_lock.clone(),
      self.disconnect_signal.1.clone(),
    ));
  }

  fn disconnected_error() -> Error {
    Error::new(
      Status::InvalidArg,
      "Consumer is disconnected; subscribe cannot be used after disconnect".to_string(),
    )
  }

  fn ensure_connected(&self) -> Result<()> {
    if self.disconnected.load(Ordering::Acquire) {
      Err(Self::disconnected_error())
    } else {
      Ok(())
    }
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
    // Count live event tasks so a second `onEvents()` is not invalidated when
    // the first task stops, and so `commit('Async')` stops being authorized
    // once every task has exited (notably after `disconnect()`).
    self.event_listeners_active.fetch_add(1, Ordering::AcqRel);
    let active = Arc::clone(&self.event_listeners_active);
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
      active.fetch_sub(1, Ordering::AcqRel);
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
    // This is an inexpensive fast path for an already terminal consumer. The
    // lifecycle lock below is still required at the actual assignment point,
    // because disconnect may race this check while metadata is being fetched.
    self.ensure_connected()?;
    if let Some(managed) = &self.managed {
      // A failed managed consumer has left the group and must not rejoin it.
      if let ManagedStatus::Failed(error) = managed.state.status() {
        return Err(Error::new(
          Status::InvalidArg,
          format!(
            "[{}] subscribe is not available after a terminal managed failure: {}",
            managed_error_code(&error),
            managed_error_message(&error)
          ),
        ));
      }
    }

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

    self.ensure_connected()?;

    let has_manual_assignment = topics
      .iter()
      .any(|t| t.all_offsets.is_some() || t.partition_offset.is_some());

    if has_manual_assignment {
      self.ensure_legacy("manual partition assignment")?;
      let consumer = self.stream_consumer.clone();
      let timeout = self.fetch_metadata_timeout;
      let assigned_topics = topics.clone();
      let tpl = tokio::task::spawn_blocking(move || {
        let mut tpl = RdTopicPartitionList::new();
        for item in &assigned_topics {
          add_topic_partitions_to_tpl(
            &mut tpl,
            &item.topic,
            item.partition_offset.as_ref(),
            item.all_offsets.as_ref(),
            consumer.as_ref(),
            timeout,
          )?;
        }
        Ok::<_, anyhow::Error>(tpl)
      })
      .await
      .map_err(|e| e.into_napi_error("Failed to join metadata fetch"))?
      .map_err(|e| e.into_napi_error("Failed to assign topic partitions"))?;

      if tpl.count() == 0 {
        return Err(Error::new(
          Status::InvalidArg,
          "subscribe resolved no partitions; check topic names and offsets".to_string(),
        ));
      }

      // Metadata is fetched outside the lifecycle lock. Once it completes,
      // acquire the lock and re-check the terminal flag immediately before
      // assigning. If disconnect won the race, this operation cannot restore
      // an assignment after the consumer was disconnected.
      {
        let _lifecycle_guard = self.lifecycle_lock.lock().await;
        self.ensure_connected()?;
        self
          .stream_consumer
          .assign(&tpl)
          .map_err(|e| anyhow::anyhow!("Failed to assign topic partitions: {:?}", e))
          .map_err(|e| e.into_napi_error("Failed to assign partitions"))?;
      }
      debug!(
        "Assigned topic partition list with {} elements",
        tpl.count()
      );
    } else {
      let topics_name = topics
        .iter()
        .map(|x| x.topic.clone())
        .collect::<Vec<String>>();

      let _lifecycle_guard = self.lifecycle_lock.lock().await;
      self.ensure_connected()?;
      try_subscribe(&self.stream_consumer, &topics_name)
        .map_err(|e| e.into_napi_error("Failed to subscribe to topics"))?;
      self.start_managed_pump();
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
    if let Some(managed) = &self.managed {
      return Self::managed_set_paused(managed, managed.state.active_partitions(), true);
    }
    self
      .stream_consumer
      .pause(&self.get_partitions()?)
      .map_err(|e| e.into_napi_error("Failed to pause consumer"))?;
    Ok(())
  }

  /// Resumes message consumption on all assigned partitions after a pause.
  #[napi]
  pub fn resume(&self) -> Result<()> {
    if let Some(managed) = &self.managed {
      return Self::managed_set_paused(managed, managed.state.active_partitions(), false);
    }
    self
      .stream_consumer
      .resume(&self.get_partitions()?)
      .map_err(|e| e.into_napi_error("Failed to resume consumer"))?;
    Ok(())
  }

  /// Managed consumers route application pauses through the managed state so
  /// the binding's own backpressure pause/resume never undoes them.
  fn managed_set_paused(
    managed: &ManagedRuntime,
    partitions: Vec<PartitionKey>,
    paused: bool,
  ) -> Result<()> {
    managed
      .state
      .set_user_paused(&partitions, paused)
      .map_err(|error| {
        Error::new(
          Status::InvalidArg,
          format!(
            "[{}] {}",
            managed_error_code(&error),
            managed_error_message(&error)
          ),
        )
      })?;
    // Synchronous on the calling thread, like the legacy pause/resume. A
    // partition the client does not confirm is retried by the pump.
    managed.apply_flow();
    Ok(())
  }

  fn selected_partitions(&self, partitions: Vec<TopicPartitionId>) -> Result<RdTopicPartitionList> {
    self.ensure_connected()?;
    let mut selected = RdTopicPartitionList::new();
    for partition in partitions {
      if partition.topic.is_empty() || partition.partition < 0 {
        return Err(Error::new(Status::InvalidArg, "Invalid topic or partition"));
      }
      selected.add_partition(&partition.topic, partition.partition);
    }
    Ok(selected)
  }

  /// Pauses only these assigned partitions. Already delivered messages remain caller-owned.
  #[napi]
  pub fn pause_partitions(&self, partitions: Vec<TopicPartitionId>) -> Result<()> {
    let selected = self.selected_partitions(partitions)?;
    if let Some(managed) = &self.managed {
      return Self::managed_set_paused(managed, tpl_keys(&selected), true);
    }
    self
      .stream_consumer
      .pause(&selected)
      .map_err(|e| e.into_napi_error("Failed to pause partitions"))?;
    for partition in selected.elements() {
      partition
        .error()
        .map_err(|e| e.into_napi_error("Failed to pause partition"))?;
    }
    Ok(())
  }

  /// Resumes only these assigned partitions, leaving other paused partitions unchanged.
  #[napi]
  pub fn resume_partitions(&self, partitions: Vec<TopicPartitionId>) -> Result<()> {
    let selected = self.selected_partitions(partitions)?;
    if let Some(managed) = &self.managed {
      return Self::managed_set_paused(managed, tpl_keys(&selected), false);
    }
    self
      .stream_consumer
      .resume(&selected)
      .map_err(|e| e.into_napi_error("Failed to resume partitions"))?;
    for partition in selected.elements() {
      partition
        .error()
        .map_err(|e| e.into_napi_error("Failed to resume partition"))?;
    }
    Ok(())
  }

  /// Unsubscribes from all currently subscribed topics.
  /// After calling this method, the consumer will no longer receive messages.
  #[napi]
  pub fn unsubscribe(&self) -> Result<()> {
    self.ensure_legacy("unsubscribe")?;
    info!("Unsubscribing from topics");
    self.stream_consumer.unsubscribe();
    Ok(())
  }

  /// Disconnects the consumer and waits for native closure.
  /// An optional AbortSignal cancels only this call's wait, not native cleanup.
  /// Use AbortSignal.timeout(ms) to bound the wait. Pending receives are interrupted.
  #[napi(
    ts_args_type = "options?: { signal?: AbortSignal }",
    ts_return_type = "Promise<void>"
  )]
  pub fn disconnect<'env>(
    &self,
    env: &'env Env,
    options: Option<Object<'env>>,
  ) -> Result<PromiseRaw<'env, ()>> {
    let mut completion = self
      .disconnect_completion
      .get_or_init(|| {
        let (sender, receiver) = watch::channel(None);
        let consumer = self.stream_consumer.clone();
        let lifecycle_lock = self.lifecycle_lock.clone();
        let disconnected = self.disconnected.clone();
        let disconnect_signal = self.disconnect_signal.0.clone();
        let async_commit_queue = self.async_commit_queue.clone();
        let managed = self.managed.as_ref().map(|managed| managed.state.clone());
        if let Some(managed) = &managed {
          // Synchronously, in the disconnect call itself: managed operations
          // issued after it are refused, whenever the close task starts.
          managed.close();
        }
        // This task owns native close independently of any caller's cancellation.
        napi::bindgen_prelude::spawn(async move {
          let result: Result<()> = async {
            let lifecycle_guard = lifecycle_lock.lock().await;
            disconnected.store(true, Ordering::Release);
            if let Some(managed) = &managed {
              // Stops admission, refuses new managed operations, wakes the
              // reserved receive and settles commits not dispatched yet,
              // before the close-time revocation is served. A commit already
              // dispatched keeps the client alive through its poller until
              // the broker result (or close) settles it.
              managed.close();
            }
            info!("Disconnecting consumer - This will stop the consumer from receiving messages");
            consumer.unsubscribe();
            if disconnect_signal.send(()).is_err() {
              warn!("Disconnect signal could not be sent - no active receivers");
            }
            async_commit_queue
              .lock()
              .unwrap_or_else(|e| e.into_inner())
              .take();

            // SAFETY: the Arc keeps the client alive. Queue/error handles are released
            // synchronously, and no raw pointer is retained across an await.
            unsafe {
              let native = consumer.client().native_ptr();
              let queue = rdsys::rd_kafka_queue_get_consumer(native);
              let error = rdsys::rd_kafka_consumer_close_queue(native, queue);
              rdsys::rd_kafka_queue_destroy(queue);
              if !error.is_null() {
                let message = std::ffi::CStr::from_ptr(rdsys::rd_kafka_error_string(error))
                  .to_string_lossy()
                  .into_owned();
                rdsys::rd_kafka_error_destroy(error);
                return Err(Error::new(Status::GenericFailure, message));
              }
            }
            // Subscription mutations can now acquire the lock and reject the terminal state.
            drop(lifecycle_guard);
            while unsafe { rdsys::rd_kafka_consumer_closed(consumer.client().native_ptr()) == 0 } {
              // StreamConsumer must keep serving rebalance events until close completes.
              let _ = tokio::time::timeout(Duration::from_millis(100), consumer.recv()).await;
            }
            Ok(())
          }
          .await;
          sender.send_replace(Some(result.map_err(|error| error.reason)));
        });
        receiver
      })
      .clone();

    let closed = env.spawn_future(async move {
      loop {
        let outcome = completion.borrow().clone();
        if let Some(result) = outcome {
          return result.map_err(|reason| Error::new(Status::GenericFailure, reason));
        }
        completion.changed().await.map_err(|_| {
          Error::new(
            Status::GenericFailure,
            "Native consumer close task stopped before completion",
          )
        })?;
      }
    })?;
    // Keep AbortSignal handling on the JS thread and remove listeners on every outcome.
    // Native close has its own task and continues even after this promise is aborted.
    let observe: DisconnectObserver<'env> = env.run_script(
      r#"
        ((closed, options) => {
          const signal = options?.signal;
          if (signal === undefined) return closed;
          return new Promise((resolve, reject) => {
            let subscription;
            const cleanup = () => subscription?.[Symbol.dispose]();
            const abort = () => { cleanup(); reject(signal.reason); };
            closed.then(
              () => { cleanup(); resolve(); },
              error => { cleanup(); reject(error); }
            );
            subscription = process.getBuiltinModule('node:events').addAbortListener(signal, abort);
            if (signal.aborted) abort();
          });
        })
      "#,
    )?;
    observe.call((closed, options).into())
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
    self.ensure_legacy("seek")?;
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
    if self.disconnected.load(Ordering::Acquire) {
      return Ok(Vec::new());
    }
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
    self.ensure_legacy("recv")?;
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
    self.ensure_legacy("recvBatch")?;
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
    self.ensure_legacy("recvStream")?;
    let normalized_prefetch_size = normalize_serial_stream_prefetch_size(prefetch_size);
    let normalized_prefetch_timeout_ms =
      normalize_serial_stream_prefetch_timeout(prefetch_timeout_ms);
    let (cancel_sender, cancel_receiver) = watch::channel(false);
    let stream_state = SerialStreamState {
      stream_consumer: self.stream_consumer.clone(),
      disconnect_signal: self.disconnect_signal.1.clone(),
      cancel_signal: cancel_receiver,
      pending_messages: VecDeque::with_capacity(normalized_prefetch_size as usize),
      pending_bytes: 0,
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
    let (sender, receiver) =
      mpsc::channel::<Result<(Vec<Message>, usize)>>(DEFAULT_STREAM_BUFFER_CAPACITY);
    let (cancel_sender, mut cancel_receiver) = watch::channel(false);
    let mut task_cancel_rx = cancel_receiver.clone();
    let budget = Arc::new(ByteBudget::new(DEFAULT_STREAM_BUFFER_BYTES));
    let producer_budget = budget.clone();

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

                let wired = messages_wired_bytes(&messages);
                // Preserve disconnect observed during the byte-budget wait: `changed()`
                // marks this receiver observed, so a later `has_changed()` alone
                // would miss it and skip the drain deadline.
                let mut reserve_disconnected =
                  disconnected || disconnect_signal.has_changed().unwrap_or(false);
                if !reserve_disconnected {
                  tokio::select! {
                    biased;
                    _ = cancel_receiver.changed() => {
                      break;
                    }
                    _ = disconnect_signal.changed() => {
                      reserve_disconnected = true;
                    }
                    _ = producer_budget.reserve(wired) => {}
                  }
                  if *cancel_receiver.borrow() {
                    break;
                  }
                }

                let mut send_failed = false;
                let pending_messages = messages.len();
                let send_fut = sender.send(Ok((messages, wired)));
                tokio::pin!(send_fut);
                let mut is_disconnected =
                  reserve_disconnected || disconnect_signal.has_changed().unwrap_or(false);
                let mut grace_timer = None::<std::pin::Pin<Box<tokio::time::Sleep>>>;

                loop {
                  if *cancel_receiver.borrow() {
                    send_failed = true;
                    break;
                  }

                  if is_disconnected && grace_timer.is_none() {
                    grace_timer = Some(Box::pin(tokio::time::sleep(DRAIN_GRACE_PERIOD)));
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
                    }
                    _ = async {
                      grace_timer
                        .as_mut()
                        .expect("disconnect drain timer")
                        .as_mut()
                        .await
                    }, if is_disconnected => {
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

    let inner = stream::unfold((receiver, budget), |(mut receiver, budget)| async move {
      receiver.recv().await.map(|item| {
        let item = item.map(|(messages, wired)| {
          budget.release(wired);
          messages
        });
        (item, (receiver, budget))
      })
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
      mpsc::channel::<Result<(CompactMessageBatch, usize)>>(DEFAULT_STREAM_BUFFER_CAPACITY);
    let (cancel_sender, mut cancel_receiver) = watch::channel(false);
    let mut task_cancel_rx = cancel_receiver.clone();
    let budget = Arc::new(ByteBudget::new(DEFAULT_STREAM_BUFFER_BYTES));
    let producer_budget = budget.clone();

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

                let wired = compact_wired_bytes(&batch_data);
                // Same disconnect preservation as the regular path: a disconnect
                // winning the budget wait must still activate the drain deadline.
                let mut reserve_disconnected =
                  disconnected || disconnect_signal.has_changed().unwrap_or(false);
                if !reserve_disconnected {
                  tokio::select! {
                    biased;
                    _ = cancel_receiver.changed() => {
                      break;
                    }
                    _ = disconnect_signal.changed() => {
                      reserve_disconnected = true;
                    }
                    _ = producer_budget.reserve(wired) => {}
                  }
                  if *cancel_receiver.borrow() {
                    break;
                  }
                }

                let mut send_failed = false;
                let pending_messages = batch_data.payloads.len();
                let send_fut = sender.send(Ok((batch_data, wired)));
                tokio::pin!(send_fut);
                let mut is_disconnected =
                  reserve_disconnected || disconnect_signal.has_changed().unwrap_or(false);
                let mut grace_timer = None::<std::pin::Pin<Box<tokio::time::Sleep>>>;
                loop {
                  if *cancel_receiver.borrow() {
                    send_failed = true;
                    break;
                  }

                  if is_disconnected && grace_timer.is_none() {
                    grace_timer = Some(Box::pin(tokio::time::sleep(DRAIN_GRACE_PERIOD)));
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
                    }
                    _ = async {
                      grace_timer
                        .as_mut()
                        .expect("disconnect drain timer")
                        .as_mut()
                        .await
                    }, if is_disconnected => {
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

    let inner = stream::unfold((receiver, budget), |(mut receiver, budget)| async move {
      receiver.recv().await.map(|item| {
        let item = item.map(|(batch, wired)| {
          budget.release(wired);
          batch
        });
        (item, (receiver, budget))
      })
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
    self.ensure_legacy("recvBatchStream")?;
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
    self.ensure_legacy("recvBatchStreamCompact")?;
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
    self.ensure_legacy("commit")?;
    if matches!(commit, CommitMode::Async) {
      // Serialize validation and scheduling with disconnect. The short native
      // enqueue below does not wait for the broker's response.
      let _lifecycle_guard = self.lifecycle_lock.lock().await;
      if self.disconnected.load(Ordering::Acquire) {
        return Err(Error::new(
          Status::InvalidArg,
          "commit('Async') requires an active onEvents() listener; the event listener is stopped after disconnect"
            .to_string(),
        ));
      }
      if self.event_listeners_active.load(Ordering::Acquire) == 0 {
        return Err(Error::new(
          Status::InvalidArg,
          "commit('Async') requires onEvents() so commit failures are observable; use 'Sync' or register onEvents first"
            .to_string(),
        ));
      }

      let mut tpl = RdTopicPartitionList::new();
      tpl
        .add_partition_offset(&topic, partition, Offset::Offset(offset))
        .map_err(|e| e.into_napi_error("Failed to add partition offset"))?;
      let mut queue = self
        .async_commit_queue
        .lock()
        .unwrap_or_else(|e| e.into_inner());
      if queue.is_none() {
        let replies = Arc::new(
          AsyncCommitQueue::new(self.stream_consumer.clone())
            .map_err(|e| e.into_napi_error("Failed to create commit reply queue"))?,
        );
        replies.start(self.disconnect_signal.1.clone());
        *queue = Some(replies);
      }
      return queue
        .as_ref()
        .expect("commit reply queue initialized")
        .enqueue(&tpl)
        .map_err(|e| e.into_napi_error("Failed to schedule commit offset"));
    }

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

  /// Whether this consumer was created with `managed: true` (RFC-0017).
  #[napi(getter)]
  pub fn managed(&self) -> bool {
    self.managed.is_some()
  }

  /// Experimental (RFC-0017). Returns the next managed record in native order:
  /// a message with the identity of its partition assignment, or an assignment
  /// transition. Every transition that preceded a message in librdkafka is
  /// returned before it; transitions are returned even when no message exists.
  ///
  /// Only one call may be pending, from the call until its promise settles: a
  /// second call in between rejects with `ERR_MANAGED_RECEIVE_PENDING`.
  /// Resolves `{ kind: 'idle' }` after `timeoutMs` (or `cancelManagedReceive`)
  /// and `null` when the consumer is disconnected while the call is pending.
  /// Other rejections (`error.code`): `ERR_MANAGED_CLOSED`, the terminal
  /// `ERR_MANAGED_CONTROL_OVERFLOW`, `ERR_MANAGED_CONTROL_FAILURE`,
  /// `ERR_MANAGED_BACKPRESSURE_FAILURE` and `ERR_MANAGED_RECEIVE_STALLED`,
  /// `ERR_MANAGED_RECEIVE_FAILED` (with `kafkaCode`; not terminal) and
  /// `ERR_MANAGED_MODE` on a legacy consumer.
  /// @param timeoutMs - Optional wait bound in milliseconds; omit to wait
  #[napi(ts_return_type = "Promise<ManagedRecord | null>")]
  pub fn recv_managed<'env>(
    &self,
    env: &'env Env,
    timeout_ms: Option<i64>,
  ) -> Result<PromiseRaw<'env, Option<ManagedRecord>>> {
    // Validated and reserved here, on the JavaScript thread, so the outcome of
    // two calls in a row, or of a cancel or disconnect right after this call,
    // does not depend on when the future below starts.
    let reserved = match &self.managed {
      None => Err(ManagedError::NotManaged),
      Some(managed) => managed_timeout("timeoutMs", timeout_ms).and_then(|timeout| {
        managed.state.reserve_receive()?;
        Ok((managed.state.clone(), timeout))
      }),
    };
    let reservation = reserved.as_ref().ok().map(|(state, _)| state.clone());
    let promise = env.spawn_future_with_callback(
      async move {
        Ok(match reserved {
          Err(error) => (None, Err(error)),
          Ok((state, timeout)) => {
            let received = state.receive_reserved(timeout).await;
            (Some(state), received)
          }
        })
      },
      |env, (state, received)| {
        // The reservation ends where the result becomes visible to JavaScript.
        if let Some(state) = state {
          state.finish_receive();
        }
        match received {
          Ok(Received::Entry(ManagedEntry::Message {
            message,
            assignment,
          })) => Ok(Some(ManagedRecord::Message {
            message,
            assignment,
          })),
          Ok(Received::Entry(ManagedEntry::Assign { partitions })) => {
            Ok(Some(ManagedRecord::Assign {
              partitions: managed_partitions(partitions),
            }))
          }
          Ok(Received::Entry(ManagedEntry::Revoke { partitions, lost })) => {
            Ok(Some(ManagedRecord::Revoke {
              partitions: managed_partitions(partitions),
              lost,
            }))
          }
          Ok(Received::Entry(ManagedEntry::ReceiveError(failure))) => {
            Err(managed_js_error(env, &ManagedError::ReceiveFailed(failure)))
          }
          Ok(Received::Idle(reason)) => Ok(Some(ManagedRecord::Idle {
            reason: reason.to_owned(),
          })),
          Ok(Received::End) => Ok(None),
          Err(error) => Err(managed_js_error(env, &error)),
        }
      },
    );
    if promise.is_err() {
      if let Some(state) = reservation {
        state.finish_receive();
      }
    }
    promise
  }

  /// Experimental (RFC-0017). Cancels the pending `recvManaged` if its result
  /// is not decided yet; it then resolves `{ kind: 'idle', reason:
  /// 'cancelled' }` without taking a record from the queue.
  /// @returns Whether a pending receive was cancelled
  #[napi]
  pub fn cancel_managed_receive(&self, env: &Env) -> Result<bool> {
    Ok(self.managed_state_or_throw(env)?.cancel_receive())
  }

  /// Experimental (RFC-0017). Commits `nextOffset` (the next offset to consume,
  /// as in `commit`) for a partition under the assignment identity that
  /// delivered the work. The binding does not check that the work up to
  /// `nextOffset` was completed.
  ///
  /// Authorization happens synchronously in this call, ordered against
  /// revocation. Commits of a partition reach the client one at a time in call
  /// order; a `nextOffset` lower than one already authorized under the
  /// identity rejects with `ERR_MANAGED_COMMIT_REGRESSION`, and an equal one
  /// is sent again. The promise settles with the broker result. There is no
  /// automatic retry.
  ///
  /// `timeoutMs` only bounds the wait: on expiry the promise rejects with
  /// `ERR_MANAGED_COMMIT_TIMEOUT` and the request stays in flight.
  ///
  /// Rejects with `error.code`: `ERR_MANAGED_STALE_ASSIGNMENT`,
  /// `ERR_MANAGED_UNKNOWN_ASSIGNMENT`, `ERR_MANAGED_COMMIT_REGRESSION`,
  /// `ERR_MANAGED_COMMIT_BACKLOG`, `ERR_MANAGED_CLOSED`, a terminal failure
  /// code, `ERR_MANAGED_COMMIT_FAILED` (with `kafkaCode`/`kafkaErrorCode`),
  /// `ERR_MANAGED_COMMIT_UNKNOWN`, `ERR_MANAGED_COMMIT_TIMEOUT`,
  /// `ERR_MANAGED_INVALID_ARGUMENT` or `ERR_MANAGED_MODE`. Commit errors carry
  /// `outcomeUnknown`, which is `false` only when nothing was sent.
  ///
  /// The identity is a local authorization, not broker-side fencing: a request
  /// dispatched before a revocation cannot be recalled and may still be applied.
  #[napi(ts_return_type = "Promise<void>")]
  pub fn commit_managed<'env>(
    &self,
    env: &'env Env,
    topic: String,
    partition: i32,
    assignment: String,
    next_offset: i64,
    timeout_ms: Option<i64>,
  ) -> Result<PromiseRaw<'env, ()>> {
    let submitted = match &self.managed {
      None => Err(ManagedError::NotManaged),
      Some(managed) => managed_timeout("timeoutMs", timeout_ms).and_then(|timeout| {
        managed
          .submit_commit(&topic, partition, &assignment, next_offset)
          .map(|result| (result, timeout))
      }),
    };
    env.spawn_future_with_callback(
      async move {
        let (result, timeout) = match submitted {
          Ok(submitted) => submitted,
          Err(error) => return Ok(Err(error)),
        };
        // The broker result is awaited without holding any lock.
        let settled = match timeout {
          None => result.await,
          Some(timeout) => match tokio::time::timeout(timeout, result).await {
            Ok(settled) => settled,
            // Only the observation ends; the request is not cancelled.
            Err(_) => return Ok(Err(ManagedError::CommitTimeout)),
          },
        };
        Ok(settled.unwrap_or_else(|_| {
          Err(ManagedError::CommitUnknown(
            "commit result channel closed".to_owned(),
          ))
        }))
      },
      |env, outcome: std::result::Result<(), ManagedError>| {
        outcome.map_err(|error| managed_js_error(env, &error))
      },
    )
  }

  /// Experimental (RFC-0017). Whether `assignment` is the identity that
  /// currently owns the partition, read from native state without waiting for
  /// `recvManaged`. A `true` result can be invalidated at any time; only
  /// `commitManaged` orders the check against revocation.
  #[napi]
  pub fn is_managed_assignment_active(
    &self,
    env: &Env,
    topic: String,
    partition: i32,
    assignment: String,
  ) -> Result<bool> {
    Ok(
      self
        .managed_state_or_throw(env)?
        .is_assignment_active(&topic, partition, &assignment),
    )
  }

  /// Experimental (RFC-0017). Diagnostic snapshot of the managed native state.
  #[napi]
  pub fn managed_state(&self, env: &Env) -> Result<ManagedStateSnapshot> {
    let snapshot = self.managed_state_or_throw(env)?.snapshot();
    let (status, failure) = match &snapshot.status {
      ManagedStatus::Active => ("active", None),
      ManagedStatus::Closed => ("closed", None),
      ManagedStatus::Failed(error) => ("failed", Some(managed_error_code(error).to_owned())),
    };
    Ok(ManagedStateSnapshot {
      status: status.to_owned(),
      failure,
      assignments: snapshot
        .assignments
        .into_iter()
        .map(|(topic, partition, assignment)| ManagedPartition {
          topic,
          partition,
          assignment: Some(assignment),
          previous_assignment: None,
        })
        .collect(),
      queued_control_records: snapshot.queued_control_records as u32,
      buffered_messages: snapshot.buffered_messages as u32,
      buffered_bytes: snapshot.buffered_bytes as f64,
      backpressure_paused: snapshot.backpressure_paused,
      pending_commits: snapshot.pending_commits as u32,
      dropped_receive_errors: snapshot.dropped_receive_errors as f64,
      purged_messages: snapshot.purged_messages as f64,
    })
  }
}

impl Drop for KafkaConsumer {
  fn drop(&mut self) {
    // The JavaScript consumer was collected without disconnect: nothing polls
    // any more (the pump ends with the disconnect sender dropped here, and
    // there is none before subscribe), so settle a receive that is still
    // pending and refuse commits that were not dispatched. The blocking
    // native close itself runs on the blocking pool (`LoggingConsumer::drop`),
    // whichever thread releases the last reference.
    if let Some(managed) = &self.managed {
      managed.state.close();
    }
  }
}

#[cfg(test)]
mod managed_error_tests {
  use super::*;

  fn failed(code: rdsys::rd_kafka_resp_err_t, dispatched: bool) -> ManagedError {
    ManagedError::CommitFailed {
      kafka: KafkaCode {
        name: format!("{code:?}"),
        code: code as i32,
      },
      dispatched,
    }
  }

  /// librdkafka may retry or defer a commit after an attempt whose fate is
  /// unknown (`_TRANSPORT`, then for example `_WAIT_COORD` or a generation
  /// error), so no final code of a dispatched request is treated as proof
  /// that nothing was applied.
  #[test]
  fn outcome_is_unknown_for_every_error_of_a_dispatched_commit() {
    use rdsys::rd_kafka_resp_err_t as E;
    for code in [
      E::RD_KAFKA_RESP_ERR__TRANSPORT,
      E::RD_KAFKA_RESP_ERR__WAIT_COORD,
      E::RD_KAFKA_RESP_ERR__TIMED_OUT,
      E::RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT,
      E::RD_KAFKA_RESP_ERR_NOT_COORDINATOR,
      E::RD_KAFKA_RESP_ERR_COORDINATOR_NOT_AVAILABLE,
      E::RD_KAFKA_RESP_ERR_ILLEGAL_GENERATION,
      E::RD_KAFKA_RESP_ERR_REBALANCE_IN_PROGRESS,
      E::RD_KAFKA_RESP_ERR_UNKNOWN_MEMBER_ID,
    ] {
      assert_eq!(commit_outcome_unknown(&failed(code, true)), Some(true));
      // The same code is definitive when the request was never handed over.
      assert_eq!(commit_outcome_unknown(&failed(code, false)), Some(false));
    }
    assert_eq!(
      commit_outcome_unknown(&ManagedError::CommitUnknown(String::new())),
      Some(true)
    );
    assert_eq!(
      commit_outcome_unknown(&ManagedError::CommitTimeout),
      Some(true)
    );
    for definitive in [
      ManagedError::StaleAssignment,
      ManagedError::UnknownAssignment(String::new()),
      ManagedError::CommitRegression { authorized: 1 },
      ManagedError::CommitBacklog,
    ] {
      assert_eq!(commit_outcome_unknown(&definitive), Some(false));
    }
    // Not commit outcomes: the property is absent.
    assert_eq!(commit_outcome_unknown(&ManagedError::Closed), None);
    assert_eq!(commit_outcome_unknown(&ManagedError::ReceivePending), None);
  }
}
