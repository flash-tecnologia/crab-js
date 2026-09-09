use std::{
  sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
  },
  time::Duration,
};

use dashmap::{mapref::entry::Entry, DashMap};

use nanoid::nanoid;
use napi::{Error, Result, Status};
use rdkafka::message::{DeliveryResult, OwnedHeaders};
use rdkafka::producer::Partitioner;
use rdkafka::producer::ThreadedProducer;
use rdkafka::{
  error::KafkaError,
  producer::{BaseRecord, NoCustomPartitioner},
  ClientConfig, ClientContext, Message, Statistics,
};
use rdkafka::{
  message::ToBytes,
  producer::{Producer, ProducerContext},
};
use tracing::{debug, info};

use crate::kafka::kafka_util::{convert_config_values_to_strings, hashmap_to_kafka_headers};

use super::model::{
  KafkaCrabError, MessageProducer, ProducerConfiguration, ProducerRecord, RecordMetadata,
};

const DEFAULT_QUEUE_TIMEOUT: i64 = 5000;
// Message ID generation constants for optimal string allocation
const PREFIX_ID_LEN: usize = 5; // nanoid!(5) generates 5 characters
const MAX_U64_DIGITS: usize = 20; // Maximum digits in u64::MAX
const CAPACITY: usize = PREFIX_ID_LEN + 1 + MAX_U64_DIGITS; // prefix + "_" + counter = 26

#[derive(Clone, Debug)]
struct DeliveryResultData {
  topic: String,
  partition: i32,
  offset: i64,
  error: Option<KafkaError>,
}

#[derive(Clone, Debug)]
enum MessageDeliveryState {
  Pending,
  Delivered(DeliveryResultData),
}

#[derive(Clone)]
struct CollectingContext<Part: Partitioner = NoCustomPartitioner> {
  entries: Arc<DashMap<String, MessageDeliveryState>>,
  partitioner: Option<Part>,
}

impl CollectingContext {
  fn new() -> CollectingContext {
    CollectingContext {
      entries: Arc::new(DashMap::new()),
      partitioner: None,
    }
  }

  fn register_pending_id(&self, id: &str) {
    self
      .entries
      .insert(id.to_string(), MessageDeliveryState::Pending);
  }

  fn unregister_pending_id(&self, id: &str) {
    self.entries.remove(id);
  }
}

impl<Part: Partitioner + Send + Sync> ClientContext for CollectingContext<Part> {
  fn stats(&self, stats: Statistics) {
    debug!("Stats: {:?}", stats);
  }
}

impl<Part: Partitioner + Send + Sync> ProducerContext<Part> for CollectingContext<Part> {
  type DeliveryOpaque = Arc<String>;

  fn delivery(&self, delivery_result: &DeliveryResult, delivery_opaque: Self::DeliveryOpaque) {
    let id = delivery_opaque.as_str();
    match self.entries.entry(id.to_string()) {
      Entry::Occupied(mut entry) => {
        if matches!(*entry.get(), MessageDeliveryState::Pending) {
          let (topic, partition, offset, err) = match *delivery_result {
            Ok(ref message) => (
              message.topic().to_string(),
              message.partition(),
              message.offset(),
              None,
            ),
            Err((ref err, ref message)) => (
              message.topic().to_string(),
              message.partition(),
              message.offset(),
              Some(err.clone()),
            ),
          };
          *entry.get_mut() = MessageDeliveryState::Delivered(DeliveryResultData {
            topic,
            partition,
            offset,
            error: err,
          });
        }
      }
      Entry::Vacant(_) => {
        debug!(
          "Discarding late delivery result for expired or untracked message ID: {}",
          id
        );
      }
    }
  }

  fn get_custom_partitioner(&self) -> Option<&Part> {
    self.partitioner.as_ref()
  }
}

fn threaded_producer_with_context<Part, C>(
  context: C,
  client_config: ClientConfig,
) -> Result<ThreadedProducer<C, Part>>
where
  Part: Partitioner + Send + Sync + 'static,
  C: ProducerContext<Part>,
{
  client_config
    .create_with_context::<C, ThreadedProducer<_, _>>(context)
    .map_err(|e| {
      Error::new(
        Status::GenericFailure,
        format!("Failed to create producer: {e}"),
      )
    })
}

#[napi]
pub struct KafkaProducer {
  queue_timeout: Duration,
  auto_flush: bool,
  context: CollectingContext,
  producer: Arc<ThreadedProducer<CollectingContext>>,
  counter: Arc<AtomicU64>,
  // Pre-calculated prefix for efficient message ID generation (nanoid(5) + "_")
  id_prefix: String,
  last_delivery_results: Arc<Mutex<Vec<RecordMetadata>>>,
}

#[napi]
impl KafkaProducer {
  pub fn new(
    client_config: ClientConfig,
    producer_configuration: ProducerConfiguration,
  ) -> Result<Self> {
    let mut producer_config = client_config;

    if let Some(config) = producer_configuration.configuration {
      let string_config = convert_config_values_to_strings(config);
      producer_config.extend(string_config);
    }

    let queue_timeout = Duration::from_millis(
      producer_configuration
        .queue_timeout
        .unwrap_or(DEFAULT_QUEUE_TIMEOUT)
        .try_into()
        .map_err(|e| Error::new(Status::GenericFailure, e))?,
    );

    let auto_flush = producer_configuration.auto_flush.unwrap_or(true);

    if !auto_flush {
      info!("Auto flush is disabled. You must call flush() manually.");
    }

    let context = CollectingContext::new();
    let producer: ThreadedProducer<CollectingContext> =
      threaded_producer_with_context(context.clone(), producer_config)?;

    let id_prefix = format!("{}_", nanoid!(PREFIX_ID_LEN));

    Ok(KafkaProducer {
      queue_timeout,
      auto_flush,
      context,
      producer: Arc::new(producer),
      counter: Arc::new(AtomicU64::new(1)),
      id_prefix,
      last_delivery_results: Arc::new(Mutex::new(Vec::new())),
    })
  }

  /// Returns the number of messages that are currently in-flight (sent but not yet acknowledged).
  /// This can be used to implement backpressure or monitor producer health.
  #[napi]
  pub fn in_flight_count(&self) -> Result<i32> {
    Ok(self.producer.in_flight_count())
  }

  /// Returns the confirmed delivery results from the most recent send operation.
  /// Useful for recovering delivery metadata when a send operation encounters a partial failure.
  #[napi]
  pub fn get_last_delivery_results(&self) -> Vec<RecordMetadata> {
    self
      .last_delivery_results
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .clone()
  }

  /// Flushes all pending messages to the Kafka broker and waits for delivery confirmation.
  /// When autoFlush is enabled (default), this returns an empty array as messages are flushed automatically.
  /// When autoFlush is disabled, this must be called manually to send buffered messages.
  /// @returns Array of RecordMetadata for each delivered message
  #[napi]
  pub async fn flush(&self) -> Result<Vec<RecordMetadata>> {
    if self.auto_flush {
      let producer = self.producer.clone();
      let queue_timeout = self.queue_timeout;
      tokio::task::spawn_blocking(move || producer.flush(queue_timeout))
        .await
        .map_err(|e| {
          Error::new(
            Status::GenericFailure,
            format!("Flush task join error: {e}"),
          )
        })?
        .map_err(|e| Error::new(Status::GenericFailure, e))?;
      Ok(vec![])
    } else {
      self.flush_delivery_results().await
    }
  }

  /// Sends one or more messages to a Kafka topic.
  /// Messages are sent asynchronously and delivery is confirmed based on the autoFlush setting.
  /// @param producerRecord - The record containing the topic and messages to send
  /// @returns Array of RecordMetadata for each delivered message (empty if autoFlush is disabled)
  #[napi]
  pub async fn send(&self, producer_record: ProducerRecord) -> Result<Vec<RecordMetadata>> {
    let topic = producer_record.topic.as_str();

    let ids: Vec<String> = (0..producer_record.messages.len())
      .map(|_| self.generate_message_id())
      .collect();

    let mut sent_ids = Vec::with_capacity(producer_record.messages.len());
    let mut send_err = None;

    for (message, record_id) in producer_record.messages.into_iter().zip(ids.iter()) {
      self.context.register_pending_id(record_id);
      match self.send_single_message(topic, &message, record_id) {
        Ok(()) => {
          sent_ids.push(record_id.clone());
        }
        Err(e) => {
          self.context.unregister_pending_id(record_id);
          send_err = Some(e);
          break;
        }
      }
    }

    if let Some(err) = send_err {
      // Registration is prefix-ordered until the break above: everything from
      // sent_ids.len() on was never enqueued (the failed id was already
      // unregistered; removing it again is a harmless no-op).
      for id in ids.iter().skip(sent_ids.len()) {
        self.context.unregister_pending_id(id);
      }

      let (confirmed, flush_err) = if self.auto_flush && !sent_ids.is_empty() {
        self.flush_delivery_results_with_filter(&sent_ids).await
      } else {
        (Vec::new(), None)
      };

      let confirmed_count = confirmed.len();
      *self
        .last_delivery_results
        .lock()
        .unwrap_or_else(|e| e.into_inner()) = confirmed;

      let underlying_err = flush_err
        .map(|e| e.reason)
        .unwrap_or_else(|| err.to_string());

      return Err(Error::new(
        Status::GenericFailure,
        partial_send_error_message(sent_ids.len(), ids.len(), confirmed_count, &underlying_err),
      ));
    }

    if self.auto_flush {
      let (confirmed, flush_err) = self.flush_delivery_results_with_filter(&sent_ids).await;
      *self
        .last_delivery_results
        .lock()
        .unwrap_or_else(|e| e.into_inner()) = confirmed.clone();
      if let Some(err) = flush_err {
        return Err(err);
      }
      Ok(confirmed)
    } else {
      Ok(vec![])
    }
  }

  /// Generates a fast, unique message ID using atomic counter and pre-allocated prefix
  /// This is ~2-3x faster than the previous format!() approach for high-throughput scenarios
  fn generate_message_id(&self) -> String {
    let id = self.counter.fetch_add(1, Ordering::Relaxed);

    // Use pre-allocated prefix and efficient string building with constant capacity
    let mut result = String::with_capacity(CAPACITY);
    result.push_str(&self.id_prefix);

    // Use write! macro for efficient integer formatting directly into the string
    use std::fmt::Write;
    let _ = write!(result, "{id}"); // write! to String never fails

    result
  }

  fn send_single_message(
    &self,
    topic: &str,
    message: &MessageProducer,
    record_id: &str,
  ) -> Result<()> {
    let headers = message
      .headers
      .as_ref()
      .map_or_else(OwnedHeaders::new, hashmap_to_kafka_headers);

    // Preserve Kafka semantics: None => no key (round-robin), Some => hashed partition
    let key = message.key.as_deref().map(ToBytes::to_bytes);

    let opaque = Arc::new(record_id.to_string());
    let mut record: BaseRecord<'_, [u8], [u8], Arc<String>> =
      BaseRecord::with_opaque_to(topic, opaque).headers(headers);

    if message.is_tombstone == Some(true) && message.payload.is_some() {
      return Err(Error::new(
        Status::InvalidArg,
        "isTombstone: true cannot be combined with a payload; omit the payload for tombstones"
          .to_string(),
      ));
    }

    let is_tombstone = message.is_tombstone.unwrap_or(false) || message.payload.is_none();
    if !is_tombstone {
      if let Some(ref payload) = message.payload {
        record = record.payload(payload.as_ref());
      }
    }

    if let Some(key) = key {
      record = record.key(key);
    }

    self
      .producer
      .send(record)
      .map_err(|(e, _)| Error::new(Status::GenericFailure, e.to_string()))?;

    Ok(())
  }

  async fn flush_delivery_results(&self) -> Result<Vec<RecordMetadata>> {
    let producer = self.producer.clone();
    let queue_timeout = self.queue_timeout;
    let target_keys: Vec<String> = self
      .context
      .entries
      .iter()
      .map(|entry| entry.key().clone())
      .collect();

    let flush_res = tokio::task::spawn_blocking(move || producer.flush(queue_timeout))
      .await
      .map_err(|e| {
        Error::new(
          Status::GenericFailure,
          format!("Flush task join error: {e}"),
        )
      })?
      .map_err(|e| Error::new(Status::GenericFailure, e));

    let mut result = Vec::with_capacity(target_keys.len());
    let mut last_err = None;
    for key in &target_keys {
      if let Some((_, MessageDeliveryState::Delivered(item))) = self.context.entries.remove(key) {
        if let Some(ref err) = item.error {
          last_err = Some(err.clone());
        }
        result.push(to_record_metadata(&item));
      }
    }

    *self
      .last_delivery_results
      .lock()
      .unwrap_or_else(|e| e.into_inner()) = result.clone();

    if let Err(e) = flush_res {
      // The snapshot had a full flush window and still did not confirm: evict
      // stuck Pending entries so the map stays bounded across outages.
      // Late deliveries for evicted ids are discarded (see delivery()).
      for key in &target_keys {
        if let Entry::Occupied(entry) = self.context.entries.entry(key.clone()) {
          if matches!(entry.get(), MessageDeliveryState::Pending) {
            entry.remove();
          }
        }
      }
      if result.is_empty() {
        return Err(e);
      }
      return Err(Error::new(
        Status::GenericFailure,
        format!(
          "Flush completed with error ({} messages confirmed): {}",
          result.len(),
          e
        ),
      ));
    }

    if let Some(e) = last_err {
      return Err(Error::new(
        Status::GenericFailure,
        format!("Message delivery failed: {e}"),
      ));
    }

    Ok(result)
  }

  async fn flush_delivery_results_with_filter(
    &self,
    ids: &[String],
  ) -> (Vec<RecordMetadata>, Option<Error>) {
    let producer = self.producer.clone();
    let queue_timeout = self.queue_timeout;
    let flush_res = tokio::task::spawn_blocking(move || producer.flush(queue_timeout))
      .await
      .map_err(|e| {
        Error::new(
          Status::GenericFailure,
          format!("Flush task join error: {e}"),
        )
      })
      .and_then(|r| r.map_err(|e| Error::new(Status::GenericFailure, e)));

    let mut result = Vec::with_capacity(ids.len());
    let mut last_err = None;
    let mut unavailable = 0;
    for id in ids {
      match self.context.entries.entry(id.clone()) {
        Entry::Occupied(entry) => {
          // Pending entries stay: still in flight, attributed to a later flush.
          if matches!(entry.get(), MessageDeliveryState::Delivered(_)) {
            let state = entry.remove();
            if let MessageDeliveryState::Delivered(item) = state {
              if let Some(ref err) = item.error {
                last_err = Some(err.clone());
              }
              result.push(to_record_metadata(&item));
            }
          }
        }
        // Every id was registered before enqueue, so a missing entry means its
        // result was consumed or expired by another flush.
        Entry::Vacant(_) => {
          unavailable += 1;
        }
      }
    }

    let err = match flush_res {
      Err(e) => {
        // Same bounded-map guarantee as flush_delivery_results: evict snapshot
        // entries that never confirmed within the flush window.
        for id in ids {
          if let Entry::Occupied(entry) = self.context.entries.entry(id.clone()) {
            if matches!(entry.get(), MessageDeliveryState::Pending) {
              entry.remove();
            }
          }
        }
        Some(e)
      }
      Ok(_) => {
        let mut err = last_err.map(|e| {
          Error::new(
            Status::GenericFailure,
            format!("Message delivery failed: {e}"),
          )
        });
        if unavailable > 0 {
          let concurrent = Error::new(
            Status::GenericFailure,
            format!(
              "{unavailable} of {} delivery results were consumed or expired by another flush; avoid concurrent flush() with in-flight send()",
              ids.len()
            ),
          );
          err = Some(match err {
            Some(e) => Error::new(
              Status::GenericFailure,
              format!("{}; {}", e.reason, concurrent.reason),
            ),
            None => concurrent,
          });
        }
        err
      }
    };

    (result, err)
  }
}

fn to_record_metadata(data: &DeliveryResultData) -> RecordMetadata {
  RecordMetadata {
    topic: data.topic.clone(),
    partition: data.partition,
    offset: data.offset,
    error: data.error.as_ref().map(|err| KafkaCrabError {
      code: err
        .rdkafka_error_code()
        .unwrap_or(rdkafka::types::RDKafkaErrorCode::Unknown) as i32,
      message: err.to_string(),
    }),
  }
}
/// Machine-readable partial-failure message shared with the JS layer, which
/// parses `enqueued X of Y, confirmed Z` to fill `SendFailureError`. The exact
/// wording is pinned by the M06 partial-failure regression test in
/// js-tests/unit/regressions.test.ts; change both sides together.
fn partial_send_error_message(
  enqueued: usize,
  total: usize,
  confirmed: usize,
  underlying: &str,
) -> String {
  format!("Failed to send all messages (enqueued {enqueued} of {total}, confirmed {confirmed}): {underlying}")
}
