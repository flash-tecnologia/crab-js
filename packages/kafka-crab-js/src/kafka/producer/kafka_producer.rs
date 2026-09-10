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
use std::{
  sync::{Arc, Mutex},
  time::Duration,
};
use tokio::sync::oneshot;
use tracing::{debug, info};

use crate::kafka::kafka_util::{convert_config_values_to_strings, hashmap_to_kafka_headers};

use super::model::{
  KafkaCrabError, MessageProducer, ProducerConfiguration, ProducerRecord, RecordMetadata,
};

const DEFAULT_QUEUE_TIMEOUT: i64 = 5000;

/// Per-message delivery result moved through a `oneshot` channel owned by the
/// send (auto mode) or the manual pending list (manual mode). A dropped
/// receiver (timeout, shutdown) turns the late callback into a harmless no-op:
/// there is no shared map to leak or to steal from.
#[derive(Debug)]
struct DeliveryResultData {
  topic: String,
  partition: i32,
  offset: i64,
  error: Option<KafkaError>,
}

#[derive(Clone)]
struct CollectingContext<Part: Partitioner = NoCustomPartitioner> {
  partitioner: Option<Part>,
}

impl CollectingContext {
  fn new() -> CollectingContext {
    CollectingContext { partitioner: None }
  }
}

impl<Part: Partitioner + Send + Sync> ClientContext for CollectingContext<Part> {
  fn stats(&self, stats: Statistics) {
    debug!("Stats: {:?}", stats);
  }
}

impl<Part: Partitioner + Send + Sync> ProducerContext<Part> for CollectingContext<Part> {
  type DeliveryOpaque = Box<oneshot::Sender<DeliveryResultData>>;

  fn delivery(&self, delivery_result: &DeliveryResult, delivery_opaque: Self::DeliveryOpaque) {
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
    // `Err` means the receiver is gone (timeout or shutdown): the result has
    // no owner anymore and is dropped. This is the late-callback path, and it
    // needs no map lookup, expiry, or eviction to be safe.
    let _ = delivery_opaque.send(DeliveryResultData {
      topic,
      partition,
      offset,
      error: err,
    });
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
  producer: Arc<ThreadedProducer<CollectingContext>>,
  /// Receivers stashed by `send()` in manual mode, drained by `flush()`.
  /// Each receiver has exactly one owner at a time, so concurrent operations
  /// cannot steal each other's confirmations.
  manual_pending: Arc<Mutex<Vec<oneshot::Receiver<DeliveryResultData>>>>,
  /// Last confirmed batch, kept for the `getLastDeliveryResults()` compat API.
  /// Exact for serial use; concurrent native-direct sends may overwrite it.
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
      threaded_producer_with_context(context, producer_config)?;

    Ok(KafkaProducer {
      queue_timeout,
      auto_flush,
      producer: Arc::new(producer),
      manual_pending: Arc::new(Mutex::new(Vec::new())),
      last_delivery_results: Arc::new(Mutex::new(Vec::new())),
    })
  }

  /// Pumps the librdkafka queue off the async runtime.
  async fn flush_queue(&self) -> Result<()> {
    let producer = self.producer.clone();
    let queue_timeout = self.queue_timeout;
    let join = tokio::task::spawn_blocking(move || producer.flush(queue_timeout)).await;
    let inner = match join {
      Ok(result) => result,
      Err(e) => {
        return Err(Error::new(
          Status::GenericFailure,
          format!("Flush task join error: {e}"),
        ))
      }
    };
    match inner {
      Ok(()) => Ok(()),
      Err(e) => Err(Error::new(Status::GenericFailure, e.to_string())),
    }
  }

  /// Returns the number of messages that are currently in-flight (sent but not yet acknowledged).
  /// This can be used to implement backpressure or monitor producer health.
  #[napi]
  pub fn in_flight_count(&self) -> Result<i32> {
    Ok(self.producer.in_flight_count())
  }
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
      self.flush_queue().await?;
      Ok(vec![])
    } else {
      let receivers = std::mem::take(
        &mut *self
          .manual_pending
          .lock()
          .unwrap_or_else(|e| e.into_inner()),
      );
      self.flush_receivers(receivers).await
    }
  }

  /// Pumps the queue, then gathers owned confirmations. Used by manual `flush()`.
  async fn flush_receivers(
    &self,
    receivers: Vec<oneshot::Receiver<DeliveryResultData>>,
  ) -> Result<Vec<RecordMetadata>> {
    let deadline = tokio::time::Instant::now() + self.queue_timeout;
    let flush_res = self.flush_queue().await;
    let (result, last_err) = Self::gather(receivers, deadline).await;

    *self
      .last_delivery_results
      .lock()
      .unwrap_or_else(|e| e.into_inner()) = result.clone();

    if let Err(e) = flush_res {
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

  /// Awaits owned receivers within `deadline`, in order. Unconfirmed results
  /// are abandoned by dropping their receivers: expiry needs no eviction pass
  /// because nothing is shared.
  async fn gather(
    receivers: Vec<oneshot::Receiver<DeliveryResultData>>,
    deadline: tokio::time::Instant,
  ) -> (Vec<RecordMetadata>, Option<KafkaError>) {
    let mut result = Vec::with_capacity(receivers.len());
    let mut last_err = None;
    for rx in receivers {
      // Anything else (sender dropped, budget expired) abandons the
      // confirmation: expiry needs no eviction pass because nothing is shared.
      if let Ok(Ok(data)) = tokio::time::timeout_at(deadline, rx).await {
        if let Some(err) = data.error.as_ref() {
          last_err = Some(err.clone());
        }
        result.push(to_record_metadata(&data));
      }
    }
    (result, last_err)
  }

  /// Sends one or more messages to a Kafka topic.
  /// Messages are sent asynchronously and delivery is confirmed based on the autoFlush setting.
  /// @param producerRecord - The record containing the topic and messages to send
  /// @returns Array of RecordMetadata for each delivered message (empty if autoFlush is disabled)
  #[napi]
  pub async fn send(&self, producer_record: ProducerRecord) -> Result<Vec<RecordMetadata>> {
    let topic = producer_record.topic.as_str();
    let total = producer_record.messages.len();
    let deadline = tokio::time::Instant::now() + self.queue_timeout;

    let mut receivers = Vec::with_capacity(total);
    let mut send_err = None;

    for message in producer_record.messages {
      let (tx, rx) = oneshot::channel();
      match self.send_single_message(topic, &message, Box::new(tx)) {
        Ok(()) => receivers.push(rx),
        // Both halves are dropped: the message was never enqueued, so no
        // callback will ever arrive for it.
        Err(e) => {
          send_err = Some(e);
          break;
        }
      }
    }
    let enqueued = receivers.len();

    if let Some(err) = send_err {
      if self.auto_flush && !receivers.is_empty() {
        let flush_res = self.flush_queue().await;
        let (confirmed, _) = Self::gather(receivers, deadline).await;
        let confirmed_count = confirmed.len();
        *self
          .last_delivery_results
          .lock()
          .unwrap_or_else(|e| e.into_inner()) = confirmed;
        let underlying_err = flush_res
          .err()
          .map(|e| e.reason)
          .unwrap_or_else(|| err.to_string());
        return Err(Error::new(
          Status::GenericFailure,
          partial_send_error_message(enqueued, total, confirmed_count, &underlying_err),
        ));
      }
      if !self.auto_flush {
        // Live receivers stay owned by the manual pending list for a later flush.
        self
          .manual_pending
          .lock()
          .unwrap_or_else(|e| e.into_inner())
          .append(&mut receivers);
        *self
          .last_delivery_results
          .lock()
          .unwrap_or_else(|e| e.into_inner()) = Vec::new();
      }
      return Err(Error::new(
        Status::GenericFailure,
        partial_send_error_message(enqueued, total, 0, &err.to_string()),
      ));
    }

    if self.auto_flush {
      let flush_res = self.flush_queue().await;
      let (confirmed, delivery_err) = Self::gather(receivers, deadline).await;
      *self
        .last_delivery_results
        .lock()
        .unwrap_or_else(|e| e.into_inner()) = confirmed.clone();
      flush_res?;
      if let Some(e) = delivery_err {
        return Err(Error::new(
          Status::GenericFailure,
          format!("Message delivery failed: {e}"),
        ));
      }
      Ok(confirmed)
    } else {
      self
        .manual_pending
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .append(&mut receivers);
      Ok(vec![])
    }
  }

  fn send_single_message(
    &self,
    topic: &str,
    message: &MessageProducer,
    opaque: Box<oneshot::Sender<DeliveryResultData>>,
  ) -> Result<()> {
    let headers = message
      .headers
      .as_ref()
      .map_or_else(OwnedHeaders::new, hashmap_to_kafka_headers);

    // Preserve Kafka semantics: None => no key (round-robin), Some => hashed partition
    let key = message.key.as_deref().map(ToBytes::to_bytes);

    let mut record: BaseRecord<'_, [u8], [u8], Box<oneshot::Sender<DeliveryResultData>>> =
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
