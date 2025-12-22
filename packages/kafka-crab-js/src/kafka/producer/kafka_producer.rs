use std::{
  collections::HashSet,
  sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
  },
  time::Duration,
};

use dashmap::DashMap;

use nanoid::nanoid;
use napi::{Error, Result, Status};
use rdkafka::{
  error::KafkaError,
  message::{OwnedHeaders, OwnedMessage},
  producer::{BaseRecord, DeliveryResult, NoCustomPartitioner, Partitioner, ThreadedProducer},
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

type ProducerDeliveryResult = (OwnedMessage, Option<KafkaError>, Arc<String>);

#[derive(Clone)]
struct CollectingContext<Part: Partitioner = NoCustomPartitioner> {
  results: Arc<DashMap<String, ProducerDeliveryResult>>,
  partitioner: Option<Part>,
}

impl CollectingContext {
  fn new() -> CollectingContext {
    CollectingContext {
      results: Arc::new(DashMap::new()),
      partitioner: None,
    }
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
    let (message, err) = match *delivery_result {
      Ok(ref message) => (message.detach(), None),
      Err((ref err, ref message)) => (message.detach(), Some(err.clone())),
    };
    self
      .results
      .insert((*delivery_opaque).clone(), (message, err, delivery_opaque));
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
  producer: ThreadedProducer<CollectingContext>,
  counter: Arc<AtomicU64>,
  // Pre-calculated prefix for efficient message ID generation (nanoid(5) + "_")
  id_prefix: String,
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
      producer,
      counter: Arc::new(AtomicU64::new(1)),
      id_prefix,
    })
  }

  /// Returns the number of messages that are currently in-flight (sent but not yet acknowledged).
  /// This can be used to implement backpressure or monitor producer health.
  #[napi]
  pub fn in_flight_count(&self) -> Result<i32> {
    Ok(self.producer.in_flight_count())
  }

  /// Flushes all pending messages to the Kafka broker and waits for delivery confirmation.
  /// When autoFlush is enabled (default), this returns an empty array as messages are flushed automatically.
  /// When autoFlush is disabled, this must be called manually to send buffered messages.
  /// @returns Array of RecordMetadata for each delivered message
  #[napi]
  pub async fn flush(&self) -> Result<Vec<RecordMetadata>> {
    if self.auto_flush {
      Ok(vec![])
    } else {
      self.flush_delivery_results()
    }
  }

  /// Sends one or more messages to a Kafka topic.
  /// Messages are sent asynchronously and delivery is confirmed based on the autoFlush setting.
  /// @param producerRecord - The record containing the topic and messages to send
  /// @returns Array of RecordMetadata for each delivered message (empty if autoFlush is disabled)
  #[napi]
  pub async fn send(&self, producer_record: ProducerRecord) -> Result<Vec<RecordMetadata>> {
    let topic = producer_record.topic.as_str();

    // Pre-allocate HashSet capacity for better performance
    let mut ids = HashSet::with_capacity(producer_record.messages.len());
    for _ in &producer_record.messages {
      ids.insert(self.generate_message_id());
    }

    for (message, record_id) in producer_record.messages.into_iter().zip(ids.iter()) {
      self.send_single_message(topic, &message, record_id)?;
    }

    if self.auto_flush {
      self.flush_delivery_results_with_filter(&ids)
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
      BaseRecord::with_opaque_to(topic, opaque)
        .payload(message.payload.to_bytes())
        .headers(headers);

    if let Some(key) = key {
      record = record.key(key);
    }

    self
      .producer
      .send(record)
      .map_err(|(e, _)| Error::new(Status::GenericFailure, e.to_string()))?;

    Ok(())
  }

  fn flush_delivery_results(&self) -> Result<Vec<RecordMetadata>> {
    self
      .producer
      .flush(self.queue_timeout)
      .map_err(|e| Error::new(Status::GenericFailure, e))?;

    let delivery_results = &self.context.results;
    let result: Vec<RecordMetadata> = delivery_results
      .iter()
      .map(|entry| {
        let (message, error, _) = entry.value();
        to_record_metadata(message, error)
      })
      .collect();
    delivery_results.clear();
    Ok(result)
  }

  fn flush_delivery_results_with_filter(
    &self,
    ids: &HashSet<String>,
  ) -> Result<Vec<RecordMetadata>> {
    self
      .producer
      .flush(self.queue_timeout)
      .map_err(|e| Error::new(Status::GenericFailure, e))?;

    let delivery_results = &self.context.results;
    let result: Vec<RecordMetadata> = delivery_results
      .iter()
      .filter(|entry| ids.contains(entry.key()))
      .map(|entry| {
        let (message, error, _) = entry.value();
        to_record_metadata(message, error)
      })
      .collect();

    // Remove processed entries
    delivery_results.retain(|key, _| !ids.contains(key));
    Ok(result)
  }
}

fn to_record_metadata(message: &OwnedMessage, error: &Option<KafkaError>) -> RecordMetadata {
  RecordMetadata {
    topic: message.topic().to_string(),
    partition: message.partition(),
    offset: message.offset(),
    error: error.as_ref().map(|err| KafkaCrabError {
      code: err
        .rdkafka_error_code()
        .unwrap_or(rdkafka::types::RDKafkaErrorCode::Unknown) as i32,
      message: err.to_string(),
    }),
  }
}
