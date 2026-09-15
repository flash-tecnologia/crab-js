use std::{collections::HashMap, fmt, str::FromStr, sync::Once};

use napi::{bindgen_prelude::*, Result};
use rdkafka::{
  config::{ClientConfig, RDKafkaLogLevel},
  topic_partition_list::TopicPartitionList,
  Offset,
};

use tracing::{trace, warn, Level};

use crate::kafka::kafka_util::{convert_config_values_to_strings, IntoNapiError};

use super::{
  consumer::{
    consumer_helper::convert_tpl_to_array_of_topic_partition,
    kafka_consumer::KafkaConsumer,
    model::{ConsumerConfiguration, TopicPartition},
  },
  kafka_admin::KafkaAdmin,
  producer::{kafka_producer::KafkaProducer, model::ProducerConfiguration},
};

// Global singleton for tracing initialization
static TRACING_INIT: Once = Once::new();

/// Initialize tracing subscriber once globally
/// This prevents "global default trace dispatcher already set" errors
/// when multiple KafkaClient instances are created
fn init_tracing_once(log_level: Option<String>) {
  TRACING_INIT.call_once(|| {
    let level = log_level
      .as_deref()
      .and_then(|s| Level::from_str(s).ok())
      .unwrap_or(Level::ERROR);

    match tracing_subscriber::fmt()
      .with_max_level(level)
      .json()
      .with_ansi(false)
      .try_init()
    {
      Ok(_) => {
        trace!("Tracing initialized successfully with level: {:?}", level);
      }
      Err(e) => {
        // This should never happen since we use Once, but handle it gracefully
        warn!("Tracing initialization failed: {:?}", e);
      }
    }
  });
}

#[derive(Clone, Debug)]
#[napi(string_enum)]
pub enum SecurityProtocol {
  Plaintext,
  Ssl,
  SaslPlaintext,
  SaslSsl,
}

impl fmt::Display for SecurityProtocol {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match self {
      SecurityProtocol::Plaintext => write!(f, "plaintext"),
      SecurityProtocol::Ssl => write!(f, "ssl"),
      SecurityProtocol::SaslPlaintext => write!(f, "sasl_plaintext"),
      SecurityProtocol::SaslSsl => write!(f, "sasl_ssl"),
    }
  }
}

#[derive(Clone, Debug)]
#[napi(object)]
pub struct KafkaConfiguration {
  pub brokers: String,
  pub client_id: String,
  pub security_protocol: Option<SecurityProtocol>,
  pub configuration: Option<HashMap<String, serde_json::Value>>,
  pub log_level: Option<String>,
  pub broker_address_family: Option<String>,
}

#[derive(Debug)]
#[napi]
pub struct KafkaClientConfig {
  rdkafka_client_config: ClientConfig,
  // #[napi(readonly)]
  kafka_configuration: KafkaConfiguration,
}

#[napi]
impl KafkaClientConfig {
  /// Creates a new KafkaClientConfig with the provided configuration.
  /// This is the main entry point for creating Kafka producers and consumers.
  /// @param kafkaConfiguration - The Kafka client configuration options
  #[napi(constructor)]
  pub fn new(kafka_configuration: KafkaConfiguration) -> Self {
    // Initialize tracing once globally to prevent "global default trace dispatcher already set" errors
    init_tracing_once(kafka_configuration.log_level.clone());
    KafkaClientConfig::with_kafka_configuration(kafka_configuration)
  }

  pub fn get_client_config(&self) -> &ClientConfig {
    &self.rdkafka_client_config
  }

  pub fn with_kafka_configuration(kafka_configuration: KafkaConfiguration) -> Self {
    let KafkaConfiguration {
      brokers,
      security_protocol,
      client_id,
      configuration,
      broker_address_family,
      log_level,
      ..
    } = kafka_configuration.clone();

    let mut rdkafka_client_config = ClientConfig::new();

    // Set rdkafka log level based on user configuration, defaulting to Error for production safety
    let rdkafka_log_level = match log_level.as_deref().unwrap_or("error") {
      "trace" | "debug" => RDKafkaLogLevel::Debug,
      "info" => RDKafkaLogLevel::Info,
      "warn" => RDKafkaLogLevel::Warning,
      "error" => RDKafkaLogLevel::Error,
      _ => RDKafkaLogLevel::Error, // Safe default for unknown levels
    };
    rdkafka_client_config.set_log_level(rdkafka_log_level);
    rdkafka_client_config.set("bootstrap.servers", brokers);
    rdkafka_client_config.set("client.id", client_id);
    rdkafka_client_config.set(
      "broker.address.family",
      broker_address_family.unwrap_or("v4".to_string()),
    );
    if let Some(security_protocol) = security_protocol {
      rdkafka_client_config.set("security.protocol", security_protocol.to_string());
    }
    if let Some(config) = configuration {
      let string_config = convert_config_values_to_strings(config);
      for (key, value) in string_config {
        rdkafka_client_config.set(key, value);
      }
    }

    KafkaClientConfig {
      rdkafka_client_config,
      kafka_configuration,
    }
  }

  /// Returns the current Kafka configuration.
  /// This is exposed as a readonly property 'kafkaConfiguration' in JavaScript.
  #[napi(getter)] // Expose this as a property named 'kafkaConfiguration' in JS
  pub fn configuration(&self) -> KafkaConfiguration {
    // Return a clone. Since KafkaConfiguration derives Clone and has
    // #[napi(object)], napi-rs knows how to convert the owned value.
    self.kafka_configuration.clone()
  }

  /// Creates a new Kafka producer with the specified configuration.
  /// @param producerConfiguration - The producer-specific configuration options
  /// @returns A new KafkaProducer instance
  #[napi]
  pub fn create_producer(
    &self,
    producer_configuration: ProducerConfiguration,
  ) -> Result<KafkaProducer> {
    match KafkaProducer::new(self.rdkafka_client_config.clone(), producer_configuration) {
      Ok(producer) => Ok(producer),
      Err(e) => Err(Error::new(Status::GenericFailure, e.to_string())),
    }
  }

  /// Deletes all records before the requested offset for each topic partition.
  /// Kafka does not delete an individual record directly: passing offset `N`
  /// deletes records with offsets lower than `N`.
  #[napi]
  pub async fn delete_records(
    &self,
    topic_partitions: Vec<TopicPartition>,
  ) -> Result<Vec<TopicPartition>> {
    if topic_partitions.is_empty() {
      return Err(Error::new(
        Status::InvalidArg,
        "deleteRecords requires at least one topic partition".to_string(),
      ));
    }

    let mut offsets = TopicPartitionList::new();
    for topic_partition in &topic_partitions {
      if topic_partition.partition_offset.is_empty() {
        return Err(Error::new(
          Status::InvalidArg,
          format!(
            "deleteRecords requires at least one partition offset for topic '{}'",
            topic_partition.topic
          ),
        ));
      }

      for partition_offset in &topic_partition.partition_offset {
        let Some(offset) = partition_offset.offset.offset else {
          return Err(Error::new(
            Status::InvalidArg,
            format!(
              "deleteRecords requires an explicit offset for topic '{}' partition {}",
              topic_partition.topic, partition_offset.partition
            ),
          ));
        };
        if offset < 0 {
          return Err(Error::new(
            Status::InvalidArg,
            format!(
              "deleteRecords offset must be non-negative for topic '{}' partition {}",
              topic_partition.topic, partition_offset.partition
            ),
          ));
        }
        offsets
          .add_partition_offset(
            &topic_partition.topic,
            partition_offset.partition,
            Offset::Offset(offset),
          )
          .map_err(|error| {
            error.into_napi_error("Failed to add delete records partition offset")
          })?;
      }
    }

    let admin = KafkaAdmin::new(&self.rdkafka_client_config, None)
      .map_err(|error| error.into_napi_error("Failed to create Kafka admin client"))?;
    let result = admin
      .delete_records(&offsets)
      .await
      .map_err(|error| error.into_napi_error("Failed to delete Kafka records"))?;

    Ok(convert_tpl_to_array_of_topic_partition(&result))
  }

  /// Creates a new Kafka consumer with the specified configuration.
  /// The consumer must be subscribed to topics before receiving messages.
  /// @param consumerConfiguration - The consumer-specific configuration options
  /// @returns A new KafkaConsumer instance
  #[napi(async_runtime)]
  pub fn create_consumer(
    &self,
    consumer_configuration: ConsumerConfiguration,
  ) -> Result<KafkaConsumer> {
    KafkaConsumer::new(self, &consumer_configuration).map_err(|e| {
      Error::new(
        Status::GenericFailure,
        format!("Failed to create stream consumer: {e:?}"),
      )
    })
  }
}
