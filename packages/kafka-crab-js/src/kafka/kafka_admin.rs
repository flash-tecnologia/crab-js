use rdkafka::{
  admin::{
    AdminClient, AdminOptions, ConfigResource, NewTopic, ResourceSpecifier, TopicReplication,
  },
  client::DefaultClientContext,
  config::ClientConfig,
  consumer::{BaseConsumer, Consumer},
  error::KafkaError,
  types::RDKafkaErrorCode,
};
use tracing::{debug, error, trace, warn};

use std::{collections::HashMap, str::FromStr, time::Duration};

const DEFAULT_FETCH_METADATA_TIMEOUT: Duration = Duration::from_millis(2000);

const DEFAULT_NUM_PARTITIONS: i32 = 3;
const DEFAULT_REPLICATION: i32 = 3;

pub struct KafkaAdmin<'a> {
  client_config: &'a ClientConfig,
  admin_client: AdminClient<DefaultClientContext>,
  fetch_metadata_timeout: Duration,
}

impl<'a> KafkaAdmin<'a> {
  pub fn new(
    client_config: &'a ClientConfig,
    fetch_metadata_timeout: Option<Duration>,
  ) -> Result<Self, KafkaError> {
    let admin_client: AdminClient<DefaultClientContext> = client_config.create()?;

    Ok(KafkaAdmin {
      client_config,
      admin_client,
      fetch_metadata_timeout: fetch_metadata_timeout.unwrap_or(DEFAULT_FETCH_METADATA_TIMEOUT),
    })
  }

  async fn fetch_config_resource(&self) -> Result<HashMap<String, String>, KafkaError> {
    let consumer: BaseConsumer = self.client_config.create()?;

    let metadata = consumer.fetch_metadata(None, self.fetch_metadata_timeout)?;

    for broker in metadata.brokers() {
      debug!(
        "Metadata broker id:{}  Host: {}:{}",
        broker.id(),
        broker.host(),
        broker.port()
      );
    }

    // Try different approaches to get broker config
    debug!(
      "Trying to fetch broker config with orig_broker_id: {}",
      metadata.orig_broker_id()
    );

    // First, try with the original broker ID
    let mut config_resource = self
      .admin_client
      .describe_configs(
        &[ResourceSpecifier::Broker(metadata.orig_broker_id())],
        &AdminOptions::new(),
      )
      .await;

    // If that fails, try with the first broker from metadata
    if config_resource.is_err() {
      if let Some(first_broker) = metadata.brokers().first() {
        warn!(
          "Failed to get config with orig_broker_id, trying with first broker id: {}",
          first_broker.id()
        );
        config_resource = self
          .admin_client
          .describe_configs(
            &[ResourceSpecifier::Broker(first_broker.id())],
            &AdminOptions::new(),
          )
          .await;
      }
    }

    // If still fails, try getting default broker config (broker id -1 or 0)
    if config_resource.is_err() {
      warn!("Failed to get config with first broker, trying with broker id 1");
      config_resource = self
        .admin_client
        .describe_configs(&[ResourceSpecifier::Broker(1)], &AdminOptions::new())
        .await;
    }

    match config_resource {
      Ok(config) => {
        debug!("Successfully fetched broker config");
        Ok(extract_config_resource(config))
      }
      Err(e) => {
        error!("Failed to fetch broker config after all attempts: {}", e);
        warn!("Falling back to empty config - will use defaults");
        Ok(HashMap::new()) // Return empty config, will use defaults
      }
    }
  }

  pub async fn create_topic(
    &self,
    topics: &Vec<String>,
    num_partitions: Option<i32>,
    replicas: Option<i32>,
  ) -> anyhow::Result<()> {
    debug!("Starting topic creation for topics: {:?}", topics);

    // Fetch broker config
    let broker_properties = self
      .fetch_config_resource()
      .await
      .map_err(anyhow::Error::new)?;
    trace!("Broker properties: {:?}", broker_properties);

    // Fetch metadata separately for broker count
    let consumer: BaseConsumer = self.client_config.create().map_err(anyhow::Error::new)?;
    let metadata = consumer
      .fetch_metadata(None, self.fetch_metadata_timeout)
      .map_err(anyhow::Error::new)?;

    // Use provided num_partitions or fall back to broker config
    let num_partitions = num_partitions.unwrap_or_else(|| {
      broker_properties
        .get("num.partitions")
        .or_else(|| broker_properties.get("kafka.num.partitions"))
        .or_else(|| broker_properties.get("default.partitions"))
        .get_parsed_or_default_value(DEFAULT_NUM_PARTITIONS)
    });

    // Use provided replicas or fall back to broker config
    let configured_replication = replicas.unwrap_or_else(|| {
      broker_properties
        .get("default.replication.factor")
        .or_else(|| broker_properties.get("kafka.default.replication.factor"))
        .or_else(|| broker_properties.get("replication.factor"))
        .get_parsed_or_default_value(DEFAULT_REPLICATION)
    });

    // Safety check: limit by available brokers
    let available_brokers = metadata.brokers().len() as i32;
    let replication_factor = std::cmp::min(configured_replication, available_brokers);

    debug!(
      "Configured replication: {}, Available brokers: {}, Final replication: {}",
      configured_replication, available_brokers, replication_factor
    );
    debug!(
      "Using num_partitions: {} (from broker config or default)",
      num_partitions
    );

    let new_topics: Vec<NewTopic> = topics
      .iter()
      .map(|topic| {
        debug!(
          "Creating topic '{}' with {} partitions, replication {}",
          topic, num_partitions, replication_factor
        );
        NewTopic {
          name: topic,
          num_partitions,
          replication: TopicReplication::Fixed(replication_factor),
          config: vec![],
        }
      })
      .collect();

    self
      .admin_client
      .create_topics(&new_topics, &AdminOptions::default())
      .await
      .map_err(anyhow::Error::new)?;

    debug!("Topic(s) {:?} was created successfully", topics);
    Ok(())
  }
}

fn extract_config_resource(
  config_resource: Vec<Result<ConfigResource, RDKafkaErrorCode>>,
) -> HashMap<String, String> {
  let mut properties: HashMap<String, String> = HashMap::new();
  for config_resource_list in config_resource {
    match config_resource_list {
      Ok(v) => {
        for config_entry_list in v.entries {
          if let Some(value) = config_entry_list.value {
            properties.insert(config_entry_list.name, value);
          }
        }
      }
      Err(e) => {
        debug!("Error on fetching config entry {:?}", e)
      }
    }
  }
  properties
}

trait ParsedOrDefaultValue {
  fn get_parsed_or_default_value<T: FromStr>(self, default_value: T) -> T;
}

impl ParsedOrDefaultValue for Option<&String> {
  fn get_parsed_or_default_value<T: FromStr>(self, default_value: T) -> T {
    match self {
      Some(ret) => ret.parse().unwrap_or(default_value),
      None => default_value,
    }
  }
}
