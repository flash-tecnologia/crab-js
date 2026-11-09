use rdkafka::{
  admin::{
    AdminClient, AdminOptions, ConfigResource, NewTopic, ResourceSpecifier, TopicReplication,
  },
  client::DefaultClientContext,
  config::ClientConfig,
  consumer::{BaseConsumer, Consumer},
  error::KafkaError,
  topic_partition_list::TopicPartitionList,
  types::RDKafkaErrorCode,
};
use tracing::{debug, error, trace, warn};

use napi::{Error, Result, Status};
use std::{
  collections::{HashMap, HashSet},
  str::FromStr,
  sync::{Arc, Mutex},
  time::{Duration, Instant},
};

use super::{kafka_admin_model::*, kafka_util::IntoNapiError, producer::model::KafkaCrabError};

const DEFAULT_FETCH_METADATA_TIMEOUT: Duration = Duration::from_millis(2000);

const DEFAULT_NUM_PARTITIONS: i32 = 3;
const DEFAULT_REPLICATION: i32 = 3;

#[napi]
pub struct KafkaAdmin {
  client_config: ClientConfig,
  admin_client: Mutex<Option<Arc<AdminClient<DefaultClientContext>>>>,
  fetch_metadata_timeout: Duration,
}

impl KafkaAdmin {
  pub fn new(
    client_config: &ClientConfig,
    fetch_metadata_timeout: Option<Duration>,
  ) -> std::result::Result<Self, KafkaError> {
    let client_config = client_config.clone();
    let admin_client: AdminClient<DefaultClientContext> = client_config.create()?;

    Ok(KafkaAdmin {
      client_config,
      admin_client: Mutex::new(Some(Arc::new(admin_client))),
      fetch_metadata_timeout: fetch_metadata_timeout.unwrap_or(DEFAULT_FETCH_METADATA_TIMEOUT),
    })
  }

  fn client(&self) -> Result<Arc<AdminClient<DefaultClientContext>>> {
    self
      .admin_client
      .lock()
      .map_err(|_| Error::new(Status::GenericFailure, "Kafka admin lock poisoned"))?
      .as_ref()
      .cloned()
      .ok_or_else(|| Error::new(Status::GenericFailure, "Kafka admin is closed"))
  }

  fn admin_options(&self) -> AdminOptions {
    AdminOptions::new()
      .request_timeout(Some(self.fetch_metadata_timeout))
      .operation_timeout(Some(self.fetch_metadata_timeout))
  }

  async fn fetch_config_resource(
    &self,
  ) -> std::result::Result<HashMap<String, String>, KafkaError> {
    let admin_client = self
      .client()
      .map_err(|_| KafkaError::AdminOp(RDKafkaErrorCode::Fail))?;
    let client_config = self.client_config.clone();
    let timeout = self.fetch_metadata_timeout;
    let metadata = tokio::task::spawn_blocking(move || {
      let consumer: BaseConsumer = client_config.create()?;
      consumer.fetch_metadata(None, timeout)
    })
    .await
    .map_err(|err| {
      warn!("Failed to join metadata fetch: {err}");
      KafkaError::AdminOp(RDKafkaErrorCode::Fail)
    })??;

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
    let mut config_resource = admin_client
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
        config_resource = admin_client
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
      config_resource = admin_client
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

  /// Deletes records before the requested offset in each topic partition.
  pub async fn delete_records(
    &self,
    offsets: &TopicPartitionList,
  ) -> anyhow::Result<TopicPartitionList> {
    let result = self
      .client()?
      .delete_records(offsets, &AdminOptions::default())
      .await
      .map_err(anyhow::Error::new)?;

    let partition_errors = result
      .elements()
      .into_iter()
      .filter_map(|tp| {
        tp.error().err().map(|cause| {
          format!(
            "topic '{}' partition {}: {cause}",
            tp.topic(),
            tp.partition()
          )
        })
      })
      .collect::<Vec<_>>();

    if !partition_errors.is_empty() {
      return Err(anyhow::anyhow!(
        "Failed to delete Kafka records for partition(s): {}. DeleteRecords may have partially succeeded; retry the failed partitions individually.",
        partition_errors.join(", ")
      ));
    }

    Ok(result)
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

    let client_config = self.client_config.clone();
    let timeout = self.fetch_metadata_timeout;
    let metadata = tokio::task::spawn_blocking(move || {
      let consumer: BaseConsumer = client_config.create()?;
      consumer.fetch_metadata(None, timeout)
    })
    .await
    .map_err(|err| anyhow::anyhow!("Failed to join metadata fetch: {err}"))?
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

    // Preserve subscribe(createTopic)'s existing fallback to available brokers.
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

    let results = self
      .client()?
      .create_topics(&new_topics, &AdminOptions::default())
      .await
      .map_err(anyhow::Error::new)?;

    let mut failed_topics = Vec::new();
    for result in results {
      match result {
        Ok(topic_name) => {
          debug!("Topic '{}' was created successfully", topic_name);
        }
        Err((topic_name, error_code)) => {
          if error_code == RDKafkaErrorCode::TopicAlreadyExists {
            debug!("Topic '{}' already exists", topic_name);
          } else {
            warn!("Topic '{}' creation failed: {:?}", topic_name, error_code);
            failed_topics.push((topic_name, error_code));
          }
        }
      }
    }

    if !failed_topics.is_empty() {
      let err_msg = failed_topics
        .iter()
        .map(|(t, c)| format!("{}: {:?}", t, c))
        .collect::<Vec<_>>()
        .join(", ");
      return Err(anyhow::anyhow!("Failed to create topic(s): {}", err_msg));
    }

    debug!("Topic(s) {:?} was created successfully", topics);
    Ok(())
  }
}

#[napi]
impl KafkaAdmin {
  /// Creates topics and returns one result per topic, including partial failures.
  /// Existing topics are successful with `created: false` and are not modified.
  #[napi]
  pub async fn create_topics(&self, options: CreateTopicsOptions) -> Result<Vec<AdminTopicResult>> {
    let client = self.client()?;
    validate_names(
      &options
        .topics
        .iter()
        .map(|topic| topic.topic.clone())
        .collect::<Vec<_>>(),
      "topics",
    )?;
    for topic in &options.topics {
      if [topic.num_partitions, topic.replication_factor]
        .iter()
        .any(|value| {
          !value.is_finite() || value.fract() != 0.0 || *value <= 0.0 || *value > i32::MAX as f64
        })
      {
        return Err(Error::new(
          Status::InvalidArg,
          "numPartitions and replicationFactor must be positive integers",
        ));
      }
    }
    let deadline = Instant::now() + self.fetch_metadata_timeout;
    let topics = options
      .topics
      .iter()
      .map(|topic| NewTopic {
        name: &topic.topic,
        num_partitions: topic.num_partitions as i32,
        replication: TopicReplication::Fixed(topic.replication_factor as i32),
        config: topic
          .config
          .as_ref()
          .map(|config| {
            config
              .iter()
              .map(|(key, value)| (key.as_str(), value.as_str()))
              .collect()
          })
          .unwrap_or_default(),
      })
      .collect::<Vec<_>>();
    let results = client
      .create_topics(&topics, &self.admin_options())
      .await
      .map_err(|error| error.into_napi_error("Failed to create Kafka topics"))?
      .into_iter()
      .map(|result| match result {
        Ok(topic) => AdminTopicResult {
          topic,
          created: true,
          error: None,
        },
        Err((topic, RDKafkaErrorCode::TopicAlreadyExists)) => AdminTopicResult {
          topic,
          created: false,
          error: None,
        },
        Err((topic, code)) => AdminTopicResult {
          topic,
          created: false,
          error: Some(admin_error(code)),
        },
      })
      .collect::<Vec<_>>();

    if options.wait_for_leaders.unwrap_or(true) {
      let successful = results
        .iter()
        .filter(|result| result.error.is_none())
        .map(|result| result.topic.clone())
        .collect::<Vec<_>>();
      if !successful.is_empty() {
        tokio::task::spawn_blocking(move || loop {
          let metadata = read_metadata(&client, &successful, deadline)?;
          if metadata.iter().all(|topic| {
            topic.error.is_none()
              && !topic.partitions.is_empty()
              && topic
                .partitions
                .iter()
                .all(|partition| partition.error.is_none() && partition.leader >= 0)
          }) {
            return Ok::<_, Error>(());
          }
          std::thread::sleep(remaining(deadline)?.min(Duration::from_millis(25)));
        })
        .await
        .map_err(|error| {
          Error::new(
            Status::GenericFailure,
            format!("Topic leader query failed: {error}"),
          )
        })??;
      }
    }
    Ok(results)
  }

  /// Fetches metadata without subscribing, joining a group, or auto-creating topics.
  #[napi]
  pub async fn fetch_topic_metadata(&self, topics: Vec<String>) -> Result<Vec<AdminTopicMetadata>> {
    let client = self.client()?;
    validate_names(&topics, "topics")?;
    let deadline = Instant::now() + self.fetch_metadata_timeout;
    tokio::task::spawn_blocking(move || read_metadata(&client, &topics, deadline))
      .await
      .map_err(|error| {
        Error::new(
          Status::GenericFailure,
          format!("Metadata query failed: {error}"),
        )
      })?
  }

  /// Fetches inclusive low and exclusive high watermarks as decimal strings.
  /// Partition failures are returned alongside successful results.
  #[napi]
  pub async fn fetch_topic_offsets(&self, topic: String) -> Result<Vec<AdminTopicOffset>> {
    let client = self.client()?;
    validate_names(std::slice::from_ref(&topic), "topics")?;
    let deadline = Instant::now() + self.fetch_metadata_timeout;
    tokio::task::spawn_blocking(move || {
      let metadata = read_metadata(&client, std::slice::from_ref(&topic), deadline)?;
      let partitions = topic_partitions(&metadata[0])?;
      partitions
        .into_iter()
        .map(|partition| {
          let watermarks = client
            .inner()
            .fetch_watermarks(&topic, partition, remaining(deadline)?);
          Ok(match watermarks {
            Ok((low, high)) => AdminTopicOffset {
              partition,
              low: Some(low.to_string()),
              high: Some(high.to_string()),
              error: None,
            },
            Err(error) => AdminTopicOffset {
              partition,
              low: None,
              high: None,
              error: Some(admin_error(
                error
                  .rdkafka_error_code()
                  .unwrap_or(RDKafkaErrorCode::Unknown),
              )),
            },
          })
        })
        .collect::<Result<Vec<_>>>()
    })
    .await
    .map_err(|error| {
      Error::new(
        Status::GenericFailure,
        format!("Watermark query failed: {error}"),
      )
    })?
  }

  /// Reads a group's committed next offsets. Never subscribes, joins, or commits.
  /// Missing commits are absent rather than being resolved to the start or end.
  #[napi]
  pub async fn fetch_offsets(
    &self,
    options: FetchOffsetsOptions,
  ) -> Result<Vec<AdminGroupOffsets>> {
    let client = self.client()?;
    validate_names(&options.topics, "topics")?;
    if options.group_id.trim().is_empty() {
      return Err(Error::new(Status::InvalidArg, "groupId must not be empty"));
    }
    let mut config = self.client_config.clone();
    config.set("group.id", &options.group_id);
    config.set("enable.auto.commit", "false");
    config.set("enable.auto.offset.store", "false");
    let deadline = Instant::now() + self.fetch_metadata_timeout;
    tokio::task::spawn_blocking(move || {
      let metadata = read_metadata(&client, &options.topics, deadline)?;
      let mut requested = TopicPartitionList::new();
      for topic in &metadata {
        for partition in topic_partitions(topic)? {
          requested.add_partition(&topic.topic, partition);
        }
      }
      let consumer: BaseConsumer = config
        .create()
        .map_err(|error| error.into_napi_error("Failed to create offset query client"))?;
      let offsets = consumer
        .committed_offsets(requested, remaining(deadline)?)
        .map_err(|error| error.into_napi_error("Failed to fetch committed offsets"))?;
      Ok(
        metadata
          .into_iter()
          .map(|topic| AdminGroupOffsets {
            partitions: offsets
              .elements_for_topic(&topic.topic)
              .into_iter()
              .map(|partition| AdminPartitionOffset {
                partition: partition.partition(),
                offset: committed_offset(partition.offset()),
                error: partition.error().err().map(|error| {
                  admin_error(
                    error
                      .rdkafka_error_code()
                      .unwrap_or(RDKafkaErrorCode::Unknown),
                  )
                }),
              })
              .collect(),
            topic: topic.topic,
          })
          .collect(),
      )
    })
    .await
    .map_err(|error| {
      Error::new(
        Status::GenericFailure,
        format!("Committed offset query failed: {error}"),
      )
    })?
  }

  /// Describes existing groups and their members without participating in them.
  /// Assignments and member metadata are raw protocol bytes.
  #[napi]
  pub async fn describe_groups(
    &self,
    group_ids: Vec<String>,
  ) -> Result<Vec<AdminGroupDescription>> {
    let client = self.client()?;
    validate_names(&group_ids, "groupIds")?;
    let deadline = Instant::now() + self.fetch_metadata_timeout;
    tokio::task::spawn_blocking(move || {
      group_ids
        .into_iter()
        .map(|group_id| {
          let groups = client
            .inner()
            .fetch_group_list(Some(&group_id), remaining(deadline)?)
            .map_err(|error| error.into_napi_error("Failed to describe Kafka group"))?;
          let group = groups
            .groups()
            .iter()
            .find(|group| group.name() == group_id)
            .ok_or_else(|| {
              Error::new(
                Status::GenericFailure,
                format!("Kafka group '{group_id}' was not found"),
              )
            })?;
          Ok(AdminGroupDescription {
            group_id,
            state: group.state().to_string(),
            protocol_type: group.protocol_type().to_string(),
            protocol: group.protocol().to_string(),
            members: group
              .members()
              .iter()
              .map(|member| AdminGroupMember {
                member_id: member.id().to_string(),
                client_id: member.client_id().to_string(),
                client_host: member.client_host().to_string(),
                assignment: member.assignment().map(|bytes| bytes.to_vec().into()),
                metadata: member.metadata().map(|bytes| bytes.to_vec().into()),
              })
              .collect(),
          })
        })
        .collect()
    })
    .await
    .map_err(|error| {
      Error::new(
        Status::GenericFailure,
        format!("Group query failed: {error}"),
      )
    })?
  }

  /// Idempotently prevents new operations. In-flight operations may finish.
  #[napi]
  pub async fn close(&self) -> Result<()> {
    let client = self
      .admin_client
      .lock()
      .map_err(|_| Error::new(Status::GenericFailure, "Kafka admin lock poisoned"))?
      .take();
    // librdkafka destruction may wait for its background thread.
    tokio::task::spawn_blocking(move || drop(client))
      .await
      .map_err(|error| {
        Error::new(
          Status::GenericFailure,
          format!("Kafka admin close failed: {error}"),
        )
      })
  }
}

fn remaining(deadline: Instant) -> Result<Duration> {
  deadline
    .checked_duration_since(Instant::now())
    .filter(|duration| !duration.is_zero())
    .ok_or_else(|| Error::new(Status::GenericFailure, "Kafka admin operation timed out"))
}

fn validate_names(names: &[String], label: &str) -> Result<()> {
  let mut unique = HashSet::new();
  if names.is_empty()
    || names
      .iter()
      .any(|name| name.trim().is_empty() || name.contains('\0') || !unique.insert(name))
  {
    return Err(Error::new(
      Status::InvalidArg,
      format!("{label} must contain non-empty, unique names without NUL bytes"),
    ));
  }
  Ok(())
}

fn admin_error(code: RDKafkaErrorCode) -> KafkaCrabError {
  KafkaCrabError {
    code: code as i32,
    message: code.to_string(),
  }
}

fn committed_offset(offset: rdkafka::Offset) -> Option<String> {
  match offset {
    rdkafka::Offset::Offset(offset) => Some(offset.to_string()),
    _ => None,
  }
}

fn topic_partitions(topic: &AdminTopicMetadata) -> Result<Vec<i32>> {
  if let Some(error) = &topic.error {
    return Err(Error::new(
      Status::GenericFailure,
      format!(
        "Topic '{}': {} ({})",
        topic.topic, error.message, error.code
      ),
    ));
  }
  if topic.partitions.is_empty() {
    return Err(Error::new(
      Status::GenericFailure,
      format!("Topic '{}' has no partitions", topic.topic),
    ));
  }
  topic
    .partitions
    .iter()
    .map(|partition| {
      if let Some(error) = &partition.error {
        return Err(Error::new(
          Status::GenericFailure,
          format!(
            "Topic '{}' partition {}: {} ({})",
            topic.topic, partition.partition, error.message, error.code
          ),
        ));
      }
      Ok(partition.partition)
    })
    .collect()
}

fn read_metadata(
  client: &AdminClient<DefaultClientContext>,
  topics: &[String],
  deadline: Instant,
) -> Result<Vec<AdminTopicMetadata>> {
  topics
    .iter()
    .map(|topic| {
      let metadata = client
        .inner()
        .fetch_metadata(Some(topic), remaining(deadline)?)
        .map_err(|error| error.into_napi_error("Failed to fetch Kafka metadata"))?;
      let Some(metadata) = metadata
        .topics()
        .iter()
        .find(|metadata| metadata.name() == topic)
      else {
        return Ok(AdminTopicMetadata {
          topic: topic.clone(),
          partitions: vec![],
          error: Some(admin_error(RDKafkaErrorCode::UnknownTopicOrPartition)),
        });
      };
      Ok(AdminTopicMetadata {
        topic: topic.clone(),
        error: metadata.error().map(|code| admin_error(code.into())),
        partitions: metadata
          .partitions()
          .iter()
          .map(|partition| AdminPartitionMetadata {
            partition: partition.id(),
            leader: partition.leader(),
            replicas: partition.replicas().to_vec(),
            isr: partition.isr().to_vec(),
            error: partition.error().map(|code| admin_error(code.into())),
          })
          .collect(),
      })
    })
    .collect()
}

fn extract_config_resource(
  config_resource: Vec<std::result::Result<ConfigResource, RDKafkaErrorCode>>,
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

#[cfg(test)]
mod admin_tests {
  use super::*;

  #[test]
  fn committed_offsets_preserve_i64_precision_and_missing_commits() {
    assert_eq!(
      committed_offset(rdkafka::Offset::Offset(i64::MAX)).as_deref(),
      Some("9223372036854775807")
    );
    assert_eq!(
      committed_offset(rdkafka::Offset::Offset(0)).as_deref(),
      Some("0")
    );
    assert_eq!(committed_offset(rdkafka::Offset::Invalid), None);
  }
}
