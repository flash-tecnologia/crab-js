use std::{collections::HashMap, time::Duration};

use rdkafka::{
  consumer::{Consumer, StreamConsumer},
  error::{KafkaError, RDKafkaError},
  types::RDKafkaErrorCode,
  ClientConfig, Offset, TopicPartitionList,
};
use tracing::{debug, warn};

use crate::kafka::{
  consumer::context::LoggingConsumer, kafka_admin::KafkaAdmin,
  kafka_util::convert_config_values_to_strings,
};

use super::{
  context::KafkaCrabContext,
  model::{ConsumerConfiguration, OffsetModel, PartitionOffset, PartitionPosition, TopicPartition},
};

/// Default `fetch.queue.backoff.ms` applied when the user did not set it.
/// Bounds librdkafka fetch pauses after the local queue fills (upstream
/// default is 1000ms, which starves decoupled prefetch streams).
const DEFAULT_FETCH_QUEUE_BACKOFF_MS: u32 = 20;

pub fn convert_to_rdkafka_offset(offset_model: &OffsetModel) -> Offset {
  match offset_model.position {
    Some(PartitionPosition::Beginning) => Offset::Beginning,
    Some(PartitionPosition::End) => Offset::End,
    Some(PartitionPosition::Stored) => Offset::Stored,
    Some(PartitionPosition::Invalid) => Offset::Invalid,
    None => match offset_model.offset {
      Some(value) => Offset::Offset(value),
      None => Offset::Stored, // Default to stored
    },
  }
}

pub fn convert_to_offset_model(offset: &Offset) -> OffsetModel {
  match offset {
    Offset::Beginning => OffsetModel {
      position: Some(PartitionPosition::Beginning),
      offset: None,
    },
    Offset::End => OffsetModel {
      position: Some(PartitionPosition::End),
      offset: None,
    },
    Offset::Stored => OffsetModel {
      position: Some(PartitionPosition::Stored),
      offset: None,
    },
    Offset::Invalid => OffsetModel {
      position: Some(PartitionPosition::Invalid),
      offset: None,
    },
    Offset::Offset(value) => OffsetModel {
      position: None,
      offset: Some(*value),
    },
    Offset::OffsetTail(value) => OffsetModel {
      position: None,
      offset: Some(*value),
    },
  }
}

pub fn build_consumer_config(
  client_config: &ClientConfig,
  consumer_configuration: &ConsumerConfiguration,
  configuration: Option<HashMap<String, serde_json::Value>>,
) -> ClientConfig {
  let ConsumerConfiguration {
    group_id,
    enable_auto_commit,
    ..
  } = consumer_configuration.clone();

  let mut consumer_config: ClientConfig = client_config.clone();

  if let Some(config) = configuration {
    let string_config = convert_config_values_to_strings(config);
    consumer_config.extend(string_config);
  }

  // Precedence: explicit consumerConfiguration.enableAutoCommit takes precedence.
  // If not provided, preserve any value already set in configuration or client_config;
  // if neither provided, librdkafka defaults to "true".
  if let Some(auto_commit) = enable_auto_commit {
    consumer_config.set("enable.auto.commit", auto_commit.to_string());
  }

  // Default fetch-queue backoff: librdkafka postpones fetching for
  // `fetch.queue.backoff.ms` (upstream default 1000ms) once the local queue
  // reaches `queued.min.messages`. With decoupled prefetch streams that 1s
  // pause starves the reader (~900ms observed on small-batch streams while
  // only ~16ms of native bank covers it). A short backoff keeps fetch hot;
  // explicit user configuration (map or client) always wins.
  if consumer_config.get("fetch.queue.backoff.ms").is_none() {
    consumer_config.set(
      "fetch.queue.backoff.ms",
      DEFAULT_FETCH_QUEUE_BACKOFF_MS.to_string(),
    );
  }

  consumer_config.set("group.id", group_id);
  consumer_config
}

pub fn create_stream_consumer(
  client_config: &ClientConfig,
  consumer_configuration: &ConsumerConfiguration,
  configuration: Option<HashMap<String, serde_json::Value>>,
) -> anyhow::Result<LoggingConsumer> {
  let context = KafkaCrabContext::new();
  let group_id = consumer_configuration.group_id.clone();
  let consumer_config = build_consumer_config(client_config, consumer_configuration, configuration);

  let consumer = consumer_config.create_with_context(context)?;

  debug!("Consumer created. Group id: {:?}", group_id);
  Ok(LoggingConsumer::new(consumer))
}

pub fn try_subscribe(consumer: &LoggingConsumer, topics: &[String]) -> anyhow::Result<()> {
  let topics_ref = topics.iter().map(|x| x.as_str()).collect::<Vec<&str>>();
  consumer.subscribe(topics_ref.as_slice()).map_err(|e| {
    anyhow::Error::msg(format!(
      "Can't subscribe to specified topic(s): {topics_ref:?}. Error: {e:?}"
    ))
  })?;
  debug!("Subscribed to topic(s): {:?}", topics_ref);
  Ok(())
}

pub async fn try_create_topic(
  topics: &Vec<String>,
  client_config: &ClientConfig,
  fetch_metadata_timeout: Duration,
  num_partitions: Option<i32>,
  replicas: Option<i32>,
) -> anyhow::Result<()> {
  let admin = KafkaAdmin::new(client_config, Some(fetch_metadata_timeout))?;
  let result = admin.create_topic(topics, num_partitions, replicas).await;
  if let Err(e) = result {
    if is_non_fatal_topic_error(&e) {
      warn!("Topic creation skipped/unauthorized/exists: {e:?}");
      return Ok(());
    }
    warn!("Fail to create topic {:?}", e);
    return Err(anyhow::Error::msg(format!("Fail to create topic: {e:?}")));
  }
  debug!("Topic(s) created: {:?}", topics);
  Ok(())
}

fn is_non_fatal_topic_error(err: &anyhow::Error) -> bool {
  if let Some(kafka_err) = err.downcast_ref::<KafkaError>() {
    return matches_non_fatal_code(kafka_err);
  }

  if let Some(rd_err) = err.downcast_ref::<RDKafkaError>() {
    return matches_non_fatal_code(&KafkaError::AdminOp(rd_err.code()));
  }

  let msg = err.to_string().to_lowercase();
  msg.contains("topic already exists") || msg.contains("topicalreadyexists")
}

fn matches_non_fatal_code(err: &KafkaError) -> bool {
  let code = match err {
    KafkaError::AdminOp(code)
    | KafkaError::ConsumerCommit(code)
    | KafkaError::ConsumerQueueClose(code)
    | KafkaError::Flush(code)
    | KafkaError::Global(code)
    | KafkaError::GroupListFetch(code)
    | KafkaError::MessageProduction(code)
    | KafkaError::MetadataFetch(code)
    | KafkaError::OffsetFetch(code)
    | KafkaError::Rebalance(code)
    | KafkaError::SetPartitionOffset(code)
    | KafkaError::StoreOffset(code)
    | KafkaError::MockCluster(code) => Some(*code),
    KafkaError::Transaction(rd_err) => Some(rd_err.code()),
    _ => None,
  };

  matches!(code, Some(RDKafkaErrorCode::TopicAlreadyExists))
}

pub fn add_topic_partitions_to_tpl(
  tpl: &mut TopicPartitionList,
  topic: &str,
  partition_offset: Option<&Vec<PartitionOffset>>,
  all_offsets: Option<&OffsetModel>,
  consumer: &StreamConsumer<KafkaCrabContext>,
  timeout: Duration,
) -> anyhow::Result<()> {
  if let Some(partition_offsets) = partition_offset {
    anyhow::ensure!(
      !partition_offsets.is_empty(),
      "Topic '{topic}' requires a non-empty partitionOffset list"
    );
    for po in partition_offsets {
      let offset = convert_to_rdkafka_offset(&po.offset);
      debug!(
        "Adding partition {:?} with offset {:?} for topic: {}",
        po.partition, offset, topic
      );
      tpl.add_partition_offset(topic, po.partition, offset)?;
    }
  } else {
    let offset = all_offsets
      .map(convert_to_rdkafka_offset)
      .unwrap_or(Offset::Stored);
    debug!(
      "Setting all partitions for topic {} to offset: {:?}",
      topic, offset
    );
    let metadata = consumer
      .fetch_metadata(Some(topic), timeout)
      .map_err(|error| anyhow::anyhow!("Failed to fetch metadata for topic '{topic}': {error}"))?;
    let meta_topic = metadata
      .topics()
      .iter()
      .find(|entry| entry.name() == topic)
      .ok_or_else(|| anyhow::anyhow!("Metadata omitted requested topic '{topic}'"))?;
    if let Some(error) = meta_topic.error() {
      anyhow::bail!(
        "Invalid metadata for topic '{topic}': {}",
        KafkaError::MetadataFetch(error.into())
      );
    }
    anyhow::ensure!(
      !meta_topic.partitions().is_empty(),
      "Metadata resolved no partitions for topic '{topic}'"
    );
    for partition in meta_topic.partitions() {
      if let Some(error) = partition.error() {
        anyhow::bail!(
          "Invalid metadata for topic '{topic}' partition {}: {}",
          partition.id(),
          KafkaError::MetadataFetch(error.into())
        );
      }
      tpl.add_partition_offset(topic, partition.id(), offset)?;
    }
  }
  Ok(())
}

pub fn convert_tpl_to_array_of_topic_partition(tpl: &TopicPartitionList) -> Vec<TopicPartition> {
  tpl
    .elements()
    .iter()
    .map(|tp| TopicPartition {
      topic: tp.topic().to_owned(),
      partition_offset: vec![PartitionOffset {
        partition: tp.partition(),
        offset: convert_to_offset_model(&tp.offset()),
      }],
    })
    .collect()
}
