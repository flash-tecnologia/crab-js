use std::{collections::HashMap, time::Duration};

use rdkafka::{
  config::RDKafkaLogLevel,
  consumer::{Consumer, StreamConsumer},
  error::{KafkaError, RDKafkaError},
  types::RDKafkaErrorCode,
  ClientConfig, Offset, TopicPartitionList,
};
use tracing::{debug, error, info, warn};

use crate::kafka::{
  consumer::context::LoggingConsumer, kafka_admin::KafkaAdmin,
  kafka_util::convert_config_values_to_strings,
};

use super::{
  context::KafkaCrabContext,
  model::{ConsumerConfiguration, OffsetModel, PartitionOffset, PartitionPosition, TopicPartition},
};

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

pub fn create_stream_consumer(
  client_config: &ClientConfig,
  consumer_configuration: &ConsumerConfiguration,
  configuration: Option<HashMap<String, serde_json::Value>>,
) -> anyhow::Result<StreamConsumer<KafkaCrabContext>> {
  let context = KafkaCrabContext::new();

  let ConsumerConfiguration {
    group_id,
    enable_auto_commit,
    ..
  } = consumer_configuration.clone();

  let mut consumer_config: ClientConfig = client_config.clone();

  if let Some(config) = configuration {
    let string_config = convert_config_values_to_strings(config);
    info!("consumer values {:?}", string_config);
    consumer_config.extend(string_config);
  }

  debug!(
    "Creating consumer with configuration: {:?}",
    consumer_config
  );

  let consumer: LoggingConsumer = consumer_config
    .clone()
    .set("group.id", group_id.clone())
    .set(
      "enable.auto.commit",
      enable_auto_commit.unwrap_or(true).to_string(),
    )
    .set_log_level(RDKafkaLogLevel::Debug)
    .create_with_context(context)?;

  debug!("Consumer created. Group id: {:?}", group_id);
  Ok(consumer)
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
  msg.contains("topic already exists")
    || msg.contains("topicalreadyexists")
    || msg.contains("authorization")
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

  matches!(
    code,
    Some(
      RDKafkaErrorCode::TopicAlreadyExists
        | RDKafkaErrorCode::TopicAuthorizationFailed
        | RDKafkaErrorCode::ClusterAuthorizationFailed
    )
  )
}

pub fn set_offset_of_all_partitions(
  offset_model: &OffsetModel,
  consumer: &StreamConsumer<KafkaCrabContext>,
  topic: &str,
  timeout: Duration,
) -> anyhow::Result<()> {
  let offset = convert_to_rdkafka_offset(offset_model);
  debug!("Setting offset to: {:?}", offset);
  let metadata = consumer.fetch_metadata(Some(topic), timeout)?;

  metadata.topics().iter().for_each(|meta_topic| {
    let mut tpl = TopicPartitionList::new();
    meta_topic.partitions().iter().for_each(|meta_partition| {
      debug!("Adding partition: {:?}", meta_partition.id());
      tpl.add_partition(topic, meta_partition.id());
    });
    match tpl.set_all_offsets(offset) {
      Ok(_) => {
        debug!("Offset set to: {:?}", offset);
      }
      Err(e) => {
        error!("Fail to set offset: {:?}", e)
      }
    };
    match consumer.assign(&tpl) {
      Ok(_) => {
        debug!("Assigning topic: {:?}", topic);
      }
      Err(e) => {
        error!("Fail to assign topic: {:?}", e);
      }
    }
  });

  Ok(())
}

pub fn assign_offset_or_use_metadata(
  topic: &str,
  partition_offset: Option<Vec<PartitionOffset>>,
  offset_model: Option<&OffsetModel>,
  consumer: &StreamConsumer<KafkaCrabContext>,
  timeout: Duration,
) -> anyhow::Result<()> {
  let mut tpl = TopicPartitionList::new();

  if let Some(value) = partition_offset {
    for item in value {
      let offset = convert_to_rdkafka_offset(&item.offset);
      debug!(
        "Adding partition: {:?} with offset: {:?} for topic: {:?}",
        item.partition, offset, topic
      );
      tpl.add_partition_offset(topic, item.partition, offset)?;
    }
  } else if let Some(offset_model) = offset_model {
    let offset = convert_to_rdkafka_offset(offset_model);
    let metadata = consumer.fetch_metadata(Some(topic), timeout)?;
    for meta_topic in metadata.topics() {
      for meta_partition in meta_topic.partitions() {
        debug!(
          "Adding partition: {:?} with offset: {:?} for topic: {:?}",
          meta_partition.id(),
          offset,
          topic
        );
        tpl.add_partition_offset(topic, meta_partition.id(), offset)?;
      }
    }
  } else {
    anyhow::bail!("At least one of partition_offset or offset_model should be provided");
  }
  consumer.assign(&tpl)?;
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
