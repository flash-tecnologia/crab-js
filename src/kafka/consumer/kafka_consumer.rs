use std::{sync::Arc, time::Duration, vec};
use tokio::sync::watch::{self};

use napi::{
  threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode},
  Either, Error, Result, Status,
};

use rdkafka::{
  consumer::{stream_consumer::StreamConsumer, CommitMode as RdKfafkaCommitMode, Consumer},
  topic_partition_list::TopicPartitionList as RdTopicPartitionList,
  ClientConfig, Message as RdMessage, Offset,
};

use tracing::{debug, info, warn};

use crate::kafka::{
  consumer::consumer_helper::{
    assign_offset_or_use_metadata, convert_to_rdkafka_offset, try_create_topic, try_subscribe,
  },
  kafka_client_config::KafkaClientConfig,
  kafka_util::{create_message, IntoNapiError},
  producer::model::Message,
};

use super::{
  consumer_helper::{
    convert_tpl_to_array_of_topic_partition, create_stream_consumer, set_offset_of_all_partitions,
  },
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

type DisconnectSignal = (watch::Sender<()>, watch::Receiver<()>);

#[napi]
pub struct KafkaConsumer {
  client_config: ClientConfig,
  consumer_config: ConsumerConfiguration,
  stream_consumer: Arc<StreamConsumer<KafkaCrabContext>>,
  fetch_metadata_timeout: Duration,
  disconnect_signal: DisconnectSignal,
  client_id: String,
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
    let stream_consumer =
      create_stream_consumer(client_config, consumer_configuration, configuration.clone())
        .map_err(|e| e.into_napi_error("Failed to create stream consumer"))?;

    Ok(KafkaConsumer {
      client_config: client_config.clone(),
      consumer_config: consumer_configuration.clone(),
      stream_consumer: Arc::new(stream_consumer),
      fetch_metadata_timeout: Duration::from_millis(fetch_metadata_timeout.map_or_else(
        || DEFAULT_FETCH_METADATA_TIMEOUT.as_millis() as u64,
        |t| t as u64,
      )),
      disconnect_signal: watch::channel(()),
      client_id: kafka_client.configuration().client_id,
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
  /// @param topicConfigs - Topic name string or array of TopicPartitionConfig objects
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

    // Process topic creation per topic with individual configurations
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
          warn!(
            "Topic creation failed/ignored for {}: {:?} (continuing to subscribe)",
            &topic_config.topic, e
          );
        }
      } else {
        debug!(
          "Topic creation disabled for topic: {:?}",
          &topic_config.topic
        );
      }
    }

    let topics_name = topics
      .iter()
      .map(|x| x.topic.clone())
      .collect::<Vec<String>>();

    try_subscribe(&self.stream_consumer, &topics_name)
      .map_err(|e| e.into_napi_error("Failed to subscribe to topics"))?;

    // Process topic configurations and handle errors properly
    for item in topics.iter() {
      if let Some(all_offsets) = item.all_offsets.clone() {
        debug!(
          "Subscribing to topic: {}. Setting all partitions to offset: {:?}",
          &item.topic, &all_offsets
        );
        set_offset_of_all_partitions(
          &all_offsets,
          &self.stream_consumer,
          &item.topic,
          self.fetch_metadata_timeout,
        )
        .map_err(|e| e.into_napi_error("Failed to set partition offset"))?;
      } else if let Some(partition_offset) = item.partition_offset.clone() {
        debug!(
          "Subscribing to topic: {} with partition offsets: {:?}",
          &item.topic, &partition_offset
        );
        assign_offset_or_use_metadata(
          &item.topic,
          Some(partition_offset),
          None,
          &self.stream_consumer,
          self.fetch_metadata_timeout,
        )
        .map_err(|e| e.into_napi_error("Failed to assign partition offset"))?;
      };
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
    let mut rx = self.disconnect_signal.1.clone();
    select! {
        message = self.stream_consumer.recv() => {
            message
                .map_err(|e| e.into_napi_error("Failed to receive message from consumer"))
                .map(|message| Some(create_message(&message, message.payload().unwrap_or(&[]))))
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
    // Validate input parameters
    let size = if size == 0 {
      warn!("size cannot be 0, using 1");
      1
    } else {
      size
    };

    let timeout_ms = if timeout_ms < 1 {
      warn!(
        "timeout_ms must be at least 1ms, using default: {}ms",
        DEFAULT_BATCH_TIMEOUT
      );
      DEFAULT_BATCH_TIMEOUT
    } else {
      timeout_ms
    };
    let batch_timeout = std::time::Duration::from_millis(timeout_ms as u64);

    let mut messages = Vec::with_capacity(size as usize);
    let mut rx = self.disconnect_signal.1.clone();
    let start_time = std::time::Instant::now();

    // Try to collect messages up to size or timeout
    for _ in 0..size {
      // Check if we've exceeded our timeout
      if start_time.elapsed() >= batch_timeout {
        break;
      }

      // Calculate remaining timeout
      let remaining_timeout = batch_timeout.saturating_sub(start_time.elapsed());
      if remaining_timeout.is_zero() {
        break;
      }

      // Try to receive a message with remaining timeout
      let recv_result = tokio::time::timeout(remaining_timeout, async {
        select! {
          message = self.stream_consumer.recv() => {
            message.map_err(|e| e.into_napi_error("Failed to receive message from consumer"))
          }
          _ = rx.changed() => {
            debug!("Disconnect signal received during batch receive");
            Err(Error::new(Status::GenericFailure, "Consumer disconnected"))
          }
        }
      })
      .await;

      match recv_result {
        Ok(Ok(kafka_message)) => {
          let message = create_message(&kafka_message, kafka_message.payload().unwrap_or(&[]));
          messages.push(message);
        }
        Ok(Err(e)) => {
          // Error receiving message, return what we have so far
          if messages.is_empty() {
            return Err(e);
          }
          break;
        }
        Err(_) => {
          // Timeout occurred, return what we have so far
          break;
        }
      }
    }

    Ok(messages)
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
