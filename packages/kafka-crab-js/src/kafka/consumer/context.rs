use rdkafka::{
  consumer::{BaseConsumer, ConsumerContext, Rebalance, StreamConsumer},
  error::KafkaResult,
  ClientContext, TopicPartitionList,
};
use std::ops::Deref;
use tokio::{runtime::Handle, sync::broadcast};
use tracing::{debug, warn};

use crate::kafka::consumer::consumer_helper::convert_tpl_to_array_of_topic_partition;

use super::model::TopicPartition;

pub type TxRxContext = (
  broadcast::Sender<KafkaEvent>,
  broadcast::Receiver<KafkaEvent>,
);

/// Keeps librdkafka's blocking close off JavaScript and Tokio worker threads,
/// regardless of whether the last owner is a consumer, stream or commit queue.
pub struct LoggingConsumer {
  consumer: Option<StreamConsumer<KafkaCrabContext>>,
  runtime: Handle,
}

impl LoggingConsumer {
  pub fn new(consumer: StreamConsumer<KafkaCrabContext>) -> Self {
    Self {
      consumer: Some(consumer),
      runtime: Handle::current(),
    }
  }
}

impl Deref for LoggingConsumer {
  type Target = StreamConsumer<KafkaCrabContext>;

  fn deref(&self) -> &Self::Target {
    self
      .consumer
      .as_ref()
      .expect("consumer is present until drop")
  }
}

impl Drop for LoggingConsumer {
  fn drop(&mut self) {
    if let Some(consumer) = self.consumer.take() {
      // BaseConsumer::drop polls until close completes, which can wait for
      // session.timeout.ms when a commit or group operation is pending.
      // Retain the complete native owner until that close finishes normally.
      self.runtime.spawn_blocking(move || drop(consumer));
    }
  }
}

#[napi(object)]
#[derive(Clone, Debug)]
pub struct KafkaEventPayload {
  pub action: Option<String>,
  pub tpl: Vec<TopicPartition>,
  pub error: Option<String>,
}

#[napi(string_enum)]
#[derive(Clone, Debug)]
pub enum KafkaEventName {
  PreRebalance,
  PostRebalance,
  CommitCallback,
}

#[napi(object)]
#[derive(Clone, Debug)]
pub struct KafkaEvent {
  pub name: KafkaEventName,
  pub payload: KafkaEventPayload,
}

pub struct KafkaCrabContext {
  pub event_channel: TxRxContext,
}

impl KafkaCrabContext {
  pub fn new() -> Self {
    // Bounded broadcast channel preserving order for live receivers without
    // unbounded growth. `broadcast::channel(100)` rounds up to 128 slots; a
    // send on a full channel overwrites the oldest retained event (it never
    // blocks or fails for a full buffer). A lagging receiver observes the loss
    // as `Lagged(skipped)` on its next read; a late subscriber only sees
    // events sent after its `resubscribe()`.
    let (tx, rx) = broadcast::channel(100);
    KafkaCrabContext {
      event_channel: (tx, rx),
    }
  }

  pub(super) fn send_event(&self, event: KafkaEvent) {
    // `send` only fails when no receiver exists yet; overflow is not an error
    // here (oldest events are overwritten) and is reported to lagging
    // receivers as `Lagged` when they read.
    if let Err(err) = self.event_channel.0.send(event) {
      warn!("Event channel send failed (no receivers): {:?}", err);
    };
  }
}

impl ClientContext for KafkaCrabContext {}

impl ConsumerContext for KafkaCrabContext {
  fn pre_rebalance(&self, consumer: &BaseConsumer<Self>, rebalance: &Rebalance) {
    let event = KafkaEvent {
      name: KafkaEventName::PreRebalance,
      payload: convert_rebalance_to_kafka_payload(rebalance),
    };

    debug!(
      "Pre rebalance {:?}, consumer closed: {} ",
      rebalance,
      consumer.closed()
    );

    self.send_event(event);
  }

  fn post_rebalance(&self, consumer: &BaseConsumer<Self>, rebalance: &Rebalance) {
    let event = KafkaEvent {
      name: KafkaEventName::PostRebalance,
      payload: convert_rebalance_to_kafka_payload(rebalance),
    };

    debug!(
      "Post rebalance {:?}, consumer closed: {} ",
      rebalance,
      consumer.closed()
    );

    self.send_event(event);
  }

  fn commit_callback(&self, result: KafkaResult<()>, offsets: &TopicPartitionList) {
    let error = match result {
      Ok(_) => offsets.elements().iter().find_map(|partition| {
        partition
          .error()
          .err()
          .map(|error| format!("{}[{}]: {error}", partition.topic(), partition.partition()))
      }),
      Err(ref e) => Some(e.to_string()),
    };

    let event = KafkaEvent {
      name: KafkaEventName::CommitCallback,
      payload: KafkaEventPayload {
        action: None,
        tpl: convert_tpl_to_array_of_topic_partition(offsets),
        error,
      },
    };

    debug!("Committing offsets: {:?}. Offset: {:?}", result, offsets);

    self.send_event(event);
  }
}

fn convert_rebalance_to_kafka_payload(rebalance: &Rebalance) -> KafkaEventPayload {
  match rebalance {
    Rebalance::Assign(partitions) => KafkaEventPayload {
      action: Some("assign".to_string()),
      tpl: convert_tpl_to_array_of_topic_partition(partitions),
      error: None,
    },
    Rebalance::Revoke(partitions) => KafkaEventPayload {
      action: Some("revoke".to_string()),
      tpl: convert_tpl_to_array_of_topic_partition(partitions),
      error: None,
    },
    Rebalance::Error(err) => KafkaEventPayload {
      action: Some("error".to_string()),
      tpl: vec![],
      error: Some(err.to_string()),
    },
  }
}
