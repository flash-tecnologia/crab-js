use rdkafka::{
  consumer::{BaseConsumer, ConsumerContext, Rebalance, StreamConsumer},
  error::KafkaResult,
  ClientContext, TopicPartitionList,
};
use tokio::sync::broadcast;
use tracing::{debug, warn};

use crate::kafka::consumer::consumer_helper::convert_tpl_to_array_of_topic_partition;

use super::model::TopicPartition;

pub type TxRxContext = (
  broadcast::Sender<KafkaEvent>,
  broadcast::Receiver<KafkaEvent>,
);

pub type LoggingConsumer = StreamConsumer<KafkaCrabContext>;

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
    // Bounded channel to preserve ordering while avoiding unbounded growth
    let (tx, rx) = broadcast::channel(100);
    KafkaCrabContext {
      event_channel: (tx, rx),
    }
  }

  fn send_event(&self, event: KafkaEvent) {
    // Use try_send to avoid blocking; drop with warning if buffer is full
    if let Err(err) = self.event_channel.0.send(event) {
      warn!(
        "Event channel send failed (buffer full or closed): {:?}",
        err
      );
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
    let event = KafkaEvent {
      name: KafkaEventName::CommitCallback,
      payload: KafkaEventPayload {
        action: None,
        tpl: convert_tpl_to_array_of_topic_partition(offsets),
        error: None,
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
