use std::collections::HashMap;

use napi::bindgen_prelude::Buffer;

use super::producer::model::KafkaCrabError;

#[napi(object)]
pub struct AdminConfiguration {
  /// Total deadline for each operation in milliseconds. Defaults to 5000.
  pub timeout_ms: Option<f64>,
}

#[napi(object)]
pub struct AdminTopic {
  pub topic: String,
  pub num_partitions: f64,
  pub replication_factor: f64,
  pub config: Option<HashMap<String, String>>,
}

#[napi(object)]
pub struct CreateTopicsOptions {
  pub topics: Vec<AdminTopic>,
  /// Wait for leaders of successful topics. Defaults to true.
  pub wait_for_leaders: Option<bool>,
}

#[napi(object)]
pub struct AdminTopicResult {
  pub topic: String,
  /// False when the topic already existed or creation failed.
  pub created: bool,
  pub error: Option<KafkaCrabError>,
}

#[napi(object)]
pub struct AdminPartitionMetadata {
  pub partition: i32,
  pub leader: i32,
  pub replicas: Vec<i32>,
  pub isr: Vec<i32>,
  pub error: Option<KafkaCrabError>,
}

#[napi(object)]
pub struct AdminTopicMetadata {
  pub topic: String,
  pub partitions: Vec<AdminPartitionMetadata>,
  pub error: Option<KafkaCrabError>,
}

#[napi(object)]
pub struct AdminTopicOffset {
  pub partition: i32,
  /// Inclusive low watermark, encoded as a decimal string without precision loss.
  pub low: Option<String>,
  /// Exclusive high watermark, encoded as a decimal string without precision loss.
  pub high: Option<String>,
  pub error: Option<KafkaCrabError>,
}

#[napi(object)]
pub struct FetchOffsetsOptions {
  pub group_id: String,
  pub topics: Vec<String>,
}

#[napi(object)]
pub struct AdminPartitionOffset {
  pub partition: i32,
  /// The committed next offset as a decimal string. Absent when never committed.
  pub offset: Option<String>,
  pub error: Option<KafkaCrabError>,
}

#[napi(object)]
pub struct AdminGroupOffsets {
  pub topic: String,
  pub partitions: Vec<AdminPartitionOffset>,
}

#[napi(object)]
pub struct AdminGroupMember {
  pub member_id: String,
  pub client_id: String,
  pub client_host: String,
  /// Raw bytes in the group's assignment protocol.
  pub assignment: Option<Buffer>,
  pub metadata: Option<Buffer>,
}

#[napi(object)]
pub struct AdminGroupDescription {
  pub group_id: String,
  pub state: String,
  pub protocol_type: String,
  pub protocol: String,
  pub members: Vec<AdminGroupMember>,
}
