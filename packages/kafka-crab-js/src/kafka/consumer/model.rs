use std::{collections::HashMap, time::Duration};

use crate::kafka::producer::model::Message;

pub const DEFAULT_FETCH_METADATA_TIMEOUT: Duration = Duration::from_millis(2000);

#[napi(string_enum)]
#[derive(Debug, PartialEq)]
pub enum CommitMode {
  Sync,
  Async,
}
#[napi(object)]
#[derive(Clone, Debug)]
pub struct ConsumerConfiguration {
  pub group_id: String,
  pub enable_auto_commit: Option<bool>,
  pub configuration: Option<HashMap<String, serde_json::Value>>,
  /// Metadata timeout in ms (default 2000, clamped to 1–300000). Zero is not
  /// fail-fast: it falls back to the default. Negative values are rejected.
  pub fetch_metadata_timeout: Option<i64>,
  /// Experimental (RFC-0017). Enables managed consumption for this consumer:
  /// `recvManaged`/`commitManaged` replace the legacy receive and commit
  /// methods. Fixed at creation; requires auto commit and auto offset store
  /// to be disabled (they default to `false` in this mode).
  pub managed: Option<bool>,
  /// Experimental (RFC-0017). Bounds for the managed native state.
  pub managed_options: Option<ManagedConsumptionOptions>,
}

/// Experimental (RFC-0017). Limits of the binding-owned managed state. They do
/// not bound librdkafka's own fetch queues or messages already handed to
/// JavaScript.
#[napi(object)]
#[derive(Clone, Debug)]
pub struct ManagedConsumptionOptions {
  /// Assignment transitions that may wait for `recvManaged` (default 1024,
  /// 1-65536). Exceeding it is a terminal `ERR_MANAGED_CONTROL_OVERFLOW`.
  pub max_control_records: Option<u32>,
  /// Messages retained natively before assigned partitions are paused
  /// (default 1024, 1-1000000). Fetching resumes at half of the limit.
  pub max_buffered_messages: Option<u32>,
  /// Bytes retained natively before assigned partitions are paused
  /// (default 32 MiB). One message is always admitted.
  pub max_buffered_bytes: Option<u32>,
}

/// One partition of a managed assignment transition.
#[napi(object)]
#[derive(Clone, Debug)]
pub struct ManagedPartition {
  pub topic: String,
  pub partition: i32,
  /// Identity that owns the partition after the transition. Absent on revoke.
  pub assignment: Option<String>,
  /// Identity that owned it before: the invalidated one on revoke, or the same
  /// value as `assignment` when an assign names a retained partition.
  pub previous_assignment: Option<String>,
}

/// Record returned by `recvManaged`, in the order defined natively.
#[napi(discriminant = "kind", discriminant_case = "lowercase")]
// Records are moved straight to JavaScript; boxing the message would only add
// an allocation per delivery.
#[allow(clippy::large_enum_variant)]
pub enum ManagedRecord {
  /// A message and the identity that owned its partition when it was admitted.
  Message {
    message: Message,
    assignment: String,
  },
  /// Partitions were assigned. May be empty (end of a cooperative rebalance).
  Assign { partitions: Vec<ManagedPartition> },
  /// Identities were invalidated. `lost` is librdkafka's assignment-lost flag.
  Revoke {
    partitions: Vec<ManagedPartition>,
    lost: bool,
  },
  /// No record: `reason` is `timeout` or `cancelled`. Not end-of-stream.
  Idle { reason: String },
}

/// Diagnostic snapshot of the managed native state.
#[napi(object)]
#[derive(Clone, Debug)]
pub struct ManagedStateSnapshot {
  /// `active`, `closed` or `failed`.
  pub status: String,
  /// Error code of the terminal failure when `status` is `failed`.
  pub failure: Option<String>,
  pub assignments: Vec<ManagedPartition>,
  pub queued_control_records: u32,
  pub buffered_messages: u32,
  pub buffered_bytes: f64,
  pub backpressure_paused: bool,
  pub pending_commits: u32,
  pub dropped_receive_errors: f64,
  /// Undelivered messages released because their identity was revoked.
  pub purged_messages: f64,
}

#[napi(string_enum)]
#[derive(Debug, Clone)]
pub enum PartitionPosition {
  Beginning,
  End,
  Stored,
  Invalid,
}
#[napi(object)]
#[derive(Clone, Debug)]
pub struct OffsetModel {
  pub offset: Option<i64>,
  pub position: Option<PartitionPosition>,
}

#[napi(object)]
#[derive(Clone, Debug)]
pub struct PartitionOffset {
  pub partition: i32,
  pub offset: OffsetModel,
}

#[napi(object)]
#[derive(Clone, Debug)]
pub struct TopicPartitionConfig {
  pub topic: String,
  pub all_offsets: Option<OffsetModel>,
  pub partition_offset: Option<Vec<PartitionOffset>>,
  pub create_topic: Option<bool>,
  pub num_partitions: Option<i32>,
  pub replicas: Option<i32>,
}

#[napi(object)]
#[derive(Clone, Debug)]
pub struct TopicPartition {
  pub topic: String,
  pub partition_offset: Vec<PartitionOffset>,
}

/// Partition identity without an offset, used by pause/resume controls.
#[napi(object)]
#[derive(Clone, Debug)]
pub struct TopicPartitionId {
  pub topic: String,
  pub partition: i32,
}
