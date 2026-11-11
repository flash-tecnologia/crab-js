use rdkafka::{
  consumer::{
    BaseConsumer, Consumer, ConsumerContext, Rebalance, RebalanceProtocol, StreamConsumer,
  },
  error::KafkaResult,
  ClientContext, TopicPartitionList,
};
use std::{ops::Deref, sync::Arc};
use tokio::{runtime::Handle, sync::broadcast};
use tracing::{debug, warn};

use crate::kafka::consumer::consumer_helper::convert_tpl_to_array_of_topic_partition;

use super::{
  managed::{ManagedState, PartitionKey},
  model::TopicPartition,
};
use crate::kafka::producer::model::Message;

pub type ManagedConsumerState = ManagedState<Message>;

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
  /// Present only for a managed consumer (RFC-0017). A legacy consumer keeps
  /// `None` and its callbacks allocate nothing beyond the broadcast event.
  pub(super) managed: Option<Arc<ManagedConsumerState>>,
}

impl KafkaCrabContext {
  pub fn new(managed: Option<Arc<ManagedConsumerState>>) -> Self {
    // Bounded broadcast channel preserving order for live receivers without
    // unbounded growth. `broadcast::channel(100)` rounds up to 128 slots; a
    // send on a full channel overwrites the oldest retained event (it never
    // blocks or fails for a full buffer). A lagging receiver observes the loss
    // as `Lagged(skipped)` on its next read; a late subscriber only sees
    // events sent after its `resubscribe()`.
    let (tx, rx) = broadcast::channel(100);
    KafkaCrabContext {
      event_channel: (tx, rx),
      managed,
    }
  }

  /// Captures the transition at its source, synchronously, inside the poll
  /// that serves the rebalance and before the native (un)assign. It only
  /// touches local state: no JavaScript and no broker round trip.
  fn capture_managed_transition(&self, consumer: &BaseConsumer<Self>, rebalance: &Rebalance) {
    let Some(managed) = &self.managed else {
      return;
    };
    match rebalance {
      Rebalance::Assign(partitions) => managed.on_assign(&partition_keys(partitions)),
      Rebalance::Revoke(partitions) => managed.on_revoke(
        &partition_keys(partitions),
        !matches!(
          consumer.rebalance_protocol(),
          RebalanceProtocol::Cooperative
        ),
        consumer.assignment_lost(),
      ),
      // Unreachable with rust-rdkafka 0.39: `handle_rebalance_event` only
      // forwards assign and revoke events. Kept for exhaustiveness; should it
      // ever fire, rust-rdkafka drops the whole assignment.
      Rebalance::Error(_) => managed.on_revoke(&[], true, false),
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
    self.capture_managed_transition(consumer, rebalance);

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

fn partition_keys(partitions: &TopicPartitionList) -> Vec<PartitionKey> {
  partitions
    .elements()
    .iter()
    .map(|partition| (partition.topic().to_owned(), partition.partition()))
    .collect()
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

#[cfg(test)]
mod tests {
  use super::*;
  use crate::kafka::consumer::managed::ManagedLimits;
  use rdkafka::ClientConfig;
  use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
  };

  fn consumer(managed: Option<Arc<ManagedConsumerState>>) -> BaseConsumer<KafkaCrabContext> {
    // The client is created without connecting; no broker is required.
    ClientConfig::new()
      .set("group.id", "context-test")
      .set("bootstrap.servers", "127.0.0.1:1")
      .create_with_context(KafkaCrabContext::new(managed))
      .expect("consumer")
  }

  fn partitions() -> TopicPartitionList {
    let mut partitions = TopicPartitionList::new();
    partitions.add_partition("topic", 0);
    partitions.add_partition("topic", 1);
    partitions
  }

  /// Counts live heap bytes per thread, so a test can measure what its own
  /// thread retains without interference from tests running in parallel.
  struct CountingAllocator;

  thread_local! {
    static LIVE_BYTES: Cell<isize> = const { Cell::new(0) };
  }

  fn track(delta: isize) {
    let _ = LIVE_BYTES.try_with(|live| live.set(live.get() + delta));
  }

  // SAFETY: defers to the system allocator; only adds thread-local counting.
  unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
      track(layout.size() as isize);
      System.alloc(layout)
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
      track(-(layout.size() as isize));
      System.dealloc(pointer, layout)
    }
  }

  #[global_allocator]
  static ALLOCATOR: CountingAllocator = CountingAllocator;

  fn retained_by(rebalances: usize, consumer: &BaseConsumer<KafkaCrabContext>) -> isize {
    let context = consumer.context();
    let partitions = partitions();
    let before = LIVE_BYTES.with(Cell::get);
    for _ in 0..rebalances {
      context.pre_rebalance(consumer, &Rebalance::Assign(&partitions));
      context.post_rebalance(consumer, &Rebalance::Assign(&partitions));
      context.pre_rebalance(consumer, &Rebalance::Revoke(&partitions));
      context.post_rebalance(consumer, &Rebalance::Revoke(&partitions));
    }
    LIVE_BYTES.with(Cell::get) - before
  }

  /// RFC-0017 invariant 7, measured: after the bounded broadcast is warm, ten
  /// thousand rebalances through the real callbacks of a legacy context
  /// retain no heap at all, while the same calls on a managed context with an
  /// unread control queue retain memory for every record.
  #[test]
  fn legacy_rebalances_retain_no_memory() {
    let legacy = consumer(None);
    // Fill the 128-slot broadcast once; later events replace older ones.
    retained_by(200, &legacy);
    let retained = retained_by(10_000, &legacy);
    assert!(
      retained.abs() < 4096,
      "legacy mode retained {retained} bytes over 10,000 rebalances"
    );
    assert!(legacy.context().managed.is_none());
    assert!(legacy.context().event_channel.0.len() <= 128);

    // The measurement is sensitive: managed records do show up in it.
    let managed = consumer(Some(Arc::new(ManagedState::new(ManagedLimits {
      max_control_records: 100_000,
      ..ManagedLimits::default()
    }))));
    retained_by(200, &managed);
    let retained = retained_by(10_000, &managed);
    assert!(
      retained > 1_000_000,
      "managed mode retained only {retained} bytes"
    );
  }

  /// Reproduction of the legacy gap (RFC-0017, "Problem and evidence"): the
  /// observational channel overwrites its oldest events, so a listener that
  /// falls behind cannot reconstruct the assignment history.
  #[test]
  fn reproduction_lagging_legacy_listener_loses_rebalance_events() {
    let consumer = consumer(None);
    let context = consumer.context();
    let partitions = partitions();
    let mut listener = context.event_channel.1.resubscribe();
    for _ in 0..100 {
      context.pre_rebalance(&consumer, &Rebalance::Revoke(&partitions));
      context.pre_rebalance(&consumer, &Rebalance::Assign(&partitions));
    }
    match listener.try_recv() {
      Err(broadcast::error::TryRecvError::Lagged(skipped)) => assert_eq!(skipped, 200 - 128),
      other => panic!("expected lost events, got {other:?}"),
    }
  }

  #[test]
  fn managed_transitions_are_captured_inside_the_rebalance_callback() {
    let managed = Arc::new(ManagedState::new(ManagedLimits::default()));
    let consumer = consumer(Some(managed.clone()));
    let context = consumer.context();
    let partitions = partitions();

    // The identity exists as soon as pre_rebalance returns: before the native
    // assign, and without any receive or JavaScript callback having run.
    context.pre_rebalance(&consumer, &Rebalance::Assign(&partitions));
    let first = managed.snapshot().assignments;
    assert_eq!(first.len(), 2);
    assert!(managed.is_assignment_active("topic", 0, &first[0].2));

    context.pre_rebalance(&consumer, &Rebalance::Revoke(&partitions));
    assert!(managed.snapshot().assignments.is_empty());
    assert!(!managed.is_assignment_active("topic", 0, &first[0].2));

    context.pre_rebalance(&consumer, &Rebalance::Assign(&partitions));
    let second = managed.snapshot().assignments;
    assert_ne!(first[0].2, second[0].2);
    assert_eq!(managed.snapshot().queued_control_records, 3);
    // post_rebalance stays observational and adds nothing to managed state.
    context.post_rebalance(&consumer, &Rebalance::Assign(&partitions));
    assert_eq!(managed.snapshot().queued_control_records, 3);
  }

  /// Managed mode with a bounded control budget: unread transitions end in
  /// the terminal overflow instead of growing or overwriting.
  #[test]
  fn managed_rebalances_without_a_reader_overflow_instead_of_growing() {
    let managed = Arc::new(ManagedState::new(ManagedLimits {
      max_control_records: 8,
      ..ManagedLimits::default()
    }));
    let consumer = consumer(Some(managed.clone()));
    let context = consumer.context();
    let partitions = partitions();
    for _ in 0..10_000 {
      context.pre_rebalance(&consumer, &Rebalance::Assign(&partitions));
      context.pre_rebalance(&consumer, &Rebalance::Revoke(&partitions));
    }
    let snapshot = managed.snapshot();
    assert_eq!(snapshot.queued_control_records, 0);
    assert!(snapshot.assignments.is_empty());
    assert!(matches!(
      snapshot.status,
      crate::kafka::consumer::managed::ManagedStatus::Failed(_)
    ));
  }
}
