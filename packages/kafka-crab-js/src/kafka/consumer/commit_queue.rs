use std::{ffi::CStr, ptr::NonNull, sync::Arc, time::Duration};

use rdkafka::{
  bindings,
  consumer::Consumer,
  error::{KafkaError, KafkaResult},
  Offset, TopicPartitionList,
};
use tokio::sync::watch;

use super::{
  consumer_helper::convert_to_offset_model,
  context::{KafkaEvent, KafkaEventName, KafkaEventPayload, LoggingConsumer},
  model::{PartitionOffset, TopicPartition},
};

/// A reply queue exclusively for manual Async commits. It never consumes
/// messages or competes with the consumer's main queue for rebalance events.
pub struct AsyncCommitQueue {
  queue: NonNull<bindings::rd_kafka_queue_t>,
  // Keep the client alive until our queue reference has been destroyed.
  consumer: Arc<LoggingConsumer>,
}

// librdkafka queues are thread safe. The owning Arc prevents destruction while
// enqueue/poll is in progress; only the forwarding task polls this queue.
unsafe impl Send for AsyncCommitQueue {}
unsafe impl Sync for AsyncCommitQueue {}

impl AsyncCommitQueue {
  pub fn new(consumer: Arc<LoggingConsumer>) -> KafkaResult<Self> {
    // SAFETY: the consumer owns a live client, retained by this struct.
    let queue = unsafe { bindings::rd_kafka_queue_new(consumer.client().native_ptr()) };
    let queue = NonNull::new(queue)
      .ok_or_else(|| KafkaError::ClientCreation("Failed to create commit reply queue".into()))?;
    Ok(Self { queue, consumer })
  }

  pub fn enqueue(&self, offsets: &TopicPartitionList) -> KafkaResult<()> {
    // SAFETY: both pointers remain live for this call. librdkafka copies the
    // offsets and retains the reply queue for the scheduled operation. Event
    // delivery is enabled by StreamConsumer, so no C callback/opaque is needed.
    let error = unsafe {
      bindings::rd_kafka_commit_queue(
        self.consumer.client().native_ptr(),
        offsets.ptr(),
        self.queue.as_ptr(),
        None,
        std::ptr::null_mut(),
      )
    };
    if error as i32 == 0 {
      Ok(())
    } else {
      Err(KafkaError::ConsumerCommit(error.into()))
    }
  }

  pub fn start(self: &Arc<Self>, mut disconnected: watch::Receiver<()>) {
    // Do not retain the consumer forever if JS drops it without disconnect.
    let queue = Arc::downgrade(self);
    napi::bindgen_prelude::spawn(async move {
      let mut interval = tokio::time::interval(Duration::from_millis(10));
      interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
      loop {
        tokio::select! {
          biased;
          _ = disconnected.changed() => break,
          _ = interval.tick() => {
            let Some(queue) = queue.upgrade() else { break };
            queue.poll_ready();
          }
        }
      }
    });
  }

  fn poll_ready(&self) {
    // Bound work per tick so a commit burst cannot monopolize a Tokio worker.
    for _ in 0..128 {
      // SAFETY: the queue is live; timeout zero never waits for broker I/O.
      let event = unsafe { bindings::rd_kafka_queue_poll(self.queue.as_ptr(), 0) };
      let Some(event) = NonNull::new(event) else {
        break;
      };
      let event = CommitEvent(event);
      // SAFETY: the event guard owns the pointer until the end of this iteration.
      unsafe {
        if bindings::rd_kafka_event_type(event.0.as_ptr()) != bindings::RD_KAFKA_EVENT_OFFSET_COMMIT
        {
          continue;
        }
        let error = bindings::rd_kafka_event_error(event.0.as_ptr());
        let error = if error as i32 == 0 {
          None
        } else {
          Some(KafkaError::ConsumerCommit(error.into()).to_string())
        };
        let mut payload = KafkaEventPayload {
          action: None,
          tpl: Vec::new(),
          error,
        };
        let offsets = bindings::rd_kafka_event_topic_partition_list(event.0.as_ptr());
        if !offsets.is_null() && (*offsets).cnt > 0 {
          // The event owns the list and its strings. Copy all fields before
          // destroying it; no borrowed pointer escapes this iteration.
          for partition in std::slice::from_raw_parts((*offsets).elems, (*offsets).cnt as usize) {
            let topic = CStr::from_ptr(partition.topic)
              .to_string_lossy()
              .into_owned();
            if payload.error.is_none() && partition.err as i32 != 0 {
              payload.error = Some(format!(
                "{}[{}]: {}",
                topic,
                partition.partition,
                KafkaError::ConsumerCommit(partition.err.into())
              ));
            }
            payload.tpl.push(TopicPartition {
              topic,
              partition_offset: vec![PartitionOffset {
                partition: partition.partition,
                offset: convert_to_offset_model(&Offset::from_raw(partition.offset)),
              }],
            });
          }
        }
        self.consumer.context().send_event(KafkaEvent {
          name: KafkaEventName::CommitCallback,
          payload,
        });
      }
    }
  }
}

impl Drop for AsyncCommitQueue {
  fn drop(&mut self) {
    // SAFETY: release our sole owned reference before dropping the client Arc.
    unsafe { bindings::rd_kafka_queue_destroy(self.queue.as_ptr()) };
  }
}

struct CommitEvent(NonNull<bindings::rd_kafka_event_t>);

impl Drop for CommitEvent {
  fn drop(&mut self) {
    // SAFETY: queue_poll transferred ownership of this event to us.
    unsafe { bindings::rd_kafka_event_destroy(self.0.as_ptr()) };
  }
}
