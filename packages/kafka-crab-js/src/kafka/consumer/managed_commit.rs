use std::{
  ffi::c_void,
  ptr::NonNull,
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
  },
};

use rdkafka::{
  bindings,
  consumer::Consumer,
  error::{KafkaError, KafkaResult},
  types::RDKafkaErrorCode,
  TopicPartitionList,
};

use super::{
  context::{LoggingConsumer, ManagedConsumerState},
  managed::{KafkaCode, ManagedError},
};

const POLL_TIMEOUT_MS: i32 = 100;

pub fn kafka_code(error: bindings::rd_kafka_resp_err_t) -> KafkaCode {
  KafkaCode {
    name: format!("{:?}", RDKafkaErrorCode::from(error)),
    code: error as i32,
  }
}

/// Reply queue for managed commits (RFC-0017). Unlike the RFC-0013 queue used
/// by `commit('Async')`, each result is delivered to the promise of the commit
/// that produced it, identified by the opaque passed at dispatch, and is not
/// republished as an observational `CommitCallback` event.
pub struct ManagedCommitQueue {
  queue: NonNull<bindings::rd_kafka_queue_t>,
  polling: AtomicBool,
  // Keep the client alive until our queue reference has been destroyed.
  consumer: Arc<LoggingConsumer>,
}

// librdkafka queues are thread safe. The owning Arc prevents destruction while
// enqueue/poll is in progress.
unsafe impl Send for ManagedCommitQueue {}
unsafe impl Sync for ManagedCommitQueue {}

impl ManagedCommitQueue {
  pub fn new(consumer: Arc<LoggingConsumer>) -> KafkaResult<Self> {
    // SAFETY: the consumer owns a live client, retained by this struct.
    let queue = unsafe { bindings::rd_kafka_queue_new(consumer.client().native_ptr()) };
    let queue = NonNull::new(queue)
      .ok_or_else(|| KafkaError::ClientCreation("Failed to create commit reply queue".into()))?;
    Ok(Self {
      queue,
      polling: AtomicBool::new(false),
      consumer,
    })
  }

  /// Hands the commit to librdkafka's group thread without waiting for it.
  /// With a reply queue `rd_kafka_commit_queue` only enqueues a local
  /// operation, so this is safe inside the managed state's critical section.
  pub fn enqueue(&self, offsets: &TopicPartitionList, id: u64) -> Result<(), KafkaCode> {
    // SAFETY: both pointers remain live for this call. librdkafka copies the
    // offsets and retains the reply queue for the scheduled operation. The
    // opaque is an integer id, never dereferenced.
    let error = unsafe {
      bindings::rd_kafka_commit_queue(
        self.consumer.client().native_ptr(),
        offsets.ptr(),
        self.queue.as_ptr(),
        None,
        id as usize as *mut c_void,
      )
    };
    if error as i32 == 0 {
      Ok(())
    } else {
      Err(kafka_code(error))
    }
  }

  /// Runs a blocking poller for as long as commits are unresolved. It retains
  /// the native client until every dispatched commit has an outcome.
  pub fn ensure_polling(self: &Arc<Self>, state: &Arc<ManagedConsumerState>) {
    if self.polling.swap(true, Ordering::AcqRel) {
      return;
    }
    let queue = Arc::clone(self);
    let state = Arc::clone(state);
    tokio::task::spawn_blocking(move || loop {
      let served = queue.poll(&state, POLL_TIMEOUT_MS);
      if state.pending_commits() == 0 {
        queue.polling.store(false, Ordering::Release);
        // A commit dispatched after the check above saw `polling == true`.
        if state.pending_commits() > 0 && !queue.polling.swap(true, Ordering::AcqRel) {
          continue;
        }
        break;
      }
      // SAFETY: the client is retained by `queue`.
      let closed =
        unsafe { bindings::rd_kafka_consumer_closed(queue.consumer.client().native_ptr()) != 0 };
      if closed && served == 0 && queue.poll(&state, 0) == 0 {
        // No result can arrive any more. The request may still have reached
        // the broker, so the outcome is unknown rather than failed.
        state.fail_pending_commits("consumer closed before the broker result was observed");
      }
    });
  }

  fn poll(&self, state: &ManagedConsumerState, timeout_ms: i32) -> usize {
    let mut served = 0;
    let mut timeout_ms = timeout_ms;
    // Bound work per pass so the pending check above runs regularly.
    for _ in 0..128 {
      // SAFETY: the queue is live for the lifetime of `self`.
      let event = unsafe { bindings::rd_kafka_queue_poll(self.queue.as_ptr(), timeout_ms) };
      timeout_ms = 0;
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
        let id = bindings::rd_kafka_event_opaque(event.0.as_ptr()) as usize as u64;
        let mut error = bindings::rd_kafka_event_error(event.0.as_ptr());
        if error as i32 == 0 {
          let offsets = bindings::rd_kafka_event_topic_partition_list(event.0.as_ptr());
          if !offsets.is_null() && (*offsets).cnt > 0 {
            // The event owns the list; only the error codes are read.
            if let Some(partition) =
              std::slice::from_raw_parts((*offsets).elems, (*offsets).cnt as usize)
                .iter()
                .find(|partition| partition.err as i32 != 0)
            {
              error = partition.err;
            }
          }
        }
        let outcome = if error as i32 == 0 {
          Ok(())
        } else {
          Err(ManagedError::CommitFailed(kafka_code(error)))
        };
        state.complete_commit(id, outcome);
        served += 1;
      }
    }
    served
  }
}

impl Drop for ManagedCommitQueue {
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
