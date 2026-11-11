//! The single native poller of a managed consumer (RFC-0017), written against
//! a small backend trait so its loop is tested without librdkafka.

use std::{
  future::Future,
  sync::{Arc, Mutex},
  time::Duration,
};

use tokio::{select, sync::watch, time::Instant};

use super::managed::{ManagedState, ManagedStatus, PartitionKey, ReceiveFailure};

/// How long the poller waits for the application to make room in a full
/// buffer before pausing partitions. It bounds the delay of rebalance
/// callbacks under saturation; a pause costs a fetch round trip on resume.
/// Failed pause/resume calls are retried at the same interval.
pub const STALL_GRACE: Duration = Duration::from_millis(100);

pub enum PumpEvent<M> {
  Message {
    topic: String,
    partition: i32,
    bytes: usize,
    message: M,
  },
  Error {
    failure: ReceiveFailure,
    fatal: bool,
  },
}

pub trait PumpBackend: Send + Sync + 'static {
  type Message: Send + 'static;

  /// Polls the client. Rebalance callbacks run inside this call. Dropping the
  /// future must not lose a message.
  fn recv(&self) -> impl Future<Output = PumpEvent<Self::Message>> + Send;
  /// Pauses the partitions and returns those the client did not confirm.
  /// May block on the client's own threads; never called on an async worker.
  fn pause(&self, partitions: &[PartitionKey]) -> Vec<PartitionKey>;
  /// Resumes the partitions and returns those the client did not confirm.
  fn resume(&self, partitions: &[PartitionKey]) -> Vec<PartitionKey>;
  /// Leaves the group so a failed consumer does not hold partitions.
  fn unsubscribe(&self);
}

/// Applies the pause/resume plan and records only what the client confirmed.
/// `flow_lock` serializes the pump with application pause/resume calls;
/// rebalance callbacks never take it. Returns false when a call must be
/// retried.
pub fn apply_flow<B: PumpBackend>(
  backend: &B,
  state: &ManagedState<B::Message>,
  flow_lock: &Mutex<()>,
) -> bool {
  let flow_guard = flow_lock.lock().unwrap_or_else(|e| e.into_inner());
  apply_flow_locked(backend, state, &flow_guard)
}

/// Plans and applies with the flow lock already held by the caller.
fn apply_flow_locked<B: PumpBackend>(
  backend: &B,
  state: &ManagedState<B::Message>,
  _flow_guard: &std::sync::MutexGuard<'_, ()>,
) -> bool {
  let plan = state.plan_flow();
  if plan.is_empty() {
    return true;
  }
  let mut failed = Vec::new();
  if !plan.pause.is_empty() {
    failed.extend(backend.pause(&plan.pause));
  }
  if !plan.resume.is_empty() {
    failed.extend(backend.resume(&plan.resume));
  }
  if !failed.is_empty() {
    tracing::warn!("Managed pause/resume not confirmed for {:?}", failed);
  }
  state.flow_applied(&plan, &failed)
}

/// Pause/resume requested by the application, on the calling thread. If the
/// pump is applying a plan right now the caller does not wait behind it: the
/// pump is woken and applies the new plan next. Whatever the client does not
/// confirm is retried by the pump, which fails the state if that never works;
/// no error is returned to the caller.
pub fn apply_flow_from_caller<B: PumpBackend>(
  backend: &B,
  state: &ManagedState<B::Message>,
  flow_lock: &Mutex<()>,
) {
  let guard = match flow_lock.try_lock() {
    Ok(guard) => guard,
    Err(std::sync::TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
    Err(std::sync::TryLockError::WouldBlock) => {
      state.mark_flow_dirty();
      return;
    }
  };
  // Applied under the guard obtained above: the lock is never requested a
  // second time, so the pump cannot slip in and make this thread wait.
  if !apply_flow_locked(backend, state, &guard) {
    state.mark_flow_dirty();
  }
}

async fn sleep_until_some(deadline: Option<Instant>) {
  match deadline {
    Some(deadline) => tokio::time::sleep_until(deadline).await,
    None => std::future::pending().await,
  }
}

/// Serves rebalance callbacks whether or not JavaScript is receiving, admits
/// each message right after the poll that returned it, and keeps the retained
/// messages bounded. Ends when `disconnected` fires or its sender is dropped
/// (the consumer was disconnected or garbage collected) and closes the state,
/// so nothing stays pending on a consumer that no longer polls.
pub async fn run_pump<B: PumpBackend>(
  state: Arc<ManagedState<B::Message>>,
  backend: Arc<B>,
  flow_lock: Arc<Mutex<()>>,
  mut disconnected: watch::Receiver<()>,
) {
  state.arm_receive_watchdog();
  let watchdog_tick = state
    .limits()
    .max_receive_idle
    .map(|limit| (limit / 4).clamp(Duration::from_millis(5), Duration::from_secs(1)));
  let mut released = false;
  let mut retry_flow_at: Option<Instant> = None;
  loop {
    if state.is_saturated() {
      // The retained-message bound is reached: take no more messages.
      // A consumer that is only slower than the fetcher makes room within
      // the grace period and never pays for a pause. If it stalls, pause
      // the partitions so polling can resume for assignment control.
      select! {
        biased;
        _ = disconnected.changed() => break,
        _ = state.pump_wake().notified() => {}
        _ = tokio::time::sleep(STALL_GRACE) => state.engage_backpressure(),
      }
    } else {
      select! {
        biased;
        _ = disconnected.changed() => break,
        _ = state.pump_wake().notified() => {}
        _ = sleep_until_some(retry_flow_at) => {
          retry_flow_at = None;
          state.mark_flow_dirty();
        }
        _ = sleep_until_some(watchdog_tick.map(|tick| Instant::now() + tick)) => {}
        event = backend.recv() => match event {
          PumpEvent::Message { topic, partition, bytes, message } => {
            state.admit_message(&topic, partition, bytes, message);
          }
          PumpEvent::Error { failure, fatal } => state.push_receive_error(failure, fatal),
        }
      }
    }
    state.check_receive_watchdog();
    match state.status() {
      ManagedStatus::Active => {
        if state.take_flow_dirty() {
          // Pause/resume wait on librdkafka's threads without a timeout, so
          // they run on the blocking pool. Polling pauses meanwhile.
          let (backend, state, flow_lock) = (backend.clone(), state.clone(), flow_lock.clone());
          let applied =
            tokio::task::spawn_blocking(move || apply_flow(&*backend, &state, &flow_lock)).await;
          if !applied.unwrap_or(false) {
            retry_flow_at = Some(Instant::now() + STALL_GRACE);
          }
        }
      }
      ManagedStatus::Failed(_) if !released => {
        // Fail closed: leave the group so the partitions are not held by a
        // consumer that can no longer commit. Polling continues so the
        // resulting revocation is served until disconnect.
        released = true;
        let backend = backend.clone();
        let state = state.clone();
        // Under the membership lock, so a subscribe that was in progress when
        // the state failed cannot be applied after this and rejoin the group.
        let _ =
          tokio::task::spawn_blocking(move || state.with_membership(|_| backend.unsubscribe()))
            .await;
      }
      _ => {}
    }
  }
  state.close();
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::kafka::consumer::managed::{
    ManagedEntry, ManagedError, ManagedLimits, Received, MAX_FLOW_FAILURES,
  };
  use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
  use tokio::{
    sync::{mpsc, Mutex as AsyncMutex},
    time::timeout,
  };

  /// A client whose deliveries and pause/resume results the test controls.
  struct FakeClient {
    events: AsyncMutex<mpsc::UnboundedReceiver<PumpEvent<u64>>>,
    polls: AtomicUsize,
    confirmed_resumes: AtomicUsize,
    fail_pause: AtomicBool,
    fail_resume: AtomicBool,
    pauses: Mutex<Vec<Vec<PartitionKey>>>,
    resumes: Mutex<Vec<Vec<PartitionKey>>>,
    unsubscribed: AtomicBool,
  }

  impl PumpBackend for FakeClient {
    type Message = u64;

    async fn recv(&self) -> PumpEvent<u64> {
      self.polls.fetch_add(1, Ordering::SeqCst);
      match self.events.lock().await.recv().await {
        Some(event) => event,
        None => std::future::pending().await,
      }
    }

    fn pause(&self, partitions: &[PartitionKey]) -> Vec<PartitionKey> {
      self.pauses.lock().unwrap().push(partitions.to_vec());
      if self.fail_pause.load(Ordering::SeqCst) {
        partitions.to_vec()
      } else {
        Vec::new()
      }
    }

    fn resume(&self, partitions: &[PartitionKey]) -> Vec<PartitionKey> {
      // The outcome is decided before the attempt becomes visible, and a
      // success is counted only once it is complete.
      let fail = self.fail_resume.load(Ordering::SeqCst);
      self.resumes.lock().unwrap().push(partitions.to_vec());
      if fail {
        partitions.to_vec()
      } else {
        self.confirmed_resumes.fetch_add(1, Ordering::SeqCst);
        Vec::new()
      }
    }

    fn unsubscribe(&self) {
      self.unsubscribed.store(true, Ordering::SeqCst);
    }
  }

  struct Harness {
    state: Arc<ManagedState<u64>>,
    client: Arc<FakeClient>,
    deliver: mpsc::UnboundedSender<PumpEvent<u64>>,
    disconnect: Option<watch::Sender<()>>,
    pump: tokio::task::JoinHandle<()>,
  }

  fn key(partition: i32) -> PartitionKey {
    ("topic".to_owned(), partition)
  }

  fn start(limits: ManagedLimits) -> Harness {
    let (deliver, events) = mpsc::unbounded_channel();
    let client = Arc::new(FakeClient {
      events: AsyncMutex::new(events),
      polls: AtomicUsize::new(0),
      confirmed_resumes: AtomicUsize::new(0),
      fail_pause: AtomicBool::new(false),
      fail_resume: AtomicBool::new(false),
      pauses: Mutex::default(),
      resumes: Mutex::default(),
      unsubscribed: AtomicBool::new(false),
    });
    let state = Arc::new(ManagedState::new(limits));
    let (disconnect, disconnected) = watch::channel(());
    let pump = tokio::spawn(run_pump(
      state.clone(),
      client.clone(),
      Arc::new(Mutex::new(())),
      disconnected,
    ));
    Harness {
      state,
      client,
      deliver,
      disconnect: Some(disconnect),
      pump,
    }
  }

  impl Harness {
    fn message(&self, offset: u64) {
      let _ = self.deliver.send(PumpEvent::Message {
        topic: "topic".to_owned(),
        partition: 0,
        bytes: 10,
        message: offset,
      });
    }

    async fn until(&self, what: &str, condition: impl Fn(&Harness) -> bool) {
      let deadline = Instant::now() + Duration::from_secs(15);
      while !condition(self) {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(2)).await;
      }
    }

    async fn next(&self) -> ManagedEntry<u64> {
      match timeout(Duration::from_secs(2), self.state.receive(None)).await {
        Ok(Ok(Received::Entry(entry))) => entry,
        other => panic!("expected a record, got {other:?}"),
      }
    }
  }

  fn small(max_buffered_messages: usize) -> ManagedLimits {
    ManagedLimits {
      max_buffered_messages,
      ..ManagedLimits::default()
    }
  }

  #[tokio::test]
  async fn a_full_buffer_stops_polling_and_pauses_only_after_the_grace_period() {
    let harness = start(small(4));
    harness.state.on_assign(&[key(0), key(1)]);
    for offset in 0..6 {
      harness.message(offset);
    }
    harness
      .until("the buffer to fill", |h| {
        h.state.snapshot().buffered_messages == 4
      })
      .await;
    // Saturated: no poll happens and nothing is paused within the grace
    // period, so a consumer that is merely slow never pays for a pause.
    let polls = harness.client.polls.load(Ordering::SeqCst);
    tokio::time::sleep(STALL_GRACE / 4).await;
    assert_eq!(harness.client.polls.load(Ordering::SeqCst), polls);
    assert!(harness.client.pauses.lock().unwrap().is_empty());
    assert_eq!(harness.state.snapshot().buffered_messages, 4);

    // The application stalls past the grace period: every owned partition is
    // paused and polling resumes for assignment control.
    harness
      .until("the pause", |h| h.state.snapshot().backpressure_paused)
      .await;
    harness
      .until("the pause call", |h| {
        !h.client.pauses.lock().unwrap().is_empty()
      })
      .await;
    assert_eq!(
      *harness.client.pauses.lock().unwrap(),
      vec![vec![key(0), key(1)]]
    );

    // Messages already fetched when the pause took effect are still kept.
    harness
      .until("the slipped messages", |h| {
        h.state.snapshot().buffered_messages == 6
      })
      .await;

    // Draining to half of the bound resumes them.
    assert!(matches!(harness.next().await, ManagedEntry::Assign { .. }));
    for _ in 0..4 {
      harness.next().await;
    }
    harness
      .until("the resume call", |h| {
        !h.client.resumes.lock().unwrap().is_empty()
      })
      .await;
    assert_eq!(
      *harness.client.resumes.lock().unwrap(),
      vec![vec![key(0), key(1)]]
    );
    assert!(!harness.state.snapshot().backpressure_paused);
  }

  #[tokio::test]
  async fn a_consumer_that_keeps_up_is_never_paused() {
    let harness = start(small(2));
    harness.state.on_assign(&[key(0)]);
    assert!(matches!(harness.next().await, ManagedEntry::Assign { .. }));
    for offset in 0..200 {
      harness.message(offset);
    }
    for offset in 0..200 {
      match harness.next().await {
        ManagedEntry::Message { message, .. } => assert_eq!(message, offset),
        other => panic!("expected message, got {other:?}"),
      }
      assert!(harness.state.snapshot().buffered_messages <= 2);
    }
    assert!(harness.client.pauses.lock().unwrap().is_empty());
  }

  #[tokio::test]
  async fn a_pause_that_is_not_confirmed_is_retried_and_the_hard_ceiling_is_terminal() {
    let harness = start(small(4));
    harness.client.fail_pause.store(true, Ordering::SeqCst);
    harness.state.on_assign(&[key(0)]);
    for offset in 0..4 {
      harness.message(offset);
    }
    // The pause is attempted again while it keeps failing.
    harness
      .until("pause retries", |h| {
        h.client.pauses.lock().unwrap().len() >= 2
      })
      .await;
    assert_eq!(harness.state.status(), ManagedStatus::Active);
    assert!(harness.state.snapshot().backpressure_paused);

    // The partition was never paused, so it keeps delivering. The retained
    // count may pass the bound but is cut off at the hard ceiling.
    let (ceiling, _) = harness.state.hard_ceilings();
    for offset in 4..=ceiling as u64 {
      harness.message(offset);
    }
    harness
      .until("the terminal failure", |h| {
        h.state.status() != ManagedStatus::Active
      })
      .await;
    assert!(matches!(
      harness.state.status(),
      ManagedStatus::Failed(ManagedError::BackpressureFailure(_))
    ));
    assert_eq!(harness.state.snapshot().buffered_messages, 0);
    // The failed consumer leaves the group and reports the failure.
    harness
      .until("the unsubscribe", |h| {
        h.client.unsubscribed.load(Ordering::SeqCst)
      })
      .await;
    assert!(matches!(
      harness.state.receive(None).await,
      Err(ManagedError::BackpressureFailure(_))
    ));
  }

  #[tokio::test]
  async fn a_pause_confirmed_on_retry_keeps_the_consumer_alive() {
    let harness = start(small(4));
    harness.client.fail_pause.store(true, Ordering::SeqCst);
    harness.state.on_assign(&[key(0)]);
    for offset in 0..4 {
      harness.message(offset);
    }
    harness
      .until("a failed pause", |h| {
        !h.client.pauses.lock().unwrap().is_empty()
      })
      .await;
    harness.client.fail_pause.store(false, Ordering::SeqCst);
    let attempts = harness.client.pauses.lock().unwrap().len();
    harness
      .until("the retry", |h| {
        h.client.pauses.lock().unwrap().len() > attempts
      })
      .await;
    tokio::time::sleep(STALL_GRACE * 2).await;
    // Confirmed: no further attempts, and the state is healthy.
    assert_eq!(harness.client.pauses.lock().unwrap().len(), attempts + 1);
    assert_eq!(harness.state.status(), ManagedStatus::Active);
  }

  #[tokio::test]
  async fn a_resume_that_never_succeeds_is_terminal_instead_of_a_silent_stall() {
    let harness = start(small(64));
    harness.state.on_assign(&[key(0)]);
    // Only the public path is used: recording the pause wakes the pump.
    harness.state.set_user_paused(&[key(0)], true).unwrap();
    harness
      .until("the pause", |h| !h.client.pauses.lock().unwrap().is_empty())
      .await;
    harness.client.fail_resume.store(true, Ordering::SeqCst);
    harness.state.set_user_paused(&[key(0)], false).unwrap();
    harness
      .until("the terminal failure", |h| {
        h.state.status() != ManagedStatus::Active
      })
      .await;
    assert!(matches!(
      harness.state.status(),
      ManagedStatus::Failed(ManagedError::BackpressureFailure(_))
    ));
    assert_eq!(
      harness.client.resumes.lock().unwrap().len(),
      MAX_FLOW_FAILURES as usize
    );
  }

  #[tokio::test]
  async fn the_watchdog_fails_a_consumer_whose_application_stops_receiving() {
    let harness = start(ManagedLimits {
      max_receive_idle: Some(Duration::from_millis(60)),
      ..ManagedLimits::default()
    });
    harness.state.on_assign(&[key(0)]);
    assert!(matches!(harness.next().await, ManagedEntry::Assign { .. }));
    harness
      .until("the watchdog", |h| {
        h.state.status() != ManagedStatus::Active
      })
      .await;
    assert_eq!(
      harness.state.status(),
      ManagedStatus::Failed(ManagedError::ReceiveStalled)
    );
    harness
      .until("the unsubscribe", |h| {
        h.client.unsubscribed.load(Ordering::SeqCst)
      })
      .await;
  }

  #[tokio::test]
  async fn dropping_the_consumer_without_disconnect_ends_the_pump_and_closes_the_state() {
    let mut harness = start(small(64));
    harness.state.on_assign(&[key(0)]);
    assert!(matches!(harness.next().await, ManagedEntry::Assign { .. }));
    let pending = tokio::spawn({
      let state = harness.state.clone();
      async move { state.receive(None).await }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!pending.is_finished());

    // The JavaScript consumer is collected: its sender goes away.
    drop(harness.disconnect.take());
    timeout(Duration::from_secs(2), &mut harness.pump)
      .await
      .expect("the pump must end")
      .unwrap();
    assert_eq!(harness.state.status(), ManagedStatus::Closed);
    assert!(matches!(
      timeout(Duration::from_secs(2), pending)
        .await
        .unwrap()
        .unwrap(),
      Ok(Received::End)
    ));
    assert_eq!(harness.state.reserve_receive(), Err(ManagedError::Closed));
  }

  #[tokio::test]
  async fn an_application_resume_that_is_not_confirmed_is_retried_on_an_idle_topic() {
    let harness = start(small(64));
    harness.state.on_assign(&[key(0)]);
    let flow_lock = Mutex::new(());
    harness.state.set_user_paused(&[key(0)], true).unwrap();
    apply_flow_from_caller(&*harness.client, &harness.state, &flow_lock);
    harness
      .until("the pause", |h| !h.client.pauses.lock().unwrap().is_empty())
      .await;

    // The caller's resume is rejected by the client. Nothing else happens on
    // the topic, yet the pump retries until the client confirms it.
    harness.client.fail_resume.store(true, Ordering::SeqCst);
    harness.state.set_user_paused(&[key(0)], false).unwrap();
    apply_flow_from_caller(&*harness.client, &harness.state, &flow_lock);
    harness
      .until("resume retries", |h| {
        h.client.resumes.lock().unwrap().len() >= 3
      })
      .await;
    assert_eq!(harness.client.confirmed_resumes.load(Ordering::SeqCst), 0);
    harness.client.fail_resume.store(false, Ordering::SeqCst);
    // Wait for an attempt that completed successfully, whichever one it is.
    harness
      .until("the confirmed resume", |h| {
        h.client.confirmed_resumes.load(Ordering::SeqCst) == 1
      })
      .await;
    // Give the pump time to record the confirmation, then expect silence.
    tokio::time::sleep(STALL_GRACE * 2).await;
    let attempts = harness.client.resumes.lock().unwrap().len();
    tokio::time::sleep(STALL_GRACE * 3).await;
    assert_eq!(harness.client.resumes.lock().unwrap().len(), attempts);
    assert_eq!(harness.client.confirmed_resumes.load(Ordering::SeqCst), 1);
    assert_eq!(harness.state.status(), ManagedStatus::Active);
  }

  #[tokio::test]
  async fn a_caller_does_not_wait_behind_a_plan_the_pump_is_applying() {
    let harness = start(small(64));
    harness.state.on_assign(&[key(0)]);
    let flow_lock = Mutex::new(());
    // The pump is inside a client call that holds the flow lock.
    let busy = flow_lock.lock().unwrap();
    harness.state.set_user_paused(&[key(0)], true).unwrap();
    apply_flow_from_caller(&*harness.client, &harness.state, &flow_lock);
    drop(busy);
    // The request was handed to the pump, which applies it.
    harness
      .until("the pause", |h| !h.client.pauses.lock().unwrap().is_empty())
      .await;
    assert_eq!(*harness.client.pauses.lock().unwrap(), vec![vec![key(0)]]);
  }

  #[tokio::test]
  async fn a_subscribe_in_progress_cannot_rejoin_after_the_terminal_unsubscribe() {
    let harness = start(ManagedLimits {
      max_control_records: 1,
      ..ManagedLimits::default()
    });
    let (in_subscribe_tx, in_subscribe_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let subscriber = std::thread::spawn({
      let (state, client) = (harness.state.clone(), harness.client.clone());
      move || {
        state.with_membership(|status| {
          let active = *status == ManagedStatus::Active;
          in_subscribe_tx.send(()).unwrap();
          release_rx.recv().unwrap();
          if active {
            // The native subscribe of the in-progress call.
            client.unsubscribed.store(false, Ordering::SeqCst);
          }
          active
        })
      }
    });
    in_subscribe_rx.recv().unwrap();
    // The state fails while that subscribe is in progress.
    harness.state.on_assign(&[key(0)]);
    harness.state.on_assign(&[]);
    assert!(matches!(harness.state.status(), ManagedStatus::Failed(_)));
    tokio::time::sleep(Duration::from_millis(50)).await;
    // The pump's unsubscribe waits for the subscribe instead of racing it.
    assert!(!harness.client.unsubscribed.load(Ordering::SeqCst));
    release_tx.send(()).unwrap();
    assert!(subscriber.join().unwrap());
    harness
      .until("the unsubscribe", |h| {
        h.client.unsubscribed.load(Ordering::SeqCst)
      })
      .await;
    // A subscribe that reaches this point later is refused.
    assert!(!harness
      .state
      .with_membership(|status| *status == ManagedStatus::Active));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(harness.client.unsubscribed.load(Ordering::SeqCst));
  }
}
