//! Native state for managed consumption (RFC-0017).
//!
//! One `ManagedState` is shared by the consumer context (rebalance callbacks),
//! the receive pump, `recvManaged` and `commitManaged`. It is deliberately free
//! of librdkafka and N-API types so every ordering decision can be tested with
//! controllable scheduling.
//!
//! Locking rules:
//! - `inner` is a short, never-awaited `std::sync::Mutex`. The rebalance
//!   callback takes it, so nothing that waits for JavaScript or for the broker
//!   may run while it is held. The only foreign call made under it is the
//!   non-blocking commit dispatch closure (a local queue enqueue).
//! - A legacy consumer never constructs this type.

use std::{
  collections::{HashMap, HashSet, VecDeque},
  sync::{
    atomic::{AtomicU64, Ordering},
    Mutex, MutexGuard,
  },
  time::Duration,
};

use tokio::sync::{oneshot, Notify};

use super::byte_budget::DEFAULT_STREAM_BUFFER_BYTES;

pub const DEFAULT_MAX_CONTROL_RECORDS: usize = 1024;
pub const DEFAULT_MAX_BUFFERED_MESSAGES: usize = 1024;
pub const DEFAULT_MAX_BUFFERED_BYTES: usize = DEFAULT_STREAM_BUFFER_BYTES;
/// Receive errors are observational (never assignment control), so they are
/// coalesced instead of consuming the control budget without bound.
const MAX_QUEUED_RECEIVE_ERRORS: usize = 16;
const TOKEN_PREFIX: &str = "mc1";

static NEXT_INSTANCE: AtomicU64 = AtomicU64::new(1);

pub type PartitionKey = (String, i32);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ManagedLimits {
  pub max_control_records: usize,
  pub max_buffered_messages: usize,
  pub max_buffered_bytes: usize,
}

impl Default for ManagedLimits {
  fn default() -> Self {
    Self {
      max_control_records: DEFAULT_MAX_CONTROL_RECORDS,
      max_buffered_messages: DEFAULT_MAX_BUFFERED_MESSAGES,
      max_buffered_bytes: DEFAULT_MAX_BUFFERED_BYTES,
    }
  }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionChange {
  pub topic: String,
  pub partition: i32,
  /// Identity that owns the partition after the transition (assign only).
  pub assignment: Option<String>,
  /// Identity that owned it before: the invalidated one on revoke, or the
  /// unchanged one when an assign names a partition that was retained.
  pub previous_assignment: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KafkaCode {
  pub name: String,
  pub code: i32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReceiveFailure {
  pub message: String,
  pub kafka: Option<KafkaCode>,
}

#[derive(Debug)]
pub enum ManagedEntry<M> {
  Message {
    message: M,
    assignment: String,
  },
  Assign {
    partitions: Vec<PartitionChange>,
  },
  Revoke {
    partitions: Vec<PartitionChange>,
    lost: bool,
  },
  ReceiveError(ReceiveFailure),
}

#[derive(Debug)]
pub enum Received<M> {
  Entry(ManagedEntry<M>),
  /// No record within the caller's timeout, or the receive was cancelled.
  /// Not end-of-stream.
  Idle(&'static str),
  /// Clean disconnect while this receive was pending.
  End,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ManagedError {
  ReceivePending,
  Closed,
  ControlOverflow,
  ControlFailure(String),
  /// A managed operation was called on a legacy consumer.
  NotManaged,
  ReceiveFailed(ReceiveFailure),
  StaleAssignment,
  UnknownAssignment(String),
  InvalidArgument(String),
  CommitFailed(KafkaCode),
  /// The request was dispatched but no broker result was observed.
  CommitUnknown(String),
}

pub type CommitOutcome = Result<(), ManagedError>;

pub enum CommitAuthorization {
  Dispatched(oneshot::Receiver<CommitOutcome>),
  /// A commit made under an older identity of the same partition is still
  /// unresolved; wait on `commit_settled()` and authorize again.
  WaitForOlder,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ManagedStatus {
  Active,
  Closed,
  Failed(ManagedError),
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct FlowPlan {
  pub pause: Vec<PartitionKey>,
  pub resume: Vec<PartitionKey>,
}

impl FlowPlan {
  pub fn is_empty(&self) -> bool {
    self.pause.is_empty() && self.resume.is_empty()
  }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManagedSnapshot {
  pub status: ManagedStatus,
  pub assignments: Vec<(String, i32, String)>,
  pub queued_control_records: usize,
  pub buffered_messages: usize,
  pub buffered_bytes: usize,
  pub backpressure_paused: bool,
  pub pending_commits: usize,
  pub dropped_receive_errors: u64,
  pub purged_messages: u64,
}

pub enum Admission {
  Queued,
  /// The state is terminal; the message was not retained.
  Dropped,
}

struct PendingCommit {
  key: PartitionKey,
  seq: u64,
  reply: oneshot::Sender<CommitOutcome>,
}

struct QueuedMessage<M> {
  message: M,
  seq: u64,
  bytes: usize,
}

enum Queued<M> {
  Message(QueuedMessage<M>),
  Assign(Vec<PartitionChange>),
  Revoke(Vec<PartitionChange>, bool),
  ReceiveError(ReceiveFailure),
}

struct Inner<M> {
  status: ManagedStatus,
  active: HashMap<PartitionKey, u64>,
  next_seq: u64,
  queue: VecDeque<Queued<M>>,
  control_len: usize,
  error_len: usize,
  message_len: usize,
  message_bytes: usize,
  recv_pending: bool,
  cancel_requested: bool,
  commits: HashMap<u64, PendingCommit>,
  next_commit_id: u64,
  user_paused: HashSet<PartitionKey>,
  /// Partitions whose librdkafka application-pause flag we set. The flag
  /// outlives unassignment, so it is tracked until explicitly cleared.
  paused_native: HashSet<PartitionKey>,
  backpressure_paused: bool,
  dropped_receive_errors: u64,
  purged_messages: u64,
}

pub struct ManagedState<M> {
  instance: u64,
  limits: ManagedLimits,
  inner: Mutex<Inner<M>>,
  wake_receive: Notify,
  wake_pump: Notify,
  commit_settled: Notify,
}

fn token(instance: u64, seq: u64) -> String {
  format!("{TOKEN_PREFIX}.{instance:x}.{seq:x}")
}

fn parse_token(value: &str) -> Option<(u64, u64)> {
  let mut parts = value.split('.');
  if parts.next()? != TOKEN_PREFIX {
    return None;
  }
  let instance = u64::from_str_radix(parts.next()?, 16).ok()?;
  let seq = u64::from_str_radix(parts.next()?, 16).ok()?;
  parts.next().is_none().then_some((instance, seq))
}

pub struct ReceiveGuard<'a, M> {
  state: &'a ManagedState<M>,
}

impl<M> Drop for ReceiveGuard<'_, M> {
  fn drop(&mut self) {
    let mut inner = self.state.lock();
    inner.recv_pending = false;
    inner.cancel_requested = false;
  }
}

impl<M> ManagedState<M> {
  pub fn new(limits: ManagedLimits) -> Self {
    Self {
      instance: NEXT_INSTANCE.fetch_add(1, Ordering::Relaxed),
      limits,
      inner: Mutex::new(Inner {
        status: ManagedStatus::Active,
        active: HashMap::new(),
        next_seq: 1,
        queue: VecDeque::new(),
        control_len: 0,
        error_len: 0,
        message_len: 0,
        message_bytes: 0,
        recv_pending: false,
        cancel_requested: false,
        commits: HashMap::new(),
        next_commit_id: 1,
        user_paused: HashSet::new(),
        paused_native: HashSet::new(),
        backpressure_paused: false,
        dropped_receive_errors: 0,
        purged_messages: 0,
      }),
      wake_receive: Notify::new(),
      wake_pump: Notify::new(),
      commit_settled: Notify::new(),
    }
  }

  fn lock(&self) -> MutexGuard<'_, Inner<M>> {
    self.inner.lock().unwrap_or_else(|e| e.into_inner())
  }

  fn token(&self, seq: u64) -> String {
    token(self.instance, seq)
  }

  /// Enters the terminal failure: authorization is invalidated and buffers are
  /// released in the same critical section, before any JavaScript runs. The
  /// status itself is the terminal notification, so it cannot be lost to a
  /// full queue. Commits already dispatched keep their own reply slots.
  fn fail(&self, inner: &mut Inner<M>, error: ManagedError) {
    if inner.status != ManagedStatus::Active {
      return;
    }
    inner.status = ManagedStatus::Failed(error);
    Self::clear_buffers(inner);
    self.wake_receive.notify_one();
    self.wake_pump.notify_one();
  }

  fn clear_buffers(inner: &mut Inner<M>) {
    inner.active.clear();
    inner.queue.clear();
    inner.control_len = 0;
    inner.error_len = 0;
    inner.message_len = 0;
    inner.message_bytes = 0;
    inner.user_paused.clear();
    inner.backpressure_paused = false;
  }

  fn push_control(&self, inner: &mut Inner<M>, record: Queued<M>) {
    if inner.control_len >= self.limits.max_control_records {
      self.fail(inner, ManagedError::ControlOverflow);
      return;
    }
    inner.control_len += 1;
    inner.queue.push_back(record);
    self.wake_receive.notify_one();
    self.wake_pump.notify_one();
  }

  /// Rebalance callback: partitions were assigned. Runs before the native
  /// assign, so an identity exists before its first message can be fetched.
  /// A partition that is already owned keeps its identity.
  pub fn on_assign(&self, partitions: &[PartitionKey]) {
    let mut inner = self.lock();
    if inner.status != ManagedStatus::Active {
      return;
    }
    let mut changes = Vec::with_capacity(partitions.len());
    for key in partitions {
      let (assignment, previous_assignment) = match inner.active.get(key) {
        Some(seq) => (self.token(*seq), Some(self.token(*seq))),
        None => {
          let seq = inner.next_seq;
          inner.next_seq += 1;
          inner.active.insert(key.clone(), seq);
          (self.token(seq), None)
        }
      };
      changes.push(PartitionChange {
        topic: key.0.clone(),
        partition: key.1,
        assignment: Some(assignment),
        previous_assignment,
      });
    }
    self.push_control(&mut inner, Queued::Assign(changes));
  }

  /// Rebalance callback: partitions were revoked (`all` for the eager protocol
  /// and for rebalance errors, where the whole assignment is dropped).
  /// Invalidates the identities, and with them commit authorization, before
  /// the native unassign. Messages of those identities that JavaScript has not
  /// received yet are released: they can no longer be committed and the next
  /// owner refetches them from the committed offset.
  pub fn on_revoke(&self, partitions: &[PartitionKey], all: bool, lost: bool) {
    let mut inner = self.lock();
    if inner.status != ManagedStatus::Active {
      return;
    }
    let mut keys: Vec<PartitionKey> = partitions.to_vec();
    if all {
      let mut rest: Vec<PartitionKey> = inner
        .active
        .keys()
        .filter(|key| !partitions.contains(key))
        .cloned()
        .collect();
      rest.sort();
      keys.extend(rest);
    }
    let mut changes = Vec::with_capacity(keys.len());
    // Sequence numbers are unique per consumer, so they identify the revoked
    // identities without cloning partition keys while the callback runs.
    let mut revoked: HashSet<u64> = HashSet::new();
    for key in keys {
      let Some(seq) = inner.active.remove(&key) else {
        continue;
      };
      inner.user_paused.remove(&key);
      changes.push(PartitionChange {
        topic: key.0.clone(),
        partition: key.1,
        assignment: None,
        previous_assignment: Some(self.token(seq)),
      });
      revoked.insert(seq);
    }
    if !revoked.is_empty() && inner.message_len > 0 {
      let mut released = 0usize;
      let mut released_bytes = 0usize;
      inner.queue.retain(|entry| match entry {
        Queued::Message(queued) if revoked.contains(&queued.seq) => {
          released += 1;
          released_bytes += queued.bytes;
          false
        }
        _ => true,
      });
      inner.message_len -= released;
      inner.message_bytes -= released_bytes;
      inner.purged_messages += released as u64;
    }
    self.push_control(&mut inner, Queued::Revoke(changes, lost));
  }

  /// Admits a message taken from librdkafka. The caller is the only poller,
  /// and rebalance callbacks run inside that same poll, so every transition
  /// that preceded the message is already queued ahead of it and the identity
  /// read here is the one that owns this delivery.
  pub fn admit_message(&self, topic: &str, partition: i32, bytes: usize, message: M) -> Admission {
    let mut inner = self.lock();
    if inner.status != ManagedStatus::Active {
      return Admission::Dropped;
    }
    let key = (topic.to_owned(), partition);
    let Some(seq) = inner.active.get(&key).copied() else {
      self.fail(
        &mut inner,
        ManagedError::ControlFailure(format!(
          "message admitted for {topic}[{partition}] without an active assignment"
        )),
      );
      return Admission::Dropped;
    };
    inner.message_len += 1;
    inner.message_bytes += bytes;
    inner.queue.push_back(Queued::Message(QueuedMessage {
      message,
      seq,
      bytes,
    }));
    self.wake_receive.notify_one();
    Admission::Queued
  }

  pub fn push_receive_error(&self, failure: ReceiveFailure, fatal: bool) {
    let mut inner = self.lock();
    if inner.status != ManagedStatus::Active {
      return;
    }
    if fatal {
      self.fail(&mut inner, ManagedError::ControlFailure(failure.message));
      return;
    }
    if inner.error_len >= MAX_QUEUED_RECEIVE_ERRORS {
      inner.dropped_receive_errors += 1;
      return;
    }
    inner.error_len += 1;
    inner.queue.push_back(Queued::ReceiveError(failure));
    self.wake_receive.notify_one();
  }

  /// Marks the single allowed receive as pending.
  pub fn begin_receive(&self) -> Result<ReceiveGuard<'_, M>, ManagedError> {
    let mut inner = self.lock();
    match &inner.status {
      ManagedStatus::Active => {}
      ManagedStatus::Closed => return Err(ManagedError::Closed),
      ManagedStatus::Failed(error) => return Err(error.clone()),
    }
    if inner.recv_pending {
      return Err(ManagedError::ReceivePending);
    }
    inner.recv_pending = true;
    inner.cancel_requested = false;
    Ok(ReceiveGuard { state: self })
  }

  /// Returns the next record in native order. Control-only results are
  /// delivered without any message being available.
  pub async fn receive(&self, timeout: Option<Duration>) -> Result<Received<M>, ManagedError> {
    let _guard = self.begin_receive()?;
    let deadline = timeout.map(|timeout| tokio::time::Instant::now() + timeout);
    loop {
      {
        let mut inner = self.lock();
        match &inner.status {
          ManagedStatus::Active => {}
          ManagedStatus::Closed => return Ok(Received::End),
          ManagedStatus::Failed(error) => return Err(error.clone()),
        }
        if let Some(entry) = inner.queue.pop_front() {
          return Ok(Received::Entry(match entry {
            Queued::Message(queued) => {
              let was_full = self.is_full(&inner);
              inner.message_len -= 1;
              inner.message_bytes -= queued.bytes;
              // The pump is either waiting for room or holding a pause.
              if was_full || (inner.backpressure_paused && self.below_low_watermark(&inner)) {
                self.wake_pump.notify_one();
              }
              ManagedEntry::Message {
                message: queued.message,
                assignment: self.token(queued.seq),
              }
            }
            Queued::Assign(partitions) => {
              inner.control_len -= 1;
              ManagedEntry::Assign { partitions }
            }
            Queued::Revoke(partitions, lost) => {
              inner.control_len -= 1;
              ManagedEntry::Revoke { partitions, lost }
            }
            Queued::ReceiveError(failure) => {
              inner.error_len -= 1;
              ManagedEntry::ReceiveError(failure)
            }
          }));
        }
        if inner.cancel_requested {
          return Ok(Received::Idle("cancelled"));
        }
      }
      match deadline {
        Some(deadline) => {
          if tokio::time::timeout_at(deadline, self.wake_receive.notified())
            .await
            .is_err()
          {
            return Ok(Received::Idle("timeout"));
          }
        }
        None => self.wake_receive.notified().await,
      }
    }
  }

  /// Wakes the pending receive, which resolves as idle. Returns whether one
  /// was pending. A message already admitted stays queued for the next call.
  pub fn cancel_receive(&self) -> bool {
    let mut inner = self.lock();
    if !inner.recv_pending {
      return false;
    }
    inner.cancel_requested = true;
    self.wake_receive.notify_one();
    true
  }

  /// Final identity check and dispatch, ordered against revocation by `inner`:
  /// a revoke callback either completed before this call (stale) or starts
  /// after `dispatch` returned. `dispatch` must not block; it receives the
  /// commit id that later identifies the broker result.
  pub fn try_authorize_commit(
    &self,
    topic: &str,
    partition: i32,
    assignment: &str,
    dispatch: &mut dyn FnMut(u64) -> Result<(), KafkaCode>,
  ) -> Result<CommitAuthorization, ManagedError> {
    let mut inner = self.lock();
    match &inner.status {
      ManagedStatus::Active => {}
      ManagedStatus::Closed => return Err(ManagedError::Closed),
      ManagedStatus::Failed(error) => return Err(error.clone()),
    }
    let Some((instance, seq)) = parse_token(assignment) else {
      return Err(ManagedError::UnknownAssignment(
        "malformed assignment identity".to_owned(),
      ));
    };
    if instance != self.instance || seq == 0 || seq >= inner.next_seq {
      return Err(ManagedError::UnknownAssignment(
        "assignment identity was not issued by this consumer".to_owned(),
      ));
    }
    let key = (topic.to_owned(), partition);
    if inner.active.get(&key) != Some(&seq) {
      return Err(ManagedError::StaleAssignment);
    }
    if inner
      .commits
      .values()
      .any(|pending| pending.key == key && pending.seq != seq)
    {
      return Ok(CommitAuthorization::WaitForOlder);
    }
    let id = inner.next_commit_id;
    inner.next_commit_id += 1;
    let (reply, result) = oneshot::channel();
    inner.commits.insert(id, PendingCommit { key, seq, reply });
    if let Err(code) = dispatch(id) {
      inner.commits.remove(&id);
      self.commit_settled.notify_waiters();
      return Err(ManagedError::CommitFailed(code));
    }
    Ok(CommitAuthorization::Dispatched(result))
  }

  /// Delivers a broker result to the commit that produced it. The id binds the
  /// result to the original identity even after revocation or reassignment.
  pub fn complete_commit(&self, id: u64, outcome: CommitOutcome) -> bool {
    let pending = self.lock().commits.remove(&id);
    self.commit_settled.notify_waiters();
    match pending {
      Some(pending) => {
        let _ = pending.reply.send(outcome);
        true
      }
      None => false,
    }
  }

  pub fn fail_pending_commits(&self, reason: &str) {
    let pending: Vec<PendingCommit> = self.lock().commits.drain().map(|(_, c)| c).collect();
    self.commit_settled.notify_waiters();
    for commit in pending {
      let _ = commit
        .reply
        .send(Err(ManagedError::CommitUnknown(reason.to_owned())));
    }
  }

  pub fn pending_commits(&self) -> usize {
    self.lock().commits.len()
  }

  pub fn commit_settled(&self) -> &Notify {
    &self.commit_settled
  }

  pub fn pump_wake(&self) -> &Notify {
    &self.wake_pump
  }

  pub fn is_assignment_active(&self, topic: &str, partition: i32, assignment: &str) -> bool {
    let Some((instance, seq)) = parse_token(assignment) else {
      return false;
    };
    let inner = self.lock();
    instance == self.instance
      && inner.status == ManagedStatus::Active
      && inner.active.get(&(topic.to_owned(), partition)) == Some(&seq)
  }

  pub fn status(&self) -> ManagedStatus {
    self.lock().status.clone()
  }

  /// Clean disconnect: stops admission, invalidates authorization, clears the
  /// buffers and wakes the pending receive.
  pub fn close(&self) {
    let mut inner = self.lock();
    if inner.status == ManagedStatus::Active {
      inner.status = ManagedStatus::Closed;
    }
    Self::clear_buffers(&mut inner);
    self.wake_receive.notify_one();
    self.wake_pump.notify_one();
  }

  fn is_full(&self, inner: &Inner<M>) -> bool {
    inner.message_len >= self.limits.max_buffered_messages
      || inner.message_bytes >= self.limits.max_buffered_bytes
  }

  /// The retained-message bound is reached and no pause is in effect: the
  /// poller must stop taking messages. It waits for the application to make
  /// room and, if that takes too long, calls `engage_backpressure` so it can
  /// keep polling for assignment control without fetching more data.
  pub fn is_saturated(&self) -> bool {
    let inner = self.lock();
    inner.status == ManagedStatus::Active && !inner.backpressure_paused && self.is_full(&inner)
  }

  /// Requests a pause of every owned partition; `plan_flow` applies it and
  /// lifts it once the application has drained half of the bound.
  pub fn engage_backpressure(&self) {
    let mut inner = self.lock();
    if inner.status == ManagedStatus::Active && self.is_full(&inner) {
      inner.backpressure_paused = true;
    }
  }

  fn below_low_watermark(&self, inner: &Inner<M>) -> bool {
    inner.message_len <= self.limits.max_buffered_messages / 2
      && inner.message_bytes <= self.limits.max_buffered_bytes / 2
  }

  /// Computes the pause/resume calls that bring librdkafka in line with the
  /// retained-message bounds and the application's own pauses. The caller
  /// applies the plan outside this lock and reports it with `flow_applied`.
  pub fn plan_flow(&self) -> FlowPlan {
    let mut inner = self.lock();
    if inner.status != ManagedStatus::Active {
      return FlowPlan::default();
    }
    if inner.backpressure_paused && self.below_low_watermark(&inner) {
      inner.backpressure_paused = false;
    }
    let mut plan = FlowPlan::default();
    for key in inner.active.keys() {
      let wanted = inner.backpressure_paused || inner.user_paused.contains(key);
      if wanted && !inner.paused_native.contains(key) {
        plan.pause.push(key.clone());
      }
    }
    for key in &inner.paused_native {
      let wanted = inner.active.contains_key(key)
        && (inner.backpressure_paused || inner.user_paused.contains(key));
      if !wanted {
        plan.resume.push(key.clone());
      }
    }
    plan.pause.sort();
    plan.resume.sort();
    plan
  }

  pub fn flow_applied(&self, plan: &FlowPlan) {
    let mut inner = self.lock();
    for key in &plan.pause {
      inner.paused_native.insert(key.clone());
    }
    for key in &plan.resume {
      inner.paused_native.remove(key);
    }
  }

  /// Records an application pause/resume. Only owned partitions can be paused;
  /// a pause does not survive revocation.
  pub fn set_user_paused(
    &self,
    partitions: &[PartitionKey],
    paused: bool,
  ) -> Result<(), ManagedError> {
    let mut inner = self.lock();
    match &inner.status {
      ManagedStatus::Active => {}
      ManagedStatus::Closed => return Err(ManagedError::Closed),
      ManagedStatus::Failed(error) => return Err(error.clone()),
    }
    if paused {
      if let Some((topic, partition)) = partitions
        .iter()
        .find(|key| !inner.active.contains_key(*key))
      {
        return Err(ManagedError::InvalidArgument(format!(
          "{topic}[{partition}] is not assigned to this consumer"
        )));
      }
    }
    for key in partitions {
      if paused {
        inner.user_paused.insert(key.clone());
      } else {
        inner.user_paused.remove(key);
      }
    }
    Ok(())
  }

  pub fn active_partitions(&self) -> Vec<PartitionKey> {
    let mut keys: Vec<PartitionKey> = self.lock().active.keys().cloned().collect();
    keys.sort();
    keys
  }

  pub fn snapshot(&self) -> ManagedSnapshot {
    let inner = self.lock();
    let mut assignments: Vec<(String, i32, String)> = inner
      .active
      .iter()
      .map(|((topic, partition), seq)| (topic.clone(), *partition, self.token(*seq)))
      .collect();
    assignments.sort();
    ManagedSnapshot {
      status: inner.status.clone(),
      assignments,
      queued_control_records: inner.control_len,
      buffered_messages: inner.message_len,
      buffered_bytes: inner.message_bytes,
      backpressure_paused: inner.backpressure_paused,
      pending_commits: inner.commits.len(),
      dropped_receive_errors: inner.dropped_receive_errors,
      purged_messages: inner.purged_messages,
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::sync::{mpsc, Arc};
  use tokio::time::timeout;

  type State = ManagedState<u64>;

  fn key(partition: i32) -> PartitionKey {
    ("topic".to_owned(), partition)
  }

  fn small(max_control_records: usize, max_buffered_messages: usize) -> State {
    ManagedState::new(ManagedLimits {
      max_control_records,
      max_buffered_messages,
      max_buffered_bytes: 1 << 20,
    })
  }

  async fn next(state: &State) -> ManagedEntry<u64> {
    match timeout(Duration::from_secs(1), state.receive(None)).await {
      Ok(Ok(Received::Entry(entry))) => entry,
      other => panic!("expected a record, got {other:?}"),
    }
  }

  async fn assigned(state: &State) -> Vec<PartitionChange> {
    match next(state).await {
      ManagedEntry::Assign { partitions } => partitions,
      other => panic!("expected assign, got {other:?}"),
    }
  }

  async fn revoked(state: &State) -> Vec<PartitionChange> {
    match next(state).await {
      ManagedEntry::Revoke { partitions, .. } => partitions,
      other => panic!("expected revoke, got {other:?}"),
    }
  }

  fn identity(state: &State, partition: i32) -> String {
    state
      .snapshot()
      .assignments
      .into_iter()
      .find(|(_, p, _)| *p == partition)
      .map(|(_, _, token)| token)
      .expect("partition is assigned")
  }

  fn dispatch_ok(state: &State, partition: i32, token: &str) -> oneshot::Receiver<CommitOutcome> {
    match state.try_authorize_commit("topic", partition, token, &mut |_| Ok(())) {
      Ok(CommitAuthorization::Dispatched(result)) => result,
      Ok(CommitAuthorization::WaitForOlder) => panic!("unexpected wait"),
      Err(error) => panic!("unexpected {error:?}"),
    }
  }

  #[tokio::test]
  async fn revoke_then_reassign_issues_a_distinct_identity() {
    let state = small(16, 16);
    state.on_assign(&[key(0), key(1)]);
    let first = assigned(&state).await;
    assert_eq!(first.len(), 2);
    assert!(first.iter().all(|p| p.previous_assignment.is_none()));
    let old = first[0].assignment.clone().unwrap();
    assert_ne!(first[0].assignment, first[1].assignment);

    state.on_revoke(&[key(0)], false, false);
    let gone = revoked(&state).await;
    assert_eq!(gone.len(), 1);
    assert_eq!(gone[0].previous_assignment.as_deref(), Some(old.as_str()));
    assert_eq!(gone[0].assignment, None);
    assert!(!state.is_assignment_active("topic", 0, &old));

    state.on_assign(&[key(0)]);
    let again = assigned(&state).await;
    let new = again[0].assignment.clone().unwrap();
    assert_ne!(new, old);
    assert!(state.is_assignment_active("topic", 0, &new));
    assert!(!state.is_assignment_active("topic", 0, &old));
  }

  #[tokio::test]
  async fn cooperative_retained_partition_and_empty_assign_keep_identity() {
    let state = small(16, 16);
    state.on_assign(&[key(0), key(1)]);
    let first = assigned(&state).await;
    let kept = first[1].assignment.clone().unwrap();

    // Incremental revoke of partition 0 only, then the empty assign that ends
    // a cooperative rebalance, then an incremental assign of a new partition.
    state.on_revoke(&[key(0)], false, false);
    state.on_assign(&[]);
    state.on_assign(&[key(2)]);
    revoked(&state).await;
    assert!(assigned(&state).await.is_empty());
    let added = assigned(&state).await;
    assert_eq!(added[0].partition, 2);
    assert_eq!(identity(&state, 1), kept);
    assert!(state.is_assignment_active("topic", 1, &kept));

    // An assign naming an owned partition reports it as retained.
    state.on_assign(&[key(1)]);
    let retained = assigned(&state).await;
    assert_eq!(retained[0].assignment.as_deref(), Some(kept.as_str()));
    assert_eq!(
      retained[0].previous_assignment.as_deref(),
      Some(kept.as_str())
    );
  }

  #[tokio::test]
  async fn eager_revoke_drops_every_identity() {
    let state = small(16, 16);
    state.on_assign(&[key(0), key(1), key(2)]);
    state.on_revoke(&[key(1)], true, true);
    assigned(&state).await;
    match next(&state).await {
      ManagedEntry::Revoke { partitions, lost } => {
        assert!(lost);
        let mut ids: Vec<i32> = partitions.iter().map(|p| p.partition).collect();
        ids.sort();
        assert_eq!(ids, vec![0, 1, 2]);
      }
      other => panic!("expected revoke, got {other:?}"),
    }
    assert!(state.snapshot().assignments.is_empty());
  }

  #[tokio::test]
  async fn transitions_are_delivered_before_the_message_they_precede() {
    let state = small(16, 16);
    state.on_assign(&[key(0)]);
    let a1 = identity(&state, 0);
    assert!(matches!(
      state.admit_message("topic", 0, 10, 100),
      Admission::Queued
    ));
    // One poll serves a revoke, a reassign and then yields a message.
    state.on_revoke(&[key(0)], false, false);
    state.on_assign(&[key(0)]);
    let a2 = identity(&state, 0);
    state.admit_message("topic", 0, 10, 200);

    assigned(&state).await;
    // Message 100 was never handed to JavaScript and its identity is gone.
    assert_eq!(
      revoked(&state).await[0].previous_assignment.as_deref(),
      Some(a1.as_str())
    );
    assert_eq!(
      assigned(&state).await[0].assignment.as_deref(),
      Some(a2.as_str())
    );
    match next(&state).await {
      ManagedEntry::Message {
        message,
        assignment,
      } => {
        assert_eq!(message, 200);
        assert_eq!(assignment, a2);
      }
      other => panic!("expected message, got {other:?}"),
    }
    let snapshot = state.snapshot();
    assert_eq!(snapshot.purged_messages, 1);
    assert_eq!(snapshot.buffered_messages, 0);
    assert_eq!(snapshot.buffered_bytes, 0);
  }

  #[tokio::test]
  async fn message_is_stamped_with_the_identity_active_at_admission() {
    let state = small(16, 16);
    state.on_assign(&[key(0), key(1)]);
    let a0 = identity(&state, 0);
    state.admit_message("topic", 0, 1, 1);
    // A later transition of another partition does not restamp it.
    state.on_revoke(&[key(1)], false, false);
    assigned(&state).await;
    match next(&state).await {
      ManagedEntry::Message { assignment, .. } => assert_eq!(assignment, a0),
      other => panic!("expected message, got {other:?}"),
    }
    revoked(&state).await;
  }

  #[tokio::test]
  async fn control_is_delivered_without_messages_and_wakes_a_pending_receive() {
    let state = Arc::new(small(16, 16));
    let waiter = tokio::spawn({
      let state = state.clone();
      async move { state.receive(None).await }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!waiter.is_finished());
    state.on_assign(&[key(0)]);
    let record = timeout(Duration::from_secs(1), waiter)
      .await
      .unwrap()
      .unwrap();
    assert!(matches!(
      record,
      Ok(Received::Entry(ManagedEntry::Assign { .. }))
    ));
  }

  #[tokio::test]
  async fn message_without_an_active_assignment_is_a_terminal_control_failure() {
    let state = small(16, 16);
    assert!(matches!(
      state.admit_message("topic", 0, 1, 1),
      Admission::Dropped
    ));
    assert!(matches!(
      state.receive(None).await,
      Err(ManagedError::ControlFailure(_))
    ));
  }

  #[tokio::test]
  async fn stale_and_foreign_identities_cannot_authorize_a_commit() {
    let state = small(16, 16);
    state.on_assign(&[key(0), key(1)]);
    let old = identity(&state, 0);
    let other = identity(&state, 1);
    state.on_revoke(&[key(0)], false, false);
    let mut dispatched = 0;
    let mut dispatch = |_| {
      dispatched += 1;
      Ok(())
    };
    assert_eq!(
      state
        .try_authorize_commit("topic", 0, &old, &mut dispatch)
        .err(),
      Some(ManagedError::StaleAssignment)
    );
    state.on_assign(&[key(0)]);
    assert_eq!(
      state
        .try_authorize_commit("topic", 0, &old, &mut dispatch)
        .err(),
      Some(ManagedError::StaleAssignment)
    );
    // Another partition's identity is not valid for this partition.
    assert_eq!(
      state
        .try_authorize_commit("topic", 0, &other, &mut dispatch)
        .err(),
      Some(ManagedError::StaleAssignment)
    );
    let foreign = small(16, 16);
    foreign.on_assign(&[key(0)]);
    assert!(matches!(
      state.try_authorize_commit("topic", 0, &identity(&foreign, 0), &mut dispatch),
      Err(ManagedError::UnknownAssignment(_))
    ));
    assert!(matches!(
      state.try_authorize_commit("topic", 0, "not-a-token", &mut dispatch),
      Err(ManagedError::UnknownAssignment(_))
    ));
    assert_eq!(dispatched, 0);
  }

  #[tokio::test]
  async fn commit_result_stays_bound_to_its_original_identity() {
    let state = small(16, 16);
    state.on_assign(&[key(0)]);
    let old = identity(&state, 0);
    let mut id = 0;
    let result = match state.try_authorize_commit("topic", 0, &old, &mut |commit| {
      id = commit;
      Ok(())
    }) {
      Ok(CommitAuthorization::Dispatched(result)) => result,
      _ => panic!("expected dispatch"),
    };
    state.on_revoke(&[key(0)], false, false);
    state.on_assign(&[key(0)]);
    let new = identity(&state, 0);

    // The new identity waits for the old in-flight request instead of
    // overlapping it, and the late result does not touch the new assignment.
    assert!(matches!(
      state.try_authorize_commit("topic", 0, &new, &mut |_| Ok(())),
      Ok(CommitAuthorization::WaitForOlder)
    ));
    let settled = state.commit_settled().notified();
    tokio::pin!(settled);
    settled.as_mut().enable();
    assert!(state.complete_commit(id, Ok(())));
    timeout(Duration::from_secs(1), settled).await.unwrap();
    assert_eq!(result.await.unwrap(), Ok(()));
    assert!(state.is_assignment_active("topic", 0, &new));
    assert!(!state.complete_commit(id, Ok(())));
    dispatch_ok(&state, 0, &new);
  }

  #[tokio::test]
  async fn dispatch_failure_is_reported_with_the_kafka_code() {
    let state = small(16, 16);
    state.on_assign(&[key(0)]);
    let token = identity(&state, 0);
    let code = KafkaCode {
      name: "UnknownGroup".to_owned(),
      code: -179,
    };
    assert_eq!(
      state
        .try_authorize_commit("topic", 0, &token, &mut |_| Err(code.clone()))
        .err(),
      Some(ManagedError::CommitFailed(code))
    );
    assert_eq!(state.pending_commits(), 0);
  }

  #[tokio::test]
  async fn control_overflow_is_terminal_and_invalidates_authorization() {
    let state = Arc::new(small(2, 16));
    state.on_assign(&[key(0)]);
    let token = identity(&state, 0);
    let in_flight = dispatch_ok(&state, 0, &token);
    state.admit_message("topic", 0, 5, 1);
    state.on_assign(&[]);
    assert_eq!(state.status(), ManagedStatus::Active);

    // The third control record does not fit: nothing is overwritten.
    state.on_assign(&[]);
    assert_eq!(
      state.status(),
      ManagedStatus::Failed(ManagedError::ControlOverflow)
    );
    // Authorization is gone before any receive observed the failure.
    assert!(!state.is_assignment_active("topic", 0, &token));
    assert_eq!(
      state
        .try_authorize_commit("topic", 0, &token, &mut |_| Ok(()))
        .err(),
      Some(ManagedError::ControlOverflow)
    );
    let snapshot = state.snapshot();
    assert_eq!(snapshot.buffered_messages, 0);
    assert_eq!(snapshot.queued_control_records, 0);
    assert!(matches!(
      state.receive(None).await,
      Err(ManagedError::ControlOverflow)
    ));
    assert!(matches!(
      state.admit_message("topic", 0, 5, 2),
      Admission::Dropped
    ));
    // A request dispatched before the overflow still reports its own result.
    assert_eq!(state.pending_commits(), 1);
    drop(in_flight);
  }

  #[tokio::test]
  async fn overflow_wakes_a_pending_receive() {
    let state = Arc::new(small(0, 16));
    let waiter = tokio::spawn({
      let state = state.clone();
      async move { state.receive(None).await }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!waiter.is_finished());
    state.on_assign(&[key(0)]);
    let result = timeout(Duration::from_secs(1), waiter)
      .await
      .unwrap()
      .unwrap();
    assert!(matches!(result, Err(ManagedError::ControlOverflow)));
  }

  #[tokio::test]
  async fn concurrent_receive_is_rejected_and_cancel_keeps_admitted_messages() {
    let state = Arc::new(small(16, 16));
    let waiter = tokio::spawn({
      let state = state.clone();
      async move { state.receive(None).await }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(matches!(
      state.receive(None).await,
      Err(ManagedError::ReceivePending)
    ));
    assert!(state.cancel_receive());
    assert!(matches!(
      timeout(Duration::from_secs(1), waiter)
        .await
        .unwrap()
        .unwrap(),
      Ok(Received::Idle("cancelled"))
    ));
    assert!(!state.cancel_receive());
    assert!(matches!(
      state.receive(Some(Duration::from_millis(10))).await,
      Ok(Received::Idle("timeout"))
    ));
    state.on_assign(&[key(0)]);
    state.admit_message("topic", 0, 1, 7);
    assigned(&state).await;
    assert!(matches!(
      next(&state).await,
      ManagedEntry::Message { message: 7, .. }
    ));
  }

  #[tokio::test]
  async fn close_wakes_the_pending_receive_and_refuses_new_operations() {
    let state = Arc::new(small(16, 16));
    state.on_assign(&[key(0)]);
    let token = identity(&state, 0);
    assigned(&state).await;
    state.admit_message("topic", 0, 9, 1);
    let waiter = tokio::spawn({
      let state = state.clone();
      async move {
        tokio::time::sleep(Duration::from_millis(30)).await;
        state.receive(None).await
      }
    });
    state.close();
    // The receive started after close observed the terminal state directly.
    assert!(matches!(
      timeout(Duration::from_secs(1), waiter)
        .await
        .unwrap()
        .unwrap(),
      Err(ManagedError::Closed)
    ));
    assert_eq!(
      state
        .try_authorize_commit("topic", 0, &token, &mut |_| Ok(()))
        .err(),
      Some(ManagedError::Closed)
    );
    let snapshot = state.snapshot();
    assert_eq!(snapshot.buffered_messages, 0);
    assert_eq!(snapshot.buffered_bytes, 0);
    assert!(snapshot.assignments.is_empty());
    // Revocations served while closing are not accumulated.
    state.on_revoke(&[key(0)], true, false);
    assert_eq!(state.snapshot().queued_control_records, 0);

    let pending = Arc::new(small(16, 16));
    let waiter = tokio::spawn({
      let pending = pending.clone();
      async move { pending.receive(None).await }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    pending.close();
    assert!(matches!(
      timeout(Duration::from_secs(1), waiter)
        .await
        .unwrap()
        .unwrap(),
      Ok(Received::End)
    ));
  }

  #[tokio::test]
  async fn pending_commits_are_failed_as_unknown_when_no_result_can_arrive() {
    let state = small(16, 16);
    state.on_assign(&[key(0)]);
    let result = dispatch_ok(&state, 0, &identity(&state, 0));
    state.close();
    state.fail_pending_commits("consumer closed");
    assert!(matches!(
      result.await.unwrap(),
      Err(ManagedError::CommitUnknown(_))
    ));
  }

  /// The revoke callback cannot slip between the identity check and the
  /// dispatch: it blocks only for the non-blocking dispatch itself.
  #[test]
  fn revoke_is_ordered_after_a_dispatch_that_already_passed_the_check() {
    let state = Arc::new(small(16, 16));
    state.on_assign(&[key(0)]);
    let token = identity(&state, 0);
    let (in_dispatch_tx, in_dispatch_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let (revoked_tx, revoked_rx) = mpsc::channel();

    let committer = std::thread::spawn({
      let state = state.clone();
      let token = token.clone();
      move || {
        let mut dispatched = false;
        let outcome = state.try_authorize_commit("topic", 0, &token, &mut |_| {
          in_dispatch_tx.send(()).unwrap();
          release_rx.recv().unwrap();
          dispatched = true;
          Ok(())
        });
        (dispatched, outcome.is_ok())
      }
    });
    in_dispatch_rx.recv().unwrap();
    let callback = std::thread::spawn({
      let state = state.clone();
      move || {
        state.on_revoke(&[key(0)], false, false);
        revoked_tx.send(()).unwrap();
      }
    });
    // The callback waits for the dispatch that is already authorized.
    assert!(revoked_rx.recv_timeout(Duration::from_millis(100)).is_err());
    release_tx.send(()).unwrap();
    assert_eq!(committer.join().unwrap(), (true, true));
    revoked_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    callback.join().unwrap();
    // Any commit that starts after the callback is refused.
    assert_eq!(
      state
        .try_authorize_commit("topic", 0, &token, &mut |_| Ok(()))
        .err(),
      Some(ManagedError::StaleAssignment)
    );
  }

  /// A check made before scheduling is not enough: here the revoke lands
  /// between the early check and the dispatch, and the dispatch is refused.
  #[test]
  fn check_before_scheduling_alone_does_not_authorize_a_dispatch() {
    let state = small(16, 16);
    state.on_assign(&[key(0)]);
    let token = identity(&state, 0);
    assert!(state.is_assignment_active("topic", 0, &token));
    state.on_revoke(&[key(0)], false, false);
    let mut dispatched = false;
    assert_eq!(
      state
        .try_authorize_commit("topic", 0, &token, &mut |_| {
          dispatched = true;
          Ok(())
        })
        .err(),
      Some(ManagedError::StaleAssignment)
    );
    assert!(!dispatched);
  }

  /// Waiting for a broker result holds no lock: rebalance callbacks, receives
  /// and other commits proceed while a commit is unresolved.
  #[tokio::test]
  async fn callbacks_do_not_deadlock_with_an_unresolved_commit() {
    let state = Arc::new(small(64, 16));
    state.on_assign(&[key(0), key(1)]);
    let unresolved = dispatch_ok(&state, 0, &identity(&state, 0));
    let callbacks = tokio::task::spawn_blocking({
      let state = state.clone();
      move || {
        for _ in 0..10 {
          state.on_revoke(&[key(0)], false, false);
          state.on_assign(&[key(0)]);
        }
      }
    });
    timeout(Duration::from_secs(1), callbacks)
      .await
      .expect("callbacks must not wait for the broker result")
      .unwrap();
    dispatch_ok(&state, 1, &identity(&state, 1));
    assigned(&state).await;
    assert_eq!(state.pending_commits(), 2);
    drop(unresolved);
  }

  #[test]
  fn retained_messages_are_bounded_by_pause_and_resume() {
    let state = small(16, 4);
    state.on_assign(&[key(0), key(1)]);
    assert!(state.plan_flow().is_empty());
    for offset in 0..4 {
      assert!(!state.is_saturated());
      state.admit_message("topic", 0, 10, offset);
    }
    // Full: the poller stops taking messages, but nothing is paused until the
    // application has stalled for the grace period.
    assert!(state.is_saturated());
    assert!(state.plan_flow().is_empty());
    state.engage_backpressure();
    assert!(!state.is_saturated());
    let plan = state.plan_flow();
    assert_eq!(plan.pause, vec![key(0), key(1)]);
    assert!(plan.resume.is_empty());
    state.flow_applied(&plan);
    assert!(state.snapshot().backpressure_paused);
    assert!(state.plan_flow().is_empty());

    // A partition assigned while paused is paused too.
    state.on_assign(&[key(2)]);
    let plan = state.plan_flow();
    assert_eq!(plan.pause, vec![key(2)]);
    state.flow_applied(&plan);

    // A paused partition that is revoked has its native flag cleared.
    state.on_revoke(&[key(1)], false, false);
    let plan = state.plan_flow();
    assert_eq!(plan.resume, vec![key(1)]);
    state.flow_applied(&plan);
  }

  #[tokio::test]
  async fn draining_resumes_everything_except_application_pauses() {
    let state = small(16, 4);
    state.on_assign(&[key(0), key(1)]);
    assigned(&state).await;
    state.set_user_paused(&[key(1)], true).unwrap();
    let plan = state.plan_flow();
    assert_eq!(plan.pause, vec![key(1)]);
    state.flow_applied(&plan);
    for offset in 0..4 {
      state.admit_message("topic", 0, 10, offset);
    }
    state.engage_backpressure();
    let plan = state.plan_flow();
    assert_eq!(plan.pause, vec![key(0)]);
    state.flow_applied(&plan);
    // An application resume is deferred while backpressure holds the pause.
    state.set_user_paused(&[key(1)], false).unwrap();
    assert!(state.plan_flow().is_empty());
    state.set_user_paused(&[key(1)], true).unwrap();

    next(&state).await;
    assert!(state.plan_flow().is_empty());
    next(&state).await;
    let plan = state.plan_flow();
    assert_eq!(plan.resume, vec![key(0)]);
    state.flow_applied(&plan);
    assert!(!state.snapshot().backpressure_paused);
    assert!(matches!(
      state.set_user_paused(&[key(9)], true),
      Err(ManagedError::InvalidArgument(_))
    ));
  }

  /// A consumer that is merely slower than the fetcher never pauses: each
  /// delivery makes room and wakes the poller.
  #[tokio::test]
  async fn a_delivery_wakes_the_poller_that_waits_for_room() {
    let state = small(16, 2);
    state.on_assign(&[key(0)]);
    assigned(&state).await;
    state.admit_message("topic", 0, 1, 1);
    state.admit_message("topic", 0, 1, 2);
    assert!(state.is_saturated());
    // Drain the wake-ups left by the calls above.
    let _ = timeout(Duration::from_millis(10), state.pump_wake().notified()).await;
    let woken = state.pump_wake().notified();
    tokio::pin!(woken);
    woken.as_mut().enable();
    next(&state).await;
    timeout(Duration::from_secs(1), woken).await.unwrap();
    assert!(!state.is_saturated());
    assert!(state.plan_flow().is_empty());
    assert!(!state.snapshot().backpressure_paused);
  }

  #[tokio::test]
  async fn receive_errors_are_coalesced_without_consuming_the_control_budget() {
    let state = small(2, 16);
    for index in 0..40 {
      state.push_receive_error(
        ReceiveFailure {
          message: format!("error {index}"),
          kafka: None,
        },
        false,
      );
    }
    assert_eq!(state.status(), ManagedStatus::Active);
    assert_eq!(state.snapshot().dropped_receive_errors, 24);
    assert!(matches!(
      next(&state).await,
      ManagedEntry::ReceiveError(ReceiveFailure { .. })
    ));
    state.push_receive_error(
      ReceiveFailure {
        message: "fatal".to_owned(),
        kafka: None,
      },
      true,
    );
    assert!(matches!(
      state.status(),
      ManagedStatus::Failed(ManagedError::ControlFailure(_))
    ));
  }
}
