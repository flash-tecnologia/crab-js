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
//! - Work done under it is O(1) per operation, except the release of state
//!   that a transition invalidates (bounded by the configured limits).
//! - A legacy consumer never constructs this type.

use std::{
  collections::{hash_map::RandomState, HashMap, HashSet, VecDeque},
  hash::{BuildHasher, Hasher},
  sync::{Mutex, MutexGuard},
  time::{Duration, Instant},
};

use tokio::sync::{oneshot, Notify};

use super::byte_budget::DEFAULT_STREAM_BUFFER_BYTES;

pub const DEFAULT_MAX_CONTROL_RECORDS: usize = 1024;
pub const DEFAULT_MAX_BUFFERED_MESSAGES: usize = 1024;
pub const DEFAULT_MAX_BUFFERED_BYTES: usize = DEFAULT_STREAM_BUFFER_BYTES;
pub const DEFAULT_MAX_PENDING_COMMITS: usize = 1024;
/// Receive errors are observational (never assignment control), so they are
/// coalesced instead of consuming the control budget without bound.
const MAX_QUEUED_RECEIVE_ERRORS: usize = 16;
/// Retained messages may pass the configured bound while a pause takes effect.
/// Beyond this multiple the bound is not being enforced and the state fails.
pub const HARD_CEILING_FACTOR: usize = 2;
/// Consecutive pause/resume attempts that may fail before the state fails.
pub const MAX_FLOW_FAILURES: u32 = 50;
const TOKEN_PREFIX: &str = "mc1";

pub type PartitionKey = (String, i32);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ManagedLimits {
  pub max_control_records: usize,
  pub max_buffered_messages: usize,
  pub max_buffered_bytes: usize,
  /// Commits dispatched or waiting for dispatch, across all partitions.
  pub max_pending_commits: usize,
  /// Optional watchdog: longest time without any receive pending.
  pub max_receive_idle: Option<Duration>,
}

impl Default for ManagedLimits {
  fn default() -> Self {
    Self {
      max_control_records: DEFAULT_MAX_CONTROL_RECORDS,
      max_buffered_messages: DEFAULT_MAX_BUFFERED_MESSAGES,
      max_buffered_bytes: DEFAULT_MAX_BUFFERED_BYTES,
      max_pending_commits: DEFAULT_MAX_PENDING_COMMITS,
      max_receive_idle: None,
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
  /// A managed operation was called on a legacy consumer.
  NotManaged,
  ReceivePending,
  Closed,
  ControlOverflow,
  ControlFailure(String),
  /// Retained messages could not be kept within their bound.
  BackpressureFailure(String),
  /// The application stopped receiving for longer than the watchdog allows.
  ReceiveStalled,
  ReceiveFailed(ReceiveFailure),
  StaleAssignment,
  UnknownAssignment(String),
  InvalidArgument(String),
  /// `next_offset` is lower than one already authorized under the identity.
  CommitRegression {
    authorized: i64,
  },
  /// Too many commits are dispatched or waiting for dispatch.
  CommitBacklog,
  /// `dispatched` is false when the request never reached librdkafka.
  CommitFailed {
    kafka: KafkaCode,
    dispatched: bool,
  },
  /// The request was dispatched but no broker result was observed.
  CommitUnknown(String),
  /// The caller's observation deadline elapsed. The request is not cancelled.
  CommitTimeout,
}

pub type CommitOutcome = Result<(), ManagedError>;

/// Hands one commit to the native client without blocking:
/// `(commit id, topic, partition, next offset)`.
pub type CommitDispatch<'a> = &'a mut dyn FnMut(u64, &str, i32, i64) -> Result<(), KafkaCode>;

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

#[derive(Clone, Copy, PartialEq, Eq)]
enum ReceivePhase {
  Idle,
  /// Reserved by a `recvManaged` call whose result is not decided yet.
  Waiting,
  /// The result is decided; the reservation lasts until it is delivered.
  Completed,
}

struct Active {
  seq: u64,
  /// Highest next offset authorized under this identity.
  authorized: Option<i64>,
}

struct QueuedCommit {
  seq: u64,
  offset: i64,
  reply: oneshot::Sender<CommitOutcome>,
}

/// Commits of one partition are dispatched one at a time, in call order, so a
/// later offset cannot overtake an earlier one and a new identity never
/// overlaps a request of an older identity.
#[derive(Default)]
struct CommitChain {
  in_flight: Option<(u64, oneshot::Sender<CommitOutcome>)>,
  queued: VecDeque<QueuedCommit>,
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
  active: HashMap<PartitionKey, Active>,
  next_seq: u64,
  queue: VecDeque<Queued<M>>,
  control_len: usize,
  error_len: usize,
  message_len: usize,
  message_bytes: usize,
  receive: ReceivePhase,
  cancel_requested: bool,
  last_receive: Option<Instant>,
  chains: HashMap<PartitionKey, CommitChain>,
  in_flight: HashMap<u64, PartitionKey>,
  pending_commits: usize,
  next_commit_id: u64,
  user_paused: HashSet<PartitionKey>,
  /// Partitions whose librdkafka application-pause flag is confirmed set. The
  /// flag outlives unassignment, so it is tracked until confirmed cleared.
  paused_native: HashSet<PartitionKey>,
  backpressure_paused: bool,
  flow_dirty: bool,
  flow_failures: u32,
  dropped_receive_errors: u64,
  purged_messages: u64,
}

pub struct ManagedState<M> {
  nonce: u64,
  limits: ManagedLimits,
  inner: Mutex<Inner<M>>,
  wake_receive: Notify,
  wake_pump: Notify,
}

type Replies = Vec<(oneshot::Sender<CommitOutcome>, CommitOutcome)>;

fn send_replies(replies: Replies) {
  for (reply, outcome) in replies {
    let _ = reply.send(outcome);
  }
}

/// Random per consumer instance, so identities do not repeat across process
/// restarts or between consumers.
fn random_nonce() -> u64 {
  let mut hasher = RandomState::new().build_hasher();
  hasher.write_u128(
    std::time::SystemTime::now()
      .duration_since(std::time::UNIX_EPOCH)
      .map(|elapsed| elapsed.as_nanos())
      .unwrap_or_default(),
  );
  hasher.finish()
}

fn parse_token(value: &str) -> Option<(u64, u64)> {
  let mut parts = value.split('.');
  if parts.next()? != TOKEN_PREFIX {
    return None;
  }
  let nonce = u64::from_str_radix(parts.next()?, 16).ok()?;
  let seq = u64::from_str_radix(parts.next()?, 16).ok()?;
  parts.next().is_none().then_some((nonce, seq))
}

impl<M> ManagedState<M> {
  pub fn new(limits: ManagedLimits) -> Self {
    Self {
      nonce: random_nonce(),
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
        receive: ReceivePhase::Idle,
        cancel_requested: false,
        last_receive: None,
        chains: HashMap::new(),
        in_flight: HashMap::new(),
        pending_commits: 0,
        next_commit_id: 1,
        user_paused: HashSet::new(),
        paused_native: HashSet::new(),
        backpressure_paused: false,
        flow_dirty: false,
        flow_failures: 0,
        dropped_receive_errors: 0,
        purged_messages: 0,
      }),
      wake_receive: Notify::new(),
      wake_pump: Notify::new(),
    }
  }

  fn lock(&self) -> MutexGuard<'_, Inner<M>> {
    self.inner.lock().unwrap_or_else(|e| e.into_inner())
  }

  fn token(&self, seq: u64) -> String {
    format!("{TOKEN_PREFIX}.{:x}.{seq:x}", self.nonce)
  }

  fn status_error(status: &ManagedStatus) -> Option<ManagedError> {
    match status {
      ManagedStatus::Active => None,
      ManagedStatus::Closed => Some(ManagedError::Closed),
      ManagedStatus::Failed(error) => Some(error.clone()),
    }
  }

  /// Enters the terminal failure: authorization is invalidated and buffers are
  /// released in the same critical section, before any JavaScript runs. The
  /// status itself is the terminal notification, so it cannot be lost to a
  /// full queue. Commits already dispatched keep their own reply slots.
  fn fail(&self, inner: &mut Inner<M>, error: ManagedError, replies: &mut Replies) {
    if inner.status != ManagedStatus::Active {
      return;
    }
    inner.status = ManagedStatus::Failed(error.clone());
    Self::clear_buffers(inner);
    Self::drain_queued_commits(inner, None, &error, replies);
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

  /// Settles commits that were never dispatched (all of them, or those of one
  /// partition). Requests already handed to the client keep their own result.
  fn drain_queued_commits(
    inner: &mut Inner<M>,
    only: Option<&PartitionKey>,
    error: &ManagedError,
    replies: &mut Replies,
  ) {
    let before = replies.len();
    let mut drain = |chain: &mut CommitChain| {
      for queued in chain.queued.drain(..) {
        replies.push((queued.reply, Err(error.clone())));
      }
    };
    match only {
      Some(key) => {
        if let Some(chain) = inner.chains.get_mut(key) {
          drain(chain);
        }
      }
      None => inner.chains.values_mut().for_each(&mut drain),
    }
    inner.pending_commits -= replies.len() - before;
    inner.chains.retain(|_, chain| chain.in_flight.is_some());
  }

  fn push_control(&self, inner: &mut Inner<M>, record: Queued<M>, replies: &mut Replies) {
    if inner.control_len >= self.limits.max_control_records {
      self.fail(inner, ManagedError::ControlOverflow, replies);
      return;
    }
    inner.control_len += 1;
    inner.flow_dirty = true;
    inner.queue.push_back(record);
    self.wake_receive.notify_one();
    self.wake_pump.notify_one();
  }

  /// Rebalance callback: partitions were assigned. Runs before the native
  /// assign, so an identity exists before its first message can be fetched.
  /// A partition that is already owned keeps its identity.
  pub fn on_assign(&self, partitions: &[PartitionKey]) {
    let mut replies = Replies::new();
    {
      let mut inner = self.lock();
      if inner.status != ManagedStatus::Active {
        return;
      }
      let mut changes = Vec::with_capacity(partitions.len());
      for key in partitions {
        let (assignment, previous_assignment) = match inner.active.get(key) {
          Some(active) => (self.token(active.seq), Some(self.token(active.seq))),
          None => {
            let seq = inner.next_seq;
            inner.next_seq += 1;
            inner.active.insert(
              key.clone(),
              Active {
                seq,
                authorized: None,
              },
            );
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
      self.push_control(&mut inner, Queued::Assign(changes), &mut replies);
    }
    send_replies(replies);
  }

  /// Rebalance callback: partitions were revoked (`all` for the eager
  /// protocol, where the whole assignment is dropped). "Revoked" here is the
  /// local processing of this callback, before the native unassign: it
  /// invalidates the identities, and with them commit authorization. Commits
  /// still waiting for dispatch are refused; messages of those identities
  /// that JavaScript has not received are released, since they can no longer
  /// be committed and the next owner refetches them.
  pub fn on_revoke(&self, partitions: &[PartitionKey], all: bool, lost: bool) {
    let mut replies = Replies::new();
    {
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
        let Some(active) = inner.active.remove(&key) else {
          continue;
        };
        inner.user_paused.remove(&key);
        Self::drain_queued_commits(
          &mut inner,
          Some(&key),
          &ManagedError::StaleAssignment,
          &mut replies,
        );
        changes.push(PartitionChange {
          topic: key.0,
          partition: key.1,
          assignment: None,
          previous_assignment: Some(self.token(active.seq)),
        });
        revoked.insert(active.seq);
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
      self.push_control(&mut inner, Queued::Revoke(changes, lost), &mut replies);
    }
    send_replies(replies);
  }

  /// Admits a message taken from librdkafka. The caller is the only poller,
  /// and rebalance callbacks run inside that same poll, so every transition
  /// that preceded the message is already queued ahead of it and the identity
  /// read here is the one that owns this delivery.
  pub fn admit_message(&self, topic: &str, partition: i32, bytes: usize, message: M) -> Admission {
    let mut replies = Replies::new();
    let admission = {
      let mut inner = self.lock();
      if inner.status != ManagedStatus::Active {
        return Admission::Dropped;
      }
      let key = (topic.to_owned(), partition);
      let seq = inner.active.get(&key).map(|active| active.seq);
      let failure = match seq {
        None => Some(ManagedError::ControlFailure(format!(
          "message admitted for {topic}[{partition}] without an active assignment"
        ))),
        Some(_) if self.above_hard_ceiling(&inner) => {
          Some(ManagedError::BackpressureFailure(format!(
            "retained messages exceeded {HARD_CEILING_FACTOR}x the configured bound; partitions could not be paused"
          )))
        }
        Some(_) => None,
      };
      match (failure, seq) {
        (None, Some(seq)) => {
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
        (failure, _) => {
          let error = failure
            .unwrap_or_else(|| ManagedError::ControlFailure("message without owner".to_owned()));
          self.fail(&mut inner, error, &mut replies);
          Admission::Dropped
        }
      }
    };
    send_replies(replies);
    admission
  }

  pub fn push_receive_error(&self, failure: ReceiveFailure, fatal: bool) {
    let mut replies = Replies::new();
    {
      let mut inner = self.lock();
      if inner.status != ManagedStatus::Active {
        return;
      }
      if fatal {
        self.fail(
          &mut inner,
          ManagedError::ControlFailure(failure.message),
          &mut replies,
        );
      } else if inner.error_len >= MAX_QUEUED_RECEIVE_ERRORS {
        inner.dropped_receive_errors += 1;
      } else {
        inner.error_len += 1;
        inner.queue.push_back(Queued::ReceiveError(failure));
        self.wake_receive.notify_one();
      }
    }
    send_replies(replies);
  }

  /// Reserves the single allowed receive. Called synchronously where the
  /// request enters the binding, so two calls made in a row cannot both
  /// succeed, and a cancel or disconnect issued right after the call sees it.
  /// The reservation lasts until `finish_receive`.
  pub fn reserve_receive(&self) -> Result<(), ManagedError> {
    let mut inner = self.lock();
    if let Some(error) = Self::status_error(&inner.status) {
      return Err(error);
    }
    if inner.receive != ReceivePhase::Idle {
      return Err(ManagedError::ReceivePending);
    }
    inner.receive = ReceivePhase::Waiting;
    inner.cancel_requested = false;
    Ok(())
  }

  /// Releases the reservation once the result has been delivered.
  pub fn finish_receive(&self) {
    let mut inner = self.lock();
    inner.receive = ReceivePhase::Idle;
    inner.cancel_requested = false;
    if inner.last_receive.is_some() {
      inner.last_receive = Some(Instant::now());
    }
  }

  /// Returns the next record in native order for the reserved receive.
  /// Control-only results are delivered without any message being available.
  /// A cancellation is decided before a record is taken, so a cancelled
  /// receive never removes one from the queue.
  pub async fn receive_reserved(
    &self,
    timeout: Option<Duration>,
  ) -> Result<Received<M>, ManagedError> {
    let deadline = timeout.map(|timeout| tokio::time::Instant::now() + timeout);
    loop {
      {
        let mut inner = self.lock();
        match &inner.status {
          ManagedStatus::Active => {}
          // The receive was reserved before the consumer closed.
          ManagedStatus::Closed => return Ok(Received::End),
          ManagedStatus::Failed(error) => return Err(error.clone()),
        }
        if inner.cancel_requested {
          inner.receive = ReceivePhase::Completed;
          return Ok(Received::Idle("cancelled"));
        }
        if let Some(entry) = inner.queue.pop_front() {
          inner.receive = ReceivePhase::Completed;
          return Ok(Received::Entry(match entry {
            Queued::Message(queued) => {
              let was_full = self.is_full(&inner);
              inner.message_len -= 1;
              inner.message_bytes -= queued.bytes;
              if inner.backpressure_paused && self.below_low_watermark(&inner) {
                inner.flow_dirty = true;
                self.wake_pump.notify_one();
              } else if was_full {
                // The pump is waiting for room.
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
      }
      match deadline {
        Some(deadline) => {
          if tokio::time::timeout_at(deadline, self.wake_receive.notified())
            .await
            .is_err()
          {
            // A record that arrived with the deadline stays queued.
            self.lock().receive = ReceivePhase::Completed;
            return Ok(Received::Idle("timeout"));
          }
        }
        None => self.wake_receive.notified().await,
      }
    }
  }

  /// Reserve, receive and release in one call.
  #[cfg(test)]
  pub async fn receive(&self, timeout: Option<Duration>) -> Result<Received<M>, ManagedError> {
    self.reserve_receive()?;
    let received = self.receive_reserved(timeout).await;
    self.finish_receive();
    received
  }

  /// Cancels the reserved receive if its result is not decided yet: it then
  /// resolves as idle without taking a record. Returns whether it did.
  pub fn cancel_receive(&self) -> bool {
    let mut inner = self.lock();
    if inner.receive != ReceivePhase::Waiting {
      return false;
    }
    inner.cancel_requested = true;
    self.wake_receive.notify_one();
    true
  }

  /// Authorizes a commit and hands it to the client, or queues it behind the
  /// unresolved commit of the same partition. Runs synchronously where the
  /// request enters the binding, so commits of a partition are dispatched in
  /// call order.
  ///
  /// The identity check and the dispatch share this critical section with the
  /// revoke callback: a revocation either completed before this call (stale)
  /// or starts after `dispatch` returned. `dispatch` must not block.
  pub fn submit_commit(
    &self,
    topic: &str,
    partition: i32,
    assignment: &str,
    next_offset: i64,
    dispatch: CommitDispatch<'_>,
  ) -> Result<oneshot::Receiver<CommitOutcome>, ManagedError> {
    let mut inner = self.lock();
    if let Some(error) = Self::status_error(&inner.status) {
      return Err(error);
    }
    let Some((nonce, seq)) = parse_token(assignment) else {
      return Err(ManagedError::UnknownAssignment(
        "malformed assignment identity".to_owned(),
      ));
    };
    if nonce != self.nonce || seq == 0 || seq >= inner.next_seq {
      return Err(ManagedError::UnknownAssignment(
        "assignment identity was not issued by this consumer".to_owned(),
      ));
    }
    let key = (topic.to_owned(), partition);
    let Some(authorized) = inner
      .active
      .get(&key)
      .filter(|active| active.seq == seq)
      .map(|active| active.authorized)
    else {
      return Err(ManagedError::StaleAssignment);
    };
    if let Some(authorized) = authorized.filter(|last| next_offset < *last) {
      return Err(ManagedError::CommitRegression { authorized });
    }
    if inner.pending_commits >= self.limits.max_pending_commits {
      return Err(ManagedError::CommitBacklog);
    }
    let (reply, result) = oneshot::channel();
    let busy = inner
      .chains
      .get(&key)
      .is_some_and(|chain| chain.in_flight.is_some());
    if busy {
      inner
        .chains
        .get_mut(&key)
        .expect("busy chain exists")
        .queued
        .push_back(QueuedCommit {
          seq,
          offset: next_offset,
          reply,
        });
    } else {
      let id = inner.next_commit_id;
      inner.next_commit_id += 1;
      if let Err(kafka) = dispatch(id, topic, partition, next_offset) {
        return Err(ManagedError::CommitFailed {
          kafka,
          dispatched: false,
        });
      }
      inner.chains.entry(key.clone()).or_default().in_flight = Some((id, reply));
      inner.in_flight.insert(id, key.clone());
    }
    inner.pending_commits += 1;
    if let Some(active) = inner.active.get_mut(&key) {
      active.authorized = Some(next_offset);
    }
    Ok(result)
  }

  /// Delivers a broker result to the commit that produced it; the id binds it
  /// to the original identity even after revocation or reassignment. Then
  /// dispatches the next commit of that partition, re-validating its identity
  /// in the same critical section.
  pub fn complete_commit(
    &self,
    id: u64,
    outcome: CommitOutcome,
    dispatch: CommitDispatch<'_>,
  ) -> bool {
    let mut replies = Replies::new();
    let found = {
      let mut inner = self.lock();
      match inner.in_flight.remove(&id) {
        None => false,
        Some(key) => {
          if let Some((_, reply)) = inner
            .chains
            .get_mut(&key)
            .and_then(|chain| chain.in_flight.take())
          {
            inner.pending_commits -= 1;
            replies.push((reply, outcome));
          }
          Self::dispatch_next(&mut inner, &key, dispatch, &mut replies);
          true
        }
      }
    };
    send_replies(replies);
    found
  }

  fn dispatch_next(
    inner: &mut Inner<M>,
    key: &PartitionKey,
    dispatch: CommitDispatch<'_>,
    replies: &mut Replies,
  ) {
    loop {
      let Some(next) = inner
        .chains
        .get_mut(key)
        .and_then(|chain| chain.queued.pop_front())
      else {
        break;
      };
      // Queued commits are drained when their identity is revoked or the
      // state becomes terminal; this check is the dispatch-time guarantee.
      let owned = inner.status == ManagedStatus::Active
        && inner
          .active
          .get(key)
          .is_some_and(|active| active.seq == next.seq);
      if !owned {
        inner.pending_commits -= 1;
        let error = Self::status_error(&inner.status).unwrap_or(ManagedError::StaleAssignment);
        replies.push((next.reply, Err(error)));
        continue;
      }
      let id = inner.next_commit_id;
      inner.next_commit_id += 1;
      match dispatch(id, &key.0, key.1, next.offset) {
        Ok(()) => {
          inner
            .chains
            .get_mut(key)
            .expect("chain exists while it has queued commits")
            .in_flight = Some((id, next.reply));
          inner.in_flight.insert(id, key.clone());
          return;
        }
        Err(kafka) => {
          inner.pending_commits -= 1;
          replies.push((
            next.reply,
            Err(ManagedError::CommitFailed {
              kafka,
              dispatched: false,
            }),
          ));
        }
      }
    }
    if inner
      .chains
      .get(key)
      .is_some_and(|chain| chain.in_flight.is_none() && chain.queued.is_empty())
    {
      inner.chains.remove(key);
    }
  }

  /// No result can arrive any more (the client closed). Dispatched requests
  /// may still have reached the broker, so their outcome is unknown.
  pub fn fail_pending_commits(&self, reason: &str) {
    let mut replies = Replies::new();
    {
      let mut inner = self.lock();
      let error = Self::status_error(&inner.status).unwrap_or(ManagedError::Closed);
      Self::drain_queued_commits(&mut inner, None, &error, &mut replies);
      inner.in_flight.clear();
      for (_, chain) in inner.chains.drain() {
        if let Some((_, reply)) = chain.in_flight {
          replies.push((reply, Err(ManagedError::CommitUnknown(reason.to_owned()))));
        }
      }
      inner.pending_commits = 0;
    }
    send_replies(replies);
  }

  pub fn pending_commits(&self) -> usize {
    self.lock().pending_commits
  }

  pub fn pump_wake(&self) -> &Notify {
    &self.wake_pump
  }

  pub fn limits(&self) -> &ManagedLimits {
    &self.limits
  }

  pub fn is_assignment_active(&self, topic: &str, partition: i32, assignment: &str) -> bool {
    let Some((nonce, seq)) = parse_token(assignment) else {
      return false;
    };
    let inner = self.lock();
    nonce == self.nonce
      && inner.status == ManagedStatus::Active
      && inner
        .active
        .get(&(topic.to_owned(), partition))
        .is_some_and(|active| active.seq == seq)
  }

  pub fn status(&self) -> ManagedStatus {
    self.lock().status.clone()
  }

  /// Disconnect, or the consumer being dropped: stops admission, invalidates
  /// authorization, clears the buffers, wakes the reserved receive and refuses
  /// commits that were not dispatched yet.
  pub fn close(&self) {
    let mut replies = Replies::new();
    {
      let mut inner = self.lock();
      if inner.status == ManagedStatus::Active {
        inner.status = ManagedStatus::Closed;
      }
      Self::clear_buffers(&mut inner);
      let error = Self::status_error(&inner.status).unwrap_or(ManagedError::Closed);
      Self::drain_queued_commits(&mut inner, None, &error, &mut replies);
      self.wake_receive.notify_one();
      self.wake_pump.notify_one();
    }
    send_replies(replies);
  }

  /// Starts the receive watchdog. Before this the consumer owns nothing, so
  /// an application that is not receiving holds no partitions.
  pub fn arm_receive_watchdog(&self) {
    self.lock().last_receive = Some(Instant::now());
  }

  /// The poller serves rebalances for the application, so
  /// `max.poll.interval.ms` no longer detects a stuck handler. This optional
  /// watchdog does: without a reserved receive for longer than
  /// `max_receive_idle`, the state fails and the consumer leaves the group.
  pub fn check_receive_watchdog(&self) {
    let Some(limit) = self.limits.max_receive_idle else {
      return;
    };
    let mut replies = Replies::new();
    {
      let mut inner = self.lock();
      let stalled = inner.receive == ReceivePhase::Idle
        && inner
          .last_receive
          .is_some_and(|last| last.elapsed() > limit);
      if stalled {
        self.fail(&mut inner, ManagedError::ReceiveStalled, &mut replies);
      }
    }
    send_replies(replies);
  }

  fn is_full(&self, inner: &Inner<M>) -> bool {
    inner.message_len >= self.limits.max_buffered_messages
      || inner.message_bytes >= self.limits.max_buffered_bytes
  }

  fn above_hard_ceiling(&self, inner: &Inner<M>) -> bool {
    let limits = &self.limits;
    inner.message_len
      >= limits
        .max_buffered_messages
        .saturating_mul(HARD_CEILING_FACTOR)
      // A single message larger than the byte bound is always admitted.
      || (inner.message_len > 1
        && inner.message_bytes >= limits.max_buffered_bytes.saturating_mul(HARD_CEILING_FACTOR))
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
      inner.flow_dirty = true;
    }
  }

  fn below_low_watermark(&self, inner: &Inner<M>) -> bool {
    inner.message_len <= self.limits.max_buffered_messages / 2
      && inner.message_bytes <= self.limits.max_buffered_bytes / 2
  }

  /// Whether pause/resume state may need to change. Cleared by the call, so
  /// the poller only plans when something relevant happened.
  pub fn take_flow_dirty(&self) -> bool {
    std::mem::take(&mut self.lock().flow_dirty)
  }

  pub fn mark_flow_dirty(&self) {
    self.lock().flow_dirty = true;
  }

  /// Computes the pause/resume calls that bring librdkafka in line with the
  /// retained-message bounds and the application's own pauses. The caller
  /// applies the plan outside this lock and reports what the client confirmed
  /// with `flow_applied`.
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

  /// Records only what the client confirmed. `failed` lists partitions of the
  /// plan whose pause or resume was rejected; they stay planned and are tried
  /// again. Returns false when something failed. After `MAX_FLOW_FAILURES`
  /// consecutive failed attempts the bound (or the resumption of consumption)
  /// cannot be guaranteed and the state fails.
  pub fn flow_applied(&self, plan: &FlowPlan, failed: &[PartitionKey]) -> bool {
    let mut replies = Replies::new();
    let ok = {
      let mut inner = self.lock();
      for key in plan.pause.iter().filter(|key| !failed.contains(key)) {
        inner.paused_native.insert(key.clone());
      }
      for key in plan.resume.iter().filter(|key| !failed.contains(key)) {
        inner.paused_native.remove(key);
      }
      if failed.is_empty() {
        inner.flow_failures = 0;
        true
      } else {
        inner.flow_failures += 1;
        if inner.flow_failures >= MAX_FLOW_FAILURES {
          self.fail(
            &mut inner,
            ManagedError::BackpressureFailure(format!(
              "pause/resume failed {MAX_FLOW_FAILURES} times in a row"
            )),
            &mut replies,
          );
        }
        false
      }
    };
    send_replies(replies);
    ok
  }

  /// Records an application pause/resume. Only owned partitions can be paused;
  /// a pause does not survive revocation.
  pub fn set_user_paused(
    &self,
    partitions: &[PartitionKey],
    paused: bool,
  ) -> Result<(), ManagedError> {
    let mut inner = self.lock();
    if let Some(error) = Self::status_error(&inner.status) {
      return Err(error);
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
    inner.flow_dirty = true;
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
      .map(|((topic, partition), active)| (topic.clone(), *partition, self.token(active.seq)))
      .collect();
    assignments.sort();
    ManagedSnapshot {
      status: inner.status.clone(),
      assignments,
      queued_control_records: inner.control_len,
      buffered_messages: inner.message_len,
      buffered_bytes: inner.message_bytes,
      backpressure_paused: inner.backpressure_paused,
      pending_commits: inner.pending_commits,
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
  type Dispatched = Arc<Mutex<Vec<(u64, i32, i64)>>>;

  fn key(partition: i32) -> PartitionKey {
    ("topic".to_owned(), partition)
  }

  fn limits(max_control_records: usize, max_buffered_messages: usize) -> ManagedLimits {
    ManagedLimits {
      max_control_records,
      max_buffered_messages,
      max_buffered_bytes: 1 << 20,
      ..ManagedLimits::default()
    }
  }

  fn small(max_control_records: usize, max_buffered_messages: usize) -> State {
    ManagedState::new(limits(max_control_records, max_buffered_messages))
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

  /// Records dispatches as `(commit id, partition, offset)`.
  fn recorder(log: &Dispatched) -> impl FnMut(u64, &str, i32, i64) -> Result<(), KafkaCode> + '_ {
    move |id: u64, _: &str, partition: i32, offset: i64| {
      log.lock().unwrap().push((id, partition, offset));
      Ok(())
    }
  }

  fn submit(
    state: &State,
    log: &Dispatched,
    partition: i32,
    token: &str,
    offset: i64,
  ) -> Result<oneshot::Receiver<CommitOutcome>, ManagedError> {
    state.submit_commit("topic", partition, token, offset, &mut recorder(log))
  }

  fn complete(state: &State, log: &Dispatched, id: u64, outcome: CommitOutcome) -> bool {
    state.complete_commit(id, outcome, &mut recorder(log))
  }

  fn offsets(log: &Dispatched) -> Vec<i64> {
    log.lock().unwrap().iter().map(|entry| entry.2).collect()
  }

  fn last_id(log: &Dispatched) -> u64 {
    log.lock().unwrap().last().expect("a dispatch").0
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

  #[test]
  fn identities_carry_a_random_instance_nonce() {
    let first = small(16, 16);
    let second = small(16, 16);
    first.on_assign(&[key(0)]);
    second.on_assign(&[key(0)]);
    let (a, b) = (identity(&first, 0), identity(&second, 0));
    assert_ne!(a, b);
    assert_ne!(parse_token(&a).unwrap().0, parse_token(&b).unwrap().0);
    assert_eq!(parse_token(&a).unwrap().1, parse_token(&b).unwrap().1);
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
  async fn eager_and_lost_revocations_drop_every_identity() {
    let state = small(16, 16);
    state.on_assign(&[key(0), key(1), key(2)]);
    state.on_revoke(&[key(1)], true, true);
    assigned(&state).await;
    match next(&state).await {
      ManagedEntry::Revoke { partitions, lost } => {
        assert!(lost, "the assignment-lost flag reaches the record");
        let mut ids: Vec<i32> = partitions.iter().map(|p| p.partition).collect();
        ids.sort();
        assert_eq!(ids, vec![0, 1, 2]);
      }
      other => panic!("expected revoke, got {other:?}"),
    }
    assert!(state.snapshot().assignments.is_empty());
    state.on_assign(&[key(0)]);
    state.on_revoke(&[key(0)], false, false);
    assigned(&state).await;
    assert!(matches!(
      next(&state).await,
      ManagedEntry::Revoke { lost: false, .. }
    ));
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
  async fn the_receive_is_reserved_until_its_result_is_delivered() {
    let state = small(16, 16);
    state.on_assign(&[key(0)]);
    state.admit_message("topic", 0, 1, 7);
    // Two calls in a row with records already queued: only the first is
    // accepted, even before it has run.
    state.reserve_receive().unwrap();
    assert_eq!(state.reserve_receive(), Err(ManagedError::ReceivePending));
    assert!(matches!(
      state.receive_reserved(None).await,
      Ok(Received::Entry(ManagedEntry::Assign { .. }))
    ));
    // The result is decided but not delivered: still reserved, and a cancel
    // can no longer change it.
    assert_eq!(state.reserve_receive(), Err(ManagedError::ReceivePending));
    assert!(!state.cancel_receive());
    state.finish_receive();
    assert!(matches!(
      next(&state).await,
      ManagedEntry::Message { message: 7, .. }
    ));
  }

  #[tokio::test]
  async fn a_cancel_issued_right_after_the_call_keeps_the_queued_record() {
    let state = small(16, 16);
    state.on_assign(&[key(0)]);
    state.admit_message("topic", 0, 1, 7);
    // The receive has not run yet when the cancel arrives.
    state.reserve_receive().unwrap();
    assert!(state.cancel_receive());
    assert!(matches!(
      state.receive_reserved(None).await,
      Ok(Received::Idle("cancelled"))
    ));
    state.finish_receive();
    assert_eq!(state.snapshot().queued_control_records, 1);
    assert_eq!(state.snapshot().buffered_messages, 1);
    assigned(&state).await;
    assert!(matches!(
      next(&state).await,
      ManagedEntry::Message { message: 7, .. }
    ));
    assert!(!state.cancel_receive());
    assert!(matches!(
      state.receive(Some(Duration::from_millis(10))).await,
      Ok(Received::Idle("timeout"))
    ));
  }

  #[tokio::test]
  async fn a_receive_reserved_before_close_ends_and_later_ones_are_refused() {
    let state = Arc::new(small(16, 16));
    state.on_assign(&[key(0)]);
    assigned(&state).await;
    state.admit_message("topic", 0, 9, 1);
    // Disconnect right after the call, before the receive has run.
    state.reserve_receive().unwrap();
    state.close();
    assert!(matches!(
      state.receive_reserved(None).await,
      Ok(Received::End)
    ));
    state.finish_receive();
    assert_eq!(state.reserve_receive(), Err(ManagedError::Closed));
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
  async fn stale_and_foreign_identities_cannot_authorize_a_commit() {
    let state = small(16, 16);
    let log = Dispatched::default();
    state.on_assign(&[key(0), key(1)]);
    let old = identity(&state, 0);
    let other = identity(&state, 1);
    state.on_revoke(&[key(0)], false, false);
    assert_eq!(
      submit(&state, &log, 0, &old, 1).err(),
      Some(ManagedError::StaleAssignment)
    );
    state.on_assign(&[key(0)]);
    assert_eq!(
      submit(&state, &log, 0, &old, 1).err(),
      Some(ManagedError::StaleAssignment)
    );
    // Another partition's identity is not valid for this partition.
    assert_eq!(
      submit(&state, &log, 0, &other, 1).err(),
      Some(ManagedError::StaleAssignment)
    );
    let foreign = small(16, 16);
    foreign.on_assign(&[key(0)]);
    assert!(matches!(
      submit(&state, &log, 0, &identity(&foreign, 0), 1),
      Err(ManagedError::UnknownAssignment(_))
    ));
    assert!(matches!(
      submit(&state, &log, 0, "not-a-token", 1),
      Err(ManagedError::UnknownAssignment(_))
    ));
    assert!(offsets(&log).is_empty());
  }

  #[tokio::test]
  async fn commits_of_a_partition_are_dispatched_in_call_order() {
    let state = small(16, 16);
    let log = Dispatched::default();
    state.on_assign(&[key(0), key(1)]);
    let token = identity(&state, 0);
    // Three commits without waiting for any result.
    let first = submit(&state, &log, 0, &token, 10).unwrap();
    let second = submit(&state, &log, 0, &token, 20).unwrap();
    let third = submit(&state, &log, 0, &token, 20).unwrap();
    // Only the first reached the client; the others wait behind it.
    assert_eq!(offsets(&log), vec![10]);
    assert_eq!(state.pending_commits(), 3);
    // Another partition is independent.
    let _other = submit(&state, &log, 1, &identity(&state, 1), 5).unwrap();
    assert_eq!(offsets(&log), vec![10, 5]);

    // A lower offset than one already authorized is refused, not reordered.
    assert_eq!(
      submit(&state, &log, 0, &token, 15).err(),
      Some(ManagedError::CommitRegression { authorized: 20 })
    );

    let id = log.lock().unwrap()[0].0;
    assert!(complete(&state, &log, id, Ok(())));
    assert_eq!(first.await.unwrap(), Ok(()));
    assert_eq!(offsets(&log), vec![10, 5, 20]);
    let failure = ManagedError::CommitFailed {
      kafka: KafkaCode {
        name: "RebalanceInProgress".to_owned(),
        code: 27,
      },
      dispatched: true,
    };
    assert!(complete(&state, &log, last_id(&log), Err(failure.clone())));
    assert_eq!(second.await.unwrap(), Err(failure));
    // The same offset can be committed again, e.g. to retry a failure.
    assert_eq!(offsets(&log), vec![10, 5, 20, 20]);
    assert!(complete(&state, &log, last_id(&log), Ok(())));
    assert_eq!(third.await.unwrap(), Ok(()));
    assert_eq!(state.pending_commits(), 1);
  }

  #[tokio::test]
  async fn pending_commits_are_bounded() {
    let state = ManagedState::<u64>::new(ManagedLimits {
      max_pending_commits: 3,
      ..limits(16, 16)
    });
    let log = Dispatched::default();
    state.on_assign(&[key(0), key(1)]);
    let token = identity(&state, 0);
    let _held: Vec<_> = (0..3)
      .map(|offset| submit(&state, &log, 0, &token, offset).unwrap())
      .collect();
    assert_eq!(
      submit(&state, &log, 0, &token, 9).err(),
      Some(ManagedError::CommitBacklog)
    );
    assert_eq!(
      submit(&state, &log, 1, &identity(&state, 1), 9).err(),
      Some(ManagedError::CommitBacklog)
    );
    assert!(complete(&state, &log, last_id(&log), Ok(())));
    assert!(submit(&state, &log, 0, &token, 9).is_ok());
    assert_eq!(state.pending_commits(), 3);
  }

  #[tokio::test]
  async fn commit_result_stays_bound_to_its_original_identity() {
    let state = small(16, 16);
    let log = Dispatched::default();
    state.on_assign(&[key(0)]);
    let old = identity(&state, 0);
    let in_flight = submit(&state, &log, 0, &old, 100).unwrap();
    let id = last_id(&log);
    state.on_revoke(&[key(0)], false, false);
    state.on_assign(&[key(0)]);
    let new = identity(&state, 0);

    // The new identity does not overlap the old in-flight request: its commit
    // waits, and the late result does not touch the new assignment.
    let waiting = submit(&state, &log, 0, &new, 60).unwrap();
    assert_eq!(offsets(&log), vec![100]);
    assert!(complete(&state, &log, id, Ok(())));
    assert_eq!(in_flight.await.unwrap(), Ok(()));
    assert!(state.is_assignment_active("topic", 0, &new));
    assert_eq!(offsets(&log), vec![100, 60]);
    assert!(!complete(&state, &log, id, Ok(())));
    assert!(complete(&state, &log, last_id(&log), Ok(())));
    assert_eq!(waiting.await.unwrap(), Ok(()));
  }

  #[tokio::test]
  async fn commits_waiting_for_dispatch_are_settled_by_revoke_failure_and_close() {
    // Revoke: the commit waiting behind an older request is refused at once,
    // while the request already sent keeps its own result.
    let state = small(4, 16);
    let log = Dispatched::default();
    state.on_assign(&[key(0)]);
    let token = identity(&state, 0);
    let sent = submit(&state, &log, 0, &token, 1).unwrap();
    let waiting = submit(&state, &log, 0, &token, 2).unwrap();
    state.on_revoke(&[key(0)], false, false);
    assert_eq!(
      timeout(Duration::from_secs(1), waiting)
        .await
        .unwrap()
        .unwrap(),
      Err(ManagedError::StaleAssignment)
    );
    assert_eq!(state.pending_commits(), 1);
    assert!(complete(&state, &log, last_id(&log), Ok(())));
    assert_eq!(sent.await.unwrap(), Ok(()));
    assert_eq!(offsets(&log), vec![1]);

    // Terminal failure.
    state.on_assign(&[key(0)]);
    let token = identity(&state, 0);
    let sent = submit(&state, &log, 0, &token, 3).unwrap();
    let waiting = submit(&state, &log, 0, &token, 4).unwrap();
    for _ in 0..4 {
      state.on_assign(&[]);
    }
    assert_eq!(
      timeout(Duration::from_secs(1), waiting)
        .await
        .unwrap()
        .unwrap(),
      Err(ManagedError::ControlOverflow)
    );
    assert_eq!(state.pending_commits(), 1);
    assert!(complete(&state, &log, last_id(&log), Ok(())));
    assert_eq!(sent.await.unwrap(), Ok(()));

    // Close.
    let state = small(16, 16);
    state.on_assign(&[key(0)]);
    let token = identity(&state, 0);
    let sent = submit(&state, &log, 0, &token, 5).unwrap();
    let waiting = submit(&state, &log, 0, &token, 6).unwrap();
    state.close();
    assert_eq!(
      timeout(Duration::from_secs(1), waiting)
        .await
        .unwrap()
        .unwrap(),
      Err(ManagedError::Closed)
    );
    // The client closed without a result for the request already sent.
    state.fail_pending_commits("consumer closed");
    assert!(matches!(
      sent.await.unwrap(),
      Err(ManagedError::CommitUnknown(_))
    ));
    assert_eq!(state.pending_commits(), 0);
    assert_eq!(offsets(&log), vec![1, 3, 5]);
  }

  #[tokio::test]
  async fn dispatch_failure_is_reported_with_the_kafka_code() {
    let state = small(16, 16);
    state.on_assign(&[key(0)]);
    let token = identity(&state, 0);
    let kafka = KafkaCode {
      name: "UnknownGroup".to_owned(),
      code: -179,
    };
    assert_eq!(
      state
        .submit_commit(
          "topic",
          0,
          &token,
          1,
          &mut |_: u64, _: &str, _: i32, _: i64| Err(kafka.clone())
        )
        .err(),
      Some(ManagedError::CommitFailed {
        kafka: kafka.clone(),
        dispatched: false
      })
    );
    assert_eq!(state.pending_commits(), 0);
  }

  #[tokio::test]
  async fn control_overflow_is_terminal_and_invalidates_authorization() {
    let state = Arc::new(small(2, 16));
    let log = Dispatched::default();
    state.on_assign(&[key(0)]);
    let token = identity(&state, 0);
    let in_flight = submit(&state, &log, 0, &token, 1).unwrap();
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
      submit(&state, &log, 0, &token, 2).err(),
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
        let outcome = state.submit_commit(
          "topic",
          0,
          &token,
          1,
          &mut |_: u64, _: &str, _: i32, _: i64| {
            in_dispatch_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            dispatched = true;
            Ok(())
          },
        );
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
      submit(&state, &Dispatched::default(), 0, &token, 2).err(),
      Some(ManagedError::StaleAssignment)
    );
  }

  /// A check made before scheduling is not enough: here the revoke lands
  /// between the early check and the dispatch, and the dispatch is refused.
  #[test]
  fn check_before_scheduling_alone_does_not_authorize_a_dispatch() {
    let state = small(16, 16);
    let log = Dispatched::default();
    state.on_assign(&[key(0)]);
    let token = identity(&state, 0);
    assert!(state.is_assignment_active("topic", 0, &token));
    state.on_revoke(&[key(0)], false, false);
    assert_eq!(
      submit(&state, &log, 0, &token, 1).err(),
      Some(ManagedError::StaleAssignment)
    );
    assert!(offsets(&log).is_empty());
  }

  /// Waiting for a broker result holds no lock: rebalance callbacks, receives
  /// and other commits proceed while a commit is unresolved.
  #[tokio::test]
  async fn callbacks_do_not_deadlock_with_an_unresolved_commit() {
    let state = Arc::new(small(64, 16));
    let log = Dispatched::default();
    state.on_assign(&[key(0), key(1)]);
    let unresolved = submit(&state, &log, 0, &identity(&state, 0), 1).unwrap();
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
    let _second = submit(&state, &log, 1, &identity(&state, 1), 1).unwrap();
    assigned(&state).await;
    assert_eq!(state.pending_commits(), 2);
    drop(unresolved);
  }

  #[test]
  fn retained_messages_are_bounded_by_pause_and_resume() {
    let state = small(16, 4);
    state.on_assign(&[key(0), key(1)]);
    assert!(state.take_flow_dirty());
    assert!(state.plan_flow().is_empty());
    for offset in 0..4 {
      assert!(!state.is_saturated());
      state.admit_message("topic", 0, 10, offset);
    }
    // Full: the poller stops taking messages, but nothing is paused until the
    // application has stalled for the grace period.
    assert!(state.is_saturated());
    assert!(!state.take_flow_dirty());
    state.engage_backpressure();
    assert!(!state.is_saturated());
    assert!(state.take_flow_dirty());
    let plan = state.plan_flow();
    assert_eq!(plan.pause, vec![key(0), key(1)]);
    assert!(plan.resume.is_empty());
    assert!(state.flow_applied(&plan, &[]));
    assert!(state.snapshot().backpressure_paused);
    assert!(state.plan_flow().is_empty());

    // A partition assigned while paused is paused too.
    state.on_assign(&[key(2)]);
    let plan = state.plan_flow();
    assert_eq!(plan.pause, vec![key(2)]);
    state.flow_applied(&plan, &[]);

    // A paused partition that is revoked has its native flag cleared.
    state.on_revoke(&[key(1)], false, false);
    let plan = state.plan_flow();
    assert_eq!(plan.resume, vec![key(1)]);
    state.flow_applied(&plan, &[]);
  }

  #[test]
  fn the_byte_bound_saturates_independently_of_the_message_count() {
    let byte_limits = ManagedLimits {
      max_buffered_bytes: 100,
      ..limits(16, 1000)
    };
    let state = ManagedState::<u64>::new(byte_limits);
    state.on_assign(&[key(0)]);
    state.admit_message("topic", 0, 60, 1);
    assert!(!state.is_saturated());
    state.admit_message("topic", 0, 60, 2);
    assert!(state.is_saturated());
    assert_eq!(state.snapshot().buffered_bytes, 120);

    // One message larger than the whole bound is admitted on its own.
    let oversized = ManagedState::<u64>::new(byte_limits);
    oversized.on_assign(&[key(0)]);
    assert!(matches!(
      oversized.admit_message("topic", 0, 10_000, 1),
      Admission::Queued
    ));
    assert!(oversized.is_saturated());
    // With the pause not taking effect, a second one passes the hard ceiling.
    assert!(matches!(
      oversized.admit_message("topic", 0, 10, 2),
      Admission::Queued
    ));
    assert!(matches!(
      oversized.admit_message("topic", 0, 10, 3),
      Admission::Dropped
    ));
    assert!(matches!(
      oversized.status(),
      ManagedStatus::Failed(ManagedError::BackpressureFailure(_))
    ));
  }

  #[test]
  fn unconfirmed_pauses_are_retried_and_the_hard_ceiling_is_terminal() {
    let state = small(16, 4);
    state.on_assign(&[key(0), key(1)]);
    for offset in 0..4 {
      state.admit_message("topic", 0, 10, offset);
    }
    state.engage_backpressure();
    let plan = state.plan_flow();
    // The client confirmed only partition 1.
    assert!(!state.flow_applied(&plan, &[key(0)]));
    let retry = state.plan_flow();
    assert_eq!(retry.pause, vec![key(0)]);
    assert!(retry.resume.is_empty());

    // Partition 0 keeps delivering. The bound may be passed, but not doubled.
    for offset in 4..8 {
      assert!(matches!(
        state.admit_message("topic", 0, 10, offset),
        Admission::Queued
      ));
    }
    assert!(matches!(
      state.admit_message("topic", 0, 10, 8),
      Admission::Dropped
    ));
    assert!(matches!(
      state.status(),
      ManagedStatus::Failed(ManagedError::BackpressureFailure(_))
    ));
    assert_eq!(state.snapshot().buffered_messages, 0);
    assert!(state.snapshot().assignments.is_empty());
  }

  #[test]
  fn repeated_flow_failures_are_terminal() {
    let state = small(16, 4);
    state.on_assign(&[key(0)]);
    state.set_user_paused(&[key(0)], true).unwrap();
    for attempt in 1..=MAX_FLOW_FAILURES {
      let plan = state.plan_flow();
      assert_eq!(plan.pause, vec![key(0)], "attempt {attempt}");
      assert!(!state.flow_applied(&plan, &[key(0)]));
    }
    assert!(matches!(
      state.status(),
      ManagedStatus::Failed(ManagedError::BackpressureFailure(_))
    ));

    // A success in between resets the count.
    let state = small(16, 4);
    state.on_assign(&[key(0)]);
    state.set_user_paused(&[key(0)], true).unwrap();
    for _ in 1..MAX_FLOW_FAILURES {
      state.flow_applied(&state.plan_flow(), &[key(0)]);
    }
    assert!(state.flow_applied(&state.plan_flow(), &[]));
    state.set_user_paused(&[key(0)], false).unwrap();
    assert!(!state.flow_applied(&state.plan_flow(), &[key(0)]));
    assert_eq!(state.status(), ManagedStatus::Active);
  }

  #[tokio::test]
  async fn draining_resumes_everything_except_application_pauses() {
    let state = small(16, 4);
    state.on_assign(&[key(0), key(1)]);
    assigned(&state).await;
    state.set_user_paused(&[key(1)], true).unwrap();
    let plan = state.plan_flow();
    assert_eq!(plan.pause, vec![key(1)]);
    state.flow_applied(&plan, &[]);
    for offset in 0..4 {
      state.admit_message("topic", 0, 10, offset);
    }
    state.engage_backpressure();
    let plan = state.plan_flow();
    assert_eq!(plan.pause, vec![key(0)]);
    state.flow_applied(&plan, &[]);
    // An application resume is deferred while backpressure holds the pause.
    state.set_user_paused(&[key(1)], false).unwrap();
    assert!(state.plan_flow().is_empty());
    state.set_user_paused(&[key(1)], true).unwrap();
    state.take_flow_dirty();

    next(&state).await;
    assert!(!state.take_flow_dirty());
    next(&state).await;
    assert!(state.take_flow_dirty());
    let plan = state.plan_flow();
    assert_eq!(plan.resume, vec![key(0)]);
    state.flow_applied(&plan, &[]);
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
  async fn the_receive_watchdog_fails_a_consumer_that_stops_receiving() {
    let state = ManagedState::<u64>::new(ManagedLimits {
      max_receive_idle: Some(Duration::from_millis(40)),
      ..limits(16, 16)
    });
    // Not armed before the poller starts: nothing is owned yet.
    tokio::time::sleep(Duration::from_millis(60)).await;
    state.check_receive_watchdog();
    assert_eq!(state.status(), ManagedStatus::Active);

    state.arm_receive_watchdog();
    state.on_assign(&[key(0)]);
    let token = identity(&state, 0);
    // A reserved receive counts as the application listening.
    state.reserve_receive().unwrap();
    tokio::time::sleep(Duration::from_millis(60)).await;
    state.check_receive_watchdog();
    assert_eq!(state.status(), ManagedStatus::Active);
    state.receive_reserved(None).await.unwrap();
    state.finish_receive();
    state.check_receive_watchdog();
    assert_eq!(state.status(), ManagedStatus::Active);

    tokio::time::sleep(Duration::from_millis(60)).await;
    state.check_receive_watchdog();
    assert_eq!(
      state.status(),
      ManagedStatus::Failed(ManagedError::ReceiveStalled)
    );
    assert!(!state.is_assignment_active("topic", 0, &token));
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
