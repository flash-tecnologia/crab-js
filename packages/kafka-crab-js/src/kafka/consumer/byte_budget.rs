use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Notify;

pub const DEFAULT_STREAM_BUFFER_BYTES: usize = 32 * 1024 * 1024;

pub struct ByteBudget {
  queued: AtomicUsize,
  limit: usize,
  notify: Notify,
}

impl ByteBudget {
  pub fn new(limit: usize) -> Self {
    Self {
      queued: AtomicUsize::new(0),
      limit,
      notify: Notify::new(),
    }
  }

  pub async fn reserve(&self, incoming: usize) {
    let mut waited = false;
    loop {
      let notified = self.notify.notified();
      let queued = self.queued.load(Ordering::Acquire);
      if queued == 0 || queued.saturating_add(incoming) <= self.limit {
        let queued = self.queued.fetch_add(incoming, Ordering::AcqRel) + incoming;
        tracing::trace!(
          byte_budget_action = "reserve",
          queued_bytes = queued,
          incoming_bytes = incoming,
          limit_bytes = self.limit,
          waited
        );
        return;
      }
      if !waited {
        tracing::trace!(
          byte_budget_action = "blocked",
          queued_bytes = queued,
          incoming_bytes = incoming,
          limit_bytes = self.limit
        );
        waited = true;
      }
      notified.await;
    }
  }

  pub fn release(&self, amount: usize) {
    let previous = self
      .queued
      .fetch_update(Ordering::AcqRel, Ordering::Acquire, |queued| {
        Some(queued.saturating_sub(amount))
      })
      .unwrap_or_else(|queued| queued);
    tracing::trace!(
      byte_budget_action = "release",
      queued_bytes = previous.saturating_sub(amount),
      released_bytes = amount,
      limit_bytes = self.limit
    );
    self.notify.notify_waiters();
  }
}

#[cfg(test)]
mod tests {
  use super::ByteBudget;
  use std::sync::Arc;
  use std::time::Duration;
  use tokio::time::{sleep, timeout};

  #[tokio::test]
  async fn empty_queue_accepts_a_single_oversized_item() {
    assert_eq!(super::DEFAULT_STREAM_BUFFER_BYTES, 32 * 1024 * 1024);
    let budget = ByteBudget::new(32);
    timeout(Duration::from_millis(50), budget.reserve(100))
      .await
      .expect("an empty queue must not wait on an oversized batch");
  }

  #[tokio::test]
  async fn reserve_waits_until_release_when_over_limit() {
    let budget = Arc::new(ByteBudget::new(32));
    budget.reserve(20).await;

    let waiter = tokio::spawn({
      let budget = budget.clone();
      async move {
        budget.reserve(20).await;
      }
    });

    sleep(Duration::from_millis(40)).await;
    assert!(
      !waiter.is_finished(),
      "second reserve must block while 20 + 20 exceeds 32"
    );

    budget.release(20);
    timeout(Duration::from_millis(500), waiter)
      .await
      .expect("release must wake the waiting reserve")
      .expect("waiter task");
  }

  #[tokio::test]
  async fn release_of_partial_bytes_unblocks_the_next_fit() {
    let budget = Arc::new(ByteBudget::new(32));
    budget.reserve(30).await;

    let waiter = tokio::spawn({
      let budget = budget.clone();
      async move {
        budget.reserve(16).await;
      }
    });

    sleep(Duration::from_millis(30)).await;
    assert!(!waiter.is_finished());
    budget.release(8);
    sleep(Duration::from_millis(30)).await;
    assert!(
      !waiter.is_finished(),
      "30-8+16 still exceeds 32; waiter must stay blocked"
    );
    budget.release(8);
    timeout(Duration::from_millis(500), waiter)
      .await
      .expect("releasing enough bytes must unblock reserve")
      .expect("waiter task");
  }
}
