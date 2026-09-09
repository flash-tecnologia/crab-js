#[cfg(test)]
mod tests {
  use std::{
    future::{poll_fn, Future},
    pin::{pin, Pin},
    sync::{
      atomic::{AtomicU64, AtomicUsize, Ordering},
      Arc, Mutex,
    },
    time::Duration,
  };
  use tokio::sync::{mpsc, oneshot, watch};
  use tracing::{
    field::Visit,
    span::{Attributes, Id, Record},
    warn, Event, Metadata,
  };
  use tracing::subscriber::Subscriber;

  /// Captures the production drop report (`warn!(dropped = …)`) so the stalled
  /// test verifies the reported undelivered count, not just termination.
  /// Held thread-local via `set_default`; the tests run on Tokio
  /// `current_thread`, so no cross-test interference is possible.
  #[derive(Default)]
  struct DropReport {
    warns: AtomicUsize,
    dropped: AtomicU64,
  }

  struct DropVisit<'a> {
    report: &'a DropReport,
  }

  impl Visit for DropVisit<'_> {
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
      if field.name() == "dropped" {
        self.report.dropped.store(value, Ordering::SeqCst);
      }
    }

    fn record_debug(&mut self, _field: &tracing::field::Field, _value: &dyn std::fmt::Debug) {}
  }

  struct ReportSubscriber {
    report: Arc<DropReport>,
  }

  impl Subscriber for ReportSubscriber {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
      true
    }

    fn new_span(&self, _: &Attributes<'_>) -> Id {
      Id::from_u64(1)
    }

    fn record(&self, _: &Id, _: &Record<'_>) {}

    fn record_follows_from(&self, _: &Id, _: &Id) {}

    fn event(&self, event: &Event<'_>) {
      self.report.warns.fetch_add(1, Ordering::SeqCst);
      event.record(&mut DropVisit {
        report: &self.report,
      });
    }

    fn enter(&self, _: &Id) {}

    fn exit(&self, _: &Id) {}
  }
  type Batch = Result<Vec<u64>, ()>;
  type SendResult = Result<(), mpsc::error::SendError<Batch>>;

  struct ObservedSender {
    inner: mpsc::Sender<Batch>,
    blocked: std::sync::Mutex<Option<oneshot::Sender<()>>>,
  }

  impl ObservedSender {
    async fn closed(&self) {
      self.inner.closed().await
    }

    /// Boxed (hence `Unpin`, like Tokio's real `Send` future) so the extracted
    /// production block can poll it via `&mut` inside `select!`.
    fn send(&self, batch: Batch) -> Pin<Box<dyn Future<Output = SendResult> + Send + '_>> {
      Box::pin(async move {
        let mut sending = pin!(self.inner.send(batch));
        poll_fn(|cx| {
          let result = sending.as_mut().poll(cx);
          if result.is_pending() {
            // A real mpsc send has been polled and cannot acquire capacity.
            // The test keeps the receiver alive but does not drain it yet.
            assert_eq!(self.inner.capacity(), 0);
            if let Some(signal) = self.blocked.lock().unwrap().take() {
              signal
                .send(())
                .expect("Test stopped waiting for blocked send");
            }
          }
          result
        })
        .await
      })
    }
  }

  include!(concat!(env!("OUT_DIR"), "/native_handoff.rs"));

  async fn check_drain(compact: bool, disconnect_while_blocked: bool) {
    let report = Arc::new(DropReport::default());
    let _guard = tracing::subscriber::set_default(ReportSubscriber {
      report: Arc::clone(&report),
    });
    let (sender, mut receiver) = mpsc::channel(4);
    // Four complete batches already queued, followed by a fifth collected
    // batch waiting to be handed to the reader. Offsets identify every record.
    for batch in 0..4 {
      sender
        .send(Ok((batch * 32..(batch + 1) * 32).collect()))
        .await
        .unwrap();
    }
    let (blocked_tx, blocked_rx) = oneshot::channel();
    let sender = ObservedSender {
      inner: sender,
      blocked: std::sync::Mutex::new(Some(blocked_tx)),
    };
    let (disconnect_tx, disconnect_rx) = watch::channel(());
    let (_cancel_tx, cancel_rx) = watch::channel(false);
    let pending_offsets: Vec<u64> = (128..160).collect();
    let task = tokio::spawn(async move {
      if compact {
        compact_handoff(sender, disconnect_rx, cancel_rx, pending_offsets).await;
      } else {
        batch_handoff(sender, disconnect_rx, cancel_rx, pending_offsets).await;
      }
    });

    // No sleeps: the signal is emitted only after send has returned Pending.
    tokio::time::timeout(Duration::from_secs(2), blocked_rx)
      .await
      .expect("Native handoff never blocked")
      .expect("Native handoff exited before blocking");
    if disconnect_while_blocked {
      disconnect_tx.send(()).unwrap();
    }

    // Wake the reader only after sending disconnect. Tokio's current-thread
    // executor and the production biased select make both ready branches
    // deterministic when the native task is next polled.
    let delivered = tokio::time::timeout(Duration::from_secs(2), async {
      let mut delivered = Vec::new();
      while let Some(batch) = receiver.recv().await {
        delivered.extend(batch.unwrap());
      }
      task.await.expect("Native handoff panicked");
      delivered
    })
    .await
    .expect("Native task or reader failed to finish");

    assert_eq!(delivered, (0..160).collect::<Vec<_>>(),
            "All queued and already-collected offsets must reach the live reader; compact={compact}, disconnect={disconnect_while_blocked}");
    assert_eq!(report.warns.load(Ordering::SeqCst), 0, "Resumed reader must not drop anything");
  }

  async fn check_stalled_reader_terminates(compact: bool) {
    let report = Arc::new(DropReport::default());
    let _guard = tracing::subscriber::set_default(ReportSubscriber {
      report: Arc::clone(&report),
    });
    let (sender, _receiver) = mpsc::channel(4);
    let (disconnect_tx, disconnect_rx) = watch::channel(());
    let (_cancel_tx, cancel_rx) = watch::channel(false);

    for chunk in 0..4 {
      sender
        .send(Ok((chunk * 32..(chunk + 1) * 32).collect()))
        .await
        .unwrap();
    }

    let (blocked_tx, blocked_rx) = oneshot::channel();
    let sender = ObservedSender {
      inner: sender,
      blocked: Mutex::new(Some(blocked_tx)),
    };

    let pending_offsets = (128..160).collect();
    let task = tokio::spawn(async move {
      if compact {
        compact_handoff(sender, disconnect_rx, cancel_rx, pending_offsets).await;
      } else {
        batch_handoff(sender, disconnect_rx, cancel_rx, pending_offsets).await;
      }
    });

    tokio::time::timeout(Duration::from_secs(2), blocked_rx)
      .await
      .expect("Native handoff never blocked")
      .expect("Native handoff exited before blocking");

    disconnect_tx.send(()).unwrap();

    // Reader is stalled (receiver never drained). The native task must terminate within grace timeout.
    tokio::time::timeout(Duration::from_secs(3), task)
      .await
      .expect("Native task failed to terminate after disconnect when reader was stalled")
      .expect("Native handoff panicked");
    assert_eq!(report.warns.load(Ordering::SeqCst), 1, "Stalled reader must report exactly one drop");
    assert_eq!(
      report.dropped.load(Ordering::SeqCst),
      32,
      "The reported undelivered count must equal the blocked fifth batch (offsets 128..160)"
    );
  }

  #[tokio::test(flavor = "current_thread")]
  async fn batch_delivers_every_offset_when_reader_resumes() {
    check_drain(false, false).await
  }

  #[tokio::test(flavor = "current_thread")]
  async fn compact_delivers_every_offset_when_reader_resumes() {
    check_drain(true, false).await
  }

  #[tokio::test(flavor = "current_thread")]
  async fn batch_disconnect_preserves_the_blocked_batch() {
    check_drain(false, true).await
  }

  #[tokio::test(flavor = "current_thread")]
  async fn compact_disconnect_preserves_the_blocked_batch() {
    check_drain(true, true).await
  }

  #[tokio::test(flavor = "current_thread")]
  async fn batch_stalled_reader_terminates_after_grace() {
    check_stalled_reader_terminates(false).await
  }

  #[tokio::test(flavor = "current_thread")]
  async fn compact_stalled_reader_terminates_after_grace() {
    check_stalled_reader_terminates(true).await
  }
}
