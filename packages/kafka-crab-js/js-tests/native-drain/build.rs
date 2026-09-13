use std::{env, fs, path::PathBuf};

fn main() {
  let source_path = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap())
    .join("../../src/kafka/consumer/kafka_consumer.rs");
  println!("cargo:rerun-if-changed={}", source_path.display());
  let source = fs::read_to_string(source_path).unwrap();
  let mut generated = String::new();
  for (method, next_method, function, payload) in [
    (
      "fn recv_batch_stream_internal",
      "fn recv_batch_stream_compact_internal",
      "batch_handoff",
      "messages",
    ),
    (
      "fn recv_batch_stream_compact_internal",
      "pub fn recv_batch_stream",
      "compact_handoff",
      "batch_data",
    ),
  ] {
    let method_body = source
      .split_once(method)
      .expect("Native method missing; review harness extraction")
      .1
      .split_once(next_method)
      .expect("Next native method missing; review harness extraction")
      .0;
    // Include the byte-budget reserve through the handoff so the
    // reserve-to-drain transition (F01) is exercised, not just the isolated send.
    let start = "let wired =";
    assert_eq!(
      method_body.matches(start).count(),
      1,
      "Ambiguous native reserve block"
    );
    let tail = method_body
      .split_once(start)
      .unwrap()
      .1
      .split_once("if let Some(err) = pending_err")
      .expect("Native handoff boundary changed; review harness extraction")
      .0;
    let rest = tail
      .split_once(';')
      .expect("Native wired line changed; review harness extraction")
      .1;
    assert!(
      rest.contains("producer_budget.reserve"),
      "Missing native byte-budget reserve"
    );
    assert!(
      rest.contains("sender.send(Ok("),
      "Missing native queue send"
    );
    assert!(
      rest.contains("grace period expired"),
      "Missing drop report on grace expiry; review harness extraction"
    );
    // Harness uses Vec<u64> offsets; byte size is the batch length (one unit
    // per record). Production keeps its own wired-byte computation.
    let mut block = format!("let wired = {payload}.len();{rest}");
    // The pending-count binding is type-specific (`CompactMessageBatch` in
    // production vs `Vec<u64>` here); normalize it to the harness
    // representation. Production keeps its own line.
    let len_line = "let pending_messages = batch_data.payloads.len();";
    if block.contains(len_line) {
      block = block.replace(len_line, "let pending_messages = batch_data.len();");
    }
    // The production reserve/select block is inserted unchanged. Only its outer
    // method context and message representation are supplied by this harness.
    // `disconnected` is false here: collection already succeeded; disconnect
    // arrives during the reserve/handoff below. The one-iteration loop
    // preserves the original block's unlabelled breaks.
    generated.push_str(&format!(
      "#[allow(clippy::never_loop)]\nasync fn {function}(sender: ObservedSender, mut disconnect_signal: watch::Receiver<()>, mut cancel_receiver: watch::Receiver<bool>, producer_budget: std::sync::Arc<super::byte_budget::ByteBudget>, {payload}: Vec<u64>) {{\nconst DRAIN_GRACE_PERIOD: Duration = Duration::from_millis(1500);\nloop {{\nlet disconnected = false;\n{block}\nbreak;\n}}\n}}\n"
    ));
  }
  let out = PathBuf::from(env::var_os("OUT_DIR").unwrap()).join("native_handoff.rs");
  fs::write(out, generated).unwrap();
}
