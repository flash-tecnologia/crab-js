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
    let start = "let mut send_failed = false;";
    assert_eq!(
      method_body.matches(start).count(),
      1,
      "Ambiguous native handoff block"
    );
    let block = method_body
      .split_once(start)
      .unwrap()
      .1
      .split_once("if let Some(err) = pending_err")
      .expect("Native handoff boundary changed; review harness extraction")
      .0;
    assert!(
      block.contains("sender.send(Ok("),
      "Missing native queue send"
    );
    assert!(
      block.contains("grace period expired"),
      "Missing drop report on grace expiry; review harness extraction"
    );
    // The pending-count binding is type-specific (`CompactMessageBatch` in
    // production vs `Vec<u64>` here); normalize it to the harness
    // representation. Production keeps its own line.
    let len_line = "let pending_messages = batch_data.payloads.len();";
    let mut block = block.to_string();
    if block.contains(len_line) {
      block = block.replace(len_line, "let pending_messages = batch_data.len();");
    }
    // The production send/select block is inserted unchanged. Only its outer
    // method context and message representation are supplied by this harness.
    // The one-iteration loop preserves the original block's unlabelled breaks.
    generated.push_str(&format!(
            "#[allow(clippy::never_loop)]\nasync fn {function}(sender: ObservedSender, mut disconnect_signal: watch::Receiver<()>, mut cancel_receiver: watch::Receiver<bool>, {payload}: Vec<u64>) {{\nloop {{\n{start}{block}\nbreak;\n}}\n}}\n"
        ));
  }
  let out = PathBuf::from(env::var_os("OUT_DIR").unwrap()).join("native_handoff.rs");
  fs::write(out, generated).unwrap();
}
