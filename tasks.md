## Tasks / Ideas
- [ ] Add diagnostic subscriber example in docs showing how to listen to producer/consumer/batch channels (e.g., logging or simple tracer hook).
- [ ] Stream batch context propagation: improve instrumentation so stream batch consumer spans inherit producer/parent trace; then flip the integration test warning to a hard assertion.
- [ ] Auto-end consumer process spans on stream/batch destroy/close to prevent span leaks when `endSpan` isn’t called.
- [ ] Attach optional per-message `endSpan` helpers in stream batch consumption for consistency with `recvBatch`.

## Key observations / risks
- [ ] Diagnostics fast paths: Producer error publishing fixed, but similar patterns exist in consumer/batch instrumentation—currently they guard with “has subscribers” for receive/process channels. If channels get split further (e.g., process-only), ensure fast paths include all relevant channels.
- [ ] Stream batch propagation: OTEL adapter extracts context from headers and builds batch spans, but stream batch path doesn’t reliably propagate parent trace to stream consumers (test warns). If propagation matters, inject/carry parent context through stream/batch pipeline.
- [ ] `BaseKafkaStreamReadable._destroy` unsubscribes/disconnects without ending outstanding process spans/endSpan hooks. Consider auto-ending spans on destroy/close to prevent leaks when callers forget endSpan.
- [ ] Batch `endSpan` attachment: `recvBatch` attaches `endSpan` to the array, not to each message. Stream batch consumers have per-message spans but no per-message `endSpan` helper; consider optional message-level enders for consistency.
- [ ] Header capture: inject normalizes headers to Buffer; extractor handles Buffer/string. If baggage is added, handle multi-valued headers (current extractor picks first element).
- [ ] Metrics safety: hooks are wrapped; event contexts are per-call so timer leakage risk is low.
- [ ] Lint/tests: `pnpm lint` and `pnpm --filter kafka-crab-js-otel test:integration` pass (invalid-broker logs expected). Integration suite covers single, batch, stream, hooks, errors, large batch (100 msgs).

## Opportunities
- [ ] Add a small diagnostic subscriber example in docs (producer/consumer/batch).
- [ ] Consider an opt-in “autoEndProcessSpanOnDestroy” flag for stream consumers.
- [ ] Flip the stream batch warning to a hard assertion once propagation is supported.
