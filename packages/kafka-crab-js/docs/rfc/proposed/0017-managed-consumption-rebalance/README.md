# RFC-0017: Managed consumption and assignment-bound commits

- Status: **Proposed — native prototype in implementation; decisions 1–4 recorded below; not
  validated for release**
- Date: 2026-11-11
- Package: `kafka-crab-js`
- Priority: High
- Related work: Flashify RFC 0003, with fatal shutdown specified by Flashify RFC 0002
- Release: to be assigned after API review and joint validation with `flashify-kafka-crab`
- Prototype: branch `feat/managed-consumption-rebalance`; see
  [Prototype status](#prototype-status-2026-11-11)

## Summary

Add an explicitly enabled native consumption contract that preserves the relationship between
messages and assignment transitions across the Rust/JavaScript boundary. Associate each message
and managed commit with the incarnation of its partition assignment, and reject obsolete work
before it can authorize a new commit.

This is the native part of the single coordinated delivery described in the `flashify` repository:

- `packages/flashify-kafka-crab/docs/rfcs/rfc-0003-managed-consumption-rebalance.md`
- `packages/flashify-kafka-crab/docs/rfcs/rfc-0002-close-on-fatal-consumer-error.md`

Those documents are local proposals at the time of writing; their publication and implementation
references must be recorded before release. RFC numbers are local to each repository: this RFC
does not replace this repository's existing RFC-0003.

## Problem and evidence

The current [consumer context](../../../../src/kafka/consumer/context.rs) publishes rebalance
events through a bounded broadcast channel. A lagging receiver can lose events. The
[consumer binding](../../../../src/kafka/consumer/kafka_consumer.rs) delivers `onEvents` through
an asynchronous callback path, separately from the promise returned by `recv()`.

The application cannot infer the assignment of a received message from callback arrival order
or a later `assignment()` snapshot. Revocation followed by reassignment of the same partition
must be distinguishable from continuous ownership. A wrapper-only epoch check cannot establish
the missing native relationship.

Synchronous commits currently run in `spawn_blocking` without an assignment identity. A check
made in JavaScript, or before scheduling that task, does not close the execution-time race.

The incident document `kafka-commit-gerenciado-rebalance.md` reports missing messages under
injected event delay and full queues. Those reproductions must be imported and rerun against
identified artifacts; they are not validation results for this RFC. Flashify's existing real-broker
generation-error test validates rejection and shutdown/replay in its scenario, not this contract.

### Generation rejection during and outside a rebalance

The two scenarios must not be treated as equivalent. In the
[librdkafka v2.12.1 OffsetCommit error handler](https://github.com/confluentinc/librdkafka/blob/v2.12.1/src/rdkafka_cgrp.c#L3632-L3643),
`IllegalGeneration` does not initiate another revocation when the client already considers itself
rebalancing. Otherwise, that path invalidates the generation and requests revocation/rejoin with
assignment lost. An ongoing rebalance may still revoke partitions independently; the first branch
does not guarantee retention.

The existing Flashify proxy test injects rejection after a successful commit without deliberately
starting another member's join/leave at that point. It therefore does not establish the behavior
of the reported cooperative deployment scenario. Conversely, the incident document's successful
deferred commits with retained partitions do not demonstrate safety under delayed control delivery.

Both results can be valid. Deferring a commit while retaining the existing epoch guard may mitigate
restarts in the retained-assignment case. The proxy result does not disprove that mitigation;
neither result closes the independent message-loss path involving delayed events and full queues.
The explanation for the differing runs remains a hypothesis until native state and assignment
transitions are captured in matched reproductions on the same implementation.

## Scope and ownership

| Layer            | Responsibility                                                                                                                   |
| ---------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| Native binding   | Assignment identity, ordered delivery, bounded native state, commit authorization, structured errors, native lifecycle.          |
| Flashify runtime | Completed-prefix tracking, serial processing per partition, handler cancellation, bounded commit recovery and error propagation. |
| Application      | Idempotent effects, business retries/DLQ, and host shutdown through `fastify.close()` when configured.                           |

The binding does not know whether a handler succeeded or a DLQ publication was confirmed.
It must not infer completion or perform business retries. Flashify remains responsible for
authorizing only the completed prefix; a valid native identity alone does not prove that work
has finished.

This proposal does not provide transactions with an external database, cross-process fencing
of external writes, or exactly-once effects. Aborting a handler does not undo an external write.
Kafka group generation and local assignment identity are different concepts.

## Required invariants

1. Each delivered managed message belongs to one consumer instance and one assignment identity
   for its `topic:partition`.
2. A retained cooperative assignment keeps its identity. Revocation invalidates that identity;
   a later assignment of the same partition gets a new one. An empty assign event does not
   recreate identities for retained partitions.
3. Message admission and assignment transitions have a defined order at the native boundary,
   before JavaScript scheduling can reorder their observation.
4. An obsolete identity cannot authorize a newly dispatched commit. A late completion remains
   associated with its original identity and cannot mutate a new assignment's state.
5. Control loss invalidates managed commit authorization natively, before JavaScript observes
   the fatal error. No correctness-critical event may be silently dropped.
6. Idle topics and data backpressure must not prevent assignment control from progressing.
7. Legacy consumers do not allocate or accumulate the managed control queue.

## Proposed public contract

The names below are API proposals, not existing methods. Final NAPI representations and public
types must be reviewed before implementation is considered complete.

### Activation and capability

Select managed mode in native consumer configuration before subscription or any operation that
can establish an assignment. Activation is immutable for that consumer. Reject late activation
and contradictory settings that allow automatic offset commit/storage to bypass managed completion.
Define validation for both convenience options and raw librdkafka configuration.

Expose a versioned capability for the complete managed contract: ordered receive, identity-bound
commit, and structured errors. Checking only whether `recvManaged` exists is insufficient.
Flashify verifies support before subscribing and enables this mode for `commitMode: 'Sync'`.

Keep legacy mode as the default for direct binding users. Managed mode must reject legacy receive
methods, including batch, stream and compact stream variants, and unfenced commit methods that
could bypass its guarantees. Legacy mode must reject managed operations. Only one managed receive
may be pending per consumer; concurrent calls fail explicitly rather than racing for control events.

Operations such as manual assignment, seek and unsubscribe must either participate in the same
identity/lifecycle protocol or be rejected in managed mode. The first implementation targets
group subscription used by Flashify; unsupported combinations must be documented and tested.

### Ordered receive

`recvManaged()` returns messages and assignment transitions in a defined sequence, including
control-only results when no message is available. Each message carries an opaque identity;
transitions identify the affected partitions and old/new identities where applicable.

Prefer a discriminated sequence of records, or demonstrate that a batch envelope preserves the
same interleaving. A plain `{ events, message }` envelope is insufficient unless native assembly
proves which transitions precede that particular message. Draining all pending events after an
independent receive can associate an old message with a newer assignment.

Define explicit terminal behavior for clean disconnect, cancellation and fatal control failure.
An empty control result is not end-of-stream. Preserve payloads, keys, headers, offsets and
tombstones; the identity supplements the message contract rather than changing message contents.

### Assignment-bound commit

Add a managed commit operation taking partition, assignment identity and the next offset to
consume. Do not hide whether the API takes a processed offset or the next offset: the proposed
operation takes the latter, consistent with `commit`; `commitMessage` currently adds one.
Keep offset precision consistent with the supported binding representation.

The promise must represent the broker result, not merely local scheduling. Validate consumer
instance, partition, active identity and lifecycle state in coordination with native dispatch.
Reject a stale identity with a distinct structured error and do not resubmit it under a new token.

The binding does not retry automatically. Flashify may retry a completed prefix only after its
control state establishes that the same assignment is still valid, within its recovery budget.
Native failures must carry `kafkaCode` when a Kafka error exists; stale identity, closing and
control overflow must remain distinguishable from broker errors. A broker error's code alone
does not establish whether ownership was retained.

Revocation invalidates old commit authorization; it is not, by itself, a requirement to close the
host. The runtime may resume with a new assignment and replay unconfirmed work when control is
coherent and recovery remains within budget. Do not preserve the old epoch guard's shutdown as
an acceptance requirement of the new contract. Irrecoverable control failures, definitive errors
or exhausted recovery budgets still follow the fatal-error policy.

For managed commits, Flashify must have one owner of recovery/fatal classification. Observational
`CommitCallback` delivery must not independently destroy the processor before that decision.
The binding must provide the result association needed to avoid duplicate decisions, including
callbacks delivered before or after promise rejection. Preserve legacy/manual callback semantics;
do not globally suppress commit error events to make managed recovery work.

## Native coordination design

Use a shared native managed state for the consumer context and receive/commit paths. It owns
assignment identities, sequence/admission state, bounded buffers and terminal status. The legacy
`onEvents` broadcast remains observational; it must not be replayed into managed state a second time.

The implementation must identify and test these ordering points:

| Operation         | Required ordering decision                                                                                                      |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| Assignment        | Establish the new identity before admitting its first message.                                                                  |
| Message admission | Bind the message to the identity that owns that delivery, atomically with the relevant transition ordering.                     |
| Revocation        | Invalidate the identity and pending commit authorization before admitting subsequent work under it.                             |
| Commit dispatch   | Order the final identity check and dispatch authorization against revocation; checking before `spawn_blocking` is insufficient. |
| Completion        | Attribute the result to the original identity, even after revocation or reassignment.                                           |

Do not hold a lock across a blocking native call if a rebalance/commit callback needs that lock.
Neither JavaScript nor a broker round trip may block the rebalance callback. A proposed mutex,
worker or command queue must demonstrate both ordering and freedom from callback deadlock.
Merely choosing a single queue does not prove coordination with callbacks running elsewhere.

A commit already sent to the broker cannot be recalled. Prevent unsafe overlap between commits
of successive identities within the same consumer, and describe what happens while an older
request is unresolved. Local serialization does not serialize commits made by other group members.
The prototype must establish which broker-generation protections remain effective at actual dispatch.
If the existing native API cannot provide the claimed dispatch ordering, revise the implementation
boundary or narrow the guarantee explicitly; do not present a local token as broker-side fencing.

A lost response can make the commit outcome unknown. Do not treat it as proof that processing
failed or automatically rerun the handler. Preserve that distinction in errors and diagnostics.

## Backpressure, memory and control progress

The managed native state and the Flashify receive loop must cooperate: application handlers may
stop accepting data while the control path continues observing revocations. Define a receive pump
and buffering strategy that consumes control without creating an unbounded message backlog.
The prototype must show this with full JavaScript queues and with a delayed JavaScript event loop.

Use partition pause/resume and bounds by count and bytes for retained messages. Account separately
for binding-owned buffers, librdkafka queues and application-owned messages; do not claim that a
binding queue limit alone caps the whole process. Reuse the byte-budget work from
[RFC-0016](../../implemented/0016-byte-based-backpressure/) where its ownership model applies.

Control records also need a finite budget. Overflow must atomically invalidate authorization and
enter a terminal failure, wake pending operations and prevent new commits without waiting for JS.
The design must retain a reliable terminal notification even when its regular queue is full.
No blocking callback, unbounded fallback queue or silent overwrite is acceptable.

Settle default limits, maximum record handling, validation and accounting before release. Test
mode-off memory separately from mode-on bounded memory; no managed queue grows in legacy mode.

## Lifecycle and tracing

Disconnect stops admission, invalidates new managed operations and wakes pending receives. Preserve
the existing native-close contract and retain resources needed by operations already in flight until
they actually finish. An observation timeout must not imply that a native commit was cancelled.
Clear residual buffers and terminate control tasks; do not leave a background consumer alive.

The wrapper drains completed work before requesting disconnect according to its shutdown budget.
This RFC does not move handler drainage into the binding or authorize committing unfinished work.

Update `_instrumentConsumer` in `js-src/kafka-client.ts` and diagnostic instrumentation for managed
receives. Only message records create message-processing spans. Preserve trace context and span
completion through cancellation, replay and error, without duplicate spans from legacy callbacks.

## Files and responsibilities

Paths are relative to `packages/kafka-crab-js/` unless otherwise stated.

| Location                                                           | Planned work                                                                                                                        |
| ------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------- |
| `src/kafka/consumer/context.rs`                                    | Capture transitions at their source and coordinate identities/control state with receive and commit. Preserve observational events. |
| `src/kafka/consumer/kafka_consumer.rs`                             | Managed receive/commit operations, mode guards, dispatch coordination, pause/resume integration and lifecycle.                      |
| `src/kafka/consumer/model.rs` and event types in `context.rs`      | Configuration, identity, sequence and commit/error types exposed through NAPI.                                                      |
| `src/kafka/kafka_util.rs`                                          | Structured native error conversion without changing legacy error contracts unintentionally.                                         |
| `src/kafka/consumer/byte_budget.rs` and `commit_queue.rs`          | Evaluate reuse; preserve existing async-commit semantics from [RFC-0013](../../implemented/0013-async-commit-error-contract/).      |
| `js-src/index.ts`, `js-src/kafka-client.ts`, `js-src/diagnostics/` | Public exports, configuration/capability wiring and tracing. Generate binding declarations through the build.                       |
| Rust tests next to consumer code                                   | Deterministic transition, dispatch, overflow and deadlock tests with controllable scheduling.                                       |
| `js-tests/unit/`, `js-tests/integration/`                          | Public API, real-broker lifecycle, compatibility and message accounting.                                                            |
| `README.md`, `docs/api.md`, package metadata and repository CI     | Document the contract, publish supported artifacts and make broker validation a release gate.                                       |

Keep small logic in its existing owner. Extract shared managed state only where needed to coordinate
the context and consumer; avoid layers that only forward one call. No producer redesign is required.

## Acceptance criteria

These are required future tests, not completed evidence. Every run must record source revisions,
native artifact, broker/assignor versions, queue configuration, seed and relevant event traces.

| Scenario                                                                  | Required result                                                                                                                     |
| ------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| Initial subscribe, idle topic and control-only delivery                   | First assignment is captured; control progresses without data. Late activation and mixed/concurrent receive calls fail explicitly.  |
| Revoke/reassign the same partition and multiple rebalances during receive | Identities are distinct; old deliveries/completions cannot advance new assignment state.                                            |
| Cooperative retained partition, including empty assign                    | Identity and valid progress are preserved.                                                                                          |
| Pause before native commit dispatch, then revoke                          | No stale dispatch; a pre-scheduling check alone must fail this test.                                                                |
| Commit in flight during revoke/reassign                                   | Result remains bound to the original identity; test broker-confirmed offsets and cross-member interaction, not only local state.    |
| Full queues, delayed JS, overflow                                         | Bounded memory, control progress within capacity, and native authorization invalidation on overflow before JS receives the failure. |
| Disconnect with receive and commit pending                                | No orphan queues/tasks, early claims of native cancellation, or deadlocks.                                                          |
| Structured errors and tracing                                             | Codes survive NAPI/JS conversion; spans cover managed messages without duplicate control spans.                                     |

Joint tests with Flashify must also cover:

- At least two members joining/leaving, 600 messages and six partitions, crossing `default` and
  `partitioned` with `cooperative-sticky` and `range`. Normal rebalances within the configured
  budget must resume without fatal pipeline errors or process restarts.
- Four-second control-delivery delay, with three and 40 messages across that matrix. Configure
  and verify that the larger case actually fills queues. No missing message IDs or silent stalls.
- The last pending commit with no later message, recovery exhaustion, and propagation of failures
  from background retries. Recovery and fatal host closing belong to Flashify.
- Ten thousand rebalances without enabling managed mode: no managed queue accumulation and stable
  memory after warmup, with a recorded tolerance. Separately test bounded active-mode memory.
- Existing `recv`, batch, all stream variants, `onEvents`, legacy commits and producer expectations.
  Preserve the asynchronous commit contract and validate ESM/CommonJS exports and generated types.
- Flashify omitted/manual modes and indirect use through the older Fastify plugins. Native APIs
  remain compatible even though consumers may install a newer shared binary.

Compare produced IDs with applied/DLQ IDs and broker offsets. Lag zero and the final offset alone
do not prove absence of loss. Replay duplicates are allowed; skipping work without a confirmed
destination is not. Run real rebalances as well as broker generation-error injection.

Required broker jobs fail when their broker is unavailable; they must not pass by skipping tests.
Runtime test commands and evidence will be added when the prototype introduces the fixtures.

### Matched generation-error and callback tests

Run the following scenarios against the same candidate binding/runtime pair. Capture native
rebalance state when the commit response is handled, the Kafka error code, ordered assignment
transitions and identities, handler attempts, commit requests/results, and host/pipeline lifecycle.
An empty assign callback or the test's intended timing alone is not sufficient proof of retention.

| Scenario                                                             | Required evidence and outcome                                                                                                                                                                                                                                                       |
| -------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Rejection during a real cooperative rebalance, partition retained    | Demonstrate `IllegalGeneration` while the native client is rebalancing and continuous ownership of the partition. Preserve identity and the completed prefix, then confirm it within budget without rerunning its handler or closing the pipeline/host.                             |
| Proxy rejection outside an ongoing rebalance, followed by revocation | Demonstrate the native state and actual revocation. Invalidate old work, prevent new commits under its identity, and verify replay from broker-confirmed offsets. A new coherent assignment may resume within budget; shutdown is not mandatory solely because revocation occurred. |
| Control delay and full queues added to both cases                    | No missing IDs, commits beyond unfinished work, or silent stalls. Within supported capacity/budget, control and recovery progress; deliberate overflow must invalidate authorization and fail explicitly.                                                                           |
| Recovery impossible or budget exhausted                              | Propagate the failure to the pipeline and invoke the host's configured fatal-close policy once. Verify replay of unconfirmed work by a subsequent owner.                                                                                                                            |
| Managed `CommitCallback` before and after the commit promise rejects | A recoverable error must not destroy `PartitionedMessageProcessor` through its observational listener. A fatal error reaches the pipeline once; legacy/manual consumers retain their existing callback contract.                                                                    |

Include retained-partition runs with the last message and no subsequent traffic, plus multiple
deferred commits consolidated into one valid completed prefix. For the revocation case, include
a pending receive and reassignment of the same partition so that the new identity is exercised.

Retain the old implementation's proxy shutdown/replay result as historical evidence. Update the
candidate's assertion to the new recovery contract rather than requiring that old shutdown to
continue. Keep a separate real-broker fatal case for the RFC 0002 close/replay guarantee.

These tests distinguish mitigating deployment restarts from fixing message loss. A mitigation
comparison may be used as an experimental baseline without becoming a separate release.

## Delivery and compatibility

This RFC and Flashify RFCs 0002/0003 form one functional delivery. Do not first publish a wrapper
patch that only defers commits while keeping the old receive contract. Implementation can use
dependent PRs and candidate artifacts, but the full pair must pass joint validation before release.

This is a delivery-scope decision, not a claim that deferred commits cannot mitigate the retained
cooperative-assignment case. The full delivery also addresses control ordering and unsafe commit
authorization, which that mitigation alone does not establish.

Publish the binding first, then Flashify with its supported binding requirement. The legacy-sync
service adopts both pinned versions and lockfile changes in one rollout, initially as a dry-run
canary. Flashify `Sync` selects the new contract and fails startup with an incompatible binding;
there is no silent fallback. Native managed mode remains opt-in for direct binding consumers.

Legacy API signatures and semantics remain supported for legacy consumers. Restricting incompatible
operations on an explicitly managed consumer is part of the new contract. Choose package versions
and semver classification only after reviewing the wrapper's new minimum binding requirement.

Rollback restores a known pair of package versions and its lockfile. It does not undo effects or
recover messages skipped by an invalid commit. Keep dry-run while the new guarantees are absent.

## Prototype decisions required before implementation approval

1. Final NAPI shapes and names for configuration, capability version, sequence records, identity,
   managed commit and errors, including offset precision and explicit termination behavior.
2. The native mechanism and ordering points for callbacks, message admission and commit dispatch;
   prove callback safety and delimit guarantees for in-flight requests across members.
3. How control reaches the runtime under backpressure without unbounded buffering; define byte/count
   accounting, terminal notification, default limits and oversized-record behavior.
4. The control signal or state query that lets Flashify decide whether the same assignment is ready
   for another commit, without equating a group generation error with either retention or revocation.
5. Lifecycle deadlines and resource ownership coordinated with wrapper recovery/shutdown budgets.
6. Supported manual operations, release versions and platform artifact/CI coverage.

Begin with failing reproductions and a small native prototype addressing decisions 1–4. Update
this RFC with the selected mechanism and measured evidence before treating the public contract
as settled. The intended guarantees are requirements; they are not proof of native feasibility.

## Prototype status (2026-11-11)

This section records what the native prototype decided for decisions 1–4 and what was
measured. It does not close the acceptance criteria: decisions 5–6, the joint Flashify matrix
and the proxy generation-error scenarios are still open. The API is opt-in and experimental.

Environment of every number below: macOS arm64 (darwin-arm64 only), rustc 1.99.1, Node 24.18,
rust-rdkafka 0.39 / librdkafka 2.12.1, one `apache/kafka:3.9.1` broker, release build.

### Decision 1 — NAPI shapes

| Item              | Decision                                                                                                                                                                                                                                                               |
| ----------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Activation        | `ConsumerConfiguration.managed: true`, read once in the constructor. No method changes the mode. `managedOptions` sets the limits.                                                                                                                                     |
| Contradictions    | `enable.auto.commit` and `enable.auto.offset.store` default to `false` in managed mode; an explicit truthy value from `enableAutoCommit`, consumer or client configuration fails creation.                                                                             |
| Capability        | Exported constant `MANAGED_CONSUMPTION_VERSION` (`1`) for the whole contract, plus the `consumer.managed` getter.                                                                                                                                                      |
| Records           | `recvManaged(timeoutMs?)` resolves one record of a discriminated union on `kind`: `message` (`message`, `assignment`), `assign` / `revoke` (`partitions[]` with `assignment` and `previousAssignment`; `lost` on revoke), `idle` (`reason: 'timeout' \| 'cancelled'`). |
| Identity          | Opaque string, unique per consumer instance and assignment incarnation. A retained partition named again by an assign reports `assignment === previousAssignment`.                                                                                                     |
| Commit            | `commitManaged(topic, partition, assignment, nextOffset)`; `nextOffset` is the next offset to consume, a JavaScript number as in `commit`.                                                                                                                             |
| Termination       | Disconnect while pending resolves `null`; later calls reject `ERR_MANAGED_CLOSED`. `cancelManagedReceive()` and the timeout resolve `idle`. Overflow and control failure reject every current and later call.                                                          |
| Errors            | Real properties on the rejection: `code` (`ERR_MANAGED_*`), `kafkaCode` / `kafkaErrorCode` when a Kafka error exists, `outcomeUnknown` on commit errors. Stale, unknown identity, closing, overflow, control failure and broker failure have distinct codes.           |
| Mode guards       | Managed rejects `recv`, `recvBatch`, the three stream methods and both stream factories, `commit`, `commitMessage`, `seek`, `unsubscribe` and manual assignment. Legacy rejects every managed method.                                                                  |
| Legacy guard code | Guards on existing methods keep their N-API status (`InvalidArg`) and carry `[ERR_MANAGED_MODE]` in the message; only the new methods reject with a structured `code`. Changing that would alter the legacy signatures.                                                |

`pause`/`resume` and `pausePartitions`/`resumePartitions` stay available: they are routed
through the managed state so the binding's own pause never undoes an application pause.
Only owned partitions can be paused, and a pause does not survive revocation.

### Decision 2 — ordering mechanism

One native task per managed consumer (the pump) is the only poller. rust-rdkafka runs
rebalance callbacks inside that poll, so on that task the sequence is exactly librdkafka's:

- `pre_rebalance` updates the shared state under a short `std::sync::Mutex` and appends the
  transition record: identities are created before the native assign and invalidated before
  the native unassign. The callback touches no JavaScript and makes no broker round trip.
- When the poll returns a message, the pump admits it under the same mutex, stamps it with the
  identity owning the partition at that moment and appends it to the same queue. A transition
  that preceded the message is therefore ahead of it, and a message cannot be stamped with an
  identity created after it.
- `recvManaged` only pops that queue. JavaScript scheduling cannot reorder it.
- On revocation, messages of the revoked identities that JavaScript has not received are
  released in the callback. They cannot be committed and the next owner refetches them.
- A message for a partition without an active identity is treated as a broken ordering
  assumption and is a terminal `ERR_MANAGED_CONTROL_FAILURE`, not a silent drop.

Commit dispatch takes the same mutex: `commitManaged` re-validates lifecycle and identity and
calls `rd_kafka_commit_queue` (a local enqueue to the group thread) in one critical section,
then awaits the broker result outside it. A revoke callback either finished before the check,
and the commit is refused without dispatch, or starts after the request was handed over.
Results return on a dedicated reply queue and are matched to their promise by a per-commit
opaque, so a late result stays with its original identity. Managed results are not republished
as `CommitCallback` events; the promise is the single owner. A commit under a newer identity of
the same partition waits for unresolved commits of older identities before it is dispatched.

Callback safety: the mutex is never held across an await, a broker round trip or JavaScript.
Pause/resume calls use a separate lock that callbacks never take.

**Limits of the guarantee.** The identity is a local authorization, not broker-side fencing.

- A request handed over before a revocation cannot be recalled. librdkafka sends it with the
  member's current generation, possibly after the rebalance, and the classic protocol does not
  check partition ownership. It may therefore be applied after the partition moved.
- Such a request was authorized for completed work, so applying it cannot skip unprocessed
  messages. It can move the committed offset backwards relative to the new owner's commits,
  which causes replay.
- Commits from other members, or from legacy consumers in the same group, are outside this
  protocol. The reproduction below shows the broker accepting one for a revoked partition.

### Decision 3 — backpressure and budgets

- The pump keeps polling whether or not JavaScript calls `recvManaged`, so assignment control
  and commit authorization progress while the application is busy or its event loop is blocked.
- Retained messages are bounded by `maxBufferedMessages` (1024) and `maxBufferedBytes`
  (32 MiB, the RFC-0016 default and accounting). At the bound the pump stops taking messages.
  If the application makes room within 100 ms nothing is paused. After 100 ms without room the
  pump pauses the owned partitions and resumes polling for control only; it resumes them at
  half of the bound. A message that slips in around a pause is kept, so the bound can be
  exceeded by a few messages. One oversized message is always admitted.
- These limits cover the binding's buffer only: not librdkafka's fetch queues, and not messages
  already handed to JavaScript.
- Control records have their own budget, `maxControlRecords` (1024). Exceeding it sets a
  terminal status in the same critical section that clears the identities and buffers, wakes
  the pending receive and makes the consumer leave the group. The status is the notification,
  so it cannot be lost to a full queue. Nothing is overwritten.
- `ByteBudget` itself is not reused: it makes the producer of bytes wait, and the pump must
  never wait on the application. Only its default and byte accounting are shared.

Rejected alternative, measured: pausing as soon as the buffer filled gave 1.2k msg/s, because
each resume costs a fetch round trip. With the grace period: 69k msg/s for `recvManaged`
against 77k msg/s for legacy `recv()` (100,000 messages of 200 bytes, one partition, two runs
each, peak retained 1024).

### Decision 4 — state query for commit recovery

`isManagedAssignmentActive(topic, partition, assignment)` reads the native state without
waiting for `recvManaged`, and `managedState()` exposes the same state for diagnostics. A
`true` answer can be invalidated at once; only `commitManaged` orders the check against
revocation. The runtime therefore decides from two independent facts: the commit error
(`kafkaCode`, `outcomeUnknown`) and whether the identity is still active. In the cooperative
run below, commits on retained partitions failed with `IllegalGeneration` and
`RebalanceInProgress` while their identities stayed active, and later commits under the same
identities were confirmed.

### Evidence

Commands, from `packages/kafka-crab-js`:

| Suite                           | Command                                                                                       | Result       |
| ------------------------------- | --------------------------------------------------------------------------------------------- | ------------ |
| Rust                            | `cargo test`                                                                                  | 36 passed    |
| Unit                            | `pnpm test`                                                                                   | 80 passed    |
| Managed, real broker            | `KAFKA_BROKERS=127.0.0.1:29092 node --test js-tests/integration/managed-consumption.test.mjs` | 9 passed     |
| Legacy regressions, real broker | same, for `consumer-manual-commit`, `partition-lifecycle` and `consumer`                      | 15, 2 and 11 |

- **Reproductions of the legacy gap.** A lagging `onEvents` receiver loses 72 of 200 rebalance
  events (Rust). With `cooperative-sticky`, a legacy member that lost a partition committed
  offset 41 for it and the broker confirmed it; the managed commit with the revoked identity was
  refused and the committed offset did not move (integration).
- **Join and leave under traffic**, four partitions, 520–990 messages per run. With
  `cooperative-sticky` retained partitions kept their identity; with `range` every identity
  changed. A partition that left and returned got a new identity, commits with the old one
  were refused, and no produced id was missing. Final committed offsets equalled the high
  watermarks. Replays per run: 0–1.
- **Control without consumption.** With `maxBufferedMessages: 16` and 300 messages available,
  the retained count stayed at 16. A second member then joined while the JavaScript thread was
  blocked for 8 s: both native states showed the new assignment, the old identities were
  inactive and 16 retained messages had been released. No id was missing afterwards.
- **Overflow.** With `maxControlRecords: 1` the second transition failed the consumer; the
  identities were inactive before any receive, and the other member took every partition.
- **Legacy memory.** 10,000 simulated rebalances through the real callbacks of a legacy
  context leave no managed state and at most 128 broadcast events (Rust). This is a structural
  check, not an RSS measurement.

### Not done in the prototype

- Decisions 5 and 6, the joint Flashify matrix (600 messages, six partitions, 4 s control
  delay) and the proxy generation-error scenarios.
- A commit that is in flight across a revoke and reassign is covered deterministically in Rust
  only. The real-broker run covers disconnect with an in-flight commit.
- No deadline for an unresolved commit: a newer identity waits for as long as librdkafka keeps
  the older request open.
- Finalization of a managed consumer that is never disconnected was not measured.
- `managed` consumers do not support manual assignment, `seek` or `unsubscribe`.
- Linux and x86 artifacts, CI, package version and changelog.
