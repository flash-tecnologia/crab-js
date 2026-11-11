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

Decision recorded on 2026-11-11: `commitMode: 'Sync'` is itself the opt-in on the Flashify side,
so no additional Flashify flag is introduced. `Sync` is not Flashify's default; the omitted and
`'manual'` modes keep their current behavior. The only known user of `Sync` is the legacy-sync
service, which adopts both pinned versions together. On the binding side the opt-in is
`managed: true`, unchanged.

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
measured, after one round of independent review. It does not close the acceptance criteria:
decisions 5–6, the joint Flashify matrix and the proxy generation-error scenarios are still
open. The API is opt-in and experimental.

Environment of every number below: macOS arm64 (darwin-arm64 only), rustc 1.99.1, Node 24.18,
rust-rdkafka 0.39 / librdkafka 2.12.1, one `apache/kafka:3.9.1` broker, release build.

### Decision 1 — NAPI shapes

| Item              | Decision                                                                                                                                                                                                                                                               |
| ----------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Activation        | `ConsumerConfiguration.managed: true`, read once in the constructor. No method changes the mode. `managedOptions` sets the limits.                                                                                                                                     |
| Contradictions    | `enable.auto.commit` and `enable.auto.offset.store` default to `false` in managed mode; an explicit truthy value from `enableAutoCommit`, consumer or client configuration fails creation.                                                                             |
| Capability        | Exported constant `MANAGED_CONSUMPTION_VERSION` (`1`) for the whole contract, plus the `consumer.managed` getter.                                                                                                                                                      |
| Records           | `recvManaged(timeoutMs?)` resolves one record of a discriminated union on `kind`: `message` (`message`, `assignment`), `assign` / `revoke` (`partitions[]` with `assignment` and `previousAssignment`; `lost` on revoke), `idle` (`reason: 'timeout' \| 'cancelled'`). |
| Single receive    | The call reserves the receive synchronously, until its promise settles. A second call in between rejects, including in the same tick.                                                                                                                                  |
| Identity          | Opaque string with a random per-instance component and a per-assignment sequence. A retained partition named again by an assign reports `assignment === previousAssignment`.                                                                                           |
| Commit            | `commitManaged(topic, partition, assignment, nextOffset, timeoutMs?)`; `nextOffset` is the next offset to consume, a JavaScript number as in `commit`. Authorization and dispatch happen synchronously in the call.                                                    |
| Termination       | Disconnect, or garbage collection of the consumer, resolves a pending receive with `null`; later calls reject `ERR_MANAGED_CLOSED`. `cancelManagedReceive()` and the timeout resolve `idle` without taking a record. Terminal failures reject current and later calls. |
| Errors            | Real properties on the rejection: `code` (`ERR_MANAGED_*`), `kafkaCode` / `kafkaErrorCode` when a Kafka error exists, `outcomeUnknown` on commit errors.                                                                                                               |
| Mode guards       | Managed rejects `recv`, `recvBatch`, the three stream methods and both stream factories, `commit`, `commitMessage`, `seek`, `unsubscribe` and manual assignment. Legacy rejects every managed method.                                                                  |
| Legacy guard code | Guards on existing methods keep their N-API status (`InvalidArg`) and carry `[ERR_MANAGED_MODE]` in the message; only the new methods reject with a structured `code`.                                                                                                 |

Every `commitManaged` rejection carries `outcomeUnknown`. It is `false` when nothing was
handed to librdkafka: stale or unknown identity, regression, backlog, closed, terminal failure,
invalid argument, or a local dispatch failure. It is `true` for every error of a request that
was sent, for a deadline and for a close without a result. librdkafka retries or defers an
OffsetCommit after `_TRANSPORT`, `NOT_COORDINATOR` and similar errors and can then finish with
another code such as `_WAIT_COORD` or a generation error, so the final code does not prove that
no attempt was applied.

`pause`/`resume` and `pausePartitions`/`resumePartitions` stay available: they are routed
through the managed state so the binding's own pause never undoes an application pause.
Only owned partitions can be paused, and a pause does not survive revocation. A call that
arrives while the pump is applying its own plan does not wait behind it; the pump applies it
next. A partition librdkafka does not confirm is retried by the pump, not reported to the
caller.

The `KafkaConsumer` type gained members. Runtime behavior of legacy consumers is unchanged, but
a wrapper that type-checks a structural mock against `KafkaConsumer` must add them.

### Decision 2 — ordering mechanism

One native task per managed consumer (the pump) is the only poller. rust-rdkafka runs
rebalance callbacks inside that poll, so on that task the sequence is exactly librdkafka's:

- `pre_rebalance` updates the shared state under a short `std::sync::Mutex` and appends the
  transition record: identities are created before the native assign and invalidated before
  the native unassign. The callback touches no JavaScript and makes no broker round trip.
  "Revoked" in this document means that local step, not a broker-side fact.
- When the poll returns a message, the pump admits it under the same mutex, stamps it with the
  identity owning the partition at that moment and appends it to the same queue. A transition
  that preceded the message is therefore ahead of it, and a message cannot be stamped with an
  identity created after it.
- `recvManaged` only pops that queue. JavaScript scheduling cannot reorder it.
- On revocation, messages of the revoked identities that JavaScript has not received are
  released in the callback. They cannot be committed and the next owner refetches them.
- A message for a partition without an active identity is treated as a broken ordering
  assumption and is a terminal `ERR_MANAGED_CONTROL_FAILURE`, not a silent drop.

Commits take the same mutex. `commitManaged` validates lifecycle and identity and calls
`rd_kafka_commit_queue` (a local enqueue to the group thread) in one critical section, on the
calling thread. A revoke callback either finished before the check, and the commit is refused
without dispatch, or starts after the request was handed over.

- Commits of one partition are dispatched one at a time, in call order. A later call waits in
  a per-partition queue and is re-validated, in the same critical section, when it is
  dispatched. Two commits issued without awaiting can therefore not reach the broker reordered.
- A `nextOffset` lower than one already authorized under the identity is rejected
  (`ERR_MANAGED_COMMIT_REGRESSION`). An equal one is sent again, which is how a failed commit
  is retried.
- A commit of a newer identity never overlaps an unresolved request of an older identity of the
  same partition: it waits in the same queue.
- A revocation, a terminal failure and a close settle the commits that are still waiting
  (`ERR_MANAGED_STALE_ASSIGNMENT`, the failure code, `ERR_MANAGED_CLOSED`). A request already
  sent keeps its own result.
- Results return on a dedicated reply queue and are matched to their promise by a per-commit
  opaque, so a late result stays with its original identity. Managed results are not
  republished as `CommitCallback` events; the promise is the single owner.
- `timeoutMs` ends only the observation (`ERR_MANAGED_COMMIT_TIMEOUT`, `outcomeUnknown: true`).
  The request is not cancelled and still holds its place in the partition's queue.
- Head-of-line blocking: one request that librdkafka keeps open holds every later commit of
  that partition. They accumulate until `maxPendingCommits`, after which `commitManaged`
  rejects with `ERR_MANAGED_COMMIT_BACKLOG` for every partition. Other partitions keep
  committing until that point.
- A subscribe and the unsubscribe of a failed consumer are ordered by one lock, and the
  subscribe re-reads the status under it. A subscribe that was in progress when the state
  failed either completes first and is then undone, or is rejected.

Callback safety: the mutex is never held across an await, a broker round trip or JavaScript,
and lookups under it are by partition or commit id. Pause/resume calls use a separate lock that
callbacks never take.

**Limits of the guarantee.** The identity is a local authorization, not broker-side fencing.

- A request handed over before a revocation cannot be recalled. librdkafka sends it with the
  member's current generation, possibly after the rebalance, and the classic protocol does not
  check partition ownership. It may therefore be applied after the partition moved.
- The binding does not validate `nextOffset` against anything the application did. Only if the
  caller commits a completed prefix does a late commit never skip unprocessed messages; it can
  still move the committed offset backwards relative to the new owner's commits, which
  causes replay.
- Commits from other members, or from legacy consumers in the same group, are outside this
  protocol. The reproduction below shows the broker accepting one for a revoked partition.
- librdkafka can retry a sent request internally. Ordering is guaranteed at dispatch; a retry
  of an older request of another partition is unaffected, and within a partition the next
  commit is only dispatched after the previous one settled.

### Decision 3 — backpressure and budgets

- The pump keeps polling whether or not JavaScript calls `recvManaged`, so assignment control
  and commit authorization progress while the application is busy or its event loop is blocked.
- Consequence: `max.poll.interval.ms` no longer detects a stuck application, because the
  binding polls on its behalf. A consumer whose JavaScript stopped keeps its partitions.
- The optional watchdog `managedOptions.maxReceiveIdleMs` (off by default) covers part of that.
  It measures whether JavaScript is draining the receive: with no `recvManaged` requested, or
  with a decided result not collected, for longer than the limit, the consumer fails with the
  terminal `ERR_MANAGED_RECEIVE_STALLED` and leaves the group. A receive waiting for records
  counts as alive. It does not see handlers: a runtime that keeps receiving while its handlers
  hang is not detected, and a runtime that applies backpressure by not receiving for longer
  than the limit is failed.
- Retained messages are bounded by `maxBufferedMessages` (1024) and `maxBufferedBytes`
  (32 MiB, the RFC-0016 default and accounting). At the bound the pump stops taking messages.
  If the application makes room within 100 ms nothing is paused. After 100 ms without room the
  pump pauses the owned partitions and resumes polling for control only; it resumes them at
  half of the bound.
- The bound is best-effort up to a hard ceiling: messages already fetched when a pause takes
  effect are kept. The ceiling is twice the bound, or the bound plus 16 messages / 1 MiB if
  that is more, so a small bound is not failed by the few messages around a pause. It is
  judged on the resulting totals; the only exception is a message admitted into an empty
  buffer, so one message larger than the byte ceiling stays deliverable.
- A pause or resume is recorded only for the partitions librdkafka confirmed and is retried
  every 100 ms for the others. Passing a ceiling, or fifty consecutive failed attempts on an
  owned partition, ends in the terminal `ERR_MANAGED_BACKPRESSURE_FAILURE`, so neither
  unbounded growth nor a silent stall is possible. A resume that fails for a partition the
  consumer no longer owns (for example of a deleted topic) is set aside instead of counted. If
  that partition is assigned again its native pause state is treated as unknown and the desired
  state, paused or not, is sent again.
- Pause and resume wait on librdkafka's threads without a timeout, so the pump runs them on the
  blocking pool, not on an async worker. Polling, and therefore rebalance callbacks, wait for
  that call. `pausePartitions` and `resumePartitions` stay synchronous on the calling thread,
  as in legacy mode.
- Under saturation the pump does not poll for up to 100 ms, which delays rebalance callbacks
  by the same amount.
- Control records have their own budget, `maxControlRecords` (1024). Exceeding it sets a
  terminal status in the same critical section that clears the identities and buffers, wakes
  the pending receive and makes the consumer leave the group. The status is the notification,
  so it cannot be lost to a full queue. Nothing is overwritten.
- Commits dispatched or waiting are bounded by `maxPendingCommits` (1024); beyond it
  `commitManaged` rejects `ERR_MANAGED_COMMIT_BACKLOG` without sending anything.
- Up to 16 consumption errors are kept for `recvManaged`; further ones are counted and dropped.
- These limits cover the binding's state only: not librdkafka's fetch queues, and not messages
  already handed to JavaScript.
- `ByteBudget` itself is not reused: it makes the producer of bytes wait, and the pump must
  never wait on the application. Only its default and byte accounting are shared.

Rejected alternative, measured: pausing as soon as the buffer filled gave 1.2k msg/s, because
each resume costs a fetch round trip. With the grace period: 69k–70k msg/s for `recvManaged`
against 78k–85k msg/s for legacy `recv()` (100,000 messages of 200 bytes, one partition, two
runs each, peak retained 1024).

### Decision 4 — state query for commit recovery

`isManagedAssignmentActive(topic, partition, assignment)` reads the native state without
waiting for `recvManaged`, and `managedState()` exposes the same state for diagnostics. A
`true` answer can be invalidated at once; only `commitManaged` orders the check against
revocation. The runtime therefore decides from two independent facts: the commit error
(`kafkaCode`, `outcomeUnknown`) and whether the identity is still active. In the cooperative
run below, commits on retained partitions failed with `IllegalGeneration` and
`RebalanceInProgress` while their identities stayed active, and later commits under the same
identities were confirmed.

### Requirements for the Flashify wrapper

Flashify selects managed mode automatically for `commitMode: 'Sync'`. To use this contract it
must:

- carry the `assignment` of each message to the commit of that message's offset;
- not use `seek`, `unsubscribe`, manual assignment or the stream consumers on that consumer;
- configure `maxReceiveIdleMs`, and keep a receive pending (or pause partitions) while it
  applies backpressure, since handler deadlines remain its own responsibility;
- treat `ERR_MANAGED_STALE_ASSIGNMENT` and `ERR_MANAGED_COMMIT_REGRESSION` as non-fatal: the
  first means the work is replayed by the next owner, the second that a newer offset was
  already authorized;
- commit only a completed prefix, because the binding does not validate `nextOffset`;
- bound its own commit concurrency per partition, given the head-of-line behavior above.

### Lifecycle

- `disconnect()` closes the managed state synchronously in the call. A pending receive resolves
  `null`, commits still waiting are refused, and a request already sent is neither cancelled
  nor reported as cancelled: the native close waits for it and its promise gets the broker
  result, or `ERR_MANAGED_COMMIT_UNKNOWN` if the client closed without one.
- A consumer that is garbage collected without disconnect closes the state the same way, and
  the pump ends when its disconnect signal is dropped.
- After a terminal failure the consumer has left the group; `subscribe` is rejected.
- The blocking native close never runs on an async worker: the last reference to the client is
  released to the blocking pool, whichever thread drops it.
- A legacy consumer holds a single `None` in place of all managed state.

### Evidence

Commands, from `packages/kafka-crab-js`:

| Suite                           | Command                                                                                       | Result       |
| ------------------------------- | --------------------------------------------------------------------------------------------- | ------------ |
| Rust                            | `cargo test`                                                                                  | 60 passed    |
| Unit                            | `pnpm test`                                                                                   | 83 passed    |
| Managed, real broker            | `KAFKA_BROKERS=127.0.0.1:29092 node --test js-tests/integration/managed-consumption.test.mjs` | 11 passed    |
| Legacy regressions, real broker | same, for `consumer-manual-commit`, `partition-lifecycle` and `consumer`                      | 15, 2 and 11 |

- **Reproductions of the legacy gap.** A lagging `onEvents` receiver loses 72 of 200 rebalance
  events (Rust). With `cooperative-sticky`, a legacy member that lost a partition committed
  offset 41 for it and the broker confirmed it; the managed commit with the revoked identity was
  refused and the committed offset did not move (integration).
- **Join and leave under traffic**, four partitions, 700–1,030 messages per run. With
  `cooperative-sticky` retained partitions kept their identity; with `range` every identity
  changed. A partition that left and returned got a new identity and commits with the old one
  were refused. The offsets received under each identity formed one contiguous stream. A new
  member then restarted from the broker-committed offsets: its first offset on every partition
  equalled the committed one, no committed offset was beyond processed work and no produced id
  was missing. Replays per run: 0–1.
- **Commits across a revocation.** With commits issued continuously without awaiting, every
  outcome was one the contract allows, 4–9 waiting commits per run were refused as stale and no
  id was missing after a restart from the committed offsets. Each run also reported two
  confirmed results delivered after their identity had been revoked; that observation depends
  on broker timing and is not asserted. The held-result, reordering, waiting and regression
  cases are deterministic in Rust.
- **Disconnect with a commit in flight.** A receive and two commits were issued and
  `disconnect()` called in the same tick. The receive resolved `null` and the sent commit was
  confirmed and visible at the broker. The second commit is refused as closed unless the
  broker had already answered the first; both are accepted by the test.
- **Control without consumption.** With `maxBufferedMessages: 16` and 300 messages available,
  the retained count stayed at 16. A second member then joined while the JavaScript thread was
  blocked for 8 s: both native states showed the new assignment, the old identities were
  inactive and 16 retained messages had been released. No id was missing afterwards.
- **Overflow and watchdog.** With `maxControlRecords: 1` the second transition failed the
  consumer; with `maxReceiveIdleMs: 1500` so did 1.5 s without a receive. In both cases the
  identities were inactive before any receive and the other member took every partition.
- **Pump loop with injected failures** (Rust, fake client): no poll while saturated, pause only
  after the grace period, a consumer that keeps up is never paused, an unconfirmed pause is
  retried, twice the bound and fifty failed resumes are terminal, and dropping the consumer
  ends the pump and settles a pending receive.
- **Legacy memory.** After warm-up, 10,000 rebalances through the real callbacks of a legacy
  context retained less than 4 KiB of heap on the calling thread, measured with a counting
  allocator; the same calls on a managed context with an unread queue retained over 1 MiB.
  `getConfig()` of a legacy consumer is unchanged.

### Not done in the prototype

- Decisions 5 and 6, the joint Flashify matrix (600 messages, six partitions, 4 s control
  delay) and the proxy generation-error scenarios.
- `lost: true` is covered at the state level only; no real-broker run produced a lost
  assignment.
- The in-flight commits observed across a real revocation were all confirmed. A broker
  rejection of such a request is covered in Rust only.
- A deadline on `commitManaged` does not free the partition's commit queue: a newer commit
  waits for as long as librdkafka keeps the older request open.
- `managed` consumers do not support manual assignment, `seek` or `unsubscribe`.
- Linux and x86 artifacts, CI, package version and changelog.
