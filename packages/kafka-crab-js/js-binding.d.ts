
/**
 * Which binding artifact the generated loader actually loaded: `'native'` for
 * a native addon, otherwise the `platformArchABI` of the WASI flavor. Every
 * flavor napi-rs can build is listed, because `NAPI_RS_NATIVE_LIBRARY_PATH`
 * can point the loader at a WASI artifact this package does not build itself.
 */
export declare const __napiBindingTarget: 'native' | 'wasm32-wasi' | 'wasm32-wasip1'

export declare class KafkaAdmin {
  /**
   * Creates topics and returns one result per topic, including partial failures.
   * Existing topics are successful with `created: false` and are not modified.
   */
  createTopics(options: CreateTopicsOptions): Promise<Array<AdminTopicResult>>
  /** Fetches metadata without subscribing, joining a group, or auto-creating topics. */
  fetchTopicMetadata(topics: Array<string>): Promise<Array<AdminTopicMetadata>>
  /**
   * Fetches inclusive low and exclusive high watermarks as decimal strings.
   * Partition failures are returned alongside successful results.
   */
  fetchTopicOffsets(topic: string): Promise<Array<AdminTopicOffset>>
  /**
   * Reads a group's committed next offsets. Never subscribes, joins, or commits.
   * Missing commits are absent rather than being resolved to the start or end.
   */
  fetchOffsets(options: FetchOffsetsOptions): Promise<Array<AdminGroupOffsets>>
  /**
   * Describes existing groups and their members without participating in them.
   * Assignments and member metadata are raw protocol bytes.
   */
  describeGroups(groupIds: Array<string>): Promise<Array<AdminGroupDescription>>
  /** Idempotently prevents new operations. In-flight operations may finish. */
  close(): Promise<void>
}

export declare class KafkaClientConfig {
  /**
   * Creates a new KafkaClientConfig with the provided configuration.
   * This is the main entry point for creating Kafka producers and consumers.
   * @param kafkaConfiguration - The Kafka client configuration options
   */
  constructor(kafkaConfiguration: KafkaConfiguration)
  /**
   * Returns the current Kafka configuration.
   * This is exposed as a readonly property 'kafkaConfiguration' in JavaScript.
   */
  get configuration(): KafkaConfiguration
  /**
   * Creates a new Kafka producer with the specified configuration.
   * @param producerConfiguration - The producer-specific configuration options
   * @returns A new KafkaProducer instance
   */
  createProducer(producerConfiguration: ProducerConfiguration): KafkaProducer
  /** Creates an administrative client. No group is joined by administrative reads. */
  createAdmin(configuration?: AdminConfiguration | undefined | null): KafkaAdmin
  /**
   * Deletes all records before the requested offset for each topic partition.
   * Kafka does not delete an individual record directly: passing offset `N`
   * deletes records with offsets lower than `N`.
   */
  deleteRecords(topicPartitions: Array<TopicPartition>): Promise<Array<TopicPartition>>
  /**
   * Creates a new Kafka consumer with the specified configuration.
   * The consumer must be subscribed to topics before receiving messages.
   * @param consumerConfiguration - The consumer-specific configuration options
   * @returns A new KafkaConsumer instance
   */
  createConsumer(consumerConfiguration: ConsumerConfiguration): KafkaConsumer
}

export declare class KafkaConsumer {
  /** Returns the current consumer configuration. */
  getConfig(): ConsumerConfiguration
  /** Returns the client ID associated with this consumer */
  get clientId(): string
  /** Returns the list of topics and partitions currently subscribed to. */
  getSubscription(): Array<TopicPartition>
  /**
   * Registers a callback to receive Kafka consumer events (rebalance, errors, etc.).
   * The callback will be invoked for each event until the consumer is disconnected.
   * @param callback - Function called with each event
   */
  onEvents(callback: (error: Error | undefined, event: KafkaEvent) => void): void
  /**
   * Subscribes to one or more Kafka topics.
   * Can accept either a single topic name string or an array of topic configurations
   * with advanced options like partition offsets and topic creation settings.
   * Topics are either all group-subscribed or all manually assigned: mixing both
   * in one call is rejected. When topic creation fails, the remaining topics are
   * still created and subscribed; the error is returned afterwards, with the
   * consumer left subscribed.
   */
  subscribe(topicConfigs: string | Array<TopicPartitionConfig>): Promise<void>
  /**
   * Pauses message consumption on all assigned partitions.
   * Messages will be buffered by the broker until resume() is called.
   */
  pause(): void
  /** Resumes message consumption on all assigned partitions after a pause. */
  resume(): void
  /** Pauses only these assigned partitions. Already delivered messages remain caller-owned. */
  pausePartitions(partitions: Array<TopicPartitionId>): void
  /** Resumes only these assigned partitions, leaving other paused partitions unchanged. */
  resumePartitions(partitions: Array<TopicPartitionId>): void
  /**
   * Unsubscribes from all currently subscribed topics.
   * After calling this method, the consumer will no longer receive messages.
   */
  unsubscribe(): void
  /**
   * Disconnects the consumer and waits for native closure.
   * An optional AbortSignal cancels only this call's wait, not native cleanup.
   * Use AbortSignal.timeout(ms) to bound the wait. Pending receives are interrupted.
   */
  disconnect(options?: { signal?: AbortSignal }): Promise<void>
  /**
   * Seeks to a specific offset on a topic partition.
   * This allows repositioning the consumer to read from a specific point.
   * @param topic - The topic name
   * @param partition - The partition number
   * @param offsetModel - The offset to seek to (Beginning, End, Offset, or Stored)
   * @param timeout - Optional timeout in milliseconds (default: 1500ms, max: 300000ms)
   */
  seek(topic: string, partition: number, offsetModel: OffsetModel, timeout?: number | undefined | null): void
  /**
   * Returns the current partition assignment for this consumer.
   * This includes all topic partitions that have been assigned to this consumer
   * as part of the consumer group rebalancing.
   */
  assignment(): Array<TopicPartition>
  /**
   * Receives a single message from the subscribed topics.
   * This method will block until a message is available or the consumer is disconnected.
   * @returns The received message, or null if the consumer was disconnected
   */
  recv(): Promise<Message | null>
  /**
   * Receives multiple messages in a single call for higher throughput
   *
   * This method provides 2-5x better performance than calling recv() multiple times
   * by batching message retrieval and reducing function call overhead.
   *
   * @param size Maximum number of messages to retrieve (1-configured max, default 1000)
   * @param timeout_ms Timeout in milliseconds
   * @returns Array of messages (may be fewer than size)
   */
  recvBatch(size: number, timeoutMs: number): Promise<Array<Message>>
  /**
   * Receives messages as a native Web `ReadableStream`.
   * Uses a small internal batch prefetch to reduce native boundary crossings.
   */
  recvStream(prefetchSize?: number | undefined | null, prefetchTimeoutMs?: number | undefined | null): ReadableStream<Message>
  /** Receives batches of messages as a native Web `ReadableStream`. */
  recvBatchStream(size: number, timeoutMs: number): ReadableStream<Array<Message>>
  /**
   * Receives metadata batches as a compact native Web `ReadableStream`.
   * Intended for JS-side expansion to preserve the public `Message[]` API with less native marshalling overhead.
   */
  recvBatchStreamCompact(size: number, timeoutMs: number): ReadableStream<CompactMessageBatch>
  /**
   * Commits an offset for a specific topic partition.
   * This marks the offset as processed, so the consumer will not receive
   * messages before this offset after a restart.
   * @param topic - The topic name
   * @param partition - The partition number
   * @param offset - The offset to commit
   * @param commit - The commit mode (Sync or Async)
   */
  commit(topic: string, partition: number, offset: number, commit: CommitMode): Promise<void>
  /**
   * Commits the offset for a message.
   * This is a convenience method that automatically increments the offset by 1.
   * The offset committed is `message.offset + 1` since Kafka expects the next offset to be consumed.
   * @param message - The message to commit
   * @param commit - The commit mode (Sync or Async)
   */
  commitMessage(message: Message, commit: CommitMode): Promise<void>
  /** Whether this consumer was created with `managed: true` (RFC-0017). */
  get managed(): boolean
  /**
   * Experimental (RFC-0017). Returns the next managed record in native order:
   * a message with the identity of its partition assignment, or an assignment
   * transition. Every transition that preceded a message in librdkafka is
   * returned before it; transitions are returned even when no message exists.
   *
   * Resolves `{ kind: 'idle' }` after `timeoutMs` (or `cancelManagedReceive`)
   * and `null` when the consumer is disconnected while the call is pending.
   * Only one call may be pending. Rejects with `error.code`:
   * `ERR_MANAGED_RECEIVE_PENDING`, `ERR_MANAGED_CLOSED`,
   * `ERR_MANAGED_CONTROL_OVERFLOW`, `ERR_MANAGED_CONTROL_FAILURE` (terminal),
   * `ERR_MANAGED_RECEIVE_FAILED` (with `kafkaCode`; not terminal) or
   * `ERR_MANAGED_MODE` on a legacy consumer.
   * @param timeoutMs - Optional wait bound in milliseconds; omit to wait
   */
  recvManaged(timeoutMs?: number | undefined | null): Promise<ManagedRecord | null>
  /**
   * Experimental (RFC-0017). Wakes the pending `recvManaged`, which resolves
   * `{ kind: 'idle', reason: 'cancelled' }`. No admitted record is lost.
   * @returns Whether a receive was pending
   */
  cancelManagedReceive(): boolean
  /**
   * Experimental (RFC-0017). Commits `nextOffset` (the next offset to consume,
   * as in `commit`) for a partition under the assignment identity that
   * delivered the work. The final identity check and the native dispatch are
   * ordered against revocation; the promise settles with the broker result.
   * There is no automatic retry.
   *
   * Rejects with `error.code`: `ERR_MANAGED_STALE_ASSIGNMENT`,
   * `ERR_MANAGED_UNKNOWN_ASSIGNMENT`, `ERR_MANAGED_CLOSED`,
   * `ERR_MANAGED_CONTROL_OVERFLOW`, `ERR_MANAGED_CONTROL_FAILURE`,
   * `ERR_MANAGED_COMMIT_FAILED` (with `kafkaCode`/`kafkaErrorCode`),
   * `ERR_MANAGED_COMMIT_UNKNOWN`, `ERR_MANAGED_INVALID_ARGUMENT` or
   * `ERR_MANAGED_MODE`. Commit errors carry `outcomeUnknown`.
   *
   * The identity is a local authorization, not broker-side fencing: a request
   * dispatched before a revocation cannot be recalled and may still be applied.
   */
  commitManaged(topic: string, partition: number, assignment: string, nextOffset: number): Promise<void>
  /**
   * Experimental (RFC-0017). Whether `assignment` is the identity that
   * currently owns the partition, read from native state without waiting for
   * `recvManaged`. A `true` result can be invalidated at any time; only
   * `commitManaged` orders the check against revocation.
   */
  isManagedAssignmentActive(topic: string, partition: number, assignment: string): boolean
  /** Experimental (RFC-0017). Diagnostic snapshot of the managed native state. */
  managedState(): ManagedStateSnapshot
}

export declare class KafkaProducer {
  /**
   * Returns the number of messages that are currently in-flight (sent but not yet acknowledged).
   * This can be used to implement backpressure or monitor producer health.
   */
  inFlightCount(): number
  getLastDeliveryResults(): Array<RecordMetadata>
  /**
   * Flushes all pending messages to the Kafka broker and waits for delivery confirmation.
   * When autoFlush is enabled (default), this returns an empty array as messages are flushed automatically.
   * When autoFlush is disabled, this must be called manually to send buffered messages.
   * @returns Array of RecordMetadata for each delivered message
   */
  flush(): Promise<Array<RecordMetadata>>
  /**
   * Sends one or more messages to a Kafka topic.
   * Messages are sent asynchronously and delivery is confirmed based on the autoFlush setting.
   * @param producerRecord - The record containing the topic and messages to send
   * @returns Array of RecordMetadata for each delivered message (empty if autoFlush is disabled)
   */
  send(producerRecord: ProducerRecord): Promise<Array<RecordMetadata>>
}

export interface AdminConfiguration {
  /** Total deadline for each operation in milliseconds. Defaults to 5000. */
  timeoutMs?: number
}

export interface AdminGroupDescription {
  groupId: string
  state: string
  protocolType: string
  protocol: string
  members: Array<AdminGroupMember>
}

export interface AdminGroupMember {
  memberId: string
  clientId: string
  clientHost: string
  /** Raw bytes in the group's assignment protocol. */
  assignment?: Buffer
  metadata?: Buffer
}

export interface AdminGroupOffsets {
  topic: string
  partitions: Array<AdminPartitionOffset>
}

export interface AdminPartitionMetadata {
  partition: number
  leader: number
  replicas: Array<number>
  isr: Array<number>
  error?: KafkaCrabError
}

export interface AdminPartitionOffset {
  partition: number
  /** The committed next offset as a decimal string. Absent when never committed. */
  offset?: string
  error?: KafkaCrabError
}

export interface AdminTopic {
  topic: string
  numPartitions: number
  replicationFactor: number
  config?: Record<string, string>
}

export interface AdminTopicMetadata {
  topic: string
  partitions: Array<AdminPartitionMetadata>
  error?: KafkaCrabError
}

export interface AdminTopicOffset {
  partition: number
  /** Inclusive low watermark, encoded as a decimal string without precision loss. */
  low?: string
  /** Exclusive high watermark, encoded as a decimal string without precision loss. */
  high?: string
  error?: KafkaCrabError
}

export interface AdminTopicResult {
  topic: string
  /** False when the topic already existed or creation failed. */
  created: boolean
  error?: KafkaCrabError
}

export type CommitMode = 'Sync' | 'Async'

export interface CompactMessageBatch {
  payloads: Array<Buffer>
  keys?: Array<Buffer | undefined>
  sharedKey?: Buffer
  keyDictionary?: Array<Buffer>
  keyDictionaryIndexes?: Array<number>
  topic?: string
  topics?: Array<string>
  partitions: Array<number>
  offsets: Array<number>
  sharedHeaderKey?: string
  sharedHeaderValue?: Buffer
  sharedHeaderValues?: Array<Buffer | undefined>
  headers?: Array<Record<string, Buffer> | undefined>
  tombstones?: Array<boolean>
}

export interface ConsumerConfiguration {
  groupId: string
  enableAutoCommit?: boolean
  configuration?: Record<string, any>
  /**
   * Metadata timeout in ms (default 2000, clamped to 1–300000). Zero is not
   * fail-fast: it falls back to the default. Negative values are rejected.
   */
  fetchMetadataTimeout?: number
  /**
   * Experimental (RFC-0017). Enables managed consumption for this consumer:
   * `recvManaged`/`commitManaged` replace the legacy receive and commit
   * methods. Fixed at creation; requires auto commit and auto offset store
   * to be disabled (they default to `false` in this mode).
   */
  managed?: boolean
  /** Experimental (RFC-0017). Bounds for the managed native state. */
  managedOptions?: ManagedConsumptionOptions
}

export interface CreateTopicsOptions {
  topics: Array<AdminTopic>
  /** Wait for leaders of successful topics. Defaults to true. */
  waitForLeaders?: boolean
}

export interface FetchOffsetsOptions {
  groupId: string
  topics: Array<string>
}

export interface KafkaConfiguration {
  brokers: string
  clientId: string
  securityProtocol?: SecurityProtocol
  configuration?: Record<string, any>
  logLevel?: string
  brokerAddressFamily?: string
}

export interface KafkaCrabError {
  code: number
  message: string
}

export interface KafkaEvent {
  name: KafkaEventName
  payload: KafkaEventPayload
}

export type KafkaEventName = 'PreRebalance' | 'PostRebalance' | 'CommitCallback'

export interface KafkaEventPayload {
  action?: string
  tpl: Array<TopicPartition>
  error?: string
}

/**
 * Version of the managed consumption contract (RFC-0017) implemented by this
 * binding: ordered `recvManaged`, assignment-bound `commitManaged` and
 * structured `ERR_MANAGED_*` errors. Bumped on any incompatible change.
 * Experimental.
 */
export declare const MANAGED_CONSUMPTION_VERSION: number

/**
 * Experimental (RFC-0017). Limits of the binding-owned managed state. They do
 * not bound librdkafka's own fetch queues or messages already handed to
 * JavaScript.
 */
export interface ManagedConsumptionOptions {
  /**
   * Assignment transitions that may wait for `recvManaged` (default 1024,
   * 1-65536). Exceeding it is a terminal `ERR_MANAGED_CONTROL_OVERFLOW`.
   */
  maxControlRecords?: number
  /**
   * Messages retained natively before assigned partitions are paused
   * (default 1024, 1-1000000). Fetching resumes at half of the limit.
   */
  maxBufferedMessages?: number
  /**
   * Bytes retained natively before assigned partitions are paused
   * (default 32 MiB). One message is always admitted.
   */
  maxBufferedBytes?: number
}

/** One partition of a managed assignment transition. */
export interface ManagedPartition {
  topic: string
  partition: number
  /** Identity that owns the partition after the transition. Absent on revoke. */
  assignment?: string
  /**
   * Identity that owned it before: the invalidated one on revoke, or the same
   * value as `assignment` when an assign names a retained partition.
   */
  previousAssignment?: string
}

/** Record returned by `recvManaged`, in the order defined natively. */
export type ManagedRecord =
  | { kind: 'message'; message: Message; assignment: string }
  | { kind: 'assign'; partitions: Array<ManagedPartition> }
  | { kind: 'revoke'; partitions: Array<ManagedPartition>; lost: boolean }
  | { kind: 'idle'; reason: string }

/** Diagnostic snapshot of the managed native state. */
export interface ManagedStateSnapshot {
  /** `active`, `closed` or `failed`. */
  status: string
  /** Error code of the terminal failure when `status` is `failed`. */
  failure?: string
  assignments: Array<ManagedPartition>
  queuedControlRecords: number
  bufferedMessages: number
  bufferedBytes: number
  backpressurePaused: boolean
  pendingCommits: number
  droppedReceiveErrors: number
  /** Undelivered messages released because their identity was revoked. */
  purgedMessages: number
}

export interface Message {
  payload: Buffer
  key?: Buffer
  headers?: Record<string, Buffer>
  topic: string
  partition: number
  offset: number
  isTombstone?: boolean
}

/**
 * A single message to produce. A tombstone is `{ payload: None }` (with or
 * without `is_tombstone: true`); combining `is_tombstone: true` with a
 * payload is rejected with `InvalidArg` instead of silently dropping bytes.
 */
export interface MessageProducer {
  /** Explicit target partition. Omit to use the configured partitioner. */
  partition?: number
  payload?: Buffer
  key?: Buffer
  headers?: Record<string, Buffer>
  isTombstone?: boolean
}

export interface OffsetModel {
  offset?: number
  position?: PartitionPosition
}

export interface PartitionOffset {
  partition: number
  offset: OffsetModel
}

export type PartitionPosition = 'Beginning' | 'End' | 'Stored' | 'Invalid'

export interface ProducerConfiguration {
  queueTimeout?: number
  autoFlush?: boolean
  configuration?: Record<string, any>
}

export interface ProducerRecord {
  topic: string
  messages: Array<MessageProducer>
}

export interface RecordMetadata {
  topic: string
  partition: number
  offset: number
  error?: KafkaCrabError
}

export type SecurityProtocol = 'Plaintext' | 'Ssl' | 'SaslPlaintext' | 'SaslSsl'

export interface TopicPartition {
  topic: string
  partitionOffset: Array<PartitionOffset>
}

export interface TopicPartitionConfig {
  topic: string
  allOffsets?: OffsetModel
  partitionOffset?: Array<PartitionOffset>
  createTopic?: boolean
  numPartitions?: number
  replicas?: number
}

/** Partition identity without an offset, used by pause/resume controls. */
export interface TopicPartitionId {
  topic: string
  partition: number
}
