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
   * @param topicConfigs - Topic name string or array of TopicPartitionConfig objects
   */
  subscribe(topicConfigs: string | Array<TopicPartitionConfig>): Promise<void>
  /**
   * Pauses message consumption on all assigned partitions.
   * Messages will be buffered by the broker until resume() is called.
   */
  pause(): void
  /** Resumes message consumption on all assigned partitions after a pause. */
  resume(): void
  /**
   * Unsubscribes from all currently subscribed topics.
   * After calling this method, the consumer will no longer receive messages.
   */
  unsubscribe(): void
  /**
   * Disconnects the consumer from the Kafka broker.
   * This will unsubscribe from all topics and stop receiving messages.
   * Any pending recv() or recvBatch() calls will return immediately.
   */
  disconnect(): Promise<void>
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
}

export declare class KafkaProducer {
  /**
   * Returns the number of messages that are currently in-flight (sent but not yet acknowledged).
   * This can be used to implement backpressure or monitor producer health.
   */
  inFlightCount(): number
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

export type CommitMode =  'Sync'|
'Async';

export interface ConsumerConfiguration {
  groupId: string
  enableAutoCommit?: boolean
  configuration?: Record<string, any>
  fetchMetadataTimeout?: number
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

export type KafkaEventName =  'PreRebalance'|
'PostRebalance'|
'CommitCallback';

export interface KafkaEventPayload {
  action?: string
  tpl: Array<TopicPartition>
  error?: string
}

export interface Message {
  payload: Buffer
  key?: Buffer
  headers?: Record<string, Buffer>
  topic: string
  partition: number
  offset: number
}

export interface MessageProducer {
  payload: Buffer
  key?: Buffer
  headers?: Record<string, Buffer>
}

export interface OffsetModel {
  offset?: number
  position?: PartitionPosition
}

export interface PartitionOffset {
  partition: number
  offset: OffsetModel
}

export type PartitionPosition =  'Beginning'|
'End'|
'Stored'|
'Invalid';

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

export type SecurityProtocol =  'Plaintext'|
'Ssl'|
'SaslPlaintext'|
'SaslSsl';

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
