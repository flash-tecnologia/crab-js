import { Readable, type ReadableOptions } from 'node:stream'

import type { CommitMode, KafkaConsumer, Message, OffsetModel, TopicPartitionConfig } from '../../js-binding.js'

export interface KafkaStreamReadableOptions extends ReadableOptions {
  kafkaConsumer: KafkaConsumer
}

/**
 * Abstract base class for Kafka stream readers
 * @extends Readable
 */
export abstract class BaseKafkaStreamReadable extends Readable {
  private readonly _kafkaConsumer: KafkaConsumer

  /**
   * Creates a BaseKafkaStreamReadable instance
   */
  public constructor(streamOptions: KafkaStreamReadableOptions) {
    const { kafkaConsumer, ...opts } = streamOptions

    super(opts)

    if (!kafkaConsumer) {
      throw new Error('A valid KafkaConsumer instance is required.')
    }

    this._kafkaConsumer = kafkaConsumer
  }

  public get kafkaConsumer(): KafkaConsumer {
    return this._kafkaConsumer
  }

  /**
   * Checks if the stream is currently paused
   * @returns {boolean} True if the stream is paused
   */
  public isPaused(): boolean {
    return this.readableFlowing === false
  }

  /**
   * Subscribes to topics
   */
  public async subscribe(topics: string | TopicPartitionConfig[]) {
    if (!topics || (Array.isArray(topics) && topics.length === 0)) {
      throw new Error('Topics must be a non-empty string or array.')
    }
    await this.kafkaConsumer.subscribe(topics)
  }

  public seek(topic: string, partition: number, offsetModel: OffsetModel, timeout?: number) {
    this.kafkaConsumer.seek(topic, partition, offsetModel, timeout)
  }

  public async commit(topic: string, partition: number, offset: number, commitMode: CommitMode) {
    return this.kafkaConsumer.commit(topic, partition, offset, commitMode)
  }

  /**
   * Commits the offset for a message.
   * This is a convenience method that automatically increments the offset by 1.
   * The offset committed is `message.offset + 1` since Kafka expects the next offset to be consumed.
   * @param message - The message to commit
   * @param commitMode - The commit mode ('Sync' or 'Async')
   */
  public async commitMessage(message: Message, commitMode: CommitMode) {
    return this.kafkaConsumer.commitMessage(message, commitMode)
  }

  /**
   * Unsubscribe from topics
   */
  public unsubscribe() {
    this.kafkaConsumer.unsubscribe()
  }

  /**
   * Disconnects the Kafka consumer
   */
  public async disconnect() {
    await this.kafkaConsumer.disconnect()
  }

  /**
   * Returns the raw Kafka consumer
   * @returns {KafkaConsumer} The Kafka consumer instance
   */
  public rawConsumer() {
    return this.kafkaConsumer
  }

  /**
   * Hook for subclasses to cancel external stream readers before consumer teardown.
   */
  protected async cancelSourceReader(_reason: unknown): Promise<void> {
    if (!this._kafkaConsumer) {
      return Promise.resolve()
    }

    return Promise.resolve()
  }

  /**
   * Called when the stream is being destroyed
   * Ensures proper cleanup of the Kafka consumer
   * @private
   */
  public _destroy(error: Error | null, callback: (error?: Error | null) => void): void {
    const finalizeDestroy = (sourceCancelError?: Error) => {
      // Always unsubscribe first to stop receiving messages
      try {
        this.kafkaConsumer.unsubscribe()
      } catch {
        // Silently ignore unsubscribe errors during cleanup - they're non-critical
        // Common when consumer is already disconnected or in invalid state
      }

      const baseError = error ?? sourceCancelError ?? null

      // Disconnect the consumer
      this.kafkaConsumer
        .disconnect()
        .then(() => {
          // Success: pass through the original error (if any)
          callback(baseError)
        })
        // eslint-disable-next-line unicorn/catch-error-name
        .catch((disconnectError) => {
          // Failed to disconnect: combine errors
          if (baseError) {
            callback(new Error(`Stream error: ${baseError.message}; Disconnect error: ${disconnectError.message}`))
            return
          }

          callback(disconnectError)
        })
    }

    this.cancelSourceReader(error)
      .then(() => {
        finalizeDestroy()
      })
      // eslint-disable-next-line unicorn/catch-error-name
      .catch((sourceCancelError) => {
        const normalizedError =
          sourceCancelError instanceof Error ? sourceCancelError : new Error(String(sourceCancelError))
        finalizeDestroy(normalizedError)
      })
  }

  /**
   * Abstract method that must be implemented by concrete classes
   * @private
   */
  public abstract _read(): void
}
