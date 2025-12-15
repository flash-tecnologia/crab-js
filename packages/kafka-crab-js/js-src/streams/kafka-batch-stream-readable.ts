import type { Message } from '../../js-binding.js'
import { BaseKafkaStreamReadable, type KafkaStreamReadableOptions } from './base-kafka-stream-readable.js'

// Constants for batch configuration
const DEFAULT_BATCH_TIMEOUT = 1000

export interface KafkaBatchStreamReadableOptions extends KafkaStreamReadableOptions {
  batchSize: number
  batchTimeout?: number
}

/**
 * KafkaBatchStreamReadable class for batch message processing
 * @extends BaseKafkaStreamReadable
 */
export class KafkaBatchStreamReadable extends BaseKafkaStreamReadable {
  private pendingMessages: Message[] = []
  private readonly batchSize: number
  private readonly batchTimeout: number

  /**
   * Creates a KafkaBatchStreamReadable instance
   */
  constructor(
    streamOptions: KafkaBatchStreamReadableOptions,
  ) {
    const { batchSize, batchTimeout = DEFAULT_BATCH_TIMEOUT, ...opts } = streamOptions

    // Set highWaterMark to batch size for optimal performance
    opts.highWaterMark = Math.max(batchSize, opts.highWaterMark || 16)

    super({ ...streamOptions, ...opts })
    this.batchSize = batchSize
    this.batchTimeout = batchTimeout
  }

  /**
   * Gets current batch configuration
   * @returns {object} Current batch settings
   */
  getBatchConfig(): { batchSize: number; batchTimeout: number } {
    return {
      batchSize: this.batchSize,
      batchTimeout: this.batchTimeout,
    }
  }

  /**
   * Internal method called by the Readable stream to fetch batch messages
   * @private
   */
  async _read() {
    if (this.destroyed) {
      return
    }
    try {
      // First, push any pending messages
      while (this.pendingMessages.length > 0) {
        const message = this.pendingMessages.shift()
        if (!this.push(message)) {
          return // Backpressure, wait for next _read()
        }
      }

      // Fetch new batch if no pending messages
      const messages = await this.kafkaConsumer.recvBatch(this.batchSize, this.batchTimeout)

      if (messages.length === 0) {
        // No data this poll; schedule another read instead of ending the stream
        if (!this.destroyed) {
          setImmediate(() => this._read())
        }
        return
      }

      // Try to push messages, buffer remainder
      for (let idx = 0; idx < messages.length; idx++) {
        if (!this.push(messages[idx])) {
          // Buffer remaining messages
          this.pendingMessages = messages.slice(idx + 1)
          break
        }
      }
    } catch (error) {
      // Use destroy() instead of emit('error') to properly terminate the stream
      this.destroy(error instanceof Error ? error : new Error(String(error)))
    }
  }
}
