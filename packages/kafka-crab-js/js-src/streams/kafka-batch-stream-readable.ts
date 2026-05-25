import type { Message } from '../../js-binding.js'
import { BaseKafkaStreamReadable, type KafkaStreamReadableOptions } from './base-kafka-stream-readable.js'

// Constants for batch configuration
const DEFAULT_BATCH_TIMEOUT = 1000

export interface KafkaBatchStreamReadableOptions extends KafkaStreamReadableOptions {
  batchSize: number
  batchTimeout?: number
  sourceStream?: ReadableStream<Message[]>
}

/**
 * KafkaBatchStreamReadable class for batch message processing
 * @extends BaseKafkaStreamReadable
 */
export class KafkaBatchStreamReadable extends BaseKafkaStreamReadable {
  private pendingMessages: Message[] = []
  private pendingMessageIndex = 0
  private readonly batchSize: number
  private readonly batchTimeout: number
  private readonly sourceReader?: ReadableStreamDefaultReader<Message[]>
  private readInFlight = false
  private pendingRead = false

  /**
   * Creates a KafkaBatchStreamReadable instance
   */
  public constructor(streamOptions: KafkaBatchStreamReadableOptions) {
    const { batchSize, batchTimeout = DEFAULT_BATCH_TIMEOUT, sourceStream, kafkaConsumer, ...opts } = streamOptions

    // Set highWaterMark to batch size for optimal performance
    opts.highWaterMark = Math.max(batchSize, opts.highWaterMark || 16)

    super({ kafkaConsumer, ...opts })
    this.batchSize = batchSize
    this.batchTimeout = batchTimeout
    this.sourceReader = sourceStream?.getReader()
  }

  /**
   * Gets current batch configuration
   * @returns {object} Current batch settings
   */
  public getBatchConfig(): { batchSize: number; batchTimeout: number } {
    return {
      batchSize: this.batchSize,
      batchTimeout: this.batchTimeout,
    }
  }

  private drainPendingMessages(): boolean {
    while (this.pendingMessageIndex < this.pendingMessages.length) {
      const message = this.pendingMessages[this.pendingMessageIndex]
      this.pendingMessageIndex += 1
      if (!this.push(message)) {
        return false
      }
    }

    this.pendingMessages = []
    this.pendingMessageIndex = 0
    return true
  }

  private async readNextBatch(): Promise<Message[] | null> {
    if (this.sourceReader) {
      const chunk = await this.sourceReader.read()
      if (chunk.done) {
        return null
      }

      return chunk.value ?? []
    }

    return this.kafkaConsumer.recvBatch(this.batchSize, this.batchTimeout)
  }

  private async pullNextBatch() {
    try {
      if (!this.drainPendingMessages()) {
        return
      }

      const messages = await this.readNextBatch()
      if (!messages) {
        this.push(null)
        return
      }

      if (messages.length === 0) {
        // No data this poll; schedule another read instead of ending the stream
        if (!this.destroyed) {
          setImmediate(() => this._read())
        }
        return
      }

      this.pendingMessages = messages
      this.pendingMessageIndex = 0
      this.drainPendingMessages()
    } catch (error) {
      if (this.destroyed) {
        return
      }

      // Use destroy() instead of emit('error') to properly terminate the stream
      this.destroy(error instanceof Error ? error : new Error(String(error)))
    } finally {
      this.readInFlight = false

      if (this.pendingRead && !this.destroyed) {
        this.pendingRead = false
        this._read()
      }
    }
  }

  protected async cancelSourceReader(reason: unknown): Promise<void> {
    if (!this.sourceReader) {
      return
    }

    await this.sourceReader.cancel(reason)
  }

  /**
   * Internal method called by the Readable stream to fetch batch messages
   * @private
   */
  public _read() {
    if (this.destroyed) {
      return
    }

    if (this.readInFlight) {
      this.pendingRead = true
      return
    }

    this.readInFlight = true
    this.pullNextBatch().catch(() => undefined)
  }
}
