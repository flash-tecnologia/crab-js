import type { Message } from '../../js-binding.js'
import { BaseKafkaStreamReadable, type KafkaStreamReadableOptions } from './base-kafka-stream-readable.js'

interface KafkaWebStreamReadableOptions extends KafkaStreamReadableOptions {
  sourceStream?: ReadableStream<Message>
}

/**
 * KafkaStreamReadable class for single message processing
 * @extends BaseKafkaStreamReadable
 */
export class KafkaStreamReadable extends BaseKafkaStreamReadable {
  private readonly sourceReader?: ReadableStreamDefaultReader<Message>
  private readInFlight = false
  private pendingRead = false

  public constructor(streamOptions: KafkaWebStreamReadableOptions) {
    const { sourceStream, ...opts } = streamOptions
    super(opts)
    this.sourceReader = sourceStream?.getReader()
  }

  private async pullNextMessage() {
    try {
      if (this.sourceReader) {
        const chunk = await this.sourceReader.read()
        if (chunk.done) {
          this.push(null)
          return
        }

        if (chunk.value) {
          this.push(chunk.value)
        }
        return
      }

      const message = await this.kafkaConsumer.recv()
      if (message) {
        this.push(message)
      } else {
        this.push(null) // No more data, end of stream
      }
    } catch (error) {
      if (this.destroyed) {
        return
      }

      if (error instanceof Error) {
        this.destroy(error)
      } else {
        this.destroy(new Error(`Unknown error: ${String(error)}`))
      }
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
   * Internal method called by the Readable stream to fetch single messages
   * @private
   */
  public _read() {
    if (this.readInFlight) {
      this.pendingRead = true
      return
    }

    this.readInFlight = true
    this.pullNextMessage().catch(() => undefined)
  }
}
