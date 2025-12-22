import { BaseKafkaStreamReadable } from './base-kafka-stream-readable.js'

/**
 * KafkaStreamReadable class for single message processing
 * @extends BaseKafkaStreamReadable
 */
export class KafkaStreamReadable extends BaseKafkaStreamReadable {
  /**
   * Internal method called by the Readable stream to fetch single messages
   * @private
   */
  async _read() {
    try {
      const message = await this.kafkaConsumer.recv()
      if (message) {
        this.push(message)
      } else {
        this.push(null) // No more data, end of stream
      }
    } catch (error) {
      if (error instanceof Error) {
        this.destroy(error)
      } else {
        this.destroy(new Error(`Unknown error: ${String(error)}`))
      }
    }
  }
}
