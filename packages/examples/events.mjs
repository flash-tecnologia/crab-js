import { KafkaClient } from 'kafka-crab-js'

const TOPIC = 'foo'

const kafkaClient = new KafkaClient({
  brokers: 'localhost:9092',
  clientId: 'kakfa-crab-js',
  logLevel: 'info',
  brokerAddressFamily: 'v4',
})

const consumer = kafkaClient.createConsumer({
  groupId: 'my-group-id',
})

// If you want to consume events, you need call disconnect() to stop the consumer and release resources
consumer.onEvents((_err, event) => {
  switch (event.name) {
    case 'CommitCallback': {
      const offsetCommitted = event.payload.tpl.filter(topicPartition => topicPartition.partitionOffset.find(partitionOffset => partitionOffset.offset.offset)) // Filter only committed offsets
        .flatMap(p =>
          p.partitionOffset.map(it => ({ topic: p.topic, partition: it.partition, offset: it.offset.offset }))
        )
      console.log(
        'Offset committed:',
        offsetCommitted,
      )
      return
    }
    default: {
      console.log(
        'Relalance:',
        event.name,
        event.payload.tpl
          .map(it =>
            `Topic: ${it.topic},
                    ${
              it.partitionOffset.map(po => `partition: ${po.partition}`)
                .join(',')
            }`
          ),
      )
    }
  }
})

await consumer.subscribe([{
  topic: TOPIC,
  createTopic: true,
}])

const printMessage = async () => {
  let disconnect = false
  while (!disconnect) {
    const msg = await consumer.recv()
    if (msg) {
      console.log('Message receive', msg.payload.toString())
    } else {
      console.log('The consumer has been disconnected')
      disconnect = true
    }
  }
}

process.on('SIGINT', () => {
  consumer.disconnect()
})

await printMessage()
