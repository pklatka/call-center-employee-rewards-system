package client

import analyser.StreamingEntry
import org.apache.kafka.clients.producer._
import settings.StreamingSettings

import java.util.Properties

object ClientConsole {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", StreamingSettings.KAFKA_SERVER)
    props.put("client.id", "KafkaProducer")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    val entry = StreamingEntry("1", "John", "Lenon", 1, 5000)

    // To add one entry (thus update the state) to the stream, run:
    // val entry = analyser.ProducerEntry("id, ex: 70921", "John", "Lenon", 1,  1000 for example)

    val record = new ProducerRecord[String, String](StreamingSettings.KAFKA_TOPIC, entry.toString)
    producer.send(record)

    producer.close()
  }

}
