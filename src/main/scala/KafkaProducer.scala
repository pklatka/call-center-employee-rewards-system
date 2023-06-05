import java.util.Properties
import org.apache.kafka.clients.producer._

object KafkaProducer {
  def main(args: Array[String]): Unit = {
      val props = new Properties()
      props.put("bootstrap.servers", "localhost:9092")
      props.put("client.id", "KafkaProducer")
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

      val producer = new KafkaProducer[String, String](props)

      val topic = "ccers"

      for (i <- 1 to 50) {
        val record = new ProducerRecord(topic, "key", s"hello $i")
        producer.send(record)
      }

      producer.close()
  }

}
