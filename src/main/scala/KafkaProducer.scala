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

      val entry = ProducerEntry("Joh2n","SDfsdf", "abide", 0, 1)

      val record = new ProducerRecord[String, String](topic, entry.toString)
      producer.send(record)

      producer.close()
  }

}
