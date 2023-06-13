package settings

/**
 * StreamingSettings contains all the settings for the streaming application.
 */
object StreamingSettings {
  val SPARK_MASTER: String = "local[*]"
  val KAFKA_SERVER: String = "localhost:9092"
  val KAFKA_TOPIC: String = "ccers"
}
