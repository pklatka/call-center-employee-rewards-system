package settings

/**
 * StreamingSettings contains all the settings for the streaming application.
 */
object StreamingSettings {
  val SPARK_MASTER = "local[*]"
  val KAFKA_SERVER = "localhost:9092"
  val KAFKA_TOPIC = "ccers"
}
