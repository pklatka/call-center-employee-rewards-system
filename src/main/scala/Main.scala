import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {

    // Create spark session
    val spark = SparkSession
      .builder
      .appName("Spark Structured Streaming from Kafka")
      .master("local[*]")
      .getOrCreate()

    // Turn off logs
    spark.sparkContext.setLogLevel("OFF")

    import spark.implicits._

    val lines = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "ccers")
      .load()

    val words = lines.select(explode(split($"value", " ")).as("word"))

    val wordCounts = words.groupBy("word").count()

    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()


    query.awaitTermination()
  }
}