import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{ForeachWriter, SparkSession}

object StreamingHandler {

    // Create spark session
    private val spark = SparkSession
      .builder
      .appName("Spark Structured Streaming from Kafka")
      .master("local[*]")
      .getOrCreate()

    // Turn off logs
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    private val stream = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "ccers")
      .load()

    private val df = stream.selectExpr("CAST(value AS STRING)")
      .as[(String)]
      .map(x => {
        ProducerEntry.parseFromString(x).toTuple
      })
      .toDF(ProducerEntry.getColumns: _*)
      .groupBy(ProducerEntry.getGroupByColumns.map(col): _*)
      .agg(sum("NumberOfCalls"), sum("TotalSales"))
      .orderBy(desc("sum(TotalSales)"), desc("sum(NumberOfCalls)"))
      .withColumnRenamed("sum(NumberOfCalls)", "NumberOfCalls")
      .withColumnRenamed("sum(TotalSales)", "TotalSales")

  def init() = {
    val dfw = df.writeStream
      .queryName("aggregates") // this query name will be the table name
      .outputMode("complete")
      .format("memory")

    dfw
  }

  def run(query: StreamingQuery): Unit = {
    println("Streaming query started")
    query.awaitTermination()

    // Here return result of the best seller

    /*
    * 1. Get old ranking from csv file
    * 2. Update ranking
    * 3. Calculate bonus for each seller (considering previous statistics)
    * 4. Write result to separate csv file
    * */

    // Get the first 10 rows
    val results = spark.sql("select * from aggregates")
    results.show(10)

    // Write results to existing csv file
    results.coalesce(1)
      .write
      .mode("append")
      .option("header", "true")
      .csv("src/main/output/result.csv")

    // Stop spark session
    spark.stop()
  }
}
