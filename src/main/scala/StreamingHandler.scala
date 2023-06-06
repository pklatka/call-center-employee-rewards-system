import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQuery}
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}

object StreamingHandler {
  private val totalSalesPercentage = 0.1

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

  def init(): DataStreamWriter[Row] = {
    val dfw = df.writeStream
      .queryName("results") // this query name will be the table name
      .outputMode("complete")
      .format("memory")

    dfw
  }

  def run(query: StreamingQuery): Unit = {
    println("Streaming query started")
    query.awaitTermination()

    // Get the results
    val results = spark.sql("select * from results")

    // Calculate base bonus (round to 2 decimal places)
    var resultsWithBonus = results.withColumn("BaseBonus", round($"TotalSales" * totalSalesPercentage, 2))

    // Enumerate rows
    resultsWithBonus = resultsWithBonus.withColumn("row_number", row_number().over(Window.orderBy($"TotalSales".desc)))

    // Calculate bonus for top 5 employees and update the results for this 5 employees only
    var bonusPool = List(0.55, 0.25, 0.1, 0.07, 0.03)
    val prizePool = 1000

    bonusPool = bonusPool.map(x => x * prizePool)

    val numRowsToUpdate = bonusPool.size

    // UDF (User-Defined Function) to retrieve array elements based on row numbers
    val getBonus = udf((rowNum: Int) => bonusPool(rowNum - 1))

    resultsWithBonus = resultsWithBonus.withColumn("BaseBonus",
      when(col("row_number") <= numRowsToUpdate,
        round(col("BaseBonus") + getBonus(col("row_number")), 2)
      )
        .otherwise(col("BaseBonus"))
    )

    // Remove row_number column
    resultsWithBonus = resultsWithBonus.drop("row_number")

    // Write results to console
    resultsWithBonus.show()

    // Write results to existing csv file
    val csvFileName = s"results-${java.time.LocalDate.now.toString}.csv"

    resultsWithBonus.coalesce(1)
      .write
      .mode("append")
      .option("header", "true")
      .csv(s"src/main/output/$csvFileName")

    // Stop spark session
    spark.stop()
  }
}
