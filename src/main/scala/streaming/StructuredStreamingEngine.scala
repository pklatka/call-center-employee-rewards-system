package streaming

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQuery}
import org.apache.spark.sql.{Row, SparkSession}
import settings.{BonusSettings, StreamingSettings}

/**
 * StructuredStreamingEngine contains all the logic for the structured streaming.
 * It is responsible for reading the data from Kafka, processing it and writing the results to memory.
 */
object StructuredStreamingEngine {
  // Create spark session
  private val spark = SparkSession
    .builder
    .appName("Spark Structured Streaming from Kafka")
    .master(StreamingSettings.SPARK_MASTER)
    .getOrCreate()

  // Turn off logs
  spark.sparkContext.setLogLevel("ERROR")

  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._

  private val stream = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", StreamingSettings.KAFKA_SERVER)
    .option("subscribe", StreamingSettings.KAFKA_TOPIC)
    .load()

  private val df = stream.selectExpr("CAST(value AS STRING)")
    .as[(String)]
    .map(x => {
      StreamingEntry.parseFromString(x).toTuple
    })
    .toDF(StreamingEntry.getColumns: _*)
    .groupBy(StreamingEntry.getGroupByColumns.map(col): _*)
    .agg(sum("NumberOfCalls"), sum("TotalSales"))
    .orderBy(desc("sum(TotalSales)"), desc("sum(NumberOfCalls)"))
    .withColumnRenamed("sum(NumberOfCalls)", "NumberOfCalls")
    .withColumnRenamed("sum(TotalSales)", "TotalSales")

  /**
   * Initializes the streaming query.
   * Used to initialize the query before running it.
   * For example it can be used to stop the query when needed.
   *
   * @return DataStreamWriter[Row]
   */
  def init(): DataStreamWriter[Row] = {
    val dfw = df.writeStream
      .queryName("results")
      .outputMode("complete")
      .format("memory")

    dfw
  }

  /**
   * Runs the streaming query.
   *
   * @param query DataStreamWriter[Row]
   */
  def run(query: StreamingQuery): Unit = {
    query.awaitTermination()

    // Get the results
    val results = spark.sql("select * from results")

    // Calculate base bonus (round to 2 decimal places)
    var resultsWithBonus = results.withColumn("BaseBonus", round($"TotalSales" * BonusSettings.TOTAL_SALES_BONUS_PERCENTAGE, 2))

    // Enumerate rows
    resultsWithBonus = resultsWithBonus.withColumn("row_number", row_number().over(Window.orderBy($"TotalSales".desc)))

    resultsWithBonus = resultsWithBonus.withColumn("BaseBonus",
      when(col("row_number") <= BonusSettings.ADDITIONAL_BONUS_POOL_SIZE,
        round(col("BaseBonus") + BonusSettings.getAdditionalBonusUDF(col("row_number")), 2)
      )
        .otherwise(col("BaseBonus"))
    )

    // Remove row_number column
    resultsWithBonus = resultsWithBonus.drop("row_number")

    // Add date column
    resultsWithBonus = resultsWithBonus.withColumn("Date", lit(java.time.LocalDate.now.toString))

    // Write results to console
    resultsWithBonus.show()

    // Write results to existing csv file
    val csvFileName = s"results-${java.time.LocalDate.now.toString}.csv"

    resultsWithBonus.coalesce(1)
      .write
      .mode("append")
      .option("header", "true")
      .csv(s"output/$csvFileName")

    // Stop spark session
    spark.stop()
  }
}
