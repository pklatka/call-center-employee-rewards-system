import analyser.StructuredStreamingEngine
import org.apache.spark.sql.streaming.StreamingQuery

/**
 * Main class - entry point of the application
 * Runs the structured streaming and handles user input to stop the streaming query
 */
object Main {
  def main(args: Array[String]): Unit = {
    var query: Option[StreamingQuery] = null
    var streamingThread: Thread = null

    val loopThread = new Thread(() => {
      while (true) {
        streamingThread = new Thread(() => {
          val dsw = StructuredStreamingEngine.init()
          query = Some(dsw.start())
          println("======== Streaming query started ========")
          println("Type 'stop' to stop the program")

          if (query.isEmpty) {
            throw new Exception("Streaming query not initialized")
          }

          StructuredStreamingEngine.run(query.get)
        })

        streamingThread.start()

        // Calculate remaining seconds to the end of a day
        val secondsToMidnight = 24 * 60 * 60 - java.time.LocalTime.now().toSecondOfDay

        // Sleep until the end of the day
        Thread.sleep(secondsToMidnight * 1000)

        // Stop the query
        if (query.isDefined) {
          query.get.stop()
        }

        streamingThread.join()
        Thread.sleep(10000)
      }
    })

    loopThread.start()

    // Loop for stopping the query manually
    while (true) {
      val input = scala.io.StdIn.readLine().toLowerCase().trim()

      if (input == "stop" && query.isDefined) {
        query.get.stop()
        streamingThread.join()
        System.exit(0)
      }

    }
  }
}