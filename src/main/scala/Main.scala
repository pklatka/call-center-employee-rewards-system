import org.apache.spark.sql.streaming.StreamingQuery

object Main {
  def main(args: Array[String]): Unit = {
    // Simple REPL

    println("Type 'start' to start the streaming query")

    var query: StreamingQuery = null

    while (true){
      val input = scala.io.StdIn.readLine()
      if (input == "exit"){
        System.exit(0)
      }
      else if (input == "start"){
        val dsw = StreamingHandler.init()
        query = dsw.start()

        // Thread
        val thread = new Thread(() => {
          StreamingHandler.run(query)
        })

        thread.start()
      }
      else if (input == "stop"){
        query.stop()
      }
      else{
        println("Type 'start' to start the streaming query")
      }
    }
  }
}