package streaming

/*
* StreamingEntry is a case class that represents the data that is being processed.
*/
case class StreamingEntry(id: String = "abide",
                         name: String = "John",
                         surname: String = "Lennon",
                         numberOfCalls: Int = 0,
                         totalSales: Int = 0) {
  override def toString: String = {
    this.getClass.getDeclaredFields.map(_.get(this)).mkString(",")
  }

  def toTuple: (String, String, String, Int, Int) = {
    (id, name, surname, numberOfCalls, totalSales)
  }
}

object StreamingEntry {
  def getColumns: List[String] = {
    var columns = List[String]()

    StreamingEntry().getClass.getDeclaredFields foreach { f =>
      f.setAccessible(true)
      columns = columns :+ f.getName.capitalize
    }

    columns
  }

  def getGroupByColumns: List[String] = {
    var columns = List[String]()

    StreamingEntry().getClass.getDeclaredFields foreach { f =>
      f.setAccessible(true)
      if (f.getName != "numberOfCalls" && f.getName != "totalSales") {
        columns = columns :+ f.getName.capitalize
      }
    }

    columns
  }

  def parseFromString(str: String): StreamingEntry = {
    val fields = str.split(",")
    StreamingEntry(fields(0), fields(1), fields(2), fields(3).toInt, fields(4).toInt)
  }
}