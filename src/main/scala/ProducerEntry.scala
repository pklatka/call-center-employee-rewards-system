case class ProducerEntry(id: String = "abide",
                         name: String = "John",
                         surname: String = "Lennon",
                         numberOfCalls: Int = 0,
                         totalSales: Int = 0) {
  override def toString: String = {
    this.getClass.getDeclaredFields.map(_.get(this)).mkString(",")
  }

  def toTuple: (String, String, String, Int, Int) = {
    (name, surname, id, numberOfCalls, totalSales)
  }
}

object ProducerEntry {
  def getColumns: List[String] = {
    var columns = List[String]()
    var entry = ProducerEntry()

    entry.getClass.getDeclaredFields foreach { f =>
      f.setAccessible(true)
      columns = columns :+ f.getName.capitalize
    }

    columns
  }

  def getGroupByColumns: List[String] = {
    var columns = List[String]()
    var entry = ProducerEntry()

    entry.getClass.getDeclaredFields foreach { f =>
      f.setAccessible(true)
      if (f.getName != "numberOfCalls" && f.getName != "totalSales") {
        columns = columns :+ f.getName.capitalize
      }
    }

    columns
  }

  def parseFromString(str: String): ProducerEntry = {
    val fields = str.split(",")
    ProducerEntry(fields(0), fields(1), fields(2), fields(3).toInt, fields(4).toInt)
  }

  def main(args: Array[String]): Unit = {
    var x = ProducerEntry.getColumns

    for (i <- x) {
      println(i)
    }
  }
}