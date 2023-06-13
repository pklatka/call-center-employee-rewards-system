package streaming

import scala.util.Try
import scala.util.Success

/*
* StreamingEntry is a case class that represents the data that is being processed.
*/
case class StreamingEntry(id: String = "ABCDE",
                         name: String = "John",
                         surname: String = "Lennon",
                         numberOfCalls: Int = 1,
                         totalSales: Double = 0.0) {
  override def toString: String = {
    this.getClass.getDeclaredFields.map(_.get(this)).mkString(",")
  }

  def toTuple: (String, String, String, Int, Double) = {
    (id, name, surname, numberOfCalls, totalSales)
  }
}

object StreamingEntry {

  private val objectInstance = StreamingEntry()

  def getColumns: List[String] = {
    var columns = List[String]()

    objectInstance.getClass.getDeclaredFields foreach { f =>
      f.setAccessible(true)
      columns = columns :+ f.getName.capitalize
    }

    columns
  }

  def getGroupByColumns: List[String] = {
    var columns = List[String]()

    objectInstance.getClass.getDeclaredFields foreach { f =>
      f.setAccessible(true)
      if (f.getName != "numberOfCalls" && f.getName != "totalSales") {
        columns = columns :+ f.getName.capitalize
      }
    }

    columns
  }

  def parseFromString(str: String): StreamingEntry = {
    val fields = str.split(",")

    if(fields.length != objectInstance.getClass.getDeclaredFields.length) {
      throw new Exception("Invalid number of fields")
    }

    Try(fields(3).toInt) match {
      case Success(value) => {}
      case _ => throw new Exception("Invalid number of calls value")
    }

    Try(fields(4).toDouble) match {
      case Success(value) => {}
      case _ => throw new Exception("Invalid total sales value")
    }

    StreamingEntry(fields(0), fields(1), fields(2), fields(3).toInt, fields(4).toDouble)
  }
}