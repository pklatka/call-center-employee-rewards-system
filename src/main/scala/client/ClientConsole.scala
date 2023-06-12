package client

import org.apache.kafka.clients.producer._
import settings.StreamingSettings
import streaming.StreamingEntry

import java.util.Properties
import scala.annotation.tailrec
import scala.util.Try

object ClientConsole {
  private def help(): Unit = {
    println("Available commands:")
    println("entry - prompts for client data and sends it to server")
    println("help - prints this help")
    println("exit - exits the program")
  }

  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", StreamingSettings.KAFKA_SERVER)
    props.put("client.id", "KafkaProducer")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    // Prompting for employee data
    println("Enter your ID:")
    val id = validateID("ID")
    println("Enter your first name:")
    val firstName = validateName("first name")
    println("Enter your last name:")
    val lastName = validateName("last name")

    help()

    while (true) {
      print("Enter command: ")
      scala.io.StdIn.readLine().toLowerCase match {
        case "help" => help()
        case "exit" => producer.close(); System.exit(0)
        case "entry" => {
          // Enter client data
          // It may be useful when there is a need to persist the order data
          println("Enter clients first name:")
          val clientFirstName = validateName("client first name")
          println("Enter clients last name:")
          val clientLastName = validateName("client last name")
          println("Enter clients e-mail address:")
          val clientEmail = validateEmail("client email")
          println("Enter order description:")
          val orderDescription = scala.io.StdIn.readLine()
          println("Enter order value:")
          val orderValue = validateValue("order value")
          println(orderValue)

          // Assign order value to given employee
          val entry = StreamingEntry(id, firstName, lastName, 1, orderValue)
          val record = new ProducerRecord[String, String](StreamingSettings.KAFKA_TOPIC, entry.toString)

          producer.send(record, (metadata: RecordMetadata, exception: Exception) => {
            if (exception != null) {
              println("Error while producing data to Kafka")
              exception.printStackTrace()
            }
          })

          println("Entry sent to server.")
        }
        case _ => println("Unknown command. Type 'help' to see available commands.")
      }
    }
  }

  @tailrec
  private def validateValue(fieldName: String): Double = {
    val userInput = scala.io.StdIn.readLine().trim
    val value = Try(userInput.toDouble)

    value match {
      case scala.util.Success(value) if value > 0 =>
        BigDecimal(value).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
      case _ =>
        println(s"Invalid $fieldName. Please enter a non-negative value.")
        validateValue(fieldName)
    }
  }

  @tailrec
  private def validateName(fieldName: String): String = {
    val userInput = scala.io.StdIn.readLine()
    val name = userInput.trim().toLowerCase().capitalize.replaceAll("""[\p{Punct}]""", "")
    val nameRegex = """^[A-Z][a-z]+$""".r
    name match {
      case nameRegex() => name
      case _ =>
        println(s"Invalid $fieldName. Please enter a valid.")
        // Prompt again until a valid input is provided
        validateName(fieldName)
    }
  }

  @tailrec
  private def validateID(fieldName: String): String = {
    val userInput = scala.io.StdIn.readLine()
    val id = userInput.trim().toUpperCase().replaceAll("""[\p{Punct}]""", "")
    val IDRegex = """^[A-Z]+$""".r
    id match {
      case IDRegex() => id
      case _ =>
        println(s"Invalid $fieldName. Please enter a valid.")
        // Prompt again until a valid input is provided
        validateID(fieldName)
    }
  }

  @tailrec
  private def validateEmail(fieldName: String): String = {
    val email = scala.io.StdIn.readLine()
    val emailRegex = """^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$""".r
    email match {
      case emailRegex() => email
      case _ =>
        println(s"Invalid $fieldName. Please enter a valid email address.")
        // Prompt again until a valid input is provided
        validateEmail(fieldName)
    }
  }
}