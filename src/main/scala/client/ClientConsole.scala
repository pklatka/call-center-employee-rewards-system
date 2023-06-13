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

    val inputHandler = InputHandler()

    // Prompting for employee data
    println("Enter your ID:")
    val id = inputHandler.idInput(InputIDType.ID)
    println("Enter your first name:")
    val firstName = inputHandler.nameInput(InputNameType.FIRST_NAME)
    println("Enter your last name:")
    val lastName = inputHandler.nameInput(InputNameType.LAST_NAME)

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
          val clientFirstName = inputHandler.nameInput(InputNameType.CLIENT_FIRST_NAME)
          println("Enter clients last name:")
          val clientLastName = inputHandler.nameInput(InputNameType.CLIENT_LAST_NAME)
          println("Enter clients e-mail address:")
          val clientEmail = inputHandler.emailInput(InputEmailType.EMAIL)
          println("Enter order description:")
          val orderDescription = scala.io.StdIn.readLine()
          println("Enter order value:")
          val orderValue = inputHandler.valueInput(InputValueType.ORDER_VALUE)

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


}