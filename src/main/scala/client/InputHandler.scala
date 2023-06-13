package client

import scala.annotation.tailrec
import scala.util.Try

object InputValueType extends Enumeration {
  type InputValueType = Value
  val ORDER_VALUE = Value("order value")
}

object InputNameType extends Enumeration {
  type InputNameType = Value
  val FIRST_NAME = Value("first name")
  val LAST_NAME = Value("last name")
  val CLIENT_FIRST_NAME = Value("client first name")
  val CLIENT_LAST_NAME = Value("client last name")
}

object InputIDType extends Enumeration {
  type InputIDType = Value
  val ID = Value("ID")
}

object InputEmailType extends Enumeration {
  type InputEmailType = Value
  val EMAIL = Value("email")
}

case class InputHandler() {

  @tailrec
  final def valueInput(fieldType: InputValueType.InputValueType): Double = {
    val userInput = scala.io.StdIn.readLine().trim
    val value = Try(userInput.toDouble)

    value match {
      case scala.util.Success(value) if value > 0 =>
        BigDecimal(value).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
      case _ =>
        println(s"Invalid $fieldType. Please enter a non-negative value.")
        valueInput(fieldType)
    }
  }

  @tailrec
  final def nameInput(fieldType: InputNameType.InputNameType): String = {
    val userInput = scala.io.StdIn.readLine()
    val name = userInput.trim().toLowerCase().capitalize.replaceAll("""\p{Punct}""", "")
    val nameRegex = """^[A-Z][a-z]+$""".r
    name match {
      case nameRegex() => name
      case _ =>
        println(s"Invalid $fieldType. Please enter a valid one.")
        // Prompt again until a valid input is provided
        nameInput(fieldType)
    }
  }

  @tailrec
  final def idInput(fieldType: InputIDType.InputIDType): String = {
    val userInput = scala.io.StdIn.readLine()
    val id = userInput.trim().toUpperCase().replaceAll("""\p{Punct}""", "")
    val IDRegex = """^[A-Z]+$""".r
    id match {
      case IDRegex() => id
      case _ =>
        println(s"Invalid $fieldType. Please enter a valid one.")
        // Prompt again until a valid input is provided
        idInput(fieldType)
    }
  }

  @tailrec
  final def emailInput(fieldType: InputEmailType.InputEmailType): String = {
    val email = scala.io.StdIn.readLine()
    val emailRegex = """^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$""".r
    email match {
      case emailRegex() => email
      case _ =>
        println(s"Invalid $fieldType. Please enter a valid email address.")
        // Prompt again until a valid input is provided
        emailInput(fieldType)
    }
  }
}

