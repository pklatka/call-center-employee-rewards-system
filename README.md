# Call Center Employee Rewards System

Call Center Employee Rewards System (CCERS) - final project for Scala programming course at AGH UST.
Application is written in Scala and is using Spark and Kafka (as a message broker) to process data.

## Installation

1. Build docker image in docker directory.
2. Run docker image.
3. Build and run project in IntelliJ.

Note:
You may need to add this to JVM options in IntelliJ:

`--add-exports java.base/sun.nio.ch=ALL-UNNAMED`

More info [here](https://stackoverflow.com/questions/73465937/apache-spark-3-3-0-breaks-on-java-17-with-cannot-access-class-sun-nio-ch-direct).