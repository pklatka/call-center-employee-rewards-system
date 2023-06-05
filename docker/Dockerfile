# Config
ARG TOPICNAME=ccers

FROM ubuntu

RUN apt-get update && apt-get install -y default-jdk git wget dpkg

# Install scala 2.13.10
RUN wget https://downloads.lightbend.com/scala/2.12.17/scala-2.12.17.deb && \
    dpkg -i scala-2.12.17.deb

# Install spark 3.4.0
RUN wget https://downloads.apache.org/spark/spark-3.4.0/spark-3.4.0-bin-hadoop3.tgz && \
    tar -xvf spark-3.4.0-bin-hadoop3.tgz && \
    mv spark-3.4.0-bin-hadoop3 /opt/spark

# Install kafka 3.4.0
RUN wget https://downloads.apache.org/kafka/3.4.0/kafka_2.13-3.4.0.tgz && \
    tar -xvf kafka_2.13-3.4.0.tgz && \
    mv kafka_2.13-3.4.0 /opt/kafka

COPY server.properties /opt/kafka/config/
COPY loop.sh .

# Open ports
EXPOSE 9092 2181 7077 8080

# Run kafka and spark, create default topic
CMD /opt/kafka/bin/zookeeper-server-start.sh -daemon /opt/kafka/config/zookeeper.properties && \
    /opt/kafka/bin/kafka-server-start.sh -daemon /opt/kafka/config/server.properties && \
    /opt/kafka/bin/kafka-topics.sh --create --topic ccers --bootstrap-server localhost:9092 && \
    /opt/spark/sbin/start-master.sh && \
    /opt/spark/sbin/start-worker.sh spark://localhost:7077 && \
    chmod 777 loop.sh && ./loop.sh

# Start commands
# /opt/kafka/bin/zookeeper-server-start.sh -daemon /opt/kafka/config/zookeeper.properties
# /opt/kafka/bin/kafka-server-start.sh -daemon /opt/kafka/config/server.properties
# /opt/spark/sbin/start-master.sh
# /opt/spark/sbin/start-slave.sh spark://localhost:7077

# Kafka topic commands
# /opt/kafka/bin/kafka-topics.sh --create --topic lol --bootstrap-server localhost:9092
# /opt/kafka/bin/kafka-console-producer.sh --topic lol --bootstrap-server localhost:9092
# /opt/kafka/bin/kafka-console-consumer.sh --topic lol --bootstrap-server localhost:9092 --from-beginning

# Restart commands
# /opt/kafka/bin/kafka-server-stop.sh
# /opt/kafka/bin/kafka-server-start.sh -daemon /opt/kafka/config/server.properties
# /opt/kafka/bin/zookeeper-server-stop.sh
# /opt/kafka/bin/zookeeper-server-start.sh -daemon /opt/kafka/config/zookeeper.properties