# Docker image for kafka and spark

## Build steps

1. Build the docker image 
    ```bash
    docker build -t kafka-spark:latest .
    ```

2. Run the docker image
    ```bash
    docker run -dit -p 9092:9092 -p 7077:7077 kafka-spark:latest
    ```
   Or if you want to have access to spark GUI:

    ```bash
    docker run -dit -p 9092:9092 -p 7077:7077 -p 8080:8080 -p 8081:8081 -p 4040:4040 kafka-spark:latest
    ```
