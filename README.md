# Kafka Consumer Lag Monitor

This Go program monitors Kafka consumer groups and their lag. It connects to a Kafka cluster, retrieves consumer group offsets, and calculates the lag between the current and latest offsets for all the topics and partitions.

## Prerequisites

- Go
- Kafka broker running on `localhost:9092` or you can define your brokers
- Kafka library for Go: [sarama](https://github.com/Shopify/sarama)

## Installation

1. Clone the repository:
    ```sh
    https://github.com/anasmohammad611/kafkalagstatuschecker.git
    ```
2. Navigate to the project directory:
    ```sh
    cd kafkalagstatuschecker
    ```
3. Install the dependencies:
    ```sh
    go mod tidy
    ```

## Usage

1. Start your Kafka broker and ensure it's running on `localhost:9092` or your own defined brokers.
2. Run the Go program:
    ```sh
    go run main.go
    ```

## Program Output
This program will run indefinitely because it will keep checking the lags.

The program will print information about consumer groups and their lag in the following format:
CG: Consumer Group; T: Topic; P: Partition; CO: Curr Offset; LO: Latest Offset

If no lags are found, it will print:
No lags found

