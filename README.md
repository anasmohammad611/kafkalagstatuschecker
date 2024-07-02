# Kafka Consumer Lag Monitor

This Go program monitors Kafka consumer groups and their lag. It connects to a Kafka cluster, retrieves consumer group offsets, and calculates the lag between the current and latest offsets for specified topics and partitions.

## Prerequisites

- Go (1.16 or higher)
- Kafka broker running on `localhost:9092`
- Kafka library for Go: [sarama](https://github.com/Shopify/sarama)

## Installation

1. Clone the repository:
    ```sh
    git clone https://github.com/yourusername/kafka-consumer-lag-monitor.git
    ```
2. Navigate to the project directory:
    ```sh
    cd kafka-consumer-lag-monitor
    ```
3. Install the dependencies:
    ```sh
    go mod tidy
    ```

## Usage

1. Start your Kafka broker and ensure it's running on `localhost:9092`.
2. Run the Go program:
    ```sh
    go run main.go
    ```

## Program Output

The program will print information about consumer groups and their lag in the following format:

CG: Consumer Group; T: Topic; P: Partition; CO: Curr Offset; LO: Latest Offset
CG: consumer_group, T: topic_name, P: partition_number, CO: current_offset, LO: latest_offset, Lag: lag

If no lags are found, it will print:
No lags found

