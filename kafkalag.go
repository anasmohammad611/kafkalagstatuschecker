package main

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	// Define Kafka brokers
	brokers := []string{"localhost:9092"}

	// Configure Kafka client and admin
	config := sarama.NewConfig()
	config.Version = sarama.V3_2_0_0

	client, admin, err := setupKafka(brokers, config)
	if err != nil {
		log.Fatalf("Error setting up Kafka: %v", err)
	}
	defer client.Close()
	defer admin.Close()

	// Main loop to monitor consumer group offsets
	for {
		checkConsumerGroupOffsets(admin, client)
		time.Sleep(10 * time.Second) // Wait for 10 seconds before checking again
	}
}

// setupKafka initializes the Kafka client and cluster admin
func setupKafka(brokers []string, config *sarama.Config) (sarama.Client, sarama.ClusterAdmin, error) {
	// Create Kafka client
	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		return nil, nil, fmt.Errorf("creating client: %v", err)
	}

	// Create Kafka cluster admin client
	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		client.Close()
		return nil, nil, fmt.Errorf("creating cluster admin: %v", err)
	}

	return client, admin, nil
}

// checkConsumerGroupOffsets retrieves and checks Kafka consumer group offsets
func checkConsumerGroupOffsets(admin sarama.ClusterAdmin, client sarama.Client) {
	fmt.Println("Checking consumer group offsets...")

	// Retrieve list of consumer groups
	groups, err := admin.ListConsumerGroups()
	if err != nil {
		log.Fatalf("Error listing consumer groups: %v", err)
	}

	// Iterate over each consumer group
	for group := range groups {
		// Skip consumer groups containing "middleware"
		if strings.Contains(group, "middleware") {
			continue
		}

		// Retrieve offsets for the consumer group
		offsets, err := admin.ListConsumerGroupOffsets(group, nil)
		if err != nil {
			log.Printf("Error getting offsets for group %s: %v", group, err)
			continue
		}

		// Iterate over topics and their partitions
		for topic, partitions := range offsets.Blocks {
			// Skip topics not containing "wallet"
			if !strings.Contains(topic, "wallet") {
				continue
			}

			// Iterate over partitions and their offset information
			for partition, offsetFetchResponseBlock := range partitions {
				// Get the latest offset for the partition
				latestOffset, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
				if err != nil {
					log.Printf("Error getting latest offset for %s-%d: %v", topic, partition, err)
					continue
				}

				// Calculate lag for the partition
				lag := latestOffset - offsetFetchResponseBlock.Offset
				if lag == 0 {
					continue // Skip if no lag
				}

				// Print consumer group, topic, partition, offsets, and lag
				fmt.Printf("CG: %s, T: %s, P: %d, CO: %d, LO: %d, Lag: %d\n",
					extractGroupName(group), extractTopicName(topic), partition, offsetFetchResponseBlock.Offset, latestOffset, lag)
				fmt.Println()
			}
		}
	}

	// Print message if no lags were found
	fmt.Println("No lags found")
}

// extractGroupName extracts the meaningful group name from the full Kafka consumer group string
func extractGroupName(group string) string {
	return strings.Split(group, ".")[2]
}

// extractTopicName extracts the meaningful topic name from the full Kafka topic string
func extractTopicName(topic string) string {
	return strings.Split(topic, ".")[2]
}
