package main

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	brokers := []string{
		"localhost:9092",
	}

	config := sarama.NewConfig()
	config.Version = sarama.V3_2_0_0

	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		log.Fatalf("Error creating client: %v", err)
	}
	defer client.Close()

	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		log.Fatalf("Error creating cluster admin: %v", err)
	}
	defer admin.Close()
	var cnt int
	fmt.Println("CG: Consumer Group;", "T: Topic;", "P: Partition;", "CO: Curr Offset;", "LO: Latest Offset")

	for {
		groups, err := admin.ListConsumerGroups()
		if err != nil {
			log.Fatalf("Error listing consumer groups: %v", err)
		}

		for group := range groups {
			if strings.Contains(group, "middleware") {
				continue
			}

			offsets, err := admin.ListConsumerGroupOffsets(group, nil)
			if err != nil {
				log.Printf("Error getting offsets for group %s: %v", group, err)
				continue
			}

			for topic, partitions := range offsets.Blocks {
				if !strings.Contains(topic, "wallet") {
					continue
				}

				for partition, offsetFetchResponseBlock := range partitions {
					latestOffset, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
					if err != nil {
						log.Printf("Error getting latest offset for %s-%d: %v", topic, partition, err)
						continue
					}

					lag := latestOffset - offsetFetchResponseBlock.Offset
					if lag == 0 {
						continue
					}

					cnt += 1
					fmt.Printf("CG: %s, T: %s, P: %d, CO: %d, LO: %d, Lag: %d\n",
						string(strings.Split(group, ".")[2]), string(strings.Split(topic, ".")[2]), partition, offsetFetchResponseBlock.Offset, latestOffset, lag)
					fmt.Println()
				}
			}
		}
		if cnt <= 0 {
			fmt.Println("No lags found")
		}
		time.Sleep(10 * time.Second)
	}
}
