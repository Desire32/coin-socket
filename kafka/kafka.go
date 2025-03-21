package kafka

import (
	"fmt"
	"log"
	"sync"

	"github.com/IBM/sarama"
)

type KafkaService struct{}

func (k *KafkaService) KafkaOutput() error {
	var wg sync.WaitGroup
	topics := []string{"solana-messages", "ether-messages", "bitcoin-messages", "binance-messages"}

	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, sarama.NewConfig())
	if err != nil {
		log.Fatal(err)
	}

	kafka_sched := func(topic string) {
		partition, err := consumer.Partitions(topic)
		if err != nil {
			log.Fatal(err)
		}
		for _, part := range partition {
			pc, err := consumer.ConsumePartition(topic, part, sarama.OffsetNewest)
			if err != nil {
				log.Fatal(err)
			}
			defer pc.Close()

			for message := range pc.Messages() {
				fmt.Printf("%s: %s\n", topic, string(message.Value))
			}
		}
	}

	for _, t := range topics {
		wg.Add(1)
		go kafka_sched(t)
	}
	wg.Wait()

	return nil
}
