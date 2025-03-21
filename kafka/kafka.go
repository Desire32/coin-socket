package kafka

import (
	"broker/websockets"
	"fmt"
	"log"
	"sync"

	"github.com/IBM/sarama"
)

type KafkaService struct {
	consumer       sarama.Consumer
	socketsService *websockets.SocketsService
}

func NewKafkaService(brokers []string, socketsService *websockets.SocketsService) (*KafkaService, error) {
	consumer, err := sarama.NewConsumer(brokers, sarama.NewConfig())
	if err != nil {
		return nil, err
	}
	return &KafkaService{
		consumer:       consumer,
		socketsService: socketsService,
	}, nil
}

func (k *KafkaService) KafkaOutput() error {
	if k.consumer == nil {
		return fmt.Errorf("kafka hasnt been initialized")
	}
	var wg sync.WaitGroup
	topics := []string{"solana-messages", "ether-messages", "bitcoin-messages", "binance-messages"}

	kafka_sched := func(topic string) {
		partition, err := k.consumer.Partitions(topic)
		if err != nil {
			log.Fatal(err)
		}
		for _, part := range partition {
			pc, err := k.consumer.ConsumePartition(topic, part, sarama.OffsetNewest)
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
		go func(topic string) {
			kafka_sched(topic)
		}(t)
	}
	wg.Wait()

	return nil
}
