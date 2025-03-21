package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/IBM/sarama"
	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
)

// init function
func init() {
	_ = godotenv.Load(".env")
}

func main() {

	////////////////////////////////////////////////////
	// Kafka init
	kafkaBroker := []string{os.Getenv("KAFKA_BROKER")}
	producer, err := sarama.NewAsyncProducer(kafkaBroker, sarama.NewConfig())
	if err != nil {
		log.Fatal(err)
	}
	defer producer.AsyncClose()

	consumer, err := sarama.NewConsumer(kafkaBroker, sarama.NewConfig())
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()
	///////////////////////////////////////////////////

	////////////////////////////////////////////////////
	// Gorilla Websocket init
	conn, _, err := websocket.DefaultDialer.Dial(os.Getenv("BLOCKCHAIN_API"), nil)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	////////////////////////////////////////////////////

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	// websocket api and kafka input
	go func() {
		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				log.Println(err)
				return
			}
			fmt.Println(string(message))

			// input
			producer.Input() <- &sarama.ProducerMessage{
				Topic: "websocket-messages",
				Value: sarama.StringEncoder(string(message)),
			}
			fmt.Println("Отправлено в Kafka:", string(message))
		}
	}()

	// kafka output
	go func() {
		partitions, err := consumer.Partitions("websocket-messages")
		if err != nil {
			log.Fatal(err)
		}
		for _, partition := range partitions {
			pc, err := consumer.ConsumePartition("websocket-messages", partition, sarama.OffsetNewest)
			if err != nil {
				log.Fatal(err)
			}
			defer pc.Close()

			for message := range pc.Messages() {
				fmt.Printf("Kafka gotten message: %s\n", string(message.Value))
			}
		}
	}()

	// response latency
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	// infinite channel read
	<-interrupt
	log.Println("Turning off..")
	conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
}
