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

	//producer
	producer, err := sarama.NewAsyncProducer(kafkaBroker, sarama.NewConfig())
	if err != nil {
		log.Fatal(err)
	}
	defer producer.AsyncClose()

	// consumer
	consumer, err := sarama.NewConsumer(kafkaBroker, sarama.NewConfig())
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()
	///////////////////////////////////////////////////

	////////////////////////////////////////////////////
	// Solana Websocket init
	solana, _, err := websocket.DefaultDialer.Dial(os.Getenv("SOLANA_API"), nil)
	if err != nil {
		log.Fatal(err)
	}
	defer solana.Close()
	////////////////////////////////////////////////////

	////////////////////////////////////////////////////
	// Bitcoin Websocket init
	bitcoin, _, err := websocket.DefaultDialer.Dial(os.Getenv("BITCOIN_API"), nil)
	if err != nil {
		log.Fatal(err)
	}
	defer bitcoin.Close()
	////////////////////////////////////////////////////

	////////////////////////////////////////////////////
	// Ethereum Websocket init
	ether, _, err := websocket.DefaultDialer.Dial(os.Getenv("ETHEREUM_API"), nil)
	if err != nil {
		log.Fatal(err)
	}
	defer ether.Close()
	////////////////////////////////////////////////////

	////////////////////////////////////////////////////
	// Binance trades init
	binance, _, err := websocket.DefaultDialer.Dial(os.Getenv("BINANCE_API"), nil)
	if err != nil {
		log.Fatal(err)
	}
	defer binance.Close()
	////////////////////////////////////////////////////

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	// response latency
	ticker := time.NewTicker(2 * time.Second)
	binance_ticker := time.NewTicker(7 * time.Second)
	defer ticker.Stop()
	defer binance_ticker.Stop()

	// websockets
	go func() {
		// solana
		go func() {
			for {
				select {
				case <-ticker.C:
					_, sol_message, err := solana.ReadMessage()
					if err != nil {
						log.Println("Solana WebSocket error:", err)
						return
					}
					producer.Input() <- &sarama.ProducerMessage{
						Topic: "solana-messages",
						Value: sarama.StringEncoder(string(sol_message)),
					}
				}
			}
		}()
		// bitcoin
		go func() {
			for {
				select {
				case <-ticker.C:
					_, bit_message, err := bitcoin.ReadMessage()
					if err != nil {
						log.Println("Bitcoin WebSocket error:", err)
						return
					}
					producer.Input() <- &sarama.ProducerMessage{
						Topic: "bitcoin-messages",
						Value: sarama.StringEncoder(string(bit_message)),
					}
				}
			}
		}()

		// ether
		go func() {
			for {
				select {
				case <-ticker.C:
					_, ether_message, err := ether.ReadMessage()
					if err != nil {
						log.Println("Ether WebSocket error:", err)
						return
					}
					producer.Input() <- &sarama.ProducerMessage{
						Topic: "ether-messages",
						Value: sarama.StringEncoder(string(ether_message)),
					}
				}
			}
		}()
		// binance
		go func() {
			for {
				select {
				case <-binance_ticker.C:
					_, ether_message, err := ether.ReadMessage()
					if err != nil {
						log.Println("Binance error:", err)
						return
					}
					producer.Input() <- &sarama.ProducerMessage{
						Topic: "binance-messages",
						Value: sarama.StringEncoder(string(ether_message)),
					}
				}
			}
		}()
	}()

	// kafka output
	go func() {
		go func() {
			// solana
			sol_partitions, err := consumer.Partitions("solana-messages")
			if err != nil {
				log.Fatal(err)
			}
			for _, partition := range sol_partitions {
				pc, err := consumer.ConsumePartition("solana-messages", partition, sarama.OffsetNewest)
				if err != nil {
					log.Fatal(err)
				}
				defer pc.Close()

				for message := range pc.Messages() {
					fmt.Printf("Solana: %s\n", string(message.Value))
				}
			}
		}()

		go func() {
			// ether
			eth_partitions, err := consumer.Partitions("ether-messages")
			if err != nil {
				log.Fatal(err)
			}
			for _, partition := range eth_partitions {
				pc, err := consumer.ConsumePartition("ether-messages", partition, sarama.OffsetNewest)
				if err != nil {
					log.Fatal(err)
				}
				defer pc.Close()

				for message := range pc.Messages() {
					fmt.Printf("Ether: %s\n", string(message.Value))
				}
			}
		}()

		// bitcoin
		go func() {
			bit_partitions, err := consumer.Partitions("bitcoin-messages")
			if err != nil {
				log.Fatal(err)
			}
			for _, partition := range bit_partitions {
				pc, err := consumer.ConsumePartition("bitcoin-messages", partition, sarama.OffsetNewest)
				if err != nil {
					log.Fatal(err)
				}
				defer pc.Close()

				for message := range pc.Messages() {
					fmt.Printf("Bitcoin: %s\n", string(message.Value))
				}
			}
		}()

		// binance
		go func() {
			binance_partitions, err := consumer.Partitions("binance-messages")
			if err != nil {
				log.Fatal(err)
			}
			for _, partition := range binance_partitions {
				pc, err := consumer.ConsumePartition("binance-messages", partition, sarama.OffsetNewest)
				if err != nil {
					log.Fatal(err)
				}
				defer pc.Close()

				for message := range pc.Messages() {
					fmt.Printf("Binance trades: %s\n", string(message.Value))
				}
			}
		}()
	}()

	// infinite channel read
	<-interrupt
	log.Println("Turning off..")

	//closers
	solana.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	ether.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	bitcoin.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	binance.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
}
