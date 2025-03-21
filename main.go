package main

import (
	"log"
	"os"
	"os/signal"

	kfk "broker/kafka"
	skts "broker/websockets"

	"github.com/joho/godotenv"
)

func main() {
	_ = godotenv.Load(".env")

	kafkaBroker := os.Getenv("KAFKA_BROKER")

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	// link
	socketsService := &skts.SocketsService{}
	kafkaService, err := kfk.NewKafkaService([]string{kafkaBroker}, socketsService)
	if err != nil {
		log.Fatal(err)
	}

	//websockets
	go func() {
		if err := socketsService.SocketsInit(); err != nil {
			log.Fatal(err)
		}
	}()

	// kafka output
	go func() {
		if err := kafkaService.KafkaOutput(); err != nil {
			log.Fatal(err)
		}
	}()

	// infinite channel read
	<-interrupt
	log.Println("Turning off..")

}
