package main

import (
	"log"
	"os"
	"os/signal"
	"time"

	kfk "broker/kafka"
	skts "broker/websockets"
)

func main() {

	// link with structs
	socketsService := &skts.SocketsService{}
	kafkaService := &kfk.KafkaService{}

	//websockets
	go func() {
		if err := socketsService.SocketsInit(); err != nil {
			log.Fatal(err)
		}
	}()

	// kafka output
	go func() {
		time.Sleep(5 * time.Second)
		if err := kafkaService.KafkaOutput(); err != nil {
			log.Fatal(err)
		}
	}()

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	// infinite channel read
	<-interrupt
	log.Println("Turning off..")

}
