package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
)

func init() {
	_ = godotenv.Load(".env")
}

func main() {

	conn, _, err := websocket.DefaultDialer.Dial(os.Getenv("BLOCKCHAIN_API"), nil)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	go func() {
		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				log.Println(err)
				return
			}
			fmt.Println(string(message))
		}
	}()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	<-interrupt
	log.Println("Turning off..")
	conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
}
