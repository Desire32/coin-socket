package websockets

import (
	"fmt"
	"log"
	"sync"
	"time"

	"broker/config"

	"github.com/IBM/sarama"
	"github.com/gorilla/websocket"
)

type SocketsService struct {
	conn []*websocket.Conn
}

func (w *SocketsService) closeWebSockets() {
	for _, v := range w.conn {
		if v != nil {
			v.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			v.Close()
		}
	}
}

// websockets
func (w *SocketsService) SocketsInit() error {
	cfg := config.GetConfig()
	var wg sync.WaitGroup

	//producer
	producer, err := sarama.NewAsyncProducer(cfg.KafkaBroker, sarama.NewConfig())
	if err != nil {
		log.Fatal(err)
	}
	defer producer.AsyncClose()

	connectToWebSocket := func(url string) (*websocket.Conn, error) {
		conn, _, err := websocket.DefaultDialer.Dial(url, nil)
		if err != nil {
			log.Fatal(err)
		}
		w.conn = append(w.conn, conn)
		return conn, nil
	}

	// solana
	solana, _ := connectToWebSocket("wss://ws.coincap.io/prices?assets=solana")
	bitcoin, _ := connectToWebSocket("wss://ws.coincap.io/prices?assets=bitcoin")
	binance, _ := connectToWebSocket("wss://stream.binance.com:9443/ws/btcusdt@trade")
	ether, _ := connectToWebSocket("wss://ws.coincap.io/prices?assets=ethereum")

	ticker := time.NewTicker(1 * time.Second)
	binance_ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	defer binance_ticker.Stop()

	go_sched := func(conn *websocket.Conn, topic string, ticker *time.Ticker) {
		defer wg.Done()
		for range ticker.C {
			_, message, err := conn.ReadMessage()
			if err != nil {
				log.Println("WebSocket error:", err)
			}
			producer.Input() <- &sarama.ProducerMessage{
				Topic: topic,
				Value: sarama.StringEncoder(string(message)),
			}
			fmt.Println(string(message))
		}
	}

	wg.Add(4)
	go go_sched(solana, "solana-messages", ticker)
	go go_sched(bitcoin, "bitcoin-messages", ticker)
	go go_sched(binance, "binance-messages", binance_ticker)
	go go_sched(ether, "ether-messages", ticker)
	wg.Wait()

	w.closeWebSockets()

	return nil
}
