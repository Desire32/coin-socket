package websockets

import (
	"log"
	"os"
	"sync"
	"time"

	"broker/config"

	"github.com/IBM/sarama"
	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
)

// init function
func init() {
	_ = godotenv.Load("../.env")
}

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
	solana, err := connectToWebSocket(os.Getenv("SOLANA_API"))
	if err != nil {
		log.Fatal("Solana WebSocket error:", err)
	}
	// bitcoin
	bitcoin, err := connectToWebSocket(os.Getenv("BITCOIN_API"))
	if err != nil {
		log.Fatal("Bitcoin WebSocket error:", err)
	}
	// binance
	binance, err := connectToWebSocket("BINANCE_API")
	if err != nil {
		log.Fatal("Binance WebSocket error:", err)
	}
	// ether
	ether, err := connectToWebSocket(os.Getenv("ETHEREUM_API"))
	if err != nil {
		log.Fatal("Ethereum WebSocket error:", err)
	}

	ticker := time.NewTicker(2 * time.Second)
	binance_ticker := time.NewTicker(7 * time.Second)
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
		}
	}

	wg.Add(4)
	go go_sched(solana, "solana-messages", ticker)
	go go_sched(bitcoin, "bitcoin-messages", ticker)
	go go_sched(binance, "binance-messages", binance_ticker)
	go go_sched(ether, "ether-messages", ticker)

	w.closeWebSockets()

	return nil
}
