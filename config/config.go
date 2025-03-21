package config

import (
	"os"
	"sync"
)

type ConfInit struct {
	KafkaBroker []string
}

var instance *ConfInit
var once sync.Once

func GetConfig() *ConfInit {
	once.Do(func() {
		instance = &ConfInit{
			KafkaBroker: []string{os.Getenv("KAFKA_BROKER")},
		}
	})
	return instance
}
