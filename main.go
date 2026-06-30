package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

func getEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func main() {
	brokers := os.Getenv("KAFKA_BROKERS")
	topic := os.Getenv("KAFKA_TOPIC")

	if brokers == "" || topic == "" {
		log.Fatal("KAFKA_BROKERS and KAFKA_TOPIC must be set")
	}

	burstSize := getEnvInt("BURST_SIZE", 50)
	intervalSec := getEnvInt("BURST_INTERVAL_SECONDS", 30)

	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
	}
	defer w.Close()

	log.Printf("Producer started: topic=%s burst=%d interval=%ds", topic, burstSize, intervalSec)

	for burst := 1; ; burst++ {
		msgs := make([]kafka.Message, burstSize)
		for i := range msgs {
			msgs[i] = kafka.Message{
				Key:   []byte(fmt.Sprintf("burst-%d-msg-%d", burst, i)),
				Value: []byte(fmt.Sprintf(`{"burst":%d,"index":%d,"ts":"%s"}`, burst, i, time.Now().UTC().Format(time.RFC3339))),
			}
		}

		if err := w.WriteMessages(context.Background(), msgs...); err != nil {
			log.Printf("Write error: %v — retrying next interval", err)
		} else {
			log.Printf("Burst %d: produced %d messages to %s", burst, burstSize, topic)
		}

		log.Printf("Sleeping %ds before next burst…", intervalSec)
		time.Sleep(time.Duration(intervalSec) * time.Second)
	}
}
