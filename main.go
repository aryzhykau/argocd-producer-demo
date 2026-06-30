package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
	topic  string

	mu         sync.Mutex
	cancel     context.CancelFunc
	running    bool
	throughput int // messages per second
	produced   int64
}

type StatusResponse struct {
	Running    bool  `json:"running"`
	Throughput int   `json:"throughput_per_sec"`
	Produced   int64 `json:"total_produced"`
}

func (p *Producer) startHandler(w http.ResponseWriter, r *http.Request) {
	tStr := r.URL.Query().Get("throughput")
	throughput, err := strconv.Atoi(tStr)
	if err != nil || throughput <= 0 {
		http.Error(w, "throughput query param must be a positive integer (msg/sec)", http.StatusBadRequest)
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// stop existing run if any
	if p.cancel != nil {
		p.cancel()
	}

	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	p.running = true
	p.throughput = throughput

	go p.produce(ctx, throughput)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"status":             "started",
		"throughput_per_sec": throughput,
	})
	log.Printf("Producer started: throughput=%d msg/s topic=%s", throughput, p.topic)
}

func (p *Producer) stopHandler(w http.ResponseWriter, r *http.Request) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.running {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"status": "already stopped"})
		return
	}

	p.cancel()
	p.cancel = nil
	p.running = false

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"status":         "stopped",
		"total_produced": p.produced,
	})
	log.Printf("Producer stopped. Total produced: %d", p.produced)
}

func (p *Producer) statusHandler(w http.ResponseWriter, r *http.Request) {
	p.mu.Lock()
	defer p.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(StatusResponse{
		Running:    p.running,
		Throughput: p.throughput,
		Produced:   p.produced,
	})
}

func (p *Producer) healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// produce sends messages at the given throughput (msg/sec) until ctx is cancelled.
// It uses a ticker interval of 1s and sends `throughput` messages per tick.
func (p *Producer) produce(ctx context.Context, throughput int) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var seq int64

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			msgs := make([]kafka.Message, throughput)
			now := time.Now().UTC().Format(time.RFC3339Nano)
			for i := range msgs {
				seq++
				msgs[i] = kafka.Message{
					Key:   []byte(fmt.Sprintf("msg-%d", seq)),
					Value: []byte(fmt.Sprintf(`{"seq":%d,"throughput":%d,"ts":"%s"}`, seq, throughput, now)),
				}
			}

			if err := p.writer.WriteMessages(ctx, msgs...); err != nil {
				if ctx.Err() != nil {
					return // context cancelled, not an error
				}
				log.Printf("Write error: %v", err)
				continue
			}

			p.mu.Lock()
			p.produced += int64(throughput)
			p.mu.Unlock()
			log.Printf("Produced %d messages (seq up to %d)", throughput, seq)
		}
	}
}

func main() {
	brokers := os.Getenv("KAFKA_BROKERS")
	topic := os.Getenv("KAFKA_TOPIC")
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	if brokers == "" || topic == "" {
		log.Fatal("KAFKA_BROKERS and KAFKA_TOPIC must be set")
	}

	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
	}
	defer w.Close()

	p := &Producer{writer: w, topic: topic}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", p.healthHandler)
	mux.HandleFunc("/status", p.statusHandler)
	mux.HandleFunc("/start", p.startHandler)
	mux.HandleFunc("/stop", p.stopHandler)

	log.Printf("Producer HTTP server listening on :%s", port)
	log.Printf("  POST /start?throughput=<msg/sec>  — start producing")
	log.Printf("  POST /stop                         — stop producing")
	log.Printf("  GET  /status                       — current state")
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
