// Package main is the Producer binary.
//
// It runs a plain HTTP server that accepts POST /event, validates the JSON
// payload, then writes the event to a Redis Stream named "events:raw".
//
// Redis Concept — Streams (XADD):
//   A Redis Stream is an append-only log, similar to Kafka topics but built
//   into Redis. Each entry has:
//     - An auto-generated ID in the form <millisecond-timestamp>-<sequence>
//       (e.g. 1715000000000-0). We pass "*" to let Redis choose.
//     - A set of field-value pairs (like a hash map inline).
//   XADD events:raw MAXLEN ~ 100000 * type pageview url /home ...
//   The "~" before MAXLEN means "approximately" — Redis trims lazily to
//   avoid a blocking full scan on every write. This keeps memory bounded
//   without adding latency.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"mikiri/internal/config"
	"mikiri/internal/logger"
	"mikiri/internal/redisclient"

	"github.com/redis/go-redis/v9"
)

// Redis key for the raw event stream.
const streamKey = "events:raw"

// maxStreamLen is the approximate cap on stream length.
// Redis will trim entries older than this ceiling lazily.
const maxStreamLen = 100_000

// validTypes is the set of event types the producer will accept.
var validTypes = map[string]bool{
	"pageview": true,
	"api_call": true,
	"error":    true,
	"click":    true,
}

// event is the JSON body expected on POST /event.
type event struct {
	Type     string            `json:"type"`
	URL      string            `json:"url"`
	UserID   string            `json:"user_id"`
	Metadata map[string]string `json:"metadata"`
}

// server holds the shared dependencies so we avoid global state.
type server struct {
	rdb *redis.Client
	log *slog.Logger
}

func main() {
	cfg := config.Load()
	log := logger.New(cfg.LogLevel)

	rdb, err := redisclient.New(cfg)
	if err != nil {
		log.Error("failed to connect to redis", "error", err)
		os.Exit(1)
	}
	defer rdb.Close()

	srv := &server{rdb: rdb, log: log}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /event", srv.handleEvent)
	mux.HandleFunc("GET /health", srv.handleHealth)

	httpServer := &http.Server{
		Addr:         ":" + cfg.ProducerPort,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start serving in a goroutine so the main goroutine can wait on signals.
	go func() {
		log.Info("producer listening", "port", cfg.ProducerPort)
		if err := httpServer.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server error", "error", err)
			os.Exit(1)
		}
	}()

	// Block until we receive SIGTERM or SIGINT (Ctrl-C / docker stop).
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	<-quit

	log.Info("shutting down producer")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(ctx); err != nil {
		log.Error("graceful shutdown failed", "error", err)
	}
	log.Info("producer stopped")
}

// handleEvent validates the incoming JSON and writes it to the Redis Stream.
func (s *server) handleEvent(w http.ResponseWriter, r *http.Request) {
	var ev event
	if err := json.NewDecoder(r.Body).Decode(&ev); err != nil {
		s.log.Warn("bad request body", "error", err)
		http.Error(w, `{"error":"invalid JSON"}`, http.StatusBadRequest)
		return
	}

	// Validate required fields.
	if ev.Type == "" || ev.URL == "" || ev.UserID == "" {
		http.Error(w, `{"error":"type, url, user_id are required"}`, http.StatusBadRequest)
		return
	}
	if !validTypes[ev.Type] {
		http.Error(w, `{"error":"type must be pageview|api_call|error|click"}`, http.StatusBadRequest)
		return
	}

	// Serialize metadata to a JSON string so we can store it as a single
	// stream field (Redis field-values are flat strings, not nested maps).
	metaBytes, err := json.Marshal(ev.Metadata)
	if err != nil {
		s.log.Error("failed to marshal metadata", "error", err)
		http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
		return
	}

	// XADD writes one entry to the stream.
	// Values is a flat []interface{} of alternating field, value pairs.
	// MaxLen + Approx = "MAXLEN ~ 100000" in the wire protocol.
	id, err := s.rdb.XAdd(r.Context(), &redis.XAddArgs{
		Stream: streamKey,
		MaxLen: maxStreamLen,
		Approx: true, // "~" — lazy / amortized trimming
		ID:     "*",  // auto-generate the entry ID
		Values: []interface{}{
			"type", ev.Type,
			"url", ev.URL,
			"user_id", ev.UserID,
			"metadata", string(metaBytes),
			"ts", time.Now().UnixMilli(),
		},
	}).Result()
	if err != nil {
		s.log.Error("xadd failed", "error", err)
		http.Error(w, `{"error":"stream write failed"}`, http.StatusInternalServerError)
		return
	}

	s.log.Info("event written", "stream_id", id, "type", ev.Type, "user", ev.UserID)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	if _, err := w.Write([]byte(`{"id":"` + id + `"}`)); err != nil {
		s.log.Warn("failed to write response", "error", err)
	}
}

func (s *server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write([]byte(`{"status":"ok"}`)); err != nil {
		s.log.Warn("health write failed", "error", err)
	}
}	