// It reads events from the Redis Stream "events:raw" using a Consumer Group,
// aggregates statistics into Hashes and Sorted Sets, and moves failed messages
// to a dead-letter stream.
//
// Consumer Groups (XREADGROUP / XACK / XPENDING):
//
//	A Consumer Group is a named cursor over a Stream. Unlike a plain XREAD
//	(which every caller reads independently), a group tracks which messages
//	have been delivered to which consumer and whether they've been ACKed.
//
//	Lifecycle of a message inside a group:
//	  1. XADD     → entry lands in the stream with an ID.
//	  2. XREADGROUP GROUP mygroup consumer-1 COUNT 10 BLOCK 2000 STREAMS events:raw >
//	              → ">" means "give me only NEW (undelivered) messages".
//	                Redis moves those IDs into the group's Pending Entry List (PEL).
//	  3. Processing succeeds → XACK events:raw mygroup <id>
//	                → Redis removes the ID from the PEL. Done.
//	  3b. Processing fails / process crashes before XACK
//	                → ID stays in the PEL. XPENDING shows it as "pending".
//	  4. Recovery  → XAUTOCLAIM or XCLAIM can steal pending IDs from crashed
//	                consumers and reprocess them. We use XPENDING + XCLAIM here
//	                to keep Go 1.21 compat (XAUTOCLAIM needs Redis 6.2+,
//	                but we call XCLAIM which works on Redis 5+).
//
// Sorted Sets (ZADD / ZINCRBY):
//
//	A Sorted Set maps member → float64 score. Members are unique; scores are
//	not. ZINCRBY atomically increments a member's score — perfect for counters.
//	ZREVRANGE returns members in descending score order (highest first).
//
// Hashes (HINCRBY / HSET / HGETALL):
//
//	A Hash stores field→value pairs under one key. HINCRBY atomically
//	increments an integer field. HGETALL returns all pairs. Great for stat bags.
//
// Pipelining:
//
//	Every round-trip to Redis costs latency. A Pipeline batches multiple
//	commands into one TCP write and reads all replies in one round-trip.
//	We pipeline all stat writes per message to minimise latency.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"mikiri/internal/config"
	"mikiri/internal/logger"
	"mikiri/internal/redisclient"

	"github.com/redis/go-redis/v9"
)

// Redis key constants — one place to rename if needed.
const (
	streamKey      = "events:raw"
	deadLetterKey  = "events:dead"
	groupName      = "pipeline-group"
	statsTotalsKey = "stats:totals"
	statsErrorsKey = "stats:errors"
	leaderPages    = "leaderboard:pages"
	leaderUsers    = "leaderboard:users"
	// hourlyPrefix is combined with a UTC hour bucket string.
	hourlyPrefix = "stats:hourly:"
)

// pendingMinIdle is how long a message must be pending before we reclaim it.
const pendingMinIdle = 60 * time.Second

// maxRetries before a message is moved to the dead-letter stream.
const maxRetries = 3

// streamEntry mirrors the fields we write in the producer.
type streamEntry struct {
	ID       string
	Type     string
	URL      string
	UserID   string
	Metadata string
	TS       int64
}

func main() {
	cfg := config.Load()
	log := logger.New(cfg.LogLevel)

	rdb, err := redisclient.New(cfg)
	if err != nil {
		log.Error("redis connect failed", "error", err)
		os.Exit(1)
	}
	defer rdb.Close()

	ctx, cancel := context.WithCancel(context.Background())

	// Ensure the consumer group exists before workers start.
	// BUSYGROUP:
	//   XGROUP CREATE returns a BUSYGROUP error if the group already exists.
	//   This is normal on restart — we just ignore that specific error.
	//   "0" as the starting ID means "deliver from the very beginning of the
	//   stream" (only matters the first time the group is created).
	if err := ensureGroup(ctx, rdb, log); err != nil {
		log.Error("failed to create consumer group", "error", err)
		os.Exit(1)
	}

	// Reclaim any messages stuck in the PEL from a previous crashed worker.
	if err := recoverPending(ctx, rdb, cfg.ConsumerName, log); err != nil {
		log.Warn("pending recovery had errors", "error", err)
		// non-fatal — we continue and the next recovery cycle will retry
	}

	var wg sync.WaitGroup
	for i := 0; i < cfg.ConsumerWorkers; i++ {
		wg.Add(1)
		workerName := fmt.Sprintf("%s-%d", cfg.ConsumerName, i)
		go func(name string) {
			defer wg.Done()
			runWorker(ctx, rdb, name, log)
		}(workerName)
	}

	// Wait for stop signal.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	<-quit

	log.Info("shutdown signal received — draining workers")
	cancel() // tells all workers to stop reading new batches
	wg.Wait()
	log.Info("consumer stopped cleanly")
}

// ensureGroup creates the consumer group, ignoring BUSYGROUP if it already exists.
func ensureGroup(ctx context.Context, rdb *redis.Client, log *slog.Logger) error {
	// XGROUP CREATE <stream> <group> $ MKSTREAM
	// "$" would mean "only new messages from now". We use "0" so that on a
	// fresh restart we'd reprocess from the beginning if no ACKs exist yet.
	// MKSTREAM creates the stream key if it doesn't exist yet.
	err := rdb.XGroupCreateMkStream(ctx, streamKey, groupName, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return fmt.Errorf("XGROUP CREATE: %w", err)
	}
	if err == nil {
		log.Info("consumer group created", "group", groupName)
	} else {
		log.Info("consumer group already exists", "group", groupName)
	}
	return nil
}

// recoverPending claims messages that have been idle in the PEL for more than
// pendingMinIdle and reprocesses them under the current consumer's name.
//
// XPENDING / XCLAIM:
//
//	XPENDING events:raw pipeline-group - + 100
//	  Returns up to 100 pending entry summaries: [id, consumer, idle-ms, delivery-count]
//	XCLAIM events:raw pipeline-group new-consumer 60000 <id>
//	  Transfers ownership of <id> from whoever held it to new-consumer,
//	  but only if it has been idle for at least 60000ms.
//	  Returns the full message so we can process it immediately.
func recoverPending(ctx context.Context, rdb *redis.Client, consumerName string, log *slog.Logger) error {
	// Fetch up to 100 pending entries across all consumers in the group.
	pending, err := rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: streamKey,
		Group:  groupName,
		Start:  "-", // smallest possible ID
		End:    "+", // largest possible ID
		Count:  100,
		Idle:   pendingMinIdle,
	}).Result()
	if err != nil {
		return fmt.Errorf("XPENDING: %w", err)
	}

	if len(pending) == 0 {
		log.Info("no pending messages to recover")
		return nil
	}

	log.Info("recovering pending messages", "count", len(pending))

	for _, p := range pending {
		// XCLAIM transfers the message to us and returns its content.
		msgs, err := rdb.XClaim(ctx, &redis.XClaimArgs{
			Stream:   streamKey,
			Group:    groupName,
			Consumer: consumerName,
			MinIdle:  pendingMinIdle,
			Messages: []string{p.ID},
		}).Result()
		if err != nil {
			log.Warn("xclaim failed", "id", p.ID, "error", err)
			continue
		}
		for _, m := range msgs {
			entry := parseMessage(m)
			if err := processWithRetry(ctx, rdb, entry, int(p.RetryCount), log); err != nil {
				log.Error("recovery processing failed", "id", m.ID, "error", err)
			}
		}
	}
	return nil
}

// runWorker is the main read loop for one consumer worker.
// It calls XREADGROUP in blocking mode and processes each batch.
func runWorker(ctx context.Context, rdb *redis.Client, name string, log *slog.Logger) {
	log.Info("worker started", "consumer", name)
	for {
		// Check if we've been asked to stop.
		if ctx.Err() != nil {
			log.Info("worker stopping", "consumer", name)
			return
		}

		// XREADGROUP with ">":
		//   The special ID ">" tells Redis: "give me messages that have NOT yet
		//   been delivered to any consumer in this group."
		//   COUNT 10: deliver up to 10 messages per call.
		//   BLOCK 2000: if no messages are available, wait up to 2 seconds
		//   before returning an empty result. This avoids a tight polling loop
		//   while still being responsive to the ctx cancellation check above.
		streams, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupName,
			Consumer: name,
			Streams:  []string{streamKey, ">"},
			Count:    10,
			Block:    2 * time.Second,
			NoAck:    false, // we want explicit XACK after processing
		}).Result()

		if err != nil {
			// redis.Nil means the BLOCK timeout elapsed with no messages — normal.
			if errors.Is(err, redis.Nil) {
				continue
			}
			// Context was cancelled — clean exit.
			if ctx.Err() != nil {
				return
			}
			log.Error("xreadgroup error", "consumer", name, "error", err)
			time.Sleep(time.Second) // back off before retrying
			continue
		}

		for _, stream := range streams {
			for _, msg := range stream.Messages {
				entry := parseMessage(msg)
				if err := processWithRetry(ctx, rdb, entry, 0, log); err != nil {
					log.Error("message processing failed after retries", "id", msg.ID, "error", err)
				}
			}
		}
	}
}

// parseMessage converts a redis.XMessage into our typed streamEntry.
func parseMessage(m redis.XMessage) streamEntry {
	e := streamEntry{ID: m.ID}
	if v, ok := m.Values["type"]; ok {
		e.Type, _ = v.(string)
	}
	if v, ok := m.Values["url"]; ok {
		e.URL, _ = v.(string)
	}
	if v, ok := m.Values["user_id"]; ok {
		e.UserID, _ = v.(string)
	}
	if v, ok := m.Values["metadata"]; ok {
		e.Metadata, _ = v.(string)
	}
	if v, ok := m.Values["ts"]; ok {
		s, _ := v.(string)
		e.TS, _ = strconv.ParseInt(s, 10, 64)
	}
	return e
}

// processWithRetry tries to process the entry up to maxRetries times.
// On permanent failure it moves the message to the dead-letter stream and ACKs.
func processWithRetry(ctx context.Context, rdb *redis.Client, entry streamEntry, priorAttempts int, log *slog.Logger) error {
	var lastErr error
	for attempt := priorAttempts; attempt < maxRetries; attempt++ {
		if err := process(ctx, rdb, entry); err != nil {
			lastErr = err
			log.Warn("processing attempt failed", "id", entry.ID, "attempt", attempt+1, "error", err)
			time.Sleep(time.Duration(attempt+1) * 200 * time.Millisecond) // simple backoff
			continue
		}
		// Success — ACK the message so Redis removes it from the PEL.
		return ack(ctx, rdb, entry.ID, log)
	}

	// All retries exhausted — send to dead-letter stream, then ACK.
	log.Error("moving to dead-letter", "id", entry.ID, "error", lastErr)
	dlErr := deadLetter(ctx, rdb, entry, lastErr)
	ackErr := ack(ctx, rdb, entry.ID, log)
	if dlErr != nil {
		return fmt.Errorf("dead-letter write: %w", dlErr)
	}
	return ackErr
}

// process performs all Redis writes for a single event using a Pipeline.
//
// Pipeline:
//
//	Normally each Redis command involves one full network round-trip:
//	  client sends command → server executes → server replies → client reads.
//	A Pipeline sends N commands in one TCP write and receives N replies in one
//	TCP read. The commands execute sequentially on the server but we don't
//	read individual replies until all N are sent. This collapses N round-trips
//	into 1, which matters when each message triggers 5-6 Redis writes.
//	Note: Pipeline is NOT atomic. If you need atomicity, use MULTI/EXEC (a
//	transaction) or a Lua script. Here we don't need atomicity — a partial
//	update on crash is acceptable; the message stays in the PEL and gets
//	reprocessed.
func process(ctx context.Context, rdb *redis.Client, e streamEntry) error {
	now := time.Now()
	latencyMs := now.UnixMilli() - e.TS

	// Hour bucket for the 24h histogram, e.g. "2024-05-07T14"
	// We store one Sorted Set per hour key to keep things simple,
	// using ZINCRBY so multiple events in the same hour accumulate.
	hourBucket := now.UTC().Format("2006-01-02T15")
	hourlyKey := hourlyPrefix + hourBucket

	pipe := rdb.Pipeline()

	// --- stats:totals (Hash) ---
	// HINCRBY atomically adds delta to an integer field (creating it if needed).
	pipe.HIncrBy(ctx, statsTotalsKey, "total_events", 1)
	pipe.HIncrBy(ctx, statsTotalsKey, "type:"+e.Type, 1)
	pipe.HIncrBy(ctx, statsTotalsKey, "latency_sum", latencyMs)
	pipe.HIncrBy(ctx, statsTotalsKey, "latency_count", 1)
	pipe.HSet(ctx, statsTotalsKey, "last_updated", now.Unix())

	// --- leaderboard:pages (Sorted Set) ---
	// ZINCRBY <key> <increment> <member>
	// Adds increment to the member's score (or creates with that score).
	pipe.ZIncrBy(ctx, leaderPages, 1, e.URL)

	// --- leaderboard:users (Sorted Set) ---
	pipe.ZIncrBy(ctx, leaderUsers, 1, e.UserID)

	// --- stats:errors (Hash, only for error events) ---
	if e.Type == "error" {
		pipe.HIncrBy(ctx, statsErrorsKey, e.URL, 1)
	}

	// --- stats:hourly (Sorted Set, one per hour bucket) ---
	// We use a fixed member "count" and treat the score as the event count.
	// ZINCRBY is atomic and creates the member on first call.
	// The dashboard will ZRANGE over the last 24 hour-keys to build the histogram.
	pipe.ZIncrBy(ctx, hourlyKey, 1, "count")
	// Set expiry so old hourly keys don't accumulate forever (25h to be safe).
	pipe.Expire(ctx, hourlyKey, 25*time.Hour)

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("pipeline exec: %w", err)
	}
	return nil
}

// ack sends XACK to remove the message from the consumer group's PEL.
func ack(ctx context.Context, rdb *redis.Client, id string, log *slog.Logger) error {
	// XACK <stream> <group> <id>
	// Removes <id> from the Pending Entry List for <group>.
	// After this, the message is "done" from the group's perspective.
	if err := rdb.XAck(ctx, streamKey, groupName, id).Err(); err != nil {
		log.Error("xack failed", "id", id, "error", err)
		return fmt.Errorf("xack %s: %w", id, err)
	}
	return nil
}

// deadLetter appends the original message plus an error field to events:dead.
func deadLetter(ctx context.Context, rdb *redis.Client, e streamEntry, procErr error) error {
	errMsg := "unknown"
	if procErr != nil {
		errMsg = procErr.Error()
	}
	err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: deadLetterKey,
		ID:     "*",
		Values: []interface{}{
			"original_id", e.ID,
			"type", e.Type,
			"url", e.URL,
			"user_id", e.UserID,
			"metadata", e.Metadata,
			"ts", e.TS,
			"error", errMsg,
			"failed_at", time.Now().Unix(),
		},
	}).Err()
	if err != nil {
		return fmt.Errorf("xadd dead-letter: %w", err)
	}
	return nil
}

// metadataPreview is used only for logging; unused fields kept for completeness.
func metadataPreview(raw string) string {
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return raw
	}
	b, err := json.Marshal(m)
	if err != nil {
		return raw
	}
	if len(b) > 80 {
		return string(b[:80]) + "..."
	}
	return string(b)
}
