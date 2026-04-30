package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/redis/go-redis/v9"
)

const streamKey = "events:raw"

var pages = []string{"/home", "/about", "/pricing", "/blog", "/docs", "/login"}
var eventTypes = []string{"pageview", "pageview", "pageview", "api_call", "error"}
var userIDs = []string{"u1", "u2", "u3", "u4", "u5", "u42", "u99"}

func main() {
	// connects to redis. redis.NewClient returns a client struct that manages a connection pool internally so we dont need to manage connections ourselves

	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	defer rdb.Close()

	ctx := context.Background()

	// ping to confirm the connection works before we start
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("cannot connect to redis: %v", err)

	}
	fmt.Println("Prodcuer connected to redis. Emitting events....")

	for {
		// a random event mimicking what a real web server would log
		eventType := eventTypes[rand.Intn(len(eventTypes))]
		page := pages[rand.Intn(len(pages))]
		userID := userIDs[rand.Intn(len(userIDs))]
		latency := rand.Intn(500) + 10 		// i.e 10ms to 510 m
		
		// XADD is the Redis command to append a message to a stream.
		//
		// Anatomy of XADD:
		//   XADD  events:raw  *  field1 value1  field2 value2 ...
		//          ^stream     ^ID (let Redis auto-generate)
		//
		// The "*" tells Redis to generate the ID automatically using
		// the current millisecond timestamp. Each message is a flat
		// map of string key-value pairs — similar to a hash.
		//
		// MaxLen trims the stream to the last 10,000 entries so it
		// doesn't grow forever. The "~" makes the trim approximate
		// (faster) rather than exact.

		err := rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamKey,
			MaxLen: 10000,
			Approx: true,		// use "-" approximate trimming (faster)
			ID: "*",		// auto generate IDs
			Values: map[string]interface{}{
				"type": eventType,
				"url": page,
				"user_id": userID,
				"latency_ms": latency,
				"ts": time.Now().Unix(),	
			},
		}).Err()

		if err != nil {
			log.Printf("XADD error: %v", err)
		} else {
			fmt.Printf(" -> emitted [%s] %s user=%s latency=%dms\n",
				eventType, page, userID, latency)
		}

		time.Sleep(200*time.Millisecond)
	}
}