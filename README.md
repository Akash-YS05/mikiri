# mikiri

A production-quality real-time analytics pipeline built in Go, using Redis Streams as the sole communication layer between three independent binaries.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Redis Data Model](#redis-data-model)
- [Project Structure](#project-structure)
- [Binaries](#binaries)
  - [Producer](#producer)
  - [Consumer](#consumer)
  - [Dashboard](#dashboard)
- [Getting Started](#getting-started)
- [Configuration](#configuration)
- [API Reference](#api-reference)
- [Redis Concepts Used](#redis-concepts-used)
- [Extending the Project](#extending-the-project)

---

## Overview

mikiri is a three-service event analytics pipeline. Events are submitted to the **Producer** over HTTP, written to a **Redis Stream**, processed by the **Consumer** into aggregated statistics, and displayed in real time on the **Dashboard**.

The three binaries share no memory and hold no direct connections to each other. Redis is the only communication channel.

```
HTTP Client  →  Producer  →  Redis Stream  →  Consumer  →  Redis Hashes / Sorted Sets
                                                                      ↓
                                                               Dashboard (SSE)
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                          Docker Network                         │
│                                                                 │
│   ┌──────────────┐    XADD     ┌─────────────────────────────┐  │
│   │   Producer   │ ──────────► │      Redis                  │  │
│   │   :8081      │             │                             │  │
│   │              │             │  Streams                    │  │
│   │  POST /event │             │    events:raw               │  │
│   │  GET /health │             │    events:dead              │  │
│   └──────────────┘             │                             │  │
│                                │  Hashes                     │  │
│   ┌──────────────┐  XREADGROUP │    stats:totals             │  │ 
│   │   Consumer   │ ◄────────── │    stats:errors             │  │
│   │   (workers)  │             │                             │  │
│   │              │  XACK       │  Sorted Sets                │  │
│   │  N goroutines│ ──────────► │    leaderboard:pages        │  │
│   │  + PEL recov │             │    leaderboard:users        │  │
│   └──────────────┘             │    stats:hourly:*           │  │
│                                └─────────────────────────────┘  │
│   ┌──────────────┐    HGETALL            ▲                      │
│   │  Dashboard   │ ──────────────────────┘                      │
│   │   :8080      │    ZREVRANGE                                 │
│   │              │                                              │
│   │  GET /       │                                              │
│   │  GET /stats  │                                              │
│   │  GET /live   │                                              │
│   │  (SSE)       │                                              │
│   └──────────────┘                                              │
└─────────────────────────────────────────────────────────────────┘
```

---

## Redis Data Model

Understanding the Redis structures is key to understanding the whole system.

### Stream — `events:raw`

The append-only log that connects the Producer to the Consumer. Each entry is a flat key-value record.

```
events:raw
  1715000000000-0  →  type=pageview  url=/home  user_id=u-42  ts=1715000000000  metadata={...}
  1715000000001-0  →  type=api_call  url=/api/v1/items  user_id=u-7  ts=...
  1715000000002-0  →  type=error     url=/api/v1/pay    user_id=u-1  ts=...
  ...
```

Capped at ~100,000 entries with `MAXLEN ~` (approximate/lazy trimming).

### Stream — `events:dead`

Messages that failed processing after 3 retries land here with an extra `error` field. Never trimmed automatically — inspect manually with `redis-cli XRANGE events:dead - +`.

### Hash — `stats:totals`

A single hash holding all global counters. Updated by the Consumer with `HINCRBY`.

```
stats:totals
  total_events    →  18423
  type:pageview   →  9201
  type:api_call   →  6100
  type:error      →  812
  type:click      →  2310
  latency_sum     →  9834710
  latency_count   →  18423
  last_updated    →  1715000000
```

### Hash — `stats:errors`

Counts of error events keyed by URL.

```
stats:errors
  /api/v1/pay     →  401
  /api/v1/items   →  211
  /checkout       →  200
```

### Sorted Set — `leaderboard:pages`

Members are URLs; scores are hit counts. `ZINCRBY` atomically increments. `ZREVRANGE 0 9 WITHSCORES` gives the top 10.

```
leaderboard:pages
  /home           →  4201
  /api/v1/items   →  3800
  /dashboard      →  2100
  ...
```

### Sorted Set — `leaderboard:users`

Same structure, members are `user_id` values.

### Sorted Sets — `stats:hourly:<YYYY-MM-DDTHH>`

One key per UTC hour. Each has a single member `count` whose score is the total events for that hour. Keys expire after 25 hours automatically.

```
stats:hourly:2024-05-07T14   →  count: 3420
stats:hourly:2024-05-07T15   →  count: 4102
stats:hourly:2024-05-07T16   →  count: 1980
```

---

## Consumer Group & Pending Entry List

```
                     events:raw stream
  ┌──────┬──────┬──────┬──────┬──────┬──────┐
  │ id-1 │ id-2 │ id-3 │ id-4 │ id-5 │ id-6 │
  └──────┴──────┴──────┴──────┴──────┴──────┘
                              ▲
                        group cursor ("last delivered")

  pipeline-group PEL (Pending Entry List)
  ┌────────────────────────────────────────────┐
  │  id-4  →  worker-1-0  idle: 12s  retries:1 │  ← in-flight
  │  id-5  →  worker-1-1  idle: 63s  retries:2 │  ← stale, will be reclaimed
  └────────────────────────────────────────────┘

  On startup: XPENDING finds id-5 (idle > 60s) → XCLAIM → reprocess → XACK
  Normal flow: XREADGROUP ">" → deliver id-6 → process → XACK → removed from PEL
```

---

## Project Structure

```
mikiri/
├── Makefile
├── docker-compose.yml
├── .env.example
├── go.mod
├── go.sum
├── internal/
│   ├── config/
│   │   └── config.go          # env-based config, shared by all binaries
│   ├── redisclient/
│   │   └── client.go          # shared *redis.Client constructor
│   └── logger/
│       └── logger.go          # structured slog logger (JSON/text)
├── producer/
│   ├── Dockerfile
│   └── main.go                # HTTP server, validates + writes to stream
├── consumer/
│   ├── Dockerfile
│   └── main.go                # worker pool, XREADGROUP, PEL recovery, dead-letter
└── dashboard/
    ├── Dockerfile
    └── main.go                # HTTP + SSE server, reads Redis, serves HTML
```

---

## Binaries

### Producer

An HTTP server that accepts events and writes them to the Redis Stream.

**Responsibilities**
- Validate incoming JSON (required fields, enum check on `type`)
- Write to `events:raw` via `XADD` with auto ID and approximate max length
- Structured request logging via `slog`
- Graceful shutdown on `SIGTERM`/`SIGINT` with a 10s drain

**Event types accepted:** `pageview`, `api_call`, `error`, `click`

---

### Consumer

A worker pool that reads from the stream and aggregates statistics into Redis.

**Responsibilities**
- Create the consumer group on startup (handle `BUSYGROUP` gracefully)
- Run `XPENDING` recovery on startup — claim messages idle for >60s from crashed workers
- Run N parallel goroutines each doing `XREADGROUP COUNT 10 BLOCK 2s`
- Process each message: write to Hash and Sorted Set stats via a **Pipeline** (one round-trip per message)
- Retry failed messages up to 3 times with backoff
- Move permanently failed messages to `events:dead` then `XACK` the original
- Graceful shutdown: cancel context → workers finish in-flight batch → exit

**Message lifecycle:**

```
XADD (producer)
      │
      ▼
events:raw stream
      │
      ▼ XREADGROUP ">"
  Consumer worker
      │
      ├── success ──► pipeline writes to stats:* ──► XACK ──► removed from PEL
      │
      ├── failure (attempt < 3) ──► sleep, retry
      │
      └── failure (attempt == 3) ──► XADD events:dead ──► XACK
```

---

### Dashboard

An HTTP server that reads aggregated stats from Redis and serves them as JSON, SSE, and a self-contained HTML page.

**Responsibilities**
- Serve a self-contained HTML dashboard (no CDN, all inline CSS/JS)
- `GET /stats` — full JSON snapshot
- `GET /live` — SSE endpoint pushing a stats snapshot every 2 seconds
- Basic Auth middleware on all routes except `/health`
- Read-only access to Redis — no writes

**SSE flow:**

```
Browser                          Dashboard server                Redis
   │                                    │                          │
   │── GET /live ──────────────────────►│                          │
   │                                    │── pipeline HGETALL ─────►│
   │                                    │   ZREVRANGE              │
   │                                    │◄─ replies ───────────────│
   │◄── data: {...}\n\n ────────────────│                          │
   │                                    │   (every 2 seconds)      │
   │◄── data: {...}\n\n ────────────────│                          │
```

---

## Getting Started

**Prerequisites:** Docker and Docker Compose.

```bash
# 1. Clone and enter the project
git clone <your-repo>
cd mikiri

# 2. Create your env file
cp .env.example .env

# 3. Generate go.sum (only needed once)
docker run --rm -v "${PWD}:/app" -w /app golang:1.23-alpine go mod tidy

# 4. Start everything
make up

# 5. Wait ~15 seconds for health checks to pass, then send a test event
curl -X POST http://localhost:8081/event \
  -H "Content-Type: application/json" \
  -d '{"type":"pageview","url":"/home","user_id":"u-1","metadata":{"ref":"google"}}'

# 6. Open the dashboard
# http://localhost:8080  (login: admin / admin)
```

**Send a batch of test events:**

```bash
for i in $(seq 1 100); do
  curl -s -X POST http://localhost:8081/event \
    -H "Content-Type: application/json" \
    -d "{\"type\":\"pageview\",\"url\":\"/page-$((RANDOM % 5))\",\"user_id\":\"u-$((RANDOM % 10))\",\"metadata\":{}}" > /dev/null
done
```

---

## Configuration

All configuration is via environment variables. Copy `.env.example` to `.env` to get started.

---

## API Reference

### Producer — `:8081`

#### `POST /event`

Write an event to the pipeline.

**Request body:**
```json
{
  "type":     "pageview",
  "url":      "/home",
  "user_id":  "u-42",
  "metadata": { "referrer": "google" }
}
```

| Field | Required | Values |
|---|---|---|
| `type` | yes | `pageview`, `api_call`, `error`, `click` |
| `url` | yes | any non-empty string |
| `user_id` | yes | any non-empty string |
| `metadata` | no | any string→string map |

**Response `201`:**
```json
{ "id": "1715000000000-0" }
```

#### `GET /health`

```json
{ "status": "ok" }
```

---

### Dashboard — `:8080`

All routes except `/health` require HTTP Basic Auth.

#### `GET /stats`

Full stats snapshot as JSON.

```json
{
  "total_events": 18423,
  "by_type": {
    "pageview": 9201,
    "api_call": 6100,
    "error":    812,
    "click":    2310
  },
  "avg_latency_ms": 14.3,
  "top_pages": [
    { "name": "/home", "score": 4201 }
  ],
  "top_users": [
    { "name": "u-42", "score": 980 }
  ],
  "errors_by_url": {
    "/api/v1/pay": 401
  },
  "hourly": [
    { "hour": "14:00", "count": 3420 },
    { "hour": "15:00", "count": 4102 }
  ],
  "last_updated": 1715000000
}
```

#### `GET /live`

Server-Sent Events stream. Pushes a `/stats`-shaped JSON payload every 2 seconds.

```
Content-Type: text/event-stream

data: {"total_events":18423,...}

data: {"total_events":18431,...}
```

#### `GET /`

Self-contained HTML dashboard. No external dependencies.

#### `GET /health`

```json
{ "status": "ok" }
```

---

## Redis Concepts Used

| Concept | Command(s) | Where |
|---|---|---|
| **Streams** — append-only log with auto IDs | `XADD`, `XLEN`, `XRANGE` | Producer writes, Consumer reads |
| **Consumer Groups** — shared cursor with per-consumer delivery tracking | `XGROUP CREATE`, `XREADGROUP`, `XACK` | Consumer |
| **Pending Entry List (PEL)** — tracks unacknowledged messages | `XPENDING`, `XCLAIM` | Consumer startup recovery |
| **Dead-letter stream** — permanent failure sink | `XADD events:dead` | Consumer after 3 retries |
| **Hashes** — flat key→value store with atomic integer increment | `HINCRBY`, `HSET`, `HGETALL` | Consumer writes, Dashboard reads |
| **Sorted Sets** — scored members, ranked retrieval | `ZINCRBY`, `ZREVRANGE`, `ZRANGE`, `ZSCORE` | Consumer writes leaderboards + histogram, Dashboard reads |
| **Pipelining** — batch multiple commands into one round-trip | `Pipeline()` + `Exec()` | Consumer per-message writes, Dashboard snapshot reads |
| **Key expiry** — automatic TTL-based cleanup | `EXPIRE` | Consumer sets 25h TTL on hourly keys |
| **Connection pool** — reuse TCP connections across goroutines | `PoolSize` in client options | All binaries via shared redisclient |

---

## Inspecting the Pipeline with redis-cli

```bash
make redis-cli
```

```bash
# How many events are in the stream?
XLEN events:raw

# See the last 5 entries
XREVRANGE events:raw + - COUNT 5

# What's in the consumer group?
XINFO GROUPS events:raw

# Are any messages stuck in the PEL?
XPENDING events:raw pipeline-group - + 10

# Global stats
HGETALL stats:totals

# Top 5 pages
ZREVRANGE leaderboard:pages 0 4 WITHSCORES

# Top 5 users
ZREVRANGE leaderboard:users 0 4 WITHSCORES

# Error counts by URL
HGETALL stats:errors

# Dead-letter messages
XRANGE events:dead - +

# Current hour's event count
ZSCORE stats:hourly:2024-05-07T15 count
```

---

## Extending the Project

Some directions for taking this further:

- **Rate limiting** — sliding window counter per `user_id` using a Sorted Set (`ZADD` / `ZREMRANGEBYSCORE` / `ZCARD`)
- **Pub/Sub alerts** — `PUBLISH` to an alerts channel when error rate crosses a threshold; a separate subscriber binary reacts
- **Atomic Lua scripts** — replace the pipeline in `process()` with `EVAL` for true atomicity (all-or-nothing stat updates)
- **Deduplication** — use `WATCH` / `MULTI` / `EXEC` on a `seen:<event_id>` key to prevent double-processing on replay
- **Stream replay** — a fourth binary that reads `events:raw` via plain `XREAD` (no group) from any given ID, demonstrating streams as a replayable log
- **Prometheus metrics** — expose `/metrics` from the dashboard, scraping the same Redis keys; add Grafana to `docker-compose.yml`
- **Distributed lock** — use the `redislock` package (already in `go.mod`) to elect one consumer as hourly aggregator
- **Integration tests** — spin up real Redis with `testcontainers-go`, assert Hash and Sorted Set values after sending known events end-to-end
