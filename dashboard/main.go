// It serves:
//   GET /        — self-contained HTML dashboard (no CDN)
//   GET /stats   — JSON snapshot of all aggregated stats
//   GET /live    — Server-Sent Events stream pushing stats every 2s
//   GET /health  — liveness probe (unauthenticated)
//
// All routes except /health are protected by HTTP Basic Auth.
//
// Redis reads use plain GET / HGETALL / ZREVRANGE — no writes happen here.

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"mikiri/internal/config"
	"mikiri/internal/logger"
	"mikiri/internal/redisclient"

	"github.com/redis/go-redis/v9"
)

//redis key constants, must match consumer
const (
	statsTotalsKey = "stats:totals"
	statsErrorsKey = "stats:errors"
	leaderPages    = "leaderboard:pages"
	leaderUsers    = "leaderboard:users"
	hourlyPrefix   = "stats:hourly:"
)

//full stats payload returned by /stats and pushed on /live
type StatsSnapshot struct {
	TotalEvents  int64            `json:"total_events"`
	ByType       map[string]int64 `json:"by_type"`
	AvgLatencyMs float64          `json:"avg_latency_ms"`
	TopPages     []LeaderEntry    `json:"top_pages"`
	TopUsers     []LeaderEntry    `json:"top_users"`
	ErrorsByURL  map[string]int64 `json:"errors_by_url"`
	Hourly       []HourlyBucket   `json:"hourly"`
	LastUpdated  int64            `json:"last_updated"`
}

//represents one row in leaderboard sorted set
type LeaderEntry struct {
	Name  string  `json:"name"`
	Score float64 `json:"score"`
}

type dashServer struct {
	rdb *redis.Client
	log *slog.Logger
	cfg *config.Config
}

type HourlyBucket struct {
	Hour  string `json:"hour"`
	Count int64  `json:"count"`
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

	ds := &dashServer{rdb: rdb, log: log, cfg: cfg}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", ds.handleHealth)
	mux.Handle("GET /stats", ds.basicAuth(http.HandlerFunc(ds.handleStats)))
	mux.Handle("GET /live", ds.basicAuth(http.HandlerFunc(ds.handleLive)))
	mux.Handle("GET /", ds.basicAuth(http.HandlerFunc(ds.handleDashboard)))

	httpServer := &http.Server{
		Addr:         ":" + cfg.DashboardPort,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 0, // SSE needs unlimited write time
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Info("dashboard listening", "port", cfg.DashboardPort)
		if err := httpServer.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			log.Error("dashboard server error", "error", err)
			os.Exit(1)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	<-quit

	log.Info("shutting down dashboard")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(ctx); err != nil {
		log.Error("shutdown error", "error", err)
	}
	log.Info("dashboard stopped")
}

//middleware to enforce basic authentication
func (ds *dashServer) basicAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || user != ds.cfg.DashboardUser || pass != ds.cfg.DashboardPassword {
			w.Header().Set("WWW-Authenticate", `Basic realm="dashboard"`)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (ds *dashServer) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write([]byte(`{"status":"ok"}`)); err != nil {
		ds.log.Warn("health write failed", "error", err)
	}
}

func (ds *dashServer) handleStats(w http.ResponseWriter, r *http.Request) {
	snap, err := ds.snapshot(r.Context())
	if err != nil {
		ds.log.Error("stats fetch failed", "error", err)
		http.Error(w, `{"error":"failed to read stats"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(snap); err != nil {
		ds.log.Warn("stats encode failed", "error", err)
	}
}

// handleLive implements Server-Sent Events (SSE).
//
// SSE is a simple protocol: the server sets Content-Type to text/event-stream
// and writes lines in the format:
//   data: <payload>\n\n
// The browser's EventSource API reconnects automatically if the connection drops.
// We flush after every event so the browser receives it immediately (HTTP response buffering would otherwise hold the data until the buffer is full).

func (ds *dashServer) handleLive(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // tell nginx not to buffer SSE

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			snap, err := ds.snapshot(r.Context())
			if err != nil {
				ds.log.Warn("live snapshot failed", "error", err)
				continue
			}
			b, err := json.Marshal(snap)
			if err != nil {
				ds.log.Warn("live marshal failed", "error", err)
				continue
			}
			if _, err := fmt.Fprintf(w, "data: %s\n\n", b); err != nil {
				// Client disconnected.
				return
			}
			flusher.Flush()
		}
	}
}

//snapshot reads all the stats data from redis in one pipelined batch
func (ds *dashServer) snapshot(ctx context.Context) (*StatsSnapshot, error) {
	// Redis Concept — Pipeline for reads:
	//   Same principle as write pipelines: we send all GET/HGETALL/ZREVRANGE
	//   commands at once and receive all replies in one round-trip.
	//   Each cmd is a *redis.Cmd (or typed variant) whose .Result() we call
	//   AFTER pipe.Exec(). Before Exec the results are not yet populated.
	pipe := ds.rdb.Pipeline()

	totalsCmd := pipe.HGetAll(ctx, statsTotalsKey)
	pagesCmd := pipe.ZRevRangeWithScores(ctx, leaderPages, 0, 9)
	usersCmd := pipe.ZRevRangeWithScores(ctx, leaderUsers, 0, 9)
	errorsCmd := pipe.HGetAll(ctx, statsErrorsKey)

	// Build 24 hourly keys (last 24 hours including current).
	now := time.Now().UTC()
	hourCmds := make([]*redis.StringSliceCmd, 24)
	for i := 0; i < 24; i++ {
		t := now.Add(-time.Duration(23-i) * time.Hour)
		key := hourlyPrefix + t.Format("2006-01-02T15")
		// ZRANGE key 0 -1 returns all members; we only have "count" per key.
		hourCmds[i] = pipe.ZRange(ctx, key, 0, -1)
	}

	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("pipeline exec: %w", err)
	}

	// --- Parse totals Hash ---
	totals, err := totalsCmd.Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("totals: %w", err)
	}

	snap := &StatsSnapshot{
		ByType:      make(map[string]int64),
		ErrorsByURL: make(map[string]int64),
	}

	snap.TotalEvents = parseInt64(totals["total_events"])
	for _, t := range []string{"pageview", "api_call", "error", "click"} {
		if v, ok := totals["type:"+t]; ok {
			snap.ByType[t] = parseInt64(v)
		}
	}
	snap.LastUpdated = parseInt64(totals["last_updated"])

	latSum := parseInt64(totals["latency_sum"])
	latCount := parseInt64(totals["latency_count"])
	if latCount > 0 {
		snap.AvgLatencyMs = math.Round(float64(latSum)/float64(latCount)*100) / 100
	}

	// --- Parse leaderboards ---
	pages, err := pagesCmd.Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("pages leaderboard: %w", err)
	}
	for _, z := range pages {
		snap.TopPages = append(snap.TopPages, LeaderEntry{Name: z.Member.(string), Score: z.Score})
	}

	users, err := usersCmd.Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("users leaderboard: %w", err)
	}
	for _, z := range users {
		snap.TopUsers = append(snap.TopUsers, LeaderEntry{Name: z.Member.(string), Score: z.Score})
	}

	// --- Parse errors Hash ---
	errMap, err := errorsCmd.Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("errors hash: %w", err)
	}
	for k, v := range errMap {
		snap.ErrorsByURL[k] = parseInt64(v)
	}

	// --- Parse hourly histogram ---
	// Redis Concept — ZRANGE returns members sorted by score ascending.
	// Each hourly key has exactly one member "count" whose score is the
	// event count for that hour. We retrieve the score via ZRANGEWITHSCORES.
	for i := 0; i < 24; i++ {
		t := now.Add(-time.Duration(23-i) * time.Hour)
		label := t.Format("15:00")

		members, err := hourCmds[i].Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return nil, fmt.Errorf("hourly[%d]: %w", i, err)
		}
		var count int64
		if len(members) > 0 {
			// We need the score not the member; fetch it separately via ZSCORE.
			// This is a slight limitation of our pipeline approach for hourly —
			// acceptable for 24 small keys.
			key := hourlyPrefix + t.Format("2006-01-02T15")
			scoreRes, err := ds.rdb.ZScore(ctx, key, "count").Result()
			if err != nil && !errors.Is(err, redis.Nil) {
				return nil, fmt.Errorf("zscore hourly: %w", err)
			}
			count = int64(scoreRes)
		}
		snap.Hourly = append(snap.Hourly, HourlyBucket{Hour: label, Count: count})
	}

	return snap, nil
}

func parseInt64(s string) int64 {
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}

// handleDashboard serves the self-contained HTML+JS dashboard.
// Everything is inline — no external CDN calls, no external JS or CSS files.
func (ds *dashServer) handleDashboard(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if _, err := w.Write([]byte(dashboardHTML)); err != nil {
		ds.log.Warn("dashboard write failed", "error", err)
	}
}

// dashboardHTML is the complete single-page dashboard.
// It uses EventSource to connect to /live and updates the DOM on each event.
// Pure CSS bars are used for the hit charts — no canvas, no libraries.
const dashboardHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Mikiri Analytics</title>

<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">

<style>
* { box-sizing: border-box; margin: 0; padding: 0; }

:root {
  --bg: radial-gradient(circle at 20% 0%, #1a1d27, #0f1117 60%);
  --surface: rgba(26,29,39,0.6);
  --border: rgba(255,255,255,0.06);
  --accent: #6c63ff;
  --accent2: #00d4aa;
  --text: #e6edf3;
  --muted: #8b93a7;
  --red: #ff5d5d;
}

body {
  font-family: 'Inter', sans-serif;
  background: var(--bg);
  color: var(--text);
  padding: 32px;
}

.header {
  display:flex;
  justify-content:space-between;
  align-items:center;
  margin-bottom:32px;
}

h1 { font-size:1.8rem; font-weight:700; }

.subtitle {
  font-size:0.9rem;
  color: var(--muted);
  margin-top:4px;
}

.live-pill {
  padding:6px 14px;
  border-radius:999px;
  background: rgba(0,212,170,.1);
  color: var(--accent2);
  font-size:0.75rem;
  font-weight:600;
}

.grid {
  display:grid;
  grid-template-columns: repeat(auto-fit,minmax(180px,1fr));
  gap:18px;
  margin-bottom:28px;
}

.card {
  background: var(--surface);
  backdrop-filter: blur(16px);
  border:1px solid var(--border);
  border-radius:14px;
  padding:20px;
  transition: all .25s ease;
}

.card:hover { transform: translateY(-4px); }

.card-label {
  font-size:0.75rem;
  color: var(--muted);
  margin-bottom:8px;
}

.card-value {
  font-size:2rem;
  font-weight:700;
}

.green { color: var(--accent2); }
.red { color: var(--red); }

.section {
  background: var(--surface);
  backdrop-filter: blur(16px);
  border:1px solid var(--border);
  border-radius:14px;
  padding:22px;
  margin-bottom:22px;
}

.section-title {
  font-size:0.8rem;
  text-transform:uppercase;
  letter-spacing:.08em;
  color:var(--muted);
  margin-bottom:16px;
}

table { width:100%; border-collapse:collapse; }

td, th { padding:10px 0; }

th {
  font-size:0.75rem;
  color:var(--muted);
  font-weight:500;
}

tr { border-bottom:1px solid var(--border); }
tr:last-child { border:none; }

.bar-wrap {
  background: rgba(255,255,255,0.05);
  border-radius:6px;
  height:10px;
}

.bar-fill {
  height:100%;
  border-radius:6px;
  background: linear-gradient(90deg, var(--accent), #9d8cff);
}

.bar-fill.green {
  background: linear-gradient(90deg, var(--accent2), #00ffcc);
}

.histogram {
  display:flex;
  gap:6px;
  height:120px;
  align-items:flex-end;
}

.h-bar {
  flex:1;
  background: linear-gradient(180deg, var(--accent), transparent);
  border-radius:4px 4px 0 0;
}

.two-col {
  display:grid;
  grid-template-columns:1fr 1fr;
  gap:20px;
}

@media(max-width:800px){
  .two-col { grid-template-columns:1fr; }
}

#status {
  font-size:0.75rem;
  color:var(--muted);
  margin-top:12px;
}

.dot {
  width:8px;height:8px;border-radius:50%;
  background:var(--accent2);
  display:inline-block;
  margin-right:6px;
  animation:pulse 1.5s infinite;
}

@keyframes pulse {
  0%,100%{opacity:1}
  50%{opacity:.4}
}
</style>
</head>

<body>

<div class="header">
  <div>
    <h1>Mikiri Analytics</h1>
    <div class="subtitle">Real-time event intelligence</div>
  </div>
  <div class="live-pill"><span class="dot"></span>LIVE</div>
</div>

<div class="grid">
  <div class="card"><div class="card-label">Total Events</div><div class="card-value" id="total">—</div></div>
  <div class="card"><div class="card-label">Avg Latency</div><div class="card-value green" id="latency">—</div></div>
  <div class="card"><div class="card-label">Pageviews</div><div class="card-value" id="t-pageview">—</div></div>
  <div class="card"><div class="card-label">API Calls</div><div class="card-value" id="t-api_call">—</div></div>
  <div class="card"><div class="card-label">Errors</div><div class="card-value red" id="t-error">—</div></div>
  <div class="card"><div class="card-label">Clicks</div><div class="card-value" id="t-click">—</div></div>
</div>

<div class="section">
  <div class="section-title">Events (24h)</div>
  <div class="histogram" id="histogram"></div>
</div>

<div class="two-col">
  <div class="section">
    <div class="section-title">Top Pages</div>
    <table><tbody id="pages-body"></tbody></table>
  </div>
  <div class="section">
    <div class="section-title">Top Users</div>
    <table><tbody id="users-body"></tbody></table>
  </div>
</div>

<div class="section">
  <div class="section-title">Error URLs</div>
  <table><tbody id="errors-body"></tbody></table>
</div>

<div id="status">Connecting...</div>

<script>
function fmt(n){ return Number(n).toLocaleString(); }

function escHtml(s){
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;');
}

function renderLeader(id, rows, max){
  var el = document.getElementById(id);
  var html = '';
  for (var i=0;i<rows.length;i++){
    var r = rows[i];
    var pct = max ? (r.score/max*100) : 0;
    html += '<tr>' +
      '<td>' + escHtml(r.name) + '</td>' +
      '<td>' + fmt(r.score) + '</td>' +
      '<td><div class="bar-wrap"><div class="bar-fill ' + (id==='users-body'?'green':'') + '" style="width:' + pct + '%"></div></div></td>' +
      '</tr>';
  }
  el.innerHTML = html;
}

function renderHistogram(data){
  var el = document.getElementById('histogram');
  if(!data) return;
  var max = 1;
  for (var i=0;i<data.length;i++){
    if (data[i].count > max) max = data[i].count;
  }
  var html = '';
  for (var i=0;i<data.length;i++){
    var h = data[i].count/max*100;
    html += '<div class="h-bar" style="height:' + h + '%"></div>';
  }
  el.innerHTML = html;
}

function renderErrors(map){
  var el = document.getElementById('errors-body');
  var html = '';
  for (var k in map){
    html += '<tr><td>' + escHtml(k) + '</td><td>' + fmt(map[k]) + '</td></tr>';
  }
  el.innerHTML = html;
}

function update(d){
  document.getElementById('total').textContent = fmt(d.total_events);
  document.getElementById('latency').textContent = (d.avg_latency_ms||0).toFixed(1)+'ms';

  var types = ['pageview','api_call','error','click'];
  for (var i=0;i<types.length;i++){
    var t = types[i];
    document.getElementById('t-'+t).textContent = fmt((d.by_type||{})[t]||0);
  }

  renderLeader('pages-body', d.top_pages||[], d.top_pages && d.top_pages[0] ? d.top_pages[0].score : 0);
  renderLeader('users-body', d.top_users||[], d.top_users && d.top_users[0] ? d.top_users[0].score : 0);
  renderHistogram(d.hourly);
  renderErrors(d.errors_by_url);

  document.getElementById('status').innerHTML =
    '<span class="dot"></span>Live - ' + new Date().toLocaleTimeString();
}

var es = new EventSource('/live');
es.onmessage = function(e){ try { update(JSON.parse(e.data)); } catch(err){} };
es.onerror = function(){ document.getElementById('status').textContent = 'Reconnecting...'; };
</script>

</body>
</html>`