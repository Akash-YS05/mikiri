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
<title>Mikiri Analytics Dashboard</title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  :root {
    --bg: #0f1117;
    --surface: #1a1d27;
    --border: #2a2d3a;
    --accent: #6c63ff;
    --accent2: #00d4aa;
    --text: #e2e8f0;
    --muted: #718096;
    --red: #fc5c65;
    --bar-h: 18px;
  }
  body { background: var(--bg); color: var(--text); font-family: 'Inter', system-ui, sans-serif; padding: 24px; }
  h1 { font-size: 1.5rem; font-weight: 700; color: var(--accent); margin-bottom: 4px; }
  .subtitle { color: var(--muted); font-size: 0.85rem; margin-bottom: 24px; }
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 16px; margin-bottom: 24px; }
  .card { background: var(--surface); border: 1px solid var(--border); border-radius: 12px; padding: 20px; }
  .card-label { font-size: 0.75rem; color: var(--muted); text-transform: uppercase; letter-spacing: .06em; margin-bottom: 8px; }
  .card-value { font-size: 2rem; font-weight: 700; color: var(--accent); }
  .card-value.green { color: var(--accent2); }
  .section { background: var(--surface); border: 1px solid var(--border); border-radius: 12px; padding: 20px; margin-bottom: 20px; }
  .section-title { font-size: 0.9rem; font-weight: 600; color: var(--muted); margin-bottom: 16px; text-transform: uppercase; letter-spacing: .06em; }
  table { width: 100%; border-collapse: collapse; font-size: 0.875rem; }
  th { text-align: left; color: var(--muted); font-weight: 500; padding: 4px 8px 10px 0; border-bottom: 1px solid var(--border); }
  td { padding: 8px 8px 8px 0; border-bottom: 1px solid var(--border); vertical-align: middle; }
  tr:last-child td { border-bottom: none; }
  .bar-wrap { background: var(--border); border-radius: 4px; height: var(--bar-h); overflow: hidden; min-width: 80px; }
  .bar-fill { height: 100%; background: var(--accent); border-radius: 4px; transition: width .4s ease; }
  .bar-fill.green { background: var(--accent2); }
  .histogram { display: flex; align-items: flex-end; gap: 4px; height: 100px; }
  .h-bar-wrap { flex: 1; display: flex; flex-direction: column; align-items: center; gap: 4px; }
  .h-bar { width: 100%; background: var(--accent); border-radius: 3px 3px 0 0; transition: height .4s ease; min-height: 2px; }
  .h-label { font-size: 0.6rem; color: var(--muted); white-space: nowrap; }
  .types { display: flex; flex-wrap: wrap; gap: 8px; }
  .badge { padding: 4px 12px; border-radius: 20px; font-size: 0.8rem; font-weight: 600; }
  .badge.pageview { background: rgba(108,99,255,.2); color: var(--accent); }
  .badge.api_call  { background: rgba(0,212,170,.2); color: var(--accent2); }
  .badge.error     { background: rgba(252,92,101,.2); color: var(--red); }
  .badge.click     { background: rgba(255,200,80,.2); color: #ffc850; }
  #status { font-size: 0.75rem; color: var(--muted); margin-top: 16px; }
  .dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; background: var(--accent2); margin-right: 4px; animation: pulse 1.5s infinite; }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.4} }
  .two-col { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
  @media(max-width:700px) { .two-col { grid-template-columns: 1fr; } }
</style>
</head>
<body>
<h1>⚡ Mikiri Analytics</h1>
<p class="subtitle">Real-time event pipeline · Redis Streams</p>

<div class="grid">
  <div class="card">
    <div class="card-label">Total Events</div>
    <div class="card-value" id="total">—</div>
  </div>
  <div class="card">
    <div class="card-label">Avg Latency</div>
    <div class="card-value green" id="latency">—</div>
  </div>
  <div class="card">
    <div class="card-label">Pageviews</div>
    <div class="card-value" id="t-pageview">—</div>
  </div>
  <div class="card">
    <div class="card-label">API Calls</div>
    <div class="card-value" id="t-api_call">—</div>
  </div>
  <div class="card">
    <div class="card-label">Errors</div>
    <div class="card-value" style="color:var(--red)" id="t-error">—</div>
  </div>
  <div class="card">
    <div class="card-label">Clicks</div>
    <div class="card-value" id="t-click">—</div>
  </div>
</div>

<div class="section">
  <div class="section-title">Events last 24 hours</div>
  <div class="histogram" id="histogram"></div>
</div>

<div class="two-col">
  <div class="section">
    <div class="section-title">Top 10 Pages</div>
    <table>
      <thead><tr><th>URL</th><th>Hits</th><th style="width:120px"></th></tr></thead>
      <tbody id="pages-body"></tbody>
    </table>
  </div>
  <div class="section">
    <div class="section-title">Top 10 Users</div>
    <table>
      <thead><tr><th>User</th><th>Events</th><th style="width:120px"></th></tr></thead>
      <tbody id="users-body"></tbody>
    </table>
  </div>
</div>

<div class="section">
  <div class="section-title">Error URLs</div>
  <table>
    <thead><tr><th>URL</th><th>Count</th></tr></thead>
    <tbody id="errors-body"></tbody>
  </table>
</div>

<div id="status">Connecting…</div>

<script>
const fmt = n => Number(n).toLocaleString();

function renderLeader(tbodyId, rows, maxScore) {
  const tbody = document.getElementById(tbodyId);
  tbody.innerHTML = rows.map(r => {
    const pct = maxScore > 0 ? (r.score / maxScore * 100).toFixed(1) : 0;
    const cls = tbodyId === 'users-body' ? 'green' : '';
    return '<tr>' +
      '<td>' + escHtml(r.name) + '</td>' +
      '<td>' + fmt(r.score) + '</td>' +
      '<td><div class="bar-wrap"><div class="bar-fill ' + cls + '" style="width:' + pct + '%"></div></div></td>' +
      '</tr>';
  }).join('');
}

function escHtml(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

function renderHistogram(hourly) {
  const el = document.getElementById('histogram');
  if (!hourly || !hourly.length) return;
  const max = Math.max(...hourly.map(h => h.count), 1);
  el.innerHTML = hourly.map(h => {
    const pct = (h.count / max * 90).toFixed(1);
    return '<div class="h-bar-wrap">' +
      '<div class="h-bar" style="height:' + pct + 'px" title="' + h.hour + ': ' + h.count + '"></div>' +
      '<div class="h-label">' + h.hour.split(':')[0] + '</div>' +
      '</div>';
  }).join('');
}

function renderErrors(errMap) {
  const tbody = document.getElementById('errors-body');
  const entries = Object.entries(errMap || {}).sort((a,b) => b[1]-a[1]);
  if (!entries.length) { tbody.innerHTML = '<tr><td colspan="2" style="color:var(--muted)">No errors recorded</td></tr>'; return; }
  tbody.innerHTML = entries.map(([url, cnt]) =>
    '<tr><td>' + escHtml(url) + '</td><td>' + fmt(cnt) + '</td></tr>'
  ).join('');
}

function update(data) {
  document.getElementById('total').textContent   = fmt(data.total_events);
  document.getElementById('latency').textContent = (data.avg_latency_ms || 0).toFixed(1) + 'ms';
  ['pageview','api_call','error','click'].forEach(t => {
    const el = document.getElementById('t-' + t);
    if (el) el.textContent = fmt((data.by_type || {})[t] || 0);
  });

  const pages = data.top_pages || [];
  renderLeader('pages-body', pages, pages.length ? pages[0].score : 0);

  const users = data.top_users || [];
  renderLeader('users-body', users, users.length ? users[0].score : 0);

  renderHistogram(data.hourly);
  renderErrors(data.errors_by_url);

  const lu = data.last_updated ? new Date(data.last_updated * 1000).toLocaleTimeString() : '—';
  document.getElementById('status').innerHTML =
    '<span class="dot"></span>Live · last updated ' + lu;
}

const es = new EventSource('/live');
es.onmessage = e => { try { update(JSON.parse(e.data)); } catch(_){} };
es.onerror   = () => { document.getElementById('status').textContent = 'Reconnecting…'; };
</script>
</body>
</html>`
