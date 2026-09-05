package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// ---------------------------------------------------------------------------
// Per-backend atomic counters
// ---------------------------------------------------------------------------
type BackendStats struct {
	HTTPRequests  atomic.Int64
	WSConnections atomic.Int64
	ActiveWS      atomic.Int64
	Messages      atomic.Int64
	Errors        atomic.Int64
}

// ---------------------------------------------------------------------------
// Metrics collector — all latency slices protected by a single RWMutex
// ---------------------------------------------------------------------------
const maxSamples = 50_000

type MetricsCollector struct {
	startTime time.Time
	backends  []string

	// Global counters (atomic)
	totalHTTP     atomic.Int64
	totalWS       atomic.Int64
	activeWS      atomic.Int64
	totalMessages atomic.Int64
	totalErrors   atomic.Int64

	// Per-backend (map is read-only after init; values are atomic structs)
	perBackend map[string]*BackendStats

	// Latency samples — protected by latMu
	latMu   sync.Mutex
	httpLat []float64
	wsLat   []float64
	msgLat  []float64
}

func newMetrics(backends []string) *MetricsCollector {
	m := &MetricsCollector{
		startTime:  time.Now(),
		backends:   backends,
		perBackend: make(map[string]*BackendStats, len(backends)),
	}
	for _, b := range backends {
		m.perBackend[b] = &BackendStats{}
	}
	return m
}

func (m *MetricsCollector) appendLat(slice *[]float64, v float64) {
	*slice = append(*slice, v)
	if len(*slice) > maxSamples {
		*slice = (*slice)[len(*slice)-maxSamples:]
	}
}

func (m *MetricsCollector) RecordHTTP(backend string, ms float64) {
	m.totalHTTP.Add(1)
	if s, ok := m.perBackend[backend]; ok {
		s.HTTPRequests.Add(1)
	}
	m.latMu.Lock()
	m.appendLat(&m.httpLat, ms)
	m.latMu.Unlock()
}

func (m *MetricsCollector) RecordWSOpen(backend string, ms float64) {
	m.totalWS.Add(1)
	m.activeWS.Add(1)
	if s, ok := m.perBackend[backend]; ok {
		s.WSConnections.Add(1)
		s.ActiveWS.Add(1)
	}
	m.latMu.Lock()
	m.appendLat(&m.wsLat, ms)
	m.latMu.Unlock()
}

func (m *MetricsCollector) RecordWSClose(backend string) {
	m.activeWS.Add(-1)
	if s, ok := m.perBackend[backend]; ok {
		s.ActiveWS.Add(-1)
	}
}

func (m *MetricsCollector) RecordMessage(backend string, ms float64) {
	m.totalMessages.Add(1)
	if s, ok := m.perBackend[backend]; ok {
		s.Messages.Add(1)
	}
	m.latMu.Lock()
	m.appendLat(&m.msgLat, ms)
	m.latMu.Unlock()
}

func (m *MetricsCollector) RecordError(backend string) {
	m.totalErrors.Add(1)
	if s, ok := m.perBackend[backend]; ok {
		s.Errors.Add(1)
	}
}

// ---------------------------------------------------------------------------
// Percentile stats
// ---------------------------------------------------------------------------
type LatStats struct {
	Count int     `json:"count"`
	Avg   float64 `json:"avg_ms"`
	Min   float64 `json:"min_ms"`
	Max   float64 `json:"max_ms"`
	P50   float64 `json:"p50_ms"`
	P95   float64 `json:"p95_ms"`
	P99   float64 `json:"p99_ms"`
}

func pctStats(samples []float64) LatStats {
	if len(samples) == 0 {
		return LatStats{}
	}
	cp := make([]float64, len(samples))
	copy(cp, samples)
	sort.Float64s(cp)
	n := len(cp)

	sum := 0.0
	for _, v := range cp {
		sum += v
	}

	pct := func(p float64) float64 {
		idx := int(math.Ceil(p*float64(n))) - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= n {
			idx = n - 1
		}
		return round2(cp[idx])
	}

	return LatStats{
		Count: n,
		Avg:   round2(sum / float64(n)),
		Min:   round2(cp[0]),
		Max:   round2(cp[n-1]),
		P50:   pct(0.50),
		P95:   pct(0.95),
		P99:   pct(0.99),
	}
}

func round2(v float64) float64 {
	return math.Round(v*100) / 100
}

// ---------------------------------------------------------------------------
// Snapshot for JSON / dashboard
// ---------------------------------------------------------------------------
type BackendSnap struct {
	HTTPRequests int64 `json:"http_requests"`
	WSConns      int64 `json:"ws_connections"`
	ActiveWS     int64 `json:"active_ws"`
	Messages     int64 `json:"messages"`
	Errors       int64 `json:"errors"`
}

type Snapshot struct {
	UptimeSec     float64                `json:"uptime_s"`
	ThroughputRPS float64                `json:"throughput_rps"`
	TotalHTTP     int64                  `json:"total_http_requests"`
	TotalWS       int64                  `json:"total_ws_connections"`
	ActiveWS      int64                  `json:"active_ws_connections"`
	TotalMessages int64                  `json:"total_messages_proxied"`
	TotalErrors   int64                  `json:"total_errors"`
	PerBackend    map[string]BackendSnap `json:"per_backend"`
	HTTPLatency   LatStats               `json:"http_latency"`
	WSHandshake   LatStats               `json:"ws_handshake_latency"`
	MessageRelay  LatStats               `json:"message_relay_latency"`
}

func (m *MetricsCollector) Snapshot() Snapshot {
	uptime := time.Since(m.startTime).Seconds()
	totalReqs := m.totalHTTP.Load() + m.totalWS.Load()
	rps := 0.0
	if uptime > 0 {
		rps = round2(float64(totalReqs) / uptime)
	}

	pb := make(map[string]BackendSnap, len(m.backends))
	for _, b := range m.backends {
		s := m.perBackend[b]
		pb[b] = BackendSnap{
			HTTPRequests: s.HTTPRequests.Load(),
			WSConns:      s.WSConnections.Load(),
			ActiveWS:     s.ActiveWS.Load(),
			Messages:     s.Messages.Load(),
			Errors:       s.Errors.Load(),
		}
	}

	m.latMu.Lock()
	httpLat := pctStats(m.httpLat)
	wsLat := pctStats(m.wsLat)
	msgLat := pctStats(m.msgLat)
	m.latMu.Unlock()

	return Snapshot{
		UptimeSec:     round2(uptime),
		ThroughputRPS: rps,
		TotalHTTP:     m.totalHTTP.Load(),
		TotalWS:       m.totalWS.Load(),
		ActiveWS:      m.activeWS.Load(),
		TotalMessages: m.totalMessages.Load(),
		TotalErrors:   m.totalErrors.Load(),
		PerBackend:    pb,
		HTTPLatency:   httpLat,
		WSHandshake:   wsLat,
		MessageRelay:  msgLat,
	}
}

// ---------------------------------------------------------------------------
// Load Balancer
// ---------------------------------------------------------------------------
type LoadBalancer struct {
	backends []string
	rrIndex  atomic.Uint64
	metrics  *MetricsCollector
	upgrader websocket.Upgrader
}

func newLB(backends []string) *LoadBalancer {
	return &LoadBalancer{
		backends: backends,
		metrics:  newMetrics(backends),
		upgrader: websocket.Upgrader{
			CheckOrigin:     func(r *http.Request) bool { return true },
			ReadBufferSize:  4 * 1024 * 1024,
			WriteBufferSize: 4 * 1024 * 1024,
		},
	}
}

func (lb *LoadBalancer) nextBackend() string {
	idx := lb.rrIndex.Add(1) - 1
	return lb.backends[idx%uint64(len(lb.backends))]
}

// ---------------------------------------------------------------------------
// WebSocket reverse proxy
// ---------------------------------------------------------------------------
func (lb *LoadBalancer) handleWS(w http.ResponseWriter, r *http.Request) {
	backend := lb.nextBackend()
	wsURL := strings.Replace(backend, "http://", "ws://", 1)
	wsURL = strings.Replace(wsURL, "https://", "wss://", 1)
	wsURL = strings.TrimRight(wsURL, "/") + r.URL.RequestURI()

	log.Printf("WS  %-20s  →  %s", r.RemoteAddr, backend)

	// Upgrade the incoming client connection
	clientConn, err := lb.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WS upgrade error: %v", err)
		lb.metrics.RecordError(backend)
		return
	}
	defer clientConn.Close()

	// Dial the backend
	t0 := time.Now()
	dialer := websocket.Dialer{
		ReadBufferSize:  4 * 1024 * 1024,
		WriteBufferSize: 4 * 1024 * 1024,
	}
	backendConn, _, err := dialer.Dial(wsURL, nil)
	if err != nil {
		log.Printf("WS backend dial error (%s): %v", backend, err)
		lb.metrics.RecordError(backend)
		clientConn.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(1011, "backend unavailable"))
		return
	}
	defer backendConn.Close()

	handshakeMS := float64(time.Since(t0).Microseconds()) / 1000.0
	lb.metrics.RecordWSOpen(backend, handshakeMS)
	defer lb.metrics.RecordWSClose(backend)

	// Bidirectional relay
	var once sync.Once
	done := make(chan struct{})
	closeOnce := func() { once.Do(func() { close(done) }) }

	// Client → Backend
	go func() {
		defer closeOnce()
		for {
			mt, msg, err := clientConn.ReadMessage()
			if err != nil {
				return
			}
			t := time.Now()
			if err := backendConn.WriteMessage(mt, msg); err != nil {
				return
			}
			lb.metrics.RecordMessage(backend, float64(time.Since(t).Microseconds())/1000.0)
		}
	}()

	// Backend → Client
	go func() {
		defer closeOnce()
		for {
			mt, msg, err := backendConn.ReadMessage()
			if err != nil {
				return
			}
			t := time.Now()
			if err := clientConn.WriteMessage(mt, msg); err != nil {
				return
			}
			lb.metrics.RecordMessage(backend, float64(time.Since(t).Microseconds())/1000.0)
		}
	}()

	<-done
	log.Printf("WS  %-20s  ←  %s  (active: %d)", r.RemoteAddr, backend, lb.metrics.activeWS.Load())
}

// ---------------------------------------------------------------------------
// HTTP reverse proxy — wraps httputil.ReverseProxy with per-request backend
// ---------------------------------------------------------------------------

// responseRecorder captures the status code written by the proxy
type responseRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (rr *responseRecorder) WriteHeader(code int) {
	rr.statusCode = code
	rr.ResponseWriter.WriteHeader(code)
}

func (lb *LoadBalancer) handleHTTP(w http.ResponseWriter, r *http.Request) {
	backend := lb.nextBackend()
	target, err := url.Parse(backend)
	if err != nil {
		http.Error(w, "Bad gateway config", http.StatusBadGateway)
		return
	}

	t0 := time.Now()
	rr := &responseRecorder{ResponseWriter: w, statusCode: http.StatusOK}

	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		lb.metrics.RecordError(backend)
		log.Printf("HTTP proxy error (%s): %v", backend, err)
		http.Error(w, fmt.Sprintf("502 Bad Gateway — %s unavailable", backend), http.StatusBadGateway)
	}

	// Fix host header so backend serves the right content
	origDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		origDirector(req)
		req.Host = target.Host
	}

	proxy.ServeHTTP(rr, r)
	lb.metrics.RecordHTTP(backend, float64(time.Since(t0).Microseconds())/1000.0)
}

// ---------------------------------------------------------------------------
// ServeHTTP — router
// ---------------------------------------------------------------------------
func (lb *LoadBalancer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/lb/metrics":
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		io.WriteString(w, metricsHTML)
		return
	case "/lb/metrics.json":
		w.Header().Set("Content-Type", "application/json")
		snap := lb.metrics.Snapshot()
		json.NewEncoder(w).Encode(snap)
		return
	}

	// WebSocket upgrade detection
	if strings.ToLower(r.Header.Get("Upgrade")) == "websocket" {
		lb.handleWS(w, r)
		return
	}

	lb.handleHTTP(w, r)
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
func main() {
	backendsFlag := flag.String("backends",
		"http://10.1.75.79:3201,http://10.1.75.79:3202,http://10.1.75.79:3203,http://10.1.75.79:3204",
		"Comma-separated backend URLs")
	port := flag.Int("port", 4000, "Port to listen on")
	host := flag.String("host", "0.0.0.0", "Host to bind")
	flag.Parse()

	parts := strings.Split(*backendsFlag, ",")
	backends := make([]string, 0, len(parts))
	for _, b := range parts {
		b = strings.TrimSpace(strings.TrimRight(b, "/"))
		if b != "" {
			backends = append(backends, b)
		}
	}
	if len(backends) == 0 {
		log.Fatal("At least one backend URL is required (--backends)")
	}

	lb := newLB(backends)

	addr := fmt.Sprintf("%s:%d", *host, *port)
	log.Println("============================================================")
	log.Println("  GO LOAD BALANCER — Round-Robin")
	log.Printf("  Algorithm  : Round-Robin")
	log.Printf("  Backends   : %d", len(backends))
	for _, b := range backends {
		log.Printf("    • %s", b)
	}
	log.Printf("  Listening  : http://%s", addr)
	log.Printf("  Dashboard  : http://%s/lb/metrics", addr)
	log.Println("============================================================")

	srv := &http.Server{
		Addr:         addr,
		Handler:      lb,
		ReadTimeout:  60 * time.Second,
		WriteTimeout: 0, // 0 = no timeout (needed for long-lived WS)
		IdleTimeout:  120 * time.Second,
	}

	log.Fatal(srv.ListenAndServe())
}

// ---------------------------------------------------------------------------
// Metrics dashboard HTML (auto-refreshing every 2 s)
// ---------------------------------------------------------------------------
const metricsHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Go Load Balancer — Live Metrics</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body {
    font-family: 'Inter', system-ui, sans-serif;
    background: #0f172a; color: #e2e8f0;
    min-height: 100vh; padding: 2rem;
  }
  h1 {
    font-size: 1.6rem; font-weight: 700;
    background: linear-gradient(135deg, #38bdf8, #818cf8);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    margin-bottom: .3rem;
  }
  .subtitle { color: #64748b; font-size: .85rem; margin-bottom: 1.5rem; }
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 1rem; margin-bottom: 2rem; }
  .card {
    background: #1e293b; border-radius: 12px; padding: 1.2rem;
    border: 1px solid #334155; transition: border-color .2s;
  }
  .card:hover { border-color: #38bdf8; }
  .card .label { font-size: .75rem; color: #94a3b8; text-transform: uppercase; letter-spacing: .05em; }
  .card .value { font-size: 1.8rem; font-weight: 700; color: #f1f5f9; margin-top: .3rem; }
  .card .unit { font-size: .8rem; color: #64748b; font-weight: 400; }
  table { width: 100%; border-collapse: collapse; background: #1e293b; border-radius: 12px; overflow: hidden; border: 1px solid #334155; }
  th { background: #0f172a; font-size: .75rem; color: #94a3b8; text-transform: uppercase; letter-spacing: .05em; padding: .8rem 1rem; text-align: left; }
  td { padding: .7rem 1rem; border-top: 1px solid #1e293b; font-variant-numeric: tabular-nums; }
  tr:nth-child(even) td { background: rgba(255,255,255,.02); }
  .section-title { font-size: 1rem; font-weight: 600; margin: 1.5rem 0 .8rem; color: #cbd5e1; }
  .pill { display: inline-block; padding: .15rem .6rem; border-radius: 9999px; font-size: .7rem; font-weight: 600; }
  .pill-ok  { background: #065f4620; color: #34d399; border: 1px solid #34d39940; }
  .pill-err { background: #7f1d1d20; color: #f87171; border: 1px solid #f8717140; }
  #status { position: fixed; top: 1rem; right: 1rem; font-size: .75rem; color: #64748b; }
  .latency-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 1rem; }
  .go-badge { display:inline-block; background:#00add820; color:#00afd8; border:1px solid #00afd840; border-radius:6px; font-size:.7rem; font-weight:700; padding:.1rem .5rem; margin-left:.5rem; vertical-align:middle;}
</style>
</head>
<body>
<h1>⚖️ Go Load Balancer <span class="go-badge">Go</span></h1>
<p class="subtitle">Round-Robin · Auto-refreshes every 2 seconds</p>
<div id="status">connecting…</div>

<div class="grid" id="summary-cards"></div>

<h2 class="section-title">Per-Backend Distribution</h2>
<table id="backend-table">
  <thead><tr><th>Backend</th><th>HTTP Reqs</th><th>WS Conns</th><th>Active WS</th><th>Messages</th><th>Errors</th><th>Health</th></tr></thead>
  <tbody></tbody>
</table>

<h2 class="section-title">Latency Breakdown</h2>
<div class="latency-grid" id="latency-section"></div>

<script>
function card(label, value, unit) {
  return '<div class="card"><div class="label">'+label+'</div><div class="value">'+value+' <span class="unit">'+(unit||'')+'</span></div></div>';
}
function latCard(title, d) {
  if (!d || d.count === 0) return '<div class="card"><div class="label">'+title+'</div><div class="value">—</div></div>';
  return '<div class="card"><div class="label">'+title+' ('+d.count+' samples)</div>'
    +'<table style="margin-top:.6rem;font-size:.82rem;background:transparent;border:none;">'
    +'<tr><td style="border:none;padding:.2rem .5rem;color:#94a3b8">Avg</td><td style="border:none;padding:.2rem .5rem">'+d.avg_ms+' ms</td></tr>'
    +'<tr><td style="border:none;padding:.2rem .5rem;color:#94a3b8">P50</td><td style="border:none;padding:.2rem .5rem">'+d.p50_ms+' ms</td></tr>'
    +'<tr><td style="border:none;padding:.2rem .5rem;color:#94a3b8">P95</td><td style="border:none;padding:.2rem .5rem">'+d.p95_ms+' ms</td></tr>'
    +'<tr><td style="border:none;padding:.2rem .5rem;color:#94a3b8">P99</td><td style="border:none;padding:.2rem .5rem">'+d.p99_ms+' ms</td></tr>'
    +'<tr><td style="border:none;padding:.2rem .5rem;color:#94a3b8">Min</td><td style="border:none;padding:.2rem .5rem">'+d.min_ms+' ms</td></tr>'
    +'<tr><td style="border:none;padding:.2rem .5rem;color:#94a3b8">Max</td><td style="border:none;padding:.2rem .5rem">'+d.max_ms+' ms</td></tr>'
    +'</table></div>';
}
async function refresh() {
  try {
    const r = await fetch('/lb/metrics.json');
    const m = await r.json();
    document.getElementById('status').textContent = 'live · ' + new Date().toLocaleTimeString();
    document.getElementById('summary-cards').innerHTML = [
      card('Uptime', m.uptime_s, 's'),
      card('Throughput', m.throughput_rps, 'req/s'),
      card('HTTP Requests', m.total_http_requests),
      card('WS Connections', m.total_ws_connections, 'total'),
      card('Active WS', m.active_ws_connections, 'now'),
      card('Messages Proxied', m.total_messages_proxied),
      card('Errors', m.total_errors),
    ].join('');
    const tbody = document.querySelector('#backend-table tbody');
    tbody.innerHTML = Object.entries(m.per_backend).map(([b,d]) => {
      const health = d.errors===0
        ? '<span class="pill pill-ok">healthy</span>'
        : '<span class="pill pill-err">'+d.errors+' errors</span>';
      return '<tr><td>'+b+'</td><td>'+d.http_requests+'</td><td>'+d.ws_connections+'</td><td>'+d.active_ws+'</td><td>'+d.messages+'</td><td>'+d.errors+'</td><td>'+health+'</td></tr>';
    }).join('');
    document.getElementById('latency-section').innerHTML = [
      latCard('HTTP Latency', m.http_latency),
      latCard('WS Handshake', m.ws_handshake_latency),
      latCard('Message Relay', m.message_relay_latency),
    ].join('');
  } catch(e) {
    document.getElementById('status').textContent = 'error: '+e.message;
  }
}
refresh();
setInterval(refresh, 2000);
</script>
</body>
</html>`
