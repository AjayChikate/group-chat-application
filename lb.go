package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// BackendNode represents a single chat backend server
type BackendNode struct {
	HostPort          string
	URL               *url.URL
	Proxy             *httputil.ReverseProxy
	IsHealthy         atomic.Bool
	ActiveRequests    atomic.Int64
	ActiveWS          atomic.Int64
	TotalRequests     atomic.Int64
	TotalErrors       atomic.Int64
	ConsecutiveErrors atomic.Int64

	mu           sync.RWMutex
	EmaLatencyMs float64
}

func (b *BackendNode) LoadMetric() float64 {
	req := float64(b.ActiveRequests.Load())
	ws := float64(b.ActiveWS.Load())
	return req + (ws * 0.5)
}

func (b *BackendNode) RecordSuccess(duration time.Duration) {
	latencyMs := float64(duration.Microseconds()) / 1000.0
	b.TotalRequests.Add(1)
	b.ConsecutiveErrors.Store(0)
	b.IsHealthy.Store(true)

	b.mu.Lock()
	if b.EmaLatencyMs == 0.0 {
		b.EmaLatencyMs = latencyMs
	} else {
		b.EmaLatencyMs = 0.8*b.EmaLatencyMs + 0.2*latencyMs
	}
	b.mu.Unlock()
}

func (b *BackendNode) RecordError() {
	b.TotalRequests.Add(1)
	b.TotalErrors.Add(1)
	fails := b.ConsecutiveErrors.Add(1)
	if fails >= 3 {
		b.IsHealthy.Store(false)
	}
}

func (b *BackendNode) GetLatency() float64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.EmaLatencyMs
}

// DynamicLoadBalancer manages routing and health monitoring
type DynamicLoadBalancer struct {
	Backends       []*BackendNode
	Threshold      float64
	HealthInterval time.Duration
	CurrentBackend atomic.Pointer[BackendNode]
	Transport      *http.Transport
	HTTPClient     *http.Client
}

func NewDynamicLoadBalancer(backendAddrs []string, threshold float64, healthInterval time.Duration) *DynamicLoadBalancer {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     false,
		MaxIdleConns:          10000,
		MaxIdleConnsPerHost:   2000,
		MaxConnsPerHost:       0, // unlimited
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		DisableCompression:    true,
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   4 * time.Second,
	}

	lb := &DynamicLoadBalancer{
		Threshold:      threshold,
		HealthInterval: healthInterval,
		Transport:      transport,
		HTTPClient:     client,
	}

	for _, addr := range backendAddrs {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}
		if !strings.HasPrefix(addr, "http://") && !strings.HasPrefix(addr, "https://") {
			addr = "http://" + addr
		}
		targetURL, err := url.Parse(addr)
		if err != nil {
			log.Fatalf("Invalid backend address %s: %v", addr, err)
		}

		node := &BackendNode{
			HostPort: targetURL.Host,
			URL:      targetURL,
		}
		node.IsHealthy.Store(true)

		proxy := httputil.NewSingleHostReverseProxy(targetURL)
		proxy.Transport = transport
		proxy.FlushInterval = 50 * time.Millisecond

		origDirector := proxy.Director
		proxy.Director = func(req *http.Request) {
			origDirector(req)
			req.Header.Set("X-Served-By-Backend", node.HostPort)
			req.Host = targetURL.Host
		}

		proxy.ErrorHandler = func(w http.ResponseWriter, req *http.Request, err error) {
			node.RecordError()
			w.WriteHeader(http.StatusBadGateway)
			fmt.Fprintf(w, "Bad Gateway: backend %s error: %v", node.HostPort, err)
		}

		proxy.ModifyResponse = func(resp *http.Response) error {
			if resp.StatusCode >= 500 {
				node.RecordError()
			} else {
				node.RecordSuccess(5 * time.Millisecond)
			}
			return nil
		}

		node.Proxy = proxy
		lb.Backends = append(lb.Backends, node)
	}

	if len(lb.Backends) > 0 {
		lb.CurrentBackend.Store(lb.Backends[0])
	}

	return lb
}

func (lb *DynamicLoadBalancer) SelectBackend() *BackendNode {
	var healthy []*BackendNode
	for _, b := range lb.Backends {
		if b.IsHealthy.Load() {
			healthy = append(healthy, b)
		}
	}

	if len(healthy) == 0 {
		if len(lb.Backends) == 0 {
			return nil
		}
		best := lb.Backends[0]
		minLoad := best.LoadMetric()
		for _, b := range lb.Backends[1:] {
			if l := b.LoadMetric(); l < minLoad {
				minLoad = l
				best = b
			}
		}
		return best
	}

	current := lb.CurrentBackend.Load()
	if current != nil && current.IsHealthy.Load() {
		if current.LoadMetric() < lb.Threshold {
			return current
		}
	}

	best := healthy[0]
	bestLoad := best.LoadMetric()
	bestLatency := best.GetLatency()

	for _, b := range healthy[1:] {
		l := b.LoadMetric()
		lat := b.GetLatency()
		if l < bestLoad || (l == bestLoad && lat < bestLatency) {
			best = b
			bestLoad = l
			bestLatency = lat
		}
	}

	lb.CurrentBackend.Store(best)
	return best
}

func (lb *DynamicLoadBalancer) StartHealthChecks(ctx context.Context) {
	ticker := time.NewTicker(lb.HealthInterval)
	go func() {
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return
			case <-ticker.C:
				for _, node := range lb.Backends {
					go func(n *BackendNode) {
						healthURL := fmt.Sprintf("http://%s/health", n.HostPort)
						resp, err := lb.HTTPClient.Get(healthURL)
						if err == nil && resp.StatusCode == 200 {
							io.Copy(io.Discard, resp.Body)
							resp.Body.Close()
							n.ConsecutiveErrors.Store(0)
							n.IsHealthy.Store(true)
						} else {
							if resp != nil {
								resp.Body.Close()
							}
							fails := n.ConsecutiveErrors.Add(1)
							if fails >= 3 {
								n.IsHealthy.Store(false)
							}
						}
					}(node)
				}
			}
		}
	}()
}

func isWebSocketRequest(r *http.Request) bool {
	if strings.HasPrefix(r.URL.Path, "/ws") {
		return true
	}
	containsUpgrade := false
	for _, v := range strings.Split(r.Header.Get("Connection"), ",") {
		if strings.EqualFold(strings.TrimSpace(v), "upgrade") {
			containsUpgrade = true
			break
		}
	}
	return containsUpgrade && strings.EqualFold(r.Header.Get("Upgrade"), "websocket")
}

func (lb *DynamicLoadBalancer) handleWebSocket(backend *BackendNode, w http.ResponseWriter, r *http.Request) {
	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "WebSocket hijacking not supported", http.StatusInternalServerError)
		return
	}

	clientConn, clientBuf, err := hj.Hijack()
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	defer clientConn.Close()

	backendConn, err := net.DialTimeout("tcp", backend.HostPort, 5*time.Second)
	if err != nil {
		backend.RecordError()
		log.Printf("[WS Proxy] Failed to connect to backend %s: %v", backend.HostPort, err)
		return
	}
	defer backendConn.Close()

	// Build raw HTTP request to backend preserving all headers (especially Upgrade and Connection)
	reqURI := r.RequestURI
	if reqURI == "" {
		reqURI = r.URL.RequestURI()
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%s %s HTTP/1.1\r\n", r.Method, reqURI)
	fmt.Fprintf(&b, "Host: %s\r\n", backend.HostPort)
	hasUpgrade := false
	hasConnection := false
	for k, vv := range r.Header {
		if strings.EqualFold(k, "Host") {
			continue
		}
		if strings.EqualFold(k, "Upgrade") {
			hasUpgrade = true
		}
		if strings.EqualFold(k, "Connection") {
			hasConnection = true
		}
		for _, v := range vv {
			fmt.Fprintf(&b, "%s: %s\r\n", k, v)
		}
	}
	if !hasUpgrade {
		fmt.Fprintf(&b, "Upgrade: websocket\r\n")
	}
	if !hasConnection {
		fmt.Fprintf(&b, "Connection: Upgrade\r\n")
	}
	b.WriteString("\r\n")

	if _, err := backendConn.Write([]byte(b.String())); err != nil {
		backend.RecordError()
		log.Printf("[WS Proxy] Failed to write handshake to backend %s: %v", backend.HostPort, err)
		return
	}

	backend.ActiveWS.Add(1)
	defer backend.ActiveWS.Add(-1)

	errc := make(chan error, 2)

	// Pipe client -> backend (reading from clientBuf so buffered handshake data isn't lost)
	go func() {
		_, err := io.Copy(backendConn, clientBuf)
		backendConn.Close()
		errc <- err
	}()

	// Pipe backend -> client
	go func() {
		_, err := io.Copy(clientConn, backendConn)
		clientConn.Close()
		errc <- err
	}()

	<-errc
}

func (lb *DynamicLoadBalancer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/lb-status" {
		lb.handleStatus(w, r)
		return
	}

	// 1. WebSocket Proxy Path (Full-duplex TCP tunnel)
	if isWebSocketRequest(r) {
		backend := lb.SelectBackend()
		if backend == nil {
			http.Error(w, "No backends available in load balancer pool", http.StatusServiceUnavailable)
			return
		}
		lb.handleWebSocket(backend, w, r)
		return
	}

	// 2. HTTP Request Path
	backend := lb.SelectBackend()
	if backend == nil {
		http.Error(w, "No backends available in load balancer pool", http.StatusServiceUnavailable)
		return
	}

	backend.ActiveRequests.Add(1)
	defer backend.ActiveRequests.Add(-1)

	backend.Proxy.ServeHTTP(w, r)
}

func (lb *DynamicLoadBalancer) handleStatus(w http.ResponseWriter, r *http.Request) {
	type BackendStatus struct {
		HostPort       string  `json:"host_port"`
		Healthy        bool    `json:"healthy"`
		ActiveRequests int64   `json:"active_requests"`
		ActiveWS       int64   `json:"active_ws"`
		LoadMetric     float64 `json:"load_metric"`
		TotalRequests  int64   `json:"total_requests"`
		TotalErrors    int64   `json:"total_errors"`
		EmaLatencyMs   float64 `json:"ema_latency_ms"`
	}

	var backendStatuses []BackendStatus
	for _, b := range lb.Backends {
		backendStatuses = append(backendStatuses, BackendStatus{
			HostPort:       b.HostPort,
			Healthy:        b.IsHealthy.Load(),
			ActiveRequests: b.ActiveRequests.Load(),
			ActiveWS:       b.ActiveWS.Load(),
			LoadMetric:     b.LoadMetric(),
			TotalRequests:  b.TotalRequests.Load(),
			TotalErrors:    b.TotalErrors.Load(),
			EmaLatencyMs:   b.GetLatency(),
		})
	}

	currentHost := ""
	if curr := lb.CurrentBackend.Load(); curr != nil {
		currentHost = curr.HostPort
	}

	statusResponse := map[string]interface{}{
		"status":          "healthy",
		"threshold":       lb.Threshold,
		"current_backend": currentHost,
		"backends":        backendStatuses,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(statusResponse)
}

func main() {
	port := flag.Int("port", 5297, "Load balancer listening port")
	backendsFlag := flag.String("backends", "127.0.0.1:5298,127.0.0.1:5299,127.0.0.1:5300", "Comma-separated list of backend host:port pairs")
	threshold := flag.Float64("threshold", 10.0, "Load threshold before dynamic switching")
	healthIntervalSec := flag.Float64("health-interval", 2.0, "Health check interval in seconds")

	flag.Parse()

	rawBackends := strings.Split(*backendsFlag, ",")
	var backends []string
	for _, b := range rawBackends {
		b = strings.TrimSpace(b)
		if b != "" {
			backends = append(backends, b)
		}
	}

	fmt.Println("=================================================================")
	fmt.Println(" HIGH-PERFORMANCE GO DYNAMIC LOAD BALANCER")
	fmt.Println("=================================================================")
	fmt.Printf(" Listening Port:           %d\n", *port)
	fmt.Printf(" Registered Backends:      %v\n", backends)
	fmt.Printf(" Switching Load Threshold: %.1f\n", *threshold)
	fmt.Printf(" Health Check Interval:    %.1fs\n", *healthIntervalSec)
	fmt.Println("=================================================================")

	healthInterval := time.Duration(*healthIntervalSec * float64(time.Second))
	lb := NewDynamicLoadBalancer(backends, *threshold, healthInterval)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lb.StartHealthChecks(ctx)

	server := &http.Server{
		Addr:         fmt.Sprintf(":%d", *port),
		Handler:      lb,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	log.Printf("[LB] Server listening on http://0.0.0.0:%d", *port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("[LB] Fatal server error: %v", err)
	}
}
