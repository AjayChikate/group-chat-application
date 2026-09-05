
import asyncio
import argparse
import json
import logging
import statistics
import time
from collections import defaultdict

import aiohttp
from aiohttp import web, ClientSession, WSMsgType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [LB]  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("load_balancer")

# Keep at most this many latency samples in memory (oldest are dropped)
MAX_LATENCY_SAMPLES = 50_000


# Metrics collector

class MetricsCollector:
    """Thread-safe-ish (single event-loop) metrics store."""

    def __init__(self, backends: list[str]):
        self.backends = backends
        self.start_time = time.time()

        # Counters
        self.total_http = 0
        self.total_ws = 0
        self.active_ws = 0
        self.total_errors = 0
        self.total_messages = 0

        # Per-backend counters
        self.http_per_backend: dict[str, int] = defaultdict(int)
        self.ws_per_backend: dict[str, int] = defaultdict(int)
        self.active_ws_per_backend: dict[str, int] = defaultdict(int)
        self.errors_per_backend: dict[str, int] = defaultdict(int)
        self.messages_per_backend: dict[str, int] = defaultdict(int)

        # Latency samples: list of (timestamp, latency_ms)
        self.http_latencies: list[tuple[float, float]] = []
        self.ws_handshake_latencies: list[tuple[float, float]] = []
        self.msg_relay_latencies: list[tuple[float, float]] = []

    # ---- helpers ----
    @staticmethod
    def _percentile_stats(samples: list[tuple[float, float]]) -> dict:
        if not samples:
            return {"count": 0, "avg_ms": 0, "min_ms": 0, "max_ms": 0,
                    "p50_ms": 0, "p95_ms": 0, "p99_ms": 0}
        vals = sorted(v for _, v in samples)
        n = len(vals)
        return {
            "count": n,
            "avg_ms": round(statistics.mean(vals), 3),
            "min_ms": round(vals[0], 3),
            "max_ms": round(vals[-1], 3),
            "p50_ms": round(vals[n // 2], 3),
            "p95_ms": round(vals[int(n * 0.95)] if n >= 20 else vals[-1], 3),
            "p99_ms": round(vals[int(n * 0.99)] if n >= 100 else vals[-1], 3),
        }

    def _trim(self, lst: list) -> None:
        if len(lst) > MAX_LATENCY_SAMPLES:
            del lst[: len(lst) - MAX_LATENCY_SAMPLES]

    def record_http(self, backend: str, latency_ms: float) -> None:
        self.total_http += 1
        self.http_per_backend[backend] += 1
        self.http_latencies.append((time.time(), latency_ms))
        self._trim(self.http_latencies)

    def record_ws_open(self, backend: str, handshake_ms: float) -> None:
        self.total_ws += 1
        self.active_ws += 1
        self.ws_per_backend[backend] += 1
        self.active_ws_per_backend[backend] += 1
        self.ws_handshake_latencies.append((time.time(), handshake_ms))
        self._trim(self.ws_handshake_latencies)

    def record_ws_close(self, backend: str) -> None:
        self.active_ws -= 1
        self.active_ws_per_backend[backend] = max(
            0, self.active_ws_per_backend[backend] - 1
        )

    def record_message(self, backend: str, relay_ms: float) -> None:
        self.total_messages += 1
        self.messages_per_backend[backend] += 1
        self.msg_relay_latencies.append((time.time(), relay_ms))
        self._trim(self.msg_relay_latencies)

    def record_error(self, backend: str) -> None:
        self.total_errors += 1
        self.errors_per_backend[backend] += 1

    def snapshot(self) -> dict:
        uptime = time.time() - self.start_time
        total_reqs = self.total_http + self.total_ws
        return {
            "uptime_s": round(uptime, 1),
            "throughput_rps": round(total_reqs / uptime, 2) if uptime > 0 else 0,
            "total_http_requests": self.total_http,
            "total_ws_connections": self.total_ws,
            "active_ws_connections": self.active_ws,
            "total_messages_proxied": self.total_messages,
            "total_errors": self.total_errors,
            "per_backend": {
                b: {
                    "http_requests": self.http_per_backend.get(b, 0),
                    "ws_connections": self.ws_per_backend.get(b, 0),
                    "active_ws": self.active_ws_per_backend.get(b, 0),
                    "messages": self.messages_per_backend.get(b, 0),
                    "errors": self.errors_per_backend.get(b, 0),
                }
                for b in self.backends
            },
            "http_latency": self._percentile_stats(self.http_latencies),
            "ws_handshake_latency": self._percentile_stats(self.ws_handshake_latencies),
            "message_relay_latency": self._percentile_stats(self.msg_relay_latencies),
        }

# Load Balancer core
class LoadBalancer:
    def __init__(self, backends: list[str]):
        self.backends = backends
        self._rr_index = 0
        self.metrics = MetricsCollector(backends)
        self.session: ClientSession | None = None

    async def start(self) -> None:
        timeout = aiohttp.ClientTimeout(total=30)
        self.session = ClientSession(timeout=timeout)

    async def stop(self) -> None:
        if self.session:
            await self.session.close()

    def _next_backend(self) -> str:
        backend = self.backends[self._rr_index % len(self.backends)]
        self._rr_index += 1
        return backend

    # WebSocket proxy 
    async def handle_ws(self, request: web.Request) -> web.WebSocketResponse:
        backend = self._next_backend()
        ws_url = (
            backend.replace("http://", "ws://")
            .replace("https://", "wss://")
            .rstrip("/")
            + "/ws"
        )
        # Accept incoming client WebSocket
        ws_client = web.WebSocketResponse(
            heartbeat=30.0,         
            max_msg_size=4 * 1024 * 1024,
        )
        await ws_client.prepare(request)

        log.info("WS  %-20s  ->  %s", request.remote, backend)

        try:
            t0 = time.time()
            async with self.session.ws_connect(
                ws_url,
                heartbeat=30.0,
                max_msg_size=4 * 1024 * 1024,
            ) as ws_backend:
                handshake_ms = (time.time() - t0) * 1000
                self.metrics.record_ws_open(backend, handshake_ms)

                # --- bidirectional relay ---
                async def client_to_backend():
                    try:
                        async for msg in ws_client:
                            if msg.type == WSMsgType.TEXT:
                                t = time.time()
                                await ws_backend.send_str(msg.data)
                                self.metrics.record_message(
                                    backend, (time.time() - t) * 1000
                                )
                            elif msg.type == WSMsgType.BINARY:
                                await ws_backend.send_bytes(msg.data)
                                self.metrics.record_message(backend, 0)
                            elif msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSING,
                                              WSMsgType.CLOSED, WSMsgType.ERROR):
                                break
                    except Exception:
                        pass

                async def backend_to_client():
                    try:
                        async for msg in ws_backend:
                            if msg.type == WSMsgType.TEXT:
                                t = time.time()
                                await ws_client.send_str(msg.data)
                                self.metrics.record_message(
                                    backend, (time.time() - t) * 1000
                                )
                            elif msg.type == WSMsgType.BINARY:
                                await ws_client.send_bytes(msg.data)
                                self.metrics.record_message(backend, 0)
                            elif msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSING,
                                              WSMsgType.CLOSED, WSMsgType.ERROR):
                                break
                    except Exception:
                        pass

                # Run both directions; when either finishes, cancel the other
                task_c2b = asyncio.create_task(client_to_backend())
                task_b2c = asyncio.create_task(backend_to_client())
                _done, pending = await asyncio.wait(
                    [task_c2b, task_b2c],
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for t in pending:
                    t.cancel()

        except Exception as exc:
            log.error("WS proxy error (%s): %s", backend, exc)
            self.metrics.record_error(backend)
        finally:
            self.metrics.record_ws_close(backend)
            log.info(
                "WS  %-20s  <-  %s  (active: %d)",
                request.remote, backend, self.metrics.active_ws,
            )

        return ws_client

    # ---- HTTP reverse proxy ----
    async def handle_http(self, request: web.Request) -> web.Response:
        backend = self._next_backend()
        target = backend.rstrip("/") + request.path_qs

        try:
            t0 = time.time()
            async with self.session.request(
                method=request.method,
                url=target,
                headers={
                    k: v
                    for k, v in request.headers.items()
                    if k.lower() not in ("host", "transfer-encoding")
                },
                data=await request.read(),
                allow_redirects=False,
            ) as resp:
                latency_ms = (time.time() - t0) * 1000
                self.metrics.record_http(backend, latency_ms)

                body = await resp.read()
                headers = {
                    k: v
                    for k, v in resp.headers.items()
                    if k.lower()
                    not in ("transfer-encoding", "content-encoding", "content-length")
                }
                return web.Response(body=body, status=resp.status, headers=headers)

        except Exception as exc:
            log.error("HTTP proxy error (%s): %s", backend, exc)
            self.metrics.record_error(backend)
            return web.Response(
                text=f"502 Bad Gateway — backend {backend} unavailable\n{exc}",
                status=502,
            )

    # ---- Metrics endpoints ----
    async def handle_metrics_json(self, _request: web.Request) -> web.Response:
        return web.json_response(self.metrics.snapshot())

    async def handle_metrics_page(self, _request: web.Request) -> web.Response:
        return web.Response(text=METRICS_HTML, content_type="text/html")


# ---------------------------------------------------------------------------
# Routing: decide WS vs HTTP vs metrics
# ---------------------------------------------------------------------------
async def router(request: web.Request) -> web.StreamResponse:
    lb: LoadBalancer = request.app["lb"]
    path = request.path

    # Metrics dashboard (not proxied)
    if path == "/lb/metrics":
        return await lb.handle_metrics_page(request)
    if path == "/lb/metrics.json":
        return await lb.handle_metrics_json(request)

    # WebSocket upgrade
    if path == "/ws":
        return await lb.handle_ws(request)

    # Everything else: HTTP reverse proxy
    return await lb.handle_http(request)


# ---------------------------------------------------------------------------
# Metrics dashboard HTML (auto-refreshing)
# ---------------------------------------------------------------------------
METRICS_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Load Balancer — Live Metrics</title>
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
  table {
    width: 100%; border-collapse: collapse; background: #1e293b;
    border-radius: 12px; overflow: hidden; border: 1px solid #334155;
  }
  th { background: #0f172a; font-size: .75rem; color: #94a3b8; text-transform: uppercase;
       letter-spacing: .05em; padding: .8rem 1rem; text-align: left; }
  td { padding: .7rem 1rem; border-top: 1px solid #1e293b; font-variant-numeric: tabular-nums; }
  tr:nth-child(even) td { background: rgba(255,255,255,.02); }
  .section-title { font-size: 1rem; font-weight: 600; margin: 1.5rem 0 .8rem; color: #cbd5e1; }
  .pill {
    display: inline-block; padding: .15rem .6rem; border-radius: 9999px;
    font-size: .7rem; font-weight: 600;
  }
  .pill-ok { background: #065f4620; color: #34d399; border: 1px solid #34d39940; }
  .pill-err { background: #7f1d1d20; color: #f87171; border: 1px solid #f8717140; }
  #status { position: fixed; top: 1rem; right: 1rem; font-size: .75rem; color: #64748b; }
  .latency-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 1rem; }
</style>
</head>
<body>
<h1>⚖️ Load Balancer — Live Metrics</h1>
<p class="subtitle">Auto-refreshes every 2 seconds</p>
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
  return `<div class="card"><div class="label">${label}</div><div class="value">${value} <span class="unit">${unit||''}</span></div></div>`;
}
function latencyCard(title, d) {
  if (!d || d.count === 0) return `<div class="card"><div class="label">${title}</div><div class="value">—</div></div>`;
  return `<div class="card">
    <div class="label">${title} (${d.count} samples)</div>
    <table style="margin-top:.6rem;font-size:.82rem;background:transparent;border:none;">
      <tr><td style="border:none;padding:.2rem .5rem;color:#94a3b8">Avg</td><td style="border:none;padding:.2rem .5rem">${d.avg_ms} ms</td></tr>
      <tr><td style="border:none;padding:.2rem .5rem;color:#94a3b8">P50</td><td style="border:none;padding:.2rem .5rem">${d.p50_ms} ms</td></tr>
      <tr><td style="border:none;padding:.2rem .5rem;color:#94a3b8">P95</td><td style="border:none;padding:.2rem .5rem">${d.p95_ms} ms</td></tr>
      <tr><td style="border:none;padding:.2rem .5rem;color:#94a3b8">P99</td><td style="border:none;padding:.2rem .5rem">${d.p99_ms} ms</td></tr>
      <tr><td style="border:none;padding:.2rem .5rem;color:#94a3b8">Min</td><td style="border:none;padding:.2rem .5rem">${d.min_ms} ms</td></tr>
      <tr><td style="border:none;padding:.2rem .5rem;color:#94a3b8">Max</td><td style="border:none;padding:.2rem .5rem">${d.max_ms} ms</td></tr>
    </table>
  </div>`;
}

async function refresh() {
  try {
    const r = await fetch('/lb/metrics.json');
    const m = await r.json();
    document.getElementById('status').textContent = 'live · updated ' + new Date().toLocaleTimeString();

    document.getElementById('summary-cards').innerHTML = [
      card('Uptime', m.uptime_s, 's'),
      card('Throughput', m.throughput_rps, 'req/s'),
      card('HTTP Requests', m.total_http_requests, ''),
      card('WS Connections', m.total_ws_connections, 'total'),
      card('Active WS', m.active_ws_connections, 'now'),
      card('Messages Proxied', m.total_messages_proxied, ''),
      card('Errors', m.total_errors, ''),
    ].join('');

    const tbody = document.querySelector('#backend-table tbody');
    tbody.innerHTML = Object.entries(m.per_backend).map(([b, d]) => {
      const health = d.errors === 0
        ? '<span class="pill pill-ok">healthy</span>'
        : '<span class="pill pill-err">' + d.errors + ' errors</span>';
      return `<tr><td>${b}</td><td>${d.http_requests}</td><td>${d.ws_connections}</td><td>${d.active_ws}</td><td>${d.messages}</td><td>${d.errors}</td><td>${health}</td></tr>`;
    }).join('');

    document.getElementById('latency-section').innerHTML = [
      latencyCard('HTTP Latency', m.http_latency),
      latencyCard('WS Handshake', m.ws_handshake_latency),
      latencyCard('Message Relay', m.message_relay_latency),
    ].join('');
  } catch(e) {
    document.getElementById('status').textContent = 'error: ' + e.message;
  }
}
refresh();
setInterval(refresh, 2000);
</script>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# App factory & entry point
# ---------------------------------------------------------------------------
def create_app(backends: list[str]) -> web.Application:
    app = web.Application()
    lb = LoadBalancer(backends)
    app["lb"] = lb

    async def on_startup(_app: web.Application) -> None:
        await lb.start()
        log.info("=" * 60)
        log.info("  LOAD BALANCER STARTED")
        log.info("  Algorithm : Round-Robin")
        log.info("  Backends  : %d", len(backends))
        for b in backends:
            log.info("    • %s", b)
        log.info("  Dashboard : http://localhost:<port>/lb/metrics")
        log.info("=" * 60)

    async def on_shutdown(_app: web.Application) -> None:
        await lb.stop()
        log.info("Load balancer stopped.")

    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)

    # Catch-all route
    app.router.add_route("*", "/{path_info:.*}", router)

    return app


def main() -> None:
    parser = argparse.ArgumentParser(
        description="WebSocket-aware Round-Robin Load Balancer"
    )
    parser.add_argument(
        "--backends",
        required=True,
        help="Comma-separated backend URLs, e.g. http://10.1.75.79:3201,http://10.1.75.79:3202",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8080,
        help="Port the load balancer listens on (default: 8080)",
    )
    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Host to bind to (default: 0.0.0.0)",
    )
    args = parser.parse_args()

    backends = [b.strip().rstrip("/") for b in args.backends.split(",") if b.strip()]
    if not backends:
        parser.error("At least one backend URL is required.")

    app = create_app(backends)
    web.run_app(app, host=args.host, port=args.port, print=None)


if __name__ == "__main__":
    main()
