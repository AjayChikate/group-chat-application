"""
lb.py - Dynamic Performance-Based Load Balancer
-----------------------------------------------------------------------------
Dynamic load balancer for real-time WebSocket & HTTP chat backends.
- Evaluates backend load based on active in-flight requests and response latency.
- Dynamically switches traffic when current backend load exceeds defined threshold.
- Conducts active background health checks to prune dead or unresponsive backends.
- Exposes exact required routes: /message, /feed, and transparent /ws & HTTP proxying.
- Provides /lb-status for live observability and benchmarking.
-----------------------------------------------------------------------------
"""

import argparse
import asyncio
import time
from typing import Dict, List, Optional
import aiohttp
from aiohttp import web


class BackendNode:
    def __init__(self, host_port: str):
        self.host_port = host_port.strip()
        self.is_healthy: bool = True
        self.active_requests: int = 0
        self.active_ws: int = 0
        self.total_requests: int = 0
        self.total_errors: int = 0
        self.ema_latency_ms: float = 0.0  # Exponential Moving Average latency
        self.last_health_check: float = 0.0

    @property
    def load_metric(self) -> float:
        """
        Calculates effective performance load on the backend.
        Combines active HTTP requests and active WebSocket connections.
        """
        return float(self.active_requests + (self.active_ws * 0.5))

    def record_request_success(self, duration_s: float):
        latency_ms = duration_s * 1000.0
        self.total_requests += 1
        if self.ema_latency_ms == 0.0:
            self.ema_latency_ms = latency_ms
        else:
            # Alpha = 0.2 gives weight to recent performance
            self.ema_latency_ms = 0.8 * self.ema_latency_ms + 0.2 * latency_ms

    def record_request_error(self):
        self.total_requests += 1
        self.total_errors += 1


class DynamicLoadBalancer:
    def __init__(self, backends: List[str], threshold: float = 10.0, health_interval: float = 2.0):
        self.backends: List[BackendNode] = [BackendNode(b) for b in backends]
        self.threshold: float = threshold
        self.health_interval: float = health_interval
        self.current_backend: Optional[BackendNode] = self.backends[0] if self.backends else None
        self._lock = asyncio.Lock()
        self.session: Optional[aiohttp.ClientSession] = None

    async def start(self):
        timeout = aiohttp.ClientTimeout(total=30, connect=3)
        self.session = aiohttp.ClientSession(timeout=timeout)
        asyncio.create_task(self._health_check_loop())

    async def stop(self):
        if self.session:
            await self.session.close()

    def select_backend(self) -> Optional[BackendNode]:
        """
        Performance-Based Dynamic Selection:
        - If current backend is healthy and its load <= threshold, retain it to avoid thrashing.
        - Once current backend exceeds threshold (or is unhealthy), dynamically switch
          to the healthy backend with the minimum load.
        """
        healthy_backends = [b for b in self.backends if b.is_healthy]
        if not healthy_backends:
            # If all are marked unhealthy, attempt fallback to any backend
            return self.backends[0] if self.backends else None

        # Check if current backend is healthy and within performance threshold
        if self.current_backend and self.current_backend.is_healthy:
            if self.current_backend.load_metric <= self.threshold:
                return self.current_backend

        # Find healthy backend with the lowest load metric
        best_backend = min(healthy_backends, key=lambda b: (b.load_metric, b.ema_latency_ms))
        self.current_backend = best_backend
        return best_backend

    async def _health_check_loop(self):
        """Periodic background health monitoring of all backend nodes."""
        while True:
            await asyncio.sleep(self.health_interval)
            for node in self.backends:
                try:
                    url = f"http://{node.host_port}/health"
                    async with self.session.get(url, timeout=aiohttp.ClientTimeout(total=1.5)) as resp:
                        if resp.status == 200:
                            node.is_healthy = True
                        else:
                            node.is_healthy = False
                except Exception:
                    node.is_healthy = False
                node.last_health_check = time.time()


# Global LB instance
lb: Optional[DynamicLoadBalancer] = None


async def proxy_handler(request: web.Request) -> web.StreamResponse:
    global lb
    backend = lb.select_backend()
    if not backend:
        return web.Response(status=503, text="No backends available in load balancer pool.")

    # -----------------------------------------------------------------------
    # 1. WebSocket Upgrade Path
    # -----------------------------------------------------------------------
    if request.headers.get("Upgrade", "").lower() == "websocket":
        backend.active_ws += 1
        ws_server = web.WebSocketResponse()
        await ws_server.prepare(request)

        ws_target = f"ws://{backend.host_port}{request.path_qs}"
        try:
            async with lb.session.ws_connect(ws_target, heartbeat=30.0) as ws_client:
                async def client_to_backend():
                    async for msg in ws_server:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            await ws_client.send_str(msg.data)
                        elif msg.type == aiohttp.WSMsgType.BINARY:
                            await ws_client.send_bytes(msg.data)
                        elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSING, aiohttp.WSMsgType.ERROR):
                            break

                async def backend_to_client():
                    async for msg in ws_client:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            await ws_server.send_str(msg.data)
                        elif msg.type == aiohttp.WSMsgType.BINARY:
                            await ws_server.send_bytes(msg.data)
                        elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSING, aiohttp.WSMsgType.ERROR):
                            break

                await asyncio.gather(client_to_backend(), backend_to_client(), return_exceptions=True)
        except Exception:
            backend.record_request_error()
        finally:
            backend.active_ws = max(0, backend.active_ws - 1)
            await ws_server.close()
            return ws_server

    # -----------------------------------------------------------------------
    # 2. HTTP Request Path (including /message, /feed, and static assets)
    # -----------------------------------------------------------------------
    backend.active_requests += 1
    t0 = time.perf_counter()
    target_url = f"http://{backend.host_port}{request.path_qs}"

    # Copy headers, removing hop-by-hop
    headers = {
        k: v for k, v in request.headers.items()
        if k.lower() not in ("host", "connection", "upgrade", "content-length")
    }

    try:
        body = await request.read()
        async with lb.session.request(
            request.method,
            target_url,
            headers=headers,
            data=body,
            allow_redirects=False
        ) as resp:
            resp_body = await resp.read()
            backend.record_request_success(time.perf_counter() - t0)

            resp_headers = {
                k: v for k, v in resp.headers.items()
                if k.lower() not in ("transfer-encoding", "content-encoding")
            }
            # Attach diagnostic header showing which backend served the request
            resp_headers["X-Served-By-Backend"] = backend.host_port

            return web.Response(
                status=resp.status,
                body=resp_body,
                headers=resp_headers
            )
    except Exception as e:
        backend.record_request_error()
        # Mark backend temporarily unhealthy on connection failure
        backend.is_healthy = False
        return web.Response(status=502, text=f"Bad Gateway: backend {backend.host_port} error: {str(e)}")
    finally:
        backend.active_requests = max(0, backend.active_requests - 1)


async def status_handler(request: web.Request) -> web.Response:
    """Returns real-time status of the Load Balancer and all backends."""
    global lb
    status_data = {
        "status": "healthy",
        "threshold": lb.threshold,
        "current_backend": lb.current_backend.host_port if lb.current_backend else None,
        "backends": [
            {
                "host_port": b.host_port,
                "healthy": b.is_healthy,
                "active_requests": b.active_requests,
                "active_ws": b.active_ws,
                "load_metric": b.load_metric,
                "total_requests": b.total_requests,
                "total_errors": b.total_errors,
                "ema_latency_ms": round(b.ema_latency_ms, 2),
            }
            for b in lb.backends
        ]
    }
    return web.json_response(status_data)


def main():
    global lb

    parser = argparse.ArgumentParser(description="Performance-Based Dynamic Load Balancer")
    parser.add_argument("--port", type=int, default=5297, help="Load Balancer listening port")
    parser.add_argument(
        "--backends",
        type=str,
        default="127.0.0.1:5298,127.0.0.1:5299,127.0.0.1:5300",
        help="Comma-separated list of backend host:port pairs"
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=10.0,
        help="Performance load threshold before switching backends"
    )
    parser.add_argument(
        "--health-interval",
        type=float,
        default=2.0,
        help="Health check frequency in seconds"
    )
    args = parser.parse_args()

    backend_list = [b.strip() for b in args.backends.split(",") if b.strip()]
    print("=" * 65)
    print(" DYNAMIC PERFORMANCE-BASED LOAD BALANCER")
    print("=" * 65)
    print(f" Listening Port:          {args.port}")
    print(f" Registered Backends:     {backend_list}")
    print(f" Switching Load Threshold:{args.threshold}")
    print(f" Health Check Interval:   {args.health_interval}s")
    print("=" * 65)

    lb = DynamicLoadBalancer(
        backends=backend_list,
        threshold=args.threshold,
        health_interval=args.health_interval
    )

    app = web.Application()

    async def on_startup(app):
        await lb.start()

    async def on_cleanup(app):
        await lb.stop()

    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)

    # LB Status Route
    app.router.add_get("/lb-status", status_handler)
    # Catch-all Proxy Route (covers /message, /feed, /ws, and static SPA routes)
    app.router.add_route("*", "/{tail:.*}", proxy_handler)

    web.run_app(app, host="0.0.0.0", port=args.port)


if __name__ == "__main__":
    main()

