#!/usr/bin/env python3
"""
Load Generator for WebSocket Chat App
=======================================
Spawns many concurrent WebSocket clients, connects them through the
load balancer, joins rooms, sends messages, and measures end-to-end
latency + throughput.

Usage:
    python load_generator.py --url ws://localhost:8080/ws --clients 50 --messages 20
    python load_generator.py --url ws://localhost:8080/ws --clients 100 --messages 50 --ramp-delay 0.05

Output:
    - Real-time progress in terminal
    - Final summary table with latency percentiles
    - Saves detailed results to results_<timestamp>.json
"""

import argparse
import asyncio
import json
import random
import statistics
import string
import time
import sys
import os

import websockets


# ---------------------------------------------------------------------------
# Simulated client
# ---------------------------------------------------------------------------
class SimulatedClient:
    """One simulated WebSocket chat user."""

    def __init__(self, client_id: int, url: str, num_messages: int, message_delay: float):
        self.client_id = client_id
        self.url = url
        self.num_messages = num_messages
        self.message_delay = message_delay
        self.username = f"loadbot_{client_id}_{_rand_suffix()}"

        # Results
        self.connected = False
        self.join_latency_ms: float | None = None
        self.message_latencies: list[float] = []  # per-message round-trip
        self.messages_sent = 0
        self.messages_received = 0
        self.errors: list[str] = []
        self.start_time: float = 0
        self.end_time: float = 0

    async def run(self) -> None:
        self.start_time = time.time()
        try:
            async with websockets.connect(
                self.url,
                open_timeout=15,
                close_timeout=5,
                max_size=4 * 1024 * 1024,
            ) as ws:
                self.connected = True

                # --- Join ---
                t0 = time.time()
                await ws.send(json.dumps({
                    "type": "join",
                    "username": self.username,
                    "room": "general",
                }))

                # Wait for welcome message
                welcome = await asyncio.wait_for(self._wait_for_type(ws, "welcome"), timeout=10)
                self.join_latency_ms = (time.time() - t0) * 1000

                # --- Send messages and measure round-trip ---
                pending_acks: dict[str, float] = {}  # msg_id -> send_time

                async def sender():
                    for i in range(self.num_messages):
                        text = f"[load-test] msg {i+1}/{self.num_messages} from {self.username}"
                        msg_id = f"{self.username}_{i}"
                        pending_acks[msg_id] = time.time()
                        await ws.send(json.dumps({
                            "type": "message",
                            "text": text,
                        }))
                        self.messages_sent += 1
                        if self.message_delay > 0:
                            await asyncio.sleep(self.message_delay)

                async def receiver():
                    deadline = time.time() + (self.num_messages * (self.message_delay + 1)) + 15
                    delivered_count = 0
                    try:
                        while delivered_count < self.num_messages and time.time() < deadline:
                            raw = await asyncio.wait_for(ws.recv(), timeout=10)
                            data = json.loads(raw)
                            self.messages_received += 1

                            if data.get("type") == "delivered":
                                delivered_count += 1
                                # Match to earliest pending send
                                if pending_acks:
                                    oldest_key = min(pending_acks, key=pending_acks.get)
                                    send_time = pending_acks.pop(oldest_key)
                                    rtt = (time.time() - send_time) * 1000
                                    self.message_latencies.append(rtt)
                    except asyncio.TimeoutError:
                        pass
                    except websockets.exceptions.ConnectionClosed:
                        pass

                # Run sender and receiver concurrently
                await asyncio.gather(sender(), receiver())

                # Graceful close
                await ws.close()

        except Exception as exc:
            self.errors.append(str(exc))
        finally:
            self.end_time = time.time()

    async def _wait_for_type(self, ws, msg_type: str) -> dict:
        """Read messages until we get one of the desired type."""
        while True:
            raw = await ws.recv()
            data = json.loads(raw)
            self.messages_received += 1
            if data.get("type") == msg_type:
                return data

    def summary(self) -> dict:
        duration = self.end_time - self.start_time if self.end_time else 0
        return {
            "client_id": self.client_id,
            "username": self.username,
            "connected": self.connected,
            "join_latency_ms": round(self.join_latency_ms, 2) if self.join_latency_ms else None,
            "messages_sent": self.messages_sent,
            "messages_received": self.messages_received,
            "message_latencies_ms": [round(l, 2) for l in self.message_latencies],
            "avg_latency_ms": round(statistics.mean(self.message_latencies), 2) if self.message_latencies else None,
            "duration_s": round(duration, 2),
            "errors": self.errors,
        }


def _rand_suffix(n=4) -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=n))


# ---------------------------------------------------------------------------
# Load generator orchestrator
# ---------------------------------------------------------------------------
async def run_load_test(
    url: str,
    num_clients: int,
    num_messages: int,
    message_delay: float,
    ramp_delay: float,
) -> dict:
    """Spawn clients, run load test, return aggregated results."""

    print(f"\n{'='*60}")
    print(f"  LOAD GENERATOR — WebSocket Chat Stress Test")
    print(f"{'='*60}")
    print(f"  Target URL      : {url}")
    print(f"  Clients          : {num_clients}")
    print(f"  Messages/client  : {num_messages}")
    print(f"  Message delay    : {message_delay}s")
    print(f"  Ramp-up delay    : {ramp_delay}s")
    print(f"  Total messages   : {num_clients * num_messages}")
    print(f"{'='*60}\n")

    clients = [
        SimulatedClient(i, url, num_messages, message_delay)
        for i in range(num_clients)
    ]

    # Staggered start
    tasks = []
    overall_start = time.time()

    async def launch(client: SimulatedClient, delay: float):
        if delay > 0:
            await asyncio.sleep(delay)
        await client.run()

    for i, client in enumerate(clients):
        tasks.append(asyncio.create_task(launch(client, i * ramp_delay)))

    # Progress reporting
    async def progress_reporter():
        while not all(t.done() for t in tasks):
            done_count = sum(1 for c in clients if c.end_time > 0)
            active = sum(1 for c in clients if c.connected and c.end_time == 0)
            sent = sum(c.messages_sent for c in clients)
            print(
                f"\r  Progress: {done_count}/{num_clients} done | "
                f"{active} active | {sent} msgs sent",
                end="", flush=True,
            )
            await asyncio.sleep(0.5)
        print()

    reporter = asyncio.create_task(progress_reporter())
    await asyncio.gather(*tasks)
    reporter.cancel()
    try:
        await reporter
    except asyncio.CancelledError:
        pass

    overall_duration = time.time() - overall_start

    # --- Aggregate results ---
    all_latencies = []
    all_join_latencies = []
    total_sent = 0
    total_received = 0
    total_errors = 0
    successful_clients = 0

    for c in clients:
        s = c.summary()
        if c.connected and not c.errors:
            successful_clients += 1
        total_sent += c.messages_sent
        total_received += c.messages_received
        total_errors += len(c.errors)
        all_latencies.extend(c.message_latencies)
        if c.join_latency_ms is not None:
            all_join_latencies.append(c.join_latency_ms)

    def pstats(vals):
        if not vals:
            return {"count": 0, "avg": 0, "min": 0, "max": 0, "p50": 0, "p95": 0, "p99": 0}
        s = sorted(vals)
        n = len(s)
        return {
            "count": n,
            "avg": round(statistics.mean(s), 2),
            "min": round(s[0], 2),
            "max": round(s[-1], 2),
            "p50": round(s[n // 2], 2),
            "p95": round(s[int(n * 0.95)] if n >= 20 else s[-1], 2),
            "p99": round(s[int(n * 0.99)] if n >= 100 else s[-1], 2),
        }

    results = {
        "test_config": {
            "url": url,
            "num_clients": num_clients,
            "num_messages_per_client": num_messages,
            "message_delay_s": message_delay,
            "ramp_delay_s": ramp_delay,
        },
        "summary": {
            "duration_s": round(overall_duration, 2),
            "successful_clients": successful_clients,
            "failed_clients": num_clients - successful_clients,
            "total_messages_sent": total_sent,
            "total_messages_received": total_received,
            "throughput_msgs_per_sec": round(total_sent / overall_duration, 2) if overall_duration > 0 else 0,
            "total_errors": total_errors,
        },
        "join_latency_ms": pstats(all_join_latencies),
        "message_round_trip_ms": pstats(all_latencies),
        "per_client": [c.summary() for c in clients],
    }

    # --- Print summary ---
    print(f"\n{'='*60}")
    print(f"  RESULTS")
    print(f"{'='*60}")
    print(f"  Duration        : {results['summary']['duration_s']}s")
    print(f"  Clients OK/Fail : {successful_clients}/{num_clients - successful_clients}")
    print(f"  Messages Sent   : {total_sent}")
    print(f"  Throughput      : {results['summary']['throughput_msgs_per_sec']} msg/s")
    print()

    jl = results["join_latency_ms"]
    ml = results["message_round_trip_ms"]

    print(f"  {'Metric':<20} {'Join Latency':>14} {'Msg Round-Trip':>14}")
    print(f"  {'─'*20} {'─'*14} {'─'*14}")
    for key in ["avg", "p50", "p95", "p99", "min", "max"]:
        print(f"  {key.upper():<20} {jl[key]:>11.2f} ms {ml[key]:>11.2f} ms")
    print(f"  {'Count':<20} {jl['count']:>14} {ml['count']:>14}")
    print(f"{'='*60}\n")

    # --- Save to file ---
    ts = time.strftime("%Y%m%d_%H%M%S")
    filename = f"results_{ts}.json"
    with open(filename, "w") as f:
        json.dump(results, f, indent=2)
    print(f"  Detailed results saved to: {filename}\n")

    return results


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Load Generator for WebSocket Chat App"
    )
    parser.add_argument(
        "--url",
        default="ws://localhost:8080/ws",
        help="WebSocket URL of the load balancer (default: ws://localhost:8080/ws)",
    )
    parser.add_argument(
        "--clients",
        type=int,
        default=50,
        help="Number of concurrent simulated clients (default: 50)",
    )
    parser.add_argument(
        "--messages",
        type=int,
        default=20,
        help="Messages each client sends (default: 20)",
    )
    parser.add_argument(
        "--message-delay",
        type=float,
        default=0.1,
        help="Delay between messages in seconds (default: 0.1)",
    )
    parser.add_argument(
        "--ramp-delay",
        type=float,
        default=0.05,
        help="Delay between client spawns in seconds (default: 0.05)",
    )
    args = parser.parse_args()

    asyncio.run(run_load_test(
        url=args.url,
        num_clients=args.clients,
        num_messages=args.messages,
        message_delay=args.message_delay,
        ramp_delay=args.ramp_delay,
    ))


if __name__ == "__main__":
    main()
