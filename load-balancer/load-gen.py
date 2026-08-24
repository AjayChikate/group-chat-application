"""

Examples:
    # against the load balancer 
    ssh -p 2297 -L 5297:localhost:5297 student@10.1.75.51   (ssh port forwarding)
    python lg.py --uri ws://localhost:5297/ws --concurrency 100 --messages 20   

"""
import asyncio
import json
import time
import statistics
import argparse
import websockets


async def one_client(uri, room, n_messages, latencies, errors, client_id):
    try:
        async with websockets.connect(uri, open_timeout=10) as ws:
            await ws.send(json.dumps({"type": "join", "username": f"loadtest{client_id}", "room": room}))
            await ws.recv()  # welcome event
            for i in range(n_messages):
                t0 = time.perf_counter()
                await ws.send(json.dumps({"type": "message", "text": f"hello {i}"}))
                await ws.recv()  # broadcast echo back to sender
                latencies.append(time.perf_counter() - t0)
    except Exception as e:
        errors.append(str(e))


async def run(uri, room, concurrency, n_messages):
    latencies, errors = [], []
    t_start = time.perf_counter()
    await asyncio.gather(*[
        one_client(uri, room, n_messages, latencies, errors, i)
        for i in range(concurrency)
    ])
    total_time = time.perf_counter() - t_start
    total_msgs = concurrency * n_messages

    print(f"\n--- Results: {uri} ---")
    print(f"Concurrency: {concurrency}, Messages/client: {n_messages}")
    print(f"Total messages attempted: {total_msgs}, Errors: {len(errors)}")
    if latencies:
        print(f"Throughput: {len(latencies) / total_time:.2f} msgs/sec")
        print(f"Avg latency: {statistics.mean(latencies) * 1000:.2f} ms")
        if len(latencies) >= 20:
            print(f"p95 latency: {statistics.quantiles(latencies, n=20)[18] * 1000:.2f} ms")
        print(f"Max latency: {max(latencies) * 1000:.2f} ms")
    print(f"Wall time: {total_time:.2f} s")
    if errors:
        print(f"Sample errors (first 5): {errors[:5]}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--uri", default="ws://10.1.75.51:5297/ws", help="WebSocket URL to hit")
    p.add_argument("--room", default="general")
    p.add_argument("--concurrency", type=int, default=50, help="number of simulated clients")
    p.add_argument("--messages", type=int, default=20, help="messages sent per client")
    args = p.parse_args()
    asyncio.run(run(args.uri, args.room, args.concurrency, args.messages))
