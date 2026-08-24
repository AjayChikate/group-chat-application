"""
Usage:
    pip install websockets aiohttp
    python3 lb.py --port 5297 \
        --backends 172.17.0.99:5298,172.17.0.100:5299,172.17.0.101:5300

For the "Sys2 only" test run, pass a single backend:
    python3 lb.py --port 5297 --backends 172.17.0.99:5298
"""

import argparse
import asyncio
import itertools
import aiohttp
from aiohttp import web


next_backend = None  # Set in main()


async def proxy_handler(request: web.Request):
    backend = next(next_backend)
    target_url = f"http://{backend}{request.path_qs}"

    # --- WebSocket upgrade path ---
    if request.headers.get("Upgrade", "").lower() == "websocket":
        ws_server = web.WebSocketResponse()
        await ws_server.prepare(request)

        ws_backend_url = f"ws://{backend}{request.path_qs}"
        session = aiohttp.ClientSession()

        try:
            async with session.ws_connect(
                ws_backend_url
            ) as ws_client:

                async def client_to_backend():
                    async for msg in ws_server:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            await ws_client.send_str(msg.data)

                        elif msg.type == aiohttp.WSMsgType.BINARY:
                            await ws_client.send_bytes(msg.data)

                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            break

                async def backend_to_client():
                    async for msg in ws_client:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            await ws_server.send_str(msg.data)

                        elif msg.type == aiohttp.WSMsgType.BINARY:
                            await ws_server.send_bytes(msg.data)

                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            break

                await asyncio.gather(
                    client_to_backend(),
                    backend_to_client()
                )

        finally:
            await session.close()

        return ws_server

    # --- Plain HTTP path ---
    async with aiohttp.ClientSession() as session:
        body = await request.read()

        async with session.request(
            request.method,
            target_url,
            headers=request.headers,
            data=body
        ) as resp:

            data = await resp.read()

            return web.Response(
                status=resp.status,
                body=data,
                headers=resp.headers
            )


def main():
    global next_backend

    ap = argparse.ArgumentParser()

    ap.add_argument(
        "--port",
        type=int,
        default=5297
    )

    ap.add_argument(
        "--backends",
        required=True,
        help="comma-separated host:port list"
    )

    args = ap.parse_args()

    backend_list = [
        b.strip()
        for b in args.backends.split(",")
        if b.strip()
    ]

    next_backend = itertools.cycle(backend_list)

    print(f"Load balancing across: {backend_list}")
    print(f"Listening on 0.0.0.0:{args.port}")

    app = web.Application()

    app.router.add_route(
        "*",
        "/{tail:.*}",
        proxy_handler
    )

    web.run_app(
        app,
        host="0.0.0.0",
        port=args.port
    )


if __name__ == "__main__":
    main()
