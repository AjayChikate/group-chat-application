import asyncio
import mimetypes
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from fastapi import FastAPI, WebSocket, Request, Response
from fastapi.responses import FileResponse, JSONResponse

from server import config, logger, store
from server.rooms import RoomManager
from server.ws_server import WSServer

room_manager = RoomManager(logger)
ws_server = WSServer(room_manager, logger)

start_time = time.time()
active_requests = 0


@asynccontextmanager
async def lifespan(application: FastAPI):
    # Startup
    logger.log(
        'server_start',
        port=config.PORT,
        rooms=config.DEFAULT_ROOMS,
        admins=config.ADMIN_USERNAMES,
    )
    print(f'\nGroup Chat server running (FastAPI):')
    print(f'  Port:    {config.PORT}')
    print(f'  DB Mode: {"MongoDB Atlas (" + config.MONGO_DB_NAME + ")" if store.is_using_mongo() else "SQLite (chat.db)"}')
    print(f'  Admins:  {", ".join(config.ADMIN_USERNAMES)}\n')

    yield  # application runs here

    # Shutdown
    logger.log('server_shutdown', signal='lifespan')
    ws_server.shutdown()
    # Give in-flight sends a moment to complete before uvicorn closes sockets.
    await asyncio.sleep(0.5)


app = FastAPI(lifespan=lifespan, title='Group Chat')


@app.middleware("http")
async def track_active_requests(request: Request, call_next):
    global active_requests
    active_requests += 1
    try:
        response = await call_next(request)
        return response
    finally:
        active_requests -= 1


# ---------------------------------------------------------------------------
# Health & Status endpoint (Used by Load Balancer for performance monitoring)
# ---------------------------------------------------------------------------
@app.get('/health')
async def health():
    return {
        "status": "ok",
        "active_requests": max(0, active_requests - 1),  # exclude this health request itself
        "uptime": round(time.time() - start_time, 2),
        "db_mode": "mongodb" if store.is_using_mongo() else "sqlite"
    }


# ---------------------------------------------------------------------------
# Required API Routes: /message and /feed
# ---------------------------------------------------------------------------
@app.post('/message')
async def post_message(request: Request):
    """
    Accepts "client-name" and "msg" as input and submits a message.
    Preserves digital signatures, encryption, and deduplication.
    """
    client_name = None
    msg_text = None
    msg_id = None

    content_type = request.headers.get("content-type", "")
    if "application/json" in content_type:
        try:
            body = await request.json()
            if isinstance(body, dict):
                client_name = body.get("client-name") or body.get("client_name") or body.get("username")
                msg_text = body.get("msg") if body.get("msg") is not None else body.get("text")
                msg_id = body.get("id")
        except Exception:
            pass
    elif "application/x-www-form-urlencoded" in content_type or "multipart/form-data" in content_type:
        try:
            form = await request.form()
            client_name = form.get("client-name") or form.get("client_name") or form.get("username")
            msg_text = form.get("msg") if form.get("msg") is not None else form.get("text")
            msg_id = form.get("id")
        except Exception:
            pass

    # Fallback to query params if not found in body
    if not client_name:
        client_name = (
            request.query_params.get("client-name")
            or request.query_params.get("client_name")
            or request.query_params.get("username")
        )
    if msg_text is None:
        msg_text = request.query_params.get("msg") or request.query_params.get("text")
    if not msg_id:
        msg_id = request.query_params.get("id")

    if not client_name or msg_text is None:
        return JSONResponse(
            status_code=400,
            content={"error": "Missing required fields: 'client-name' and 'msg'"}
        )

    if not msg_id:
        msg_id = str(uuid.uuid4())

    # Pass through secure encrypt -> sign -> persist pipeline
    msg_data = {
        "id": msg_id,
        "client-name": client_name,
        "msg": str(msg_text),
        "timestamp": int(time.time() * 1000)
    }
    res = store.append_message("general", msg_data)

    # Broadcast to any active WebSocket clients in the general room
    try:
        room_manager.broadcast("general", {
            "type": "message",
            "id": msg_id,
            "username": client_name,
            "text": str(msg_text),
            "room": "general",
            "timestamp": res["timestamp"],
            "verified": res.get("verified", True)
        })
    except Exception:
        pass

    return {
        "status": "ok",
        "id": msg_id,
        "client-name": client_name,
        "msg": str(msg_text),
        "duplicate": res.get("duplicate", False),
        "timestamp": res["timestamp"]
    }


@app.get('/feed')
async def get_feed():
    """
    Retrieves all messages in chronological order.
    Re-verifies digital signatures and decrypts on the fly.
    """
    messages = store.get_all_messages()
    feed = [
        {
            "id": m["id"],
            "client-name": m.get("client-name") or m.get("sender") or m.get("username"),
            "msg": m.get("msg") if m.get("msg") is not None else m.get("text", ""),
            "timestamp": m["timestamp"],
            "room": m.get("room", "general"),
            "verified": m.get("verified", True)
        }
        for m in messages
    ]
    return feed


# ---------------------------------------------------------------------------
# WebSocket route
# ---------------------------------------------------------------------------
@app.websocket('/ws')
async def ws_route(ws: WebSocket):
    await ws_server.handle_connection(ws)


# ---------------------------------------------------------------------------
# Static frontend routes
# ---------------------------------------------------------------------------
_PUBLIC = Path(__file__).parent / 'public'


@app.get('/')
async def index():
    return FileResponse(_PUBLIC / 'index.html')


@app.get('/{filename:path}')
async def static_file(filename: str):
    target = (_PUBLIC / filename).resolve()
    # Guard against path traversal (e.g. ../../etc/passwd)
    if not str(target).startswith(str(_PUBLIC.resolve())):
        return Response(status_code=403)
    if target.is_file():
        mime, _ = mimetypes.guess_type(str(target))
        return FileResponse(target, media_type=mime or 'application/octet-stream')
    # Fall back to index.html for unknown paths (SPA-style)
    return FileResponse(_PUBLIC / 'index.html')


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, default=config.PORT, help='Port to bind server')
    args = parser.parse_args()

    uvicorn.run(
        app,
        host='0.0.0.0',
        port=args.port,
        ws_ping_interval=config.HEARTBEAT_INTERVAL_MS / 1000,
        ws_ping_timeout=config.HEARTBEAT_INTERVAL_MS / 1000,
        log_level='warning',
    )