import asyncio
import mimetypes
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from fastapi import FastAPI, WebSocket
from fastapi.responses import FileResponse, Response

from server import config, logger
from server.rooms import RoomManager
from server.ws_server import WSServer
room_manager = RoomManager(logger)
ws_server = WSServer(room_manager, logger)


@asynccontextmanager
async def lifespan(application: FastAPI):
    # startup
    logger.log(
        'server_start',
        port=config.PORT,
        rooms=config.DEFAULT_ROOMS,
        admins=config.ADMIN_USERNAMES,
    )
    print('\nGroup Chat server running (FastAPI):')
    print(f'  Local:   http://localhost:{config.PORT}')
    print(f'  Network: http://<this-machine-IP>:{config.PORT}  (use for other lab machines)')
    print(f'  Admins:  {", ".join(config.ADMIN_USERNAMES)} (set ADMIN_USERNAMES env var to change)\n')

    yield  # application runs here

    #  shutdown 
    logger.log('servr_shutdown', signal='lifespan')
    ws_server.shutdown()
    # Give in-flight sends a moment to complete before uvicorn closes sockets.
    await asyncio.sleep(0.5)


#inializaing the app here
app = FastAPI(lifespan=lifespan, title='Group Chat')



# WebSocket route
@app.websocket('/ws')
async def ws_route(ws: WebSocket):
    await ws_server.handle_connection(ws)

_PUBLIC = Path(__file__).parent / 'public'

#routes for frintend laoding
@app.get('/')
async def index():
    return FileResponse(_PUBLIC / 'index.html')


@app.get('/{filename:path}')
async def static_file(filename: str):
    """Serve any file under public/; path traversal is prevented
    by resolving the full path and confirming it stays inside _PUBLIC."""
    target = (_PUBLIC / filename).resolve()
    # Guard against path traversal (e.g. ../../etc/passwd)
    if not str(target).startswith(str(_PUBLIC.resolve())):
        return Response(status_code=403)
    if target.is_file():
        mime, _ = mimetypes.guess_type(str(target))
        return FileResponse(target, media_type=mime or 'application/octet-stream')
    # Fall back to index.html for unknown paths (SPA-style)
    return FileResponse(_PUBLIC / 'index.html')

# Entry point

if __name__ == '__main__':
    uvicorn.run(
        'app:app',
        host='0.0.0.0',
        port=config.PORT,
        # ws_ping_interval / ws_ping_timeout: uvicorn sends WebSocket
        # ping frames automatically — replaces flask-sock's
        # SOCK_SERVER_OPTIONS ping_interval.
        ws_ping_interval=config.HEARTBEAT_INTERVAL_MS / 1000,
        ws_ping_timeout=config.HEARTBEAT_INTERVAL_MS / 1000,
        log_level='warning',   # suppress uvicorn access logs; we have our own
    )