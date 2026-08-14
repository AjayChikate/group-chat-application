"""
app.py
------------------------------------------------------------
Entry point / composition root. Responsibilities kept
deliberately narrow, mirroring index.js:
  1. Serve the static frontend (public/) over HTTP.
  2. Attach a WebSocket route to the same Flask app / port
     (one port to open on the lab firewall, not two).
  3. Wire RoomManager + WSServer together.
  4. Handle process-level signals for a clean shutdown.

All actual protocol/business logic lives in ws_server.py,
rooms.py, store.py, etc. — this file just assembles them.
------------------------------------------------------------
"""
import signal
import sys
import threading

from flask import Flask, send_from_directory
from flask_sock import Sock

from server import config, logger
from server.rooms import RoomManager
from server.ws_server import WSServer

# ---------------------------------------------------------------
# Static file server for the frontend (public/)
# ---------------------------------------------------------------
app = Flask(__name__, static_folder='public', static_url_path='')

# Dead-connection detection: simple-websocket's native ping/pong,
# replaces Node's manual heartbeat setInterval (see ws_server.py
# module docstring for the full explanation of this swap).
app.config['SOCK_SERVER_OPTIONS'] = {
    'ping_interval': config.HEARTBEAT_INTERVAL_MS / 1000,
}

sock = Sock(app)


@app.route('/')
def index():
    return send_from_directory(app.static_folder, 'index.html')


@app.route('/<path:filename>')
def static_files(filename):
    # send_from_directory already guards against path traversal
    # outside static_folder, equivalent to the PUBLIC_DIR prefix
    # check in the original index.js.
    return send_from_directory(app.static_folder, filename)


# ---------------------------------------------------------------
# WebSocket route + protocol wiring
# ---------------------------------------------------------------
room_manager = RoomManager(logger)
ws_server = WSServer(room_manager, logger)


@sock.route('/ws')
def ws_route(ws):
    ws_server.handle_connection(ws)


# ---------------------------------------------------------------
# Graceful process shutdown: notify clients, close sockets, exit.
# ---------------------------------------------------------------
_shutdown_lock = threading.Lock()
_shutdown_started = False


def graceful_exit(signum, frame):
    global _shutdown_started
    with _shutdown_lock:
        if _shutdown_started:
            return
        _shutdown_started = True

    signame = signal.Signals(signum).name
    logger.log('server_shutdown', signal=signame)
    ws_server.shutdown()

    # Force-exit if sockets/threads don't wind down promptly —
    # mirrors the 3s setTimeout(() => process.exit(0)) safety net
    # in index.js.
    def force_exit():
        sys.stdout.flush()
        # os._exit skips cleanup, guaranteeing we don't hang even
        # if a non-daemon thread is still alive.
        import os
        os._exit(0)

    timer = threading.Timer(3.0, force_exit)
    timer.daemon = True
    timer.start()

    sys.exit(0)


signal.signal(signal.SIGINT, graceful_exit)
signal.signal(signal.SIGTERM, graceful_exit)


if __name__ == '__main__':
    logger.log(
        'server_start',
        port=config.PORT,
        rooms=config.DEFAULT_ROOMS,
        admins=config.ADMIN_USERNAMES,
    )
    print('\nGroup Chat server running:')
    print(f'  Local:   http://localhost:{config.PORT}')
    print(f'  Network: http://<this-machine-IP>:{config.PORT}  (use for other lab machines)')
    print(f'  Admins:  {", ".join(config.ADMIN_USERNAMES)} (set ADMIN_USERNAMES env var to change)\n')

    # threaded=True: multiple browser tabs / lab machines need
    # concurrent WS connections handled simultaneously, not
    # serialized — same concurrency guarantee Node gets for free.
    app.run(host='0.0.0.0', port=config.PORT, threaded=True)