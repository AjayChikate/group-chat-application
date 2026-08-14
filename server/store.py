"""
store.py
------------------------------------------------------------
Persistence layer for chat history.

Design choice: an append-only JSON-Lines file per room
(`data/rooms/<room>.log`), one message per line. This gives us:
  - Durability across server restarts (the baseline tutorial
    lost all history the moment the process died).
  - A write path that's just an append to a file — no schema
    migrations, no native DB driver, works on any lab machine
    with plain Python.
  - A trivial "replay last N messages" read path for history-
    on-join.

This module is intentionally the ONLY place that knows about
the on-disk format. Everything else in the app calls
append_message()/get_history() and doesn't care how or where
messages are stored — so swapping this for SQLite/Postgres
later only means rewriting this one file.
------------------------------------------------------------
"""
import json
import os
import re

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data', 'rooms')


def sanitize_name(name: str) -> str:
    """Keep filenames predictable and traversal-safe regardless of
    what a client sends as a room name."""
    cleaned = re.sub(r'[^a-zA-Z0-9_-]', '_', str(name))[:64]
    return cleaned or 'room'


def ensure_dir() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)


def room_file(room: str) -> str:
    return os.path.join(DATA_DIR, f'{sanitize_name(room)}.log')


def append_message(room: str, message: dict) -> None:
    """Append one message object to a room's durable log."""
    ensure_dir()
    with open(room_file(room), 'a', encoding='utf-8') as f:
        f.write(json.dumps(message) + '\n')


def get_history(room: str, limit: int) -> list:
    """Return up to `limit` most recent messages for a room, oldest first."""
    ensure_dir()
    file = room_file(room)
    if not os.path.exists(file):
        return []
    with open(file, 'r', encoding='utf-8') as f:
        lines = [line for line in f.read().strip().split('\n') if line]
    tail = lines[-limit:] if limit else lines
    result = []
    for line in tail:
        try:
            result.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return result