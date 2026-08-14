"""
config.py
------------------------------------------------------------
Single source of truth for tunables. Keeping this separate
(instead of magic numbers scattered through the codebase) is
a basic separation-of-concerns move: anyone tuning behavior
for a demo/lab reads one file, not five.

Everything here can be overridden with environment variables
so the same code runs unmodified on any lab machine.
------------------------------------------------------------
"""
import os


def csv(name: str, fallback: list) -> list:
    """Parse a comma-separated env var into a list; fall back if unset."""
    raw = os.environ.get(name)
    if not raw:
        return fallback
    return [s.strip() for s in raw.split(',') if s.strip()]


PORT = int(os.environ.get('PORT', '8080'))

# Rooms that always exist, even with nobody in them.
DEFAULT_ROOMS = csv('DEFAULT_ROOMS', ['general', 'random', 'tech'])

# Usernames (case-insensitive) granted moderator powers (kick/mute).
# Set ADMIN_USERNAMES="alice,bob" as an env var to change this per-run.
ADMIN_USERNAMES = [s.lower() for s in csv('ADMIN_USERNAMES', ['admin'])]

# How many past messages are replayed to a client when it joins/switches a room.
HISTORY_LIMIT = int(os.environ.get('HISTORY_LIMIT', '50'))

# Dead-connection detection: ping every N ms, drop clients that never pong.
HEARTBEAT_INTERVAL_MS = int(os.environ.get('HEARTBEAT_INTERVAL_MS', '30000'))

# Presence: mark a user "away" after this much inactivity.
IDLE_TIMEOUT_MS = int(os.environ.get('IDLE_TIMEOUT_MS', '60000'))

PRESENCE_CHECK_INTERVAL_MS = 15000

# Anti-spam token bucket: BURST tokens available immediately,
# refilling at REFILL_PER_SEC tokens/second thereafter.
RATE_LIMIT = {
    'BURST': int(os.environ.get('RATE_LIMIT_BURST', '8')),
    'REFILL_PER_SEC': float(os.environ.get('RATE_LIMIT_REFILL', '2')),
}

MAX_USERNAME_LEN = 20
MAX_MESSAGE_LEN = 1000
MAX_ROOM_NAME_LEN = 24