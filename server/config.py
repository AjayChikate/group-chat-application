
import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env file from project root
_ENV_PATH = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(_ENV_PATH)

def csv(name: str, fallback: list) -> list:
    raw = os.environ.get(name)
    if not raw:
        return fallback
    return [s.strip() for s in raw.split(',') if s.strip()]


PORT = int(os.environ.get('PORT', '5000'))

MONGO_URL = os.environ.get('Mongo_URL') or os.environ.get('MONGO_URL') or os.environ.get('MONGO_URI')
MONGO_DB_NAME = os.environ.get('MONGO_DB_NAME', 'group_chat')

# Rooms that always exist, even with nobody in them.
DEFAULT_ROOMS = csv('DEFAULT_ROOMS', ['general', 'random', 'tech'])

# Usernames (case-insensitive) granted moderator powers (kick/mute).
ADMIN_USERNAMES = [s.lower() for s in csv('ADMIN_USERNAMES', ['admin'])]

#past messages are replayed to a client when it joins a room.
HISTORY_LIMIT = int(os.environ.get('HISTORY_LIMIT', '20'))

#ping every  ms, drop clients that never poing back.
HEARTBEAT_INTERVAL_MS = int(os.environ.get('HEARTBEAT_INTERVAL_MS', '30000'))

#mark a user "away" after this much inactivity.
IDLE_TIMEOUT_MS = int(os.environ.get('IDLE_TIMEOUT_MS', '100000'))

PRESENCE_CHECK_INTERVAL_MS = 15000

RATE_LIMIT = {
    'BURST': int(os.environ.get('RATE_LIMIT_BURST', '5000')),
    'REFILL_PER_SEC': float(os.environ.get('RATE_LIMIT_REFILL', '5000.0')),
}

MAX_USERNAME_LEN = 20
MAX_MESSAGE_LEN = 1000
MAX_ROOM_NAME_LEN = 24