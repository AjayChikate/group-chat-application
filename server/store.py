import os
import sqlite3
import threading
import time
from typing import List, Dict, Any, Optional

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.asymmetric import ed25519

from server import crypto, config

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')
DB_PATH = os.path.join(DATA_DIR, 'chat.db')

_db_lock = threading.Lock()

# ---------------------------------------------------------------------------
# MongoDB Atlas setup (Shared Persistent Database across all backend instances)
# ---------------------------------------------------------------------------
_mongo_client = None
_mongo_db = None

if config.MONGO_URL:
    try:
        import pymongo
        _mongo_client = pymongo.MongoClient(config.MONGO_URL, serverSelectionTimeoutMS=5000)
        # Test connectivity
        _mongo_client.admin.command('ping')
        _mongo_db = _mongo_client[config.MONGO_DB_NAME]
        
        # Ensure unique index on message ID for strict deduplication
        _mongo_db.messages.create_index([("id", pymongo.ASCENDING)], unique=True)
        _mongo_db.messages.create_index([("room_id", pymongo.ASCENDING), ("timestamp", pymongo.DESCENDING)])
        _mongo_db.messages.create_index([("timestamp", pymongo.ASCENDING)])
        _mongo_db.user_keys.create_index([("username", pymongo.ASCENDING)], unique=True)
        print(f"[store] Connected to shared MongoDB Atlas: database '{config.MONGO_DB_NAME}'")
    except Exception as e:
        print(f"[store] Warning: Could not connect to MongoDB Atlas ({e}). Falling back to local SQLite.")
        _mongo_client = None
        _mongo_db = None


def is_using_mongo() -> bool:
    return _mongo_db is not None


def _to_bytes(val) -> bytes:
    """Helper to convert stored hex string or BLOB to bytes."""
    if isinstance(val, bytes):
        return val
    if isinstance(val, str):
        try:
            return bytes.fromhex(val)
        except ValueError:
            return val.encode('utf-8')
    return bytes(val)


def _to_hex(val) -> str:
    """Helper to convert bytes or string to hex string."""
    if isinstance(val, bytes):
        return val.hex()
    return str(val)


def get_db_connection() -> sqlite3.Connection:
    crypto.ensure_crypto_dirs()
    conn = sqlite3.connect(DB_PATH, timeout=10.0, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    # Enable WAL mode and busy timeout for safe concurrent multi-process access
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn


def init_db() -> None:
    with _db_lock:
        conn = get_db_connection()
        try:
            with conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS messages (
                        id TEXT PRIMARY KEY,
                        room_id TEXT NOT NULL,
                        sender TEXT NOT NULL,
                        ciphertext TEXT NOT NULL,
                        nonce TEXT NOT NULL,
                        signature TEXT NOT NULL,
                        timestamp INTEGER NOT NULL
                    )
                """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_messages_room_ts 
                    ON messages(room_id, timestamp)
                """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_messages_ts 
                    ON messages(timestamp)
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS user_keys (
                        username TEXT PRIMARY KEY,
                        public_key TEXT NOT NULL,
                        created_at REAL DEFAULT (strftime('%s', 'now'))
                    )
                """)
        finally:
            conn.close()


# Initialize SQLite database schema
init_db()


def save_user_public_key(username: str, public_key_bytes: bytes) -> None:
    uname = username.lower()
    hex_key = _to_hex(public_key_bytes)
    
    if is_using_mongo():
        try:
            _mongo_db.user_keys.update_one(
                {"username": uname},
                {"$set": {"public_key": hex_key, "updated_at": time.time()}},
                upsert=True
            )
            return
        except Exception as e:
            print(f"[store] Error saving user public key to MongoDB: {e}")

    with _db_lock:
        conn = get_db_connection()
        try:
            with conn:
                conn.execute("""
                    INSERT INTO user_keys (username, public_key)
                    VALUES (?, ?)
                    ON CONFLICT(username) DO UPDATE SET public_key=excluded.public_key
                """, (uname, hex_key))
        finally:
            conn.close()


def get_user_public_key(username: str) -> Optional[ed25519.Ed25519PublicKey]:
    uname = username.lower()

    if is_using_mongo():
        try:
            doc = _mongo_db.user_keys.find_one({"username": uname})
            if doc:
                raw_bytes = _to_bytes(doc['public_key'])
                return ed25519.Ed25519PublicKey.from_public_bytes(raw_bytes)
        except Exception as e:
            print(f"[store] Error retrieving user key from MongoDB: {e}")

    with _db_lock:
        conn = get_db_connection()
        try:
            row = conn.execute(
                "SELECT public_key FROM user_keys WHERE username = ?",
                (uname,)
            ).fetchone()
            if row:
                raw_bytes = _to_bytes(row['public_key'])
                return ed25519.Ed25519PublicKey.from_public_bytes(raw_bytes)
        finally:
            conn.close()
    
    # Fallback to keystore on disk if available
    _, pub = crypto.get_or_create_sender_keys(username)
    return pub


def save_message(
    msg_id: str,
    room_id: str,
    sender: str,
    ciphertext: bytes,
    nonce: bytes,
    signature: bytes,
    timestamp: int
) -> bool:
    """
    Saves a message to the persistent store.
    Enforces deduplication: if msg_id already exists, duplicate insertion is prevented.
    Returns True if the message was already a duplicate, False if newly inserted.
    """
    hex_ct = _to_hex(ciphertext)
    hex_nonce = _to_hex(nonce)
    hex_sig = _to_hex(signature)

    if is_using_mongo():
        try:
            doc = {
                "id": msg_id,
                "room_id": room_id,
                "sender": sender,
                "ciphertext": hex_ct,
                "nonce": hex_nonce,
                "signature": hex_sig,
                "timestamp": timestamp,
            }
            res = _mongo_db.messages.update_one(
                {"id": msg_id},
                {"$setOnInsert": doc},
                upsert=True
            )
            # If matched_count > 0 and upserted_id is None, it was a duplicate
            is_duplicate = (res.upserted_id is None and res.matched_count > 0)
            return is_duplicate
        except Exception as e:
            # Handle duplicate key race condition gracefully
            if "duplicate key" in str(e) or "E11000" in str(e):
                return True
            print(f"[store] Error saving message to MongoDB: {e}")

    with _db_lock:
        conn = get_db_connection()
        try:
            with conn:
                cur = conn.execute("""
                    INSERT OR IGNORE INTO messages (id, room_id, sender, ciphertext, nonce, signature, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (msg_id, room_id, sender, hex_ct, hex_nonce, hex_sig, timestamp))
                is_duplicate = (cur.rowcount == 0)
                return is_duplicate
        finally:
            conn.close()


def _process_message_dict(row: Dict[str, Any]) -> Dict[str, Any]:
    """Helper to decrypt and verify a stored message row."""
    msg_id = row['id']
    room_id = row.get('room_id', 'general')
    sender = row['sender']
    timestamp = row['timestamp']
    ciphertext = _to_bytes(row['ciphertext'])
    nonce = _to_bytes(row['nonce'])
    signature = _to_bytes(row['signature'])

    # 1. Verify Digital Signature (Ed25519)
    pub_key = get_user_public_key(sender)
    signable_payload = crypto.make_signable_payload(msg_id, room_id, sender, timestamp, nonce, ciphertext)
    
    signature_valid = False
    if pub_key:
        signature_valid = crypto.verify_signature(pub_key, signature, signable_payload)

    # 2. Decrypt Ciphertext (AES-GCM)
    decrypted_text = None
    decryption_valid = False
    try:
        decrypted_text = crypto.decrypt_message(ciphertext, nonce)
        decryption_valid = True
    except InvalidTag:
        decrypted_text = "[TAMPERED: AES-GCM Integrity Check Failed - Ciphertext Modified]"
    except Exception as e:
        decrypted_text = f"[DECRYPTION ERROR: {str(e)}]"

    if not signature_valid and decryption_valid:
        decrypted_text = f"[UNVERIFIED SIGNATURE] {decrypted_text}"

    is_tampered = not (signature_valid and decryption_valid)

    return {
        'id': msg_id,
        'username': sender,
        'sender': sender,
        'client-name': sender,
        'text': decrypted_text,
        'msg': decrypted_text,
        'room': room_id,
        'timestamp': timestamp,
        'verified': not is_tampered,
        'tampered': is_tampered,
    }


def get_message_by_id(msg_id: str) -> Optional[Dict[str, Any]]:
    if is_using_mongo():
        try:
            doc = _mongo_db.messages.find_one({"id": msg_id})
            if doc:
                return _process_message_dict(doc)
        except Exception as e:
            print(f"[store] Error finding message by id in MongoDB: {e}")

    with _db_lock:
        conn = get_db_connection()
        try:
            row = conn.execute(
                "SELECT id, room_id, sender, ciphertext, nonce, signature, timestamp FROM messages WHERE id = ?",
                (msg_id,)
            ).fetchone()
            if row:
                return _process_message_dict(dict(row))
        finally:
            conn.close()

    return None


def append_message(
    room_id: str,
    msg: Dict[str, Any],
    sender_private_key: Optional[ed25519.Ed25519PrivateKey] = None
) -> Dict[str, Any]:
    """
    Encrypts (AES-256-GCM), signs (Ed25519), and stores a message.
    Guarantees deduplication if msg_id was already persisted.
    """
    msg_id = msg['id']
    sender = msg.get('username') or msg.get('from') or msg.get('client-name') or msg.get('client_name') or 'anonymous'
    text = msg.get('text') if msg.get('text') is not None else msg.get('msg', '')
    timestamp = msg.get('timestamp') or int(time.time() * 1000)

    # Check if already in DB for deduplication
    existing = get_message_by_id(msg_id)
    if existing:
        return {
            'id': msg_id,
            'room': existing.get('room', room_id),
            'username': existing.get('sender', sender),
            'client-name': existing.get('sender', sender),
            'text': existing.get('text', text),
            'msg': existing.get('msg', text),
            'timestamp': existing.get('timestamp', timestamp),
            'verified': existing.get('verified', True),
            'duplicate': True,
        }

    # Ensure sender keys exist
    if sender_private_key is None:
        sender_private_key, sender_pub = crypto.get_or_create_sender_keys(sender)
    else:
        sender_pub = sender_private_key.public_key()

    save_user_public_key(sender, sender_pub.public_bytes_raw())

    # 1. Encrypt (AES-GCM 256)
    ciphertext, nonce = crypto.encrypt_message(text)

    # 2. Sign (Ed25519)
    signable_payload = crypto.make_signable_payload(msg_id, room_id, sender, timestamp, nonce, ciphertext)
    signature = crypto.sign_message(sender_private_key, signable_payload)

    # 3. Store in DB
    is_duplicate = save_message(msg_id, room_id, sender, ciphertext, nonce, signature, timestamp)

    return {
        'id': msg_id,
        'room': room_id,
        'username': sender,
        'client-name': sender,
        'text': text,
        'msg': text,
        'timestamp': timestamp,
        'verified': True,
        'duplicate': is_duplicate,
    }


def get_history(room_id: str, limit: int = 50) -> List[Dict[str, Any]]:
    """Retrieves and decrypts recent messages for a specific room."""
    if is_using_mongo():
        try:
            cursor = _mongo_db.messages.find({"room_id": room_id}).sort("timestamp", -1).limit(limit)
            rows = list(reversed(list(cursor)))
            return [_process_message_dict(r) for r in rows]
        except Exception as e:
            print(f"[store] Error retrieving history from MongoDB: {e}")

    with _db_lock:
        conn = get_db_connection()
        try:
            cursor = conn.execute("""
                SELECT id, room_id, sender, ciphertext, nonce, signature, timestamp
                FROM messages
                WHERE room_id = ?
                ORDER BY timestamp DESC
                LIMIT ?
            """, (room_id, limit))
            rows = cursor.fetchall()
        finally:
            conn.close()

    rows = list(reversed(rows))
    return [_process_message_dict(dict(r)) for r in rows]


def get_all_messages(limit: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Retrieves and decrypts all messages in chronological order for the /feed endpoint.
    """
    if is_using_mongo():
        try:
            cursor = _mongo_db.messages.find().sort("timestamp", 1)
            if limit:
                cursor = cursor.limit(limit)
            return [_process_message_dict(r) for r in cursor]
        except Exception as e:
            print(f"[store] Error retrieving all messages from MongoDB: {e}")

    with _db_lock:
        conn = get_db_connection()
        try:
            query = """
                SELECT id, room_id, sender, ciphertext, nonce, signature, timestamp
                FROM messages
                ORDER BY timestamp ASC
            """
            if limit:
                query += f" LIMIT {int(limit)}"
            rows = conn.execute(query).fetchall()
            return [_process_message_dict(dict(r)) for r in rows]
        finally:
            conn.close()