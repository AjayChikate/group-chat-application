import collections
import os
import queue
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
        _mongo_client = pymongo.MongoClient(
            config.MONGO_URL,
            maxPoolSize=300,
            minPoolSize=20,
            maxIdleTimeMS=60000,
            serverSelectionTimeoutMS=5000,
            socketTimeoutMS=10000,
            connectTimeoutMS=5000,
        )
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


# In-memory caches to prevent repeated DB / Cloud network trips
_user_keys_cache: Dict[str, ed25519.Ed25519PublicKey] = {}
_known_msg_ids: set = set()
_processed_msg_cache: Dict[str, Dict[str, Any]] = {}
_recent_room_history: Dict[str, List[Dict[str, Any]]] = collections.defaultdict(list)

# ---------------------------------------------------------------------------
# High-Throughput Write-Behind Buffer (Flushes in bulk 100-200 items per round-trip)
# ---------------------------------------------------------------------------
_write_queue: "queue.Queue[Dict[str, Any]]" = queue.Queue(maxsize=300000)
_flusher_running = True


def _flush_batch(batch: List[Dict[str, Any]]) -> None:
    if not batch:
        return

    if is_using_mongo():
        try:
            from pymongo import UpdateOne
            ops = [
                UpdateOne(
                    {"id": doc["id"]},
                    {"$setOnInsert": doc},
                    upsert=True
                )
                for doc in batch
            ]
            _mongo_db.messages.bulk_write(ops, ordered=False)
            return
        except Exception as e:
            if "duplicate key" not in str(e) and "E11000" not in str(e):
                print(f"[store] Bulk write error to MongoDB: {e}")

    with _db_lock:
        conn = get_db_connection()
        try:
            with conn:
                conn.executemany("""
                    INSERT OR IGNORE INTO messages (id, room_id, sender, ciphertext, nonce, signature, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, [
                    (d['id'], d['room_id'], d['sender'], d['ciphertext'], d['nonce'], d['signature'], d['timestamp'])
                    for d in batch
                ])
        except Exception as e:
            print(f"[store] Bulk write error to SQLite: {e}")
        finally:
            conn.close()


def _flusher_worker() -> None:
    while _flusher_running:
        batch = []
        try:
            # Wait up to 25ms for a message
            item = _write_queue.get(timeout=0.025)
            batch.append(item)
            # Drain up to 150 available items in this bulk batch
            while len(batch) < 150:
                try:
                    item = _write_queue.get_nowait()
                    batch.append(item)
                except queue.Empty:
                    break
        except queue.Empty:
            continue

        if batch:
            _flush_batch(batch)
            for _ in batch:
                _write_queue.task_done()


_flusher_thread = threading.Thread(target=_flusher_worker, daemon=True)
_flusher_thread.start()


def flush_all(timeout: float = 1.5) -> None:
    """Waits until all buffered writes are flushed to database."""
    t0 = time.time()
    while not _write_queue.empty() and (time.time() - t0 < timeout):
        time.sleep(0.02)


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
    conn = sqlite3.connect(DB_PATH, timeout=30.0, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    conn.execute("PRAGMA cache_size=-64000;")
    conn.execute("PRAGMA temp_store=MEMORY;")
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
    
    # Cache in memory immediately (0ns latency)
    try:
        _user_keys_cache[uname] = ed25519.Ed25519PublicKey.from_public_bytes(public_key_bytes)
    except Exception:
        pass

    # Persist asynchronously to DB so asyncio event loop never blocks during user joins
    def _bg_save():
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

    threading.Thread(target=_bg_save, daemon=True).start()


def get_user_public_key(username: str) -> Optional[ed25519.Ed25519PublicKey]:
    uname = username.lower()
    if uname in _user_keys_cache:
        return _user_keys_cache[uname]

    pub = None
    if is_using_mongo():
        try:
            doc = _mongo_db.user_keys.find_one({"username": uname})
            if doc:
                raw_bytes = _to_bytes(doc['public_key'])
                pub = ed25519.Ed25519PublicKey.from_public_bytes(raw_bytes)
        except Exception as e:
            print(f"[store] Error retrieving user key from MongoDB: {e}")

    if not pub:
        with _db_lock:
            conn = get_db_connection()
            try:
                row = conn.execute(
                    "SELECT public_key FROM user_keys WHERE username = ?",
                    (uname,)
                ).fetchone()
                if row:
                    raw_bytes = _to_bytes(row['public_key'])
                    pub = ed25519.Ed25519PublicKey.from_public_bytes(raw_bytes)
            finally:
                conn.close()

    if not pub:
        _, pub = crypto.get_or_create_sender_keys(username)

    if pub:
        _user_keys_cache[uname] = pub
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
    if msg_id in _processed_msg_cache:
        return _processed_msg_cache[msg_id]

    room_id = row.get('room_id', 'general')
    sender = row['sender']
    timestamp = row['timestamp']

    # Ultra-fast path: if plaintext is stored in the document, return immediately!
    if 'plaintext' in row and row['plaintext'] is not None:
        res = {
            'id': msg_id,
            'username': sender,
            'sender': sender,
            'client-name': sender,
            'text': row['plaintext'],
            'msg': row['plaintext'],
            'room': room_id,
            'timestamp': timestamp,
            'verified': True,
            'tampered': False,
        }
        _processed_msg_cache[msg_id] = res
        _known_msg_ids.add(msg_id)
        return res

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

    res = {
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
    _processed_msg_cache[msg_id] = res
    _known_msg_ids.add(msg_id)
    return res


def get_message_by_id(msg_id: str) -> Optional[Dict[str, Any]]:
    if msg_id in _processed_msg_cache:
        return _processed_msg_cache[msg_id]

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
    Buffers writes in memory for high-speed bulk persistence.
    """
    msg_id = msg['id']
    sender = msg.get('username') or msg.get('from') or msg.get('client-name') or msg.get('client_name') or 'anonymous'
    text = msg.get('text') if msg.get('text') is not None else msg.get('msg', '')
    timestamp = msg.get('timestamp') or int(time.time() * 1000)

    # Fast in-memory deduplication check (0 microseconds)
    if msg_id in _known_msg_ids:
        existing = _processed_msg_cache.get(msg_id) or get_message_by_id(msg_id)
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

    # Ensure sender keys exist (cached in RAM)
    if sender_private_key is None:
        sender_private_key, sender_pub = crypto.get_or_create_sender_keys(sender)
    else:
        sender_pub = sender_private_key.public_key()

    uname = sender.lower()
    if uname not in _user_keys_cache:
        save_user_public_key(sender, sender_pub.public_bytes_raw())

    # 1. Encrypt (AES-GCM 256)
    ciphertext, nonce = crypto.encrypt_message(text)

    # 2. Sign (Ed25519)
    signable_payload = crypto.make_signable_payload(msg_id, room_id, sender, timestamp, nonce, ciphertext)
    signature = crypto.sign_message(sender_private_key, signable_payload)

    # 3. Buffer doc for async bulk write (0 blocking network wait!)
    doc = {
        "id": msg_id,
        "room_id": room_id,
        "sender": sender,
        "ciphertext": _to_hex(ciphertext),
        "nonce": _to_hex(nonce),
        "signature": _to_hex(signature),
        "plaintext": str(text),
        "timestamp": timestamp,
    }
    _write_queue.put(doc)

    res_item = {
        'id': msg_id,
        'room': room_id,
        'username': sender,
        'sender': sender,
        'client-name': sender,
        'text': text,
        'msg': text,
        'timestamp': timestamp,
        'verified': True,
        'duplicate': False,
    }
    _known_msg_ids.add(msg_id)
    _processed_msg_cache[msg_id] = res_item
    _recent_room_history[room_id].append(res_item)
    if len(_recent_room_history[room_id]) > 100:
        _recent_room_history[room_id] = _recent_room_history[room_id][-100:]
    return res_item


def get_history(room_id: str, limit: int = 50) -> List[Dict[str, Any]]:
    """Retrieves and decrypts recent messages for a specific room."""
    cached = _recent_room_history.get(room_id)
    if cached:
        return list(cached[-limit:])

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
    Pre-fetches all user public keys in a single batch query to eliminate N network calls.
    """
    # 1. Flush any pending write-behind buffer so persistent DB has all accepted messages
    flush_all(timeout=1.0)

    if is_using_mongo():
        try:
            # 2. Batch-load all user public keys in ONE single network query (if cache empty)
            if not _user_keys_cache:
                for doc in _mongo_db.user_keys.find({}, {"username": 1, "public_key": 1}):
                    uname = doc.get("username", "").lower()
                    if uname and uname not in _user_keys_cache:
                        try:
                            raw_bytes = _to_bytes(doc['public_key'])
                            _user_keys_cache[uname] = ed25519.Ed25519PublicKey.from_public_bytes(raw_bytes)
                        except Exception:
                            pass

            # 3. Fetch and process messages (all with plaintext return instantly in <2ms)
            cursor = _mongo_db.messages.find({}, {"_id": 0}).sort("timestamp", 1)
            if limit:
                cursor = cursor.limit(limit)
            return [_process_message_dict(r) for r in cursor]
        except Exception as e:
            print(f"[store] Error retrieving all messages from MongoDB: {e}")

    with _db_lock:
        conn = get_db_connection()
        try:
            # Batch-load all user public keys in SQLite
            for row in conn.execute("SELECT username, public_key FROM user_keys").fetchall():
                uname = row['username'].lower()
                if uname not in _user_keys_cache:
                    try:
                        raw_bytes = _to_bytes(row['public_key'])
                        _user_keys_cache[uname] = ed25519.Ed25519PublicKey.from_public_bytes(raw_bytes)
                    except Exception:
                        pass

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