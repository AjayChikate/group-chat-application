import os
import sqlite3
import threading
from typing import List, Dict, Any, Optional

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.asymmetric import ed25519

from server import crypto

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')
DB_PATH = os.path.join(DATA_DIR, 'chat.db')

_db_lock = threading.Lock()


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
                    CREATE TABLE IF NOT EXISTS user_keys (
                        username TEXT PRIMARY KEY,
                        public_key TEXT NOT NULL,
                        created_at REAL DEFAULT (strftime('%s', 'now'))
                    )
                """)
                
                # Migrate any legacy BLOB rows to hex string format for clean SQLite Viewer compatibility
                try:
                    rows = conn.execute("SELECT id, ciphertext, nonce, signature FROM messages").fetchall()
                    for r in rows:
                        needs_update = False
                        ct, nonce, sig = r['ciphertext'], r['nonce'], r['signature']
                        if isinstance(ct, bytes):
                            ct = ct.hex()
                            needs_update = True
                        if isinstance(nonce, bytes):
                            nonce = nonce.hex()
                            needs_update = True
                        if isinstance(sig, bytes):
                            sig = sig.hex()
                            needs_update = True
                        if needs_update:
                            conn.execute(
                                "UPDATE messages SET ciphertext=?, nonce=?, signature=? WHERE id=?",
                                (ct, nonce, sig, r['id'])
                            )
                            
                    key_rows = conn.execute("SELECT username, public_key FROM user_keys").fetchall()
                    for kr in key_rows:
                        pk = kr['public_key']
                        if isinstance(pk, bytes):
                            conn.execute(
                                "UPDATE user_keys SET public_key=? WHERE username=?",
                                (pk.hex(), kr['username'])
                            )
                except Exception:
                    pass
        finally:
            conn.close()


# Initialize database on module import
init_db()


def save_user_public_key(username: str, public_key_bytes: bytes) -> None:
    with _db_lock:
        conn = get_db_connection()
        try:
            with conn:
                conn.execute("""
                    INSERT INTO user_keys (username, public_key)
                    VALUES (?, ?)
                    ON CONFLICT(username) DO UPDATE SET public_key=excluded.public_key
                """, (username.lower(), _to_hex(public_key_bytes)))
        finally:
            conn.close()


def get_user_public_key(username: str) -> Optional[ed25519.Ed25519PublicKey]:
    with _db_lock:
        conn = get_db_connection()
        try:
            row = conn.execute(
                "SELECT public_key FROM user_keys WHERE username = ?",
                (username.lower(),)
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
) -> None:
    with _db_lock:
        conn = get_db_connection()
        try:
            with conn:
                conn.execute("""
                    INSERT OR REPLACE INTO messages (id, room_id, sender, ciphertext, nonce, signature, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (msg_id, room_id, sender, _to_hex(ciphertext), _to_hex(nonce), _to_hex(signature), timestamp))
        finally:
            conn.close()


def append_message(room_id: str, msg: Dict[str, Any], sender_private_key: Optional[ed25519.Ed25519PrivateKey] = None) -> Dict[str, Any]:
    msg_id = msg['id']
    sender = msg.get('username') or msg.get('from')
    text = msg['text']
    timestamp = msg['timestamp']

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

    # 3. Store in SQLite (saved as hex-encoded non-plaintext for full DB-viewer compatibility)
    save_message(msg_id, room_id, sender, ciphertext, nonce, signature, timestamp)

    return {
        'id': msg_id,
        'room': room_id,
        'username': sender,
        'text': text,
        'timestamp': timestamp,
        'verified': True,
    }


def get_history(room_id: str, limit: int = 50) -> List[Dict[str, Any]]:
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

    # Oldest first for chat display
    rows = list(reversed(rows))
    history = []

    for row in rows:
        msg_id = row['id']
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

        history.append({
            'id': msg_id,
            'username': sender,
            'text': decrypted_text,
            'room': room_id,
            'timestamp': timestamp,
            'verified': not is_tampered,
            'tampered': is_tampered,
        })

    return history