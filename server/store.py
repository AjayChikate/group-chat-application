import os
import threading
from typing import List, Dict, Any, Optional

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.asymmetric import ed25519
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.collection import Collection

from server import crypto


MONGODB_URI = os.environ.get(
    'MONGODB_URI',
    'mongodb+srv://shashankyadavriiii_db_user:9Y2RNLoRD6OWSC4h@cluster0.7azo9pt.mongodb.net'
)
MONGODB_DB = os.environ.get('MONGODB_DB', 'group_chat')

# Single MongoClient is thread-safe and connection-pooled
_client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
_db = _client[MONGODB_DB]

# Collections
_messages: Collection = _db['messages']
_user_keys: Collection = _db['user_keys']

# Create indexes once on startup
_messages.create_index([('room_id', ASCENDING), ('timestamp', DESCENDING)])
_user_keys.create_index('username', unique=True)


def _to_bytes(val) -> bytes:
    if isinstance(val, bytes):
        return val
    if isinstance(val, str):
        try:
            return bytes.fromhex(val)
        except ValueError:
            return val.encode('utf-8')
    return bytes(val)


def _to_hex(val) -> str:
    if isinstance(val, bytes):
        return val.hex()
    return str(val)


_user_keys_cache: Dict[str, ed25519.Ed25519PublicKey] = {}

# ---------------------------------------------------------------------------
# User public keys
# ---------------------------------------------------------------------------
def save_user_public_key(username: str, public_key_bytes: bytes) -> None:
    uname = username.lower()
    try:
        pub = ed25519.Ed25519PublicKey.from_public_bytes(public_key_bytes)
        _user_keys_cache[uname] = pub
    except Exception:
        pass

    _user_keys.update_one(
        {'username': uname},
        {'$set': {'public_key': _to_hex(public_key_bytes)}},
        upsert=True,
    )


def get_user_public_key(username: str) -> Optional[ed25519.Ed25519PublicKey]:
    uname = username.lower()
    if uname in _user_keys_cache:
        return _user_keys_cache[uname]

    doc = _user_keys.find_one({'username': uname})
    if doc:
        raw_bytes = _to_bytes(doc['public_key'])
        pub = ed25519.Ed25519PublicKey.from_public_bytes(raw_bytes)
        _user_keys_cache[uname] = pub
        return pub

    # Fallback: generate/load from local keystore on disk
    _, pub = crypto.get_or_create_sender_keys(username)
    _user_keys_cache[uname] = pub
    return pub


def save_message(
    msg_id: str,
    room_id: str,
    sender: str,
    ciphertext: bytes,
    nonce: bytes,
    signature: bytes,
    timestamp: int,
) -> None:
    _messages.update_one(
        {'_id': msg_id},
        {'$set': {
            'room_id': room_id,
            'sender': sender,
            'ciphertext': _to_hex(ciphertext),
            'nonce': _to_hex(nonce),
            'signature': _to_hex(signature),
            'timestamp': timestamp,
        }},
        upsert=True,
    )


def append_message(
    room_id: str,
    msg: Dict[str, Any],
    sender_private_key: Optional[ed25519.Ed25519PrivateKey] = None,
) -> Dict[str, Any]:
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

    # 3. Store in MongoDB
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
    # Fetch newest N messages, then reverse for oldest-first display
    cursor = _messages.find(
        {'room_id': room_id},
        sort=[('timestamp', DESCENDING)],
        limit=limit,
    )
    rows = list(reversed(list(cursor)))

    history = []
    for doc in rows:
        msg_id = doc['_id']
        sender = doc['sender']
        timestamp = doc['timestamp']
        ciphertext = _to_bytes(doc['ciphertext'])
        nonce = _to_bytes(doc['nonce'])
        signature = _to_bytes(doc['signature'])

        # 1. Verify Digital Signature (Ed25519)
        pub_key = get_user_public_key(sender)
        signable_payload = crypto.make_signable_payload(msg_id, room_id, sender, timestamp, nonce, ciphertext)

        signature_valid = False
        if pub_key:
            signature_valid = crypto.verify_signature(pub_key, signature, signable_payload)

        # 2. Decrypt (AES-GCM)
        decrypted_text = None
        decryption_valid = False
        try:
            decrypted_text = crypto.decrypt_message(ciphertext, nonce)
            decryption_valid = True
        except InvalidTag:
            decrypted_text = '[TAMPERED: AES-GCM Integrity Check Failed - Ciphertext Modified]'
        except Exception as e:
            decrypted_text = f'[DECRYPTION ERROR: {e}]'

        if not signature_valid and decryption_valid:
            decrypted_text = f'[UNVERIFIED SIGNATURE] {decrypted_text}'

        is_tampered = not (signature_valid and decryption_valid)

        history.append({
            'id': msg_id,
            'username': sender,
            'client-name': sender,
            'text': decrypted_text,
            'msg': decrypted_text,
            'room': room_id,
            'timestamp': timestamp,
            'verified': not is_tampered,
            'tampered': is_tampered,
        })

    return history


def get_feed(room_id: Optional[str] = None, limit: int = 2000) -> List[Dict[str, Any]]:
    """Retrieve all messages (across all rooms or filtered) sorted chronologically."""
    query = {}
    if room_id:
        query['room_id'] = room_id

    cursor = _messages.find(
        query,
        sort=[('timestamp', DESCENDING)],
        limit=limit,
    )
    rows = list(reversed(list(cursor)))

    # Prefetch missing public keys in a single bulk query
    missing_senders = [
        doc.get('sender', '').lower()
        for doc in rows
        if doc.get('sender', '').lower() and doc.get('sender', '').lower() not in _user_keys_cache
    ]
    if missing_senders:
        for u_doc in _user_keys.find({'username': {'$in': list(set(missing_senders))}}):
            try:
                raw_bytes = _to_bytes(u_doc['public_key'])
                _user_keys_cache[u_doc['username']] = ed25519.Ed25519PublicKey.from_public_bytes(raw_bytes)
            except Exception:
                pass

    feed = []
    for doc in rows:
        msg_id = doc['_id']
        sender = doc.get('sender', 'Anonymous')
        r_id = doc.get('room_id', 'general')
        timestamp = doc.get('timestamp', 0)
        ciphertext = _to_bytes(doc.get('ciphertext', ''))
        nonce = _to_bytes(doc.get('nonce', ''))
        signature = _to_bytes(doc.get('signature', ''))

        pub_key = get_user_public_key(sender)

        signable_payload = crypto.make_signable_payload(msg_id, r_id, sender, timestamp, nonce, ciphertext)

        signature_valid = False
        if pub_key:
            signature_valid = crypto.verify_signature(pub_key, signature, signable_payload)

        decrypted_text = None
        decryption_valid = False
        try:
            decrypted_text = crypto.decrypt_message(ciphertext, nonce)
            decryption_valid = True
        except InvalidTag:
            decrypted_text = '[TAMPERED: AES-GCM Integrity Check Failed]'
        except Exception as e:
            decrypted_text = f'[DECRYPTION ERROR: {e}]'

        is_tampered = not (signature_valid and decryption_valid)

        feed.append({
            'id': msg_id,
            'client-name': sender,
            'username': sender,
            'msg': decrypted_text,
            'text': decrypted_text,
            'room': r_id,
            'timestamp': timestamp,
            'verified': not is_tampered,
            'tampered': is_tampered,
        })

    return feed