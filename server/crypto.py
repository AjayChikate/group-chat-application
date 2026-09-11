import json
import os
from typing import Tuple

from cryptography.exceptions import InvalidTag, InvalidSignature
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')
KEYS_DIR = os.path.join(DATA_DIR, 'keys')
MASTER_KEY_FILE = os.path.join(DATA_DIR, 'master.key')


# In-memory caches to avoid redundant disk I/O during high-concurrency bursts
_cached_master_key: bytes = None
_cached_sender_keys: dict = {}

def ensure_crypto_dirs():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(KEYS_DIR, exist_ok=True)

# 1. Symmetric encryption (AES-GCM 256-bit)
def get_master_key() -> bytes:
    global _cached_master_key
    if _cached_master_key is not None:
        return _cached_master_key

    ensure_crypto_dirs()
    if os.path.exists(MASTER_KEY_FILE):
        with open(MASTER_KEY_FILE, 'rb') as f:
            key = f.read()
            if len(key) == 32:
                _cached_master_key = key
                return key
    key = AESGCM.generate_key(bit_length=256)
    with open(MASTER_KEY_FILE, 'wb') as f:
        f.write(key)
    _cached_master_key = key
    return key


def encrypt_message(plaintext: str, key: bytes = None) -> Tuple[bytes, bytes]:
    if key is None:
        key = get_master_key()
    aes = AESGCM(key)
    nonce = os.urandom(12)  # 96-bit standard nonce for GCM
    ciphertext = aes.encrypt(nonce, plaintext.encode('utf-8'), None)
    return ciphertext, nonce


def decrypt_message(ciphertext: bytes, nonce: bytes, key: bytes = None) -> str:
    if key is None:
        key = get_master_key()
    aes = AESGCM(key)
    plaintext_bytes = aes.decrypt(nonce, ciphertext, None)
    return plaintext_bytes.decode('utf-8')


# 2. Digital Signatures (Ed25519 Asymmetric Key Pairs)
def get_or_create_sender_keys(username: str) -> Tuple[ed25519.Ed25519PrivateKey, ed25519.Ed25519PublicKey]:
    sanitized = "".join(c for c in username if c.isalnum() or c in ('_', '-')) or 'user'
    if sanitized in _cached_sender_keys:
        return _cached_sender_keys[sanitized]

    ensure_crypto_dirs()
    key_path = os.path.join(KEYS_DIR, f"{sanitized}.key")

    if os.path.exists(key_path):
        with open(key_path, 'rb') as f:
            raw_private = f.read()
            if len(raw_private) == 32:
                private_key = ed25519.Ed25519PrivateKey.from_private_bytes(raw_private)
                pair = (private_key, private_key.public_key())
                _cached_sender_keys[sanitized] = pair
                return pair

    private_key = ed25519.Ed25519PrivateKey.generate()
    raw_private = private_key.private_bytes_raw()
    try:
        with open(key_path, 'wb') as f:
            f.write(raw_private)
    except Exception:
        pass

    pair = (private_key, private_key.public_key())
    _cached_sender_keys[sanitized] = pair
    return pair



def make_signable_payload(msg_id: str, room_id: str, sender: str, timestamp: int, nonce: bytes, ciphertext: bytes) -> bytes:
    
    header = f"{msg_id}|{room_id}|{sender}|{timestamp}|".encode('utf-8')
    return header + nonce + b"|" + ciphertext


def sign_message(private_key: ed25519.Ed25519PrivateKey, payload: bytes) -> bytes:
    return private_key.sign(payload)


def verify_signature(public_key: ed25519.Ed25519PublicKey, signature: bytes, payload: bytes) -> bool:
    try:
        public_key.verify(signature, payload)
        return True
    except (InvalidSignature, Exception):
        return False
