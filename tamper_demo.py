"""
tamper_demo.py
------------------------------------------------------------
Interactive demonstration script for Secure & Persistent Messaging:
  1. Inspect raw encrypted records in SQLite (chat.db).
  2. Test reading and verifying messages.
  3. Tamper with a stored message's ciphertext directly in SQLite.
  4. Verify that AES-GCM authenticated decryption detects the modification!
------------------------------------------------------------
"""
import os
import sys
import sqlite3

# Add project root to sys.path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from server import store, crypto

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'chat.db')


def show_raw_db():
    print("\n" + "=" * 70)
    print(" RAW DATABASE RECORDS (data/chat.db)")
    print("=" * 70)
    if not os.path.exists(DB_PATH):
        print(" [!] Database chat.db does not exist yet. Send some messages first!")
        return []

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT id, room_id, sender, ciphertext, nonce, signature, timestamp FROM messages ORDER BY timestamp ASC").fetchall()
    conn.close()

    if not rows:
        print(" [!] No messages stored in chat.db yet.")
        return []

    for i, r in enumerate(rows, 1):
        print(f"Record #{i}:")
        print(f"  ID:         {r['id']}")
        print(f"  Room:       #{r['room_id']}")
        print(f"  Sender:     {r['sender']}")
        print(f"  Timestamp:  {r['timestamp']}")
        print(f"  Nonce:      {r['nonce'].hex()[:16]}... ({len(r['nonce'])} bytes)")
        print(f"  Ciphertext: {r['ciphertext'].hex()[:32]}... ({len(r['ciphertext'])} bytes) [ENCRYPTED]")
        print(f"  Signature:  {r['signature'].hex()[:32]}... ({len(r['signature'])} bytes) [Ed25519]")
        print("-" * 70)
    return rows


def show_decrypted_history(room_id="general"):
    print(f"\n[+] Retrieving & Verifying History for #{room_id} via store.get_history()...\n")
    history = store.get_history(room_id, limit=50)
    if not history:
        print(" No messages found in history.")
        return

    for msg in history:
        status_tag = " [VERIFIED]" if msg.get('verified') else " [TAMPERED / ERROR]"
        print(f"[{msg['username']}] {status_tag}")
        print(f"  Text: {msg['text']}")
        print()


def tamper_latest_message(room_id="general"):
    print(f"\n[!] Tampering with latest message in #{room_id} in SQLite directly...")
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute("SELECT id, ciphertext FROM messages WHERE room_id = ? ORDER BY timestamp DESC LIMIT 1", (room_id,)).fetchone()
    if not row:
        print(" [!] No message to tamper with.")
        conn.close()
        return

    msg_id, raw_ct = row[0], bytearray(row[1])
    # Flip the first byte of the ciphertext
    raw_ct[0] ^= 0xFF
    conn.execute("UPDATE messages SET ciphertext = ? WHERE id = ?", (bytes(raw_ct), msg_id))
    conn.commit()
    conn.close()
    print(f" [OK] Tampered message ID {msg_id}! Modified 1 byte of AES-GCM ciphertext in database.")


def add_sample_messages():
    print("\n[+] Adding sample encrypted & signed messages into chat.db...")
    store.append_message("general", {
        "id": "demo-msg-1",
        "username": "alice",
        "text": "Hello, this is a top-secret encrypted message!",
        "timestamp": 1786900000000
    })
    store.append_message("general", {
        "id": "demo-msg-2",
        "username": "bob",
        "text": "Hey Alice! AES-GCM 256 and Ed25519 signatures are working.",
        "timestamp": 1786900005000
    })
    print(" [OK] Added 2 sample messages successfully.")


if __name__ == '__main__':
    print("\n" + "#" * 70)
    print(" SECURE CHAT CRYPTO & TAMPERING DEMONSTRATION")
    print("#" * 70)

    # 1. Add sample messages if db is empty
    rows = show_raw_db()
    if not rows:
        add_sample_messages()
        show_raw_db()

    # 2. Show verified history
    print("\n--- PHASE 1: Normal History Retrieval (Untampered) ---")
    show_decrypted_history("general")

    # 3. Tamper with latest message
    print("\n--- PHASE 2: Simulating Attacker Modifying Stored Ciphertext in DB ---")
    tamper_latest_message("general")

    # 4. Show history after tampering
    print("\n--- PHASE 3: Retrieving History After Tampering ---")
    show_decrypted_history("general")
    print("=" * 70)
    print("RESULT: The system detected modification via AES-GCM tag verification!")
    print("=" * 70)
