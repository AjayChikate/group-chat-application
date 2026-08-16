"""
clear_db.py
------------------------------------------------------------
Clears all stored chat messages and cached keys from the SQLite database.
------------------------------------------------------------
"""
import os
import sqlite3

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'chat.db')

def clear_database():
    if not os.path.exists(DB_PATH):
        print(f"[!] Database file does not exist at {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    try:
        with conn:
            # Delete all stored messages
            msg_count = conn.execute("SELECT count(*) FROM messages").fetchone()[0]
            conn.execute("DELETE FROM messages")
            
            # Delete user public keys table rows
            conn.execute("DELETE FROM user_keys")
            
        # Run VACUUM outside transaction to reclaim disk space
        conn.execute("VACUUM")
            
        print("=" * 60)
        print(" DATABASE CLEARED SUCCESSFULLY")
        print("=" * 60)
        print(f" Deleted {msg_count} message(s) from 'messages' table.")
        print(f" Cleared 'user_keys' table.")
        print(f" Database at {DB_PATH} is now fresh and empty.")
        print("=" * 60)
    finally:
        conn.close()

if __name__ == '__main__':
    clear_database()
