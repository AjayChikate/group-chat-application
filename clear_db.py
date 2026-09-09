"""
clear_db.py
------------------------------------------------------------
Clears all stored chat messages and cached keys from MongoDB Atlas (if configured)
and the local SQLite database.
------------------------------------------------------------
"""
import os
import sqlite3
from server import config

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'chat.db')

def clear_database():
    # 1. Clear MongoDB if configured
    if config.MONGO_URL:
        try:
            import pymongo
            client = pymongo.MongoClient(config.MONGO_URL, serverSelectionTimeoutMS=5000)
            db = client[config.MONGO_DB_NAME]
            msg_res = db.messages.delete_many({})
            keys_res = db.user_keys.delete_many({})
            print("=" * 60)
            print(" MONGODB ATLAS CLEARED SUCCESSFULLY")
            print("=" * 60)
            print(f" Deleted {msg_res.deleted_count} message(s) from MongoDB collection 'messages'.")
            print(f" Deleted {keys_res.deleted_count} key(s) from MongoDB collection 'user_keys'.")
        except Exception as e:
            print(f"[!] Error clearing MongoDB Atlas: {e}")

    # 2. Clear SQLite
    if os.path.exists(DB_PATH):
        conn = sqlite3.connect(DB_PATH)
        try:
            with conn:
                msg_count = conn.execute("SELECT count(*) FROM messages").fetchone()[0]
                conn.execute("DELETE FROM messages")
                conn.execute("DELETE FROM user_keys")
            conn.execute("VACUUM")
            print("=" * 60)
            print(" SQLITE DATABASE CLEARED SUCCESSFULLY")
            print("=" * 60)
            print(f" Deleted {msg_count} message(s) from SQLite 'messages' table.")
            print(f" Cleared 'user_keys' table.")
        finally:
            conn.close()

if __name__ == '__main__':
    clear_database()
