"""
test_secure_chat.py
------------------------------------------------------------
Unit and integration tests for secure & persistent messaging.
------------------------------------------------------------
"""
import os
import sqlite3
import unittest
from cryptography.exceptions import InvalidTag

from server import crypto, store

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'chat.db')


class TestSecureChat(unittest.TestCase):
    def setUp(self):
        store.init_db()

    def test_1_aes_gcm_encrypt_decrypt(self):
        """Test AES-GCM 256 encrypt & decrypt."""
        secret_text = "This is a confidential message!"
        ct, nonce = crypto.encrypt_message(secret_text)
        self.assertIsInstance(ct, bytes)
        self.assertIsInstance(nonce, bytes)
        self.assertEqual(len(nonce), 12)  # Standard 96-bit nonce
        
        # Verify plaintext is NOT present in ciphertext
        self.assertNotIn(secret_text.encode('utf-8'), ct)

        # Successful decryption
        decrypted = crypto.decrypt_message(ct, nonce)
        self.assertEqual(decrypted, secret_text)

    def test_2_aes_gcm_tamper_detection(self):
        """Test that modifying even 1 bit of ciphertext causes InvalidTag."""
        secret_text = "Integrity protected text"
        ct, nonce = crypto.encrypt_message(secret_text)
        
        # Tamper with ciphertext
        tampered_ct = bytearray(ct)
        tampered_ct[0] ^= 0x01
        
        with self.assertRaises(InvalidTag):
            crypto.decrypt_message(bytes(tampered_ct), nonce)

    def test_3_ed25519_signatures(self):
        """Test Ed25519 asymmetric signature generation and verification."""
        priv_key, pub_key = crypto.get_or_create_sender_keys("alice")
        payload = b"msg_123|general|alice|1700000000|some_ciphertext"
        
        sig = crypto.sign_message(priv_key, payload)
        self.assertEqual(len(sig), 64)
        
        # Valid signature
        self.assertTrue(crypto.verify_signature(pub_key, sig, payload))
        
        # Invalid signature (tampered payload)
        tampered_payload = payload + b"_tampered"
        self.assertFalse(crypto.verify_signature(pub_key, sig, tampered_payload))

    def test_4_store_and_history_pipeline(self):
        """Test full Store -> Retrieve -> Verify -> Decrypt pipeline."""
        room = "security_test"
        msg = {
            "id": "test-sec-1",
            "username": "charlie",
            "text": "Hello secure room!",
            "timestamp": 1787000000000
        }
        stored = store.append_message(room, msg)
        self.assertEqual(stored['verified'], True)

        history = store.get_history(room, limit=10)
        self.assertTrue(len(history) >= 1)
        found = [m for m in history if m['id'] == "test-sec-1"][0]
        self.assertEqual(found['username'], "charlie")
        self.assertEqual(found['text'], "Hello secure room!")
        self.assertEqual(found['verified'], True)
        self.assertEqual(found['tampered'], False)

    def test_5_database_ciphertext_tamper_detection(self):
        """Test tampering a record directly in SQLite database."""
        room = "tamper_test_room"
        msg = {
            "id": "tamper-msg-99",
            "username": "dave",
            "text": "Critical financial transaction: $1000",
            "timestamp": 1787000010000
        }
        store.append_message(room, msg)

        # Directly tamper the ciphertext in SQLite
        conn = sqlite3.connect(DB_PATH)
        row = conn.execute("SELECT ciphertext FROM messages WHERE id = 'tamper-msg-99'").fetchone()
        raw_ct = bytearray(row[0])
        raw_ct[0] ^= 0xAA  # Flip bits
        conn.execute("UPDATE messages SET ciphertext = ? WHERE id = 'tamper-msg-99'", (bytes(raw_ct),))
        conn.commit()
        conn.close()

        # Read back through history
        history = store.get_history(room, limit=10)
        found = [m for m in history if m['id'] == "tamper-msg-99"][0]
        self.assertEqual(found['tampered'], True)
        self.assertIn("TAMPERED: AES-GCM Integrity Check Failed", found['text'])


if __name__ == '__main__':
    unittest.main()
