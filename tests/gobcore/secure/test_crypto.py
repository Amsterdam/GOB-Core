import unittest
import mock
import json

from gobcore.secure.crypto import is_encrypted, confidence_level, encrypt, decrypt
from gobcore.secure.crypto import read_protect, read_unprotect


class TestCrypto(unittest.TestCase):

    def setup(self):
        pass

    def test_is_encrypted(self):
        self.assertFalse(is_encrypted("any string"))
        self.assertFalse(is_encrypted(0))
        self.assertFalse(is_encrypted(True))

        self.assertFalse(is_encrypted({}))
        self.assertFalse(is_encrypted({"i": 0}))
        self.assertFalse(is_encrypted({"i": 0, "l": 0}))
        self.assertFalse(is_encrypted({"i": 0, "l": 0, "v": "value", "any": "other"}))

        self.assertTrue(is_encrypted(json.dumps({"i": 0, "l": 0, "v": "value"})))

    def test_confidence_level(self):
        self.assertEqual(confidence_level(json.dumps({"l": 5})), 5)

    @mock.patch('gobcore.secure.cryptos.config.os.getenv', lambda s, *args: s)
    def test_encrypt(self):
        self.assertEqual(json.loads(encrypt("value", 5)), {
            "i": mock.ANY,
            "l": 5,
            "v": mock.ANY
        })

    @mock.patch('gobcore.secure.cryptos.config.os.getenv', lambda s, *args: s)
    def test_decrypt(self):
        value = encrypt("value", 5)
        self.assertEqual(decrypt(value), "value")

    @mock.patch('gobcore.secure.cryptos.config.os.getenv', lambda s, *args: s)
    def test_decrypt_error(self):
        value = encrypt("value", 5)
        # Manipulate value
        value = json.loads(value)
        value['v'] = f"_{value['v']}"
        value = json.dumps(value)
        self.assertIsNone(decrypt(value))

    @mock.patch('gobcore.secure.cryptos.config.os.getenv', lambda s, *args: s)
    def test_encrypt_decrypt(self):
        value = encrypt("value", 5)
        self.assertEqual(decrypt(value), "value")

        value = encrypt(None, 5)
        self.assertIsNone(decrypt(value))

    @mock.patch('gobcore.secure.cryptos.config.os.getenv', lambda s, *args: s)
    def test_protect(self):
        value = read_protect("any value")
        self.assertEqual(read_unprotect(value), "any value")
