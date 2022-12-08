import unittest
from unittest import mock

from gobcore.secure.cryptos.fernet import FernetCrypto, DecryptionError
from gobcore.secure.cryptos.config import _getenv


class TestFernet(unittest.TestCase):

    def test_create(self):
        self.assertIsNotNone(FernetCrypto())

    def test_encrypt_decrypt(self):
        _, enc = FernetCrypto().encrypt("any value")
        self.assertEqual(FernetCrypto().decrypt(enc), "any value")

    def test_encrypt_decrypt_value_error(self):
        _, enc = FernetCrypto().encrypt("any value")
        self.assertEqual(FernetCrypto().decrypt(enc), "any value")

        with self.assertRaises(DecryptionError):
            FernetCrypto().decrypt(f"_{enc}")

    @mock.patch('gobcore.secure.cryptos.config.os.getenv')
    def test_encrypt_decrypt_key_error(self, mock_get_env):
        mock_get_env.side_effect = lambda s, *args: s
        _, enc = FernetCrypto().encrypt("any value")
        self.assertEqual(FernetCrypto().decrypt(enc), "any value")

        _, enc1 = FernetCrypto().encrypt("any value")
        _, enc2 = FernetCrypto().encrypt("any value")
        self.assertNotEqual(enc1, enc2)

        FernetCrypto._fernet = None
        mock_get_env.side_effect = lambda s, *args: f"_{s}"
        with self.assertRaises(DecryptionError):
            FernetCrypto().decrypt(enc)
