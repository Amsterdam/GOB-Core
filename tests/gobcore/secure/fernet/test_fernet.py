import unittest
import mock

from gobcore.secure.fernet.crypto import Crypto, DecryptionError
from gobcore.secure.fernet.config import _getenv


class TestFernet(unittest.TestCase):

    def test_create(self):
        self.assertIsNotNone(Crypto())

    def test_encrypt_decrypt(self):
        _, enc = Crypto().encrypt("any value")
        self.assertEqual(Crypto().decrypt(enc), "any value")

    def test_encrypt_decrypt_value_error(self):
        _, enc = Crypto().encrypt("any value")
        self.assertEqual(Crypto().decrypt(enc), "any value")

        with self.assertRaises(DecryptionError):
            Crypto().decrypt(f"_{enc}")

    @mock.patch('gobcore.secure.fernet.config.os.getenv')
    def test_encrypt_decrypt_key_error(self, mock_get_env):
        mock_get_env.side_effect = lambda s, *args: s
        _, enc = Crypto().encrypt("any value")
        self.assertEqual(Crypto().decrypt(enc), "any value")

        Crypto._fernet = None
        mock_get_env.side_effect = lambda s, *args: f"_{s}"
        with self.assertRaises(DecryptionError):
            Crypto().decrypt(enc)
