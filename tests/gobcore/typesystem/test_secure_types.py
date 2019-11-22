import unittest
import mock

from gobcore.typesystem.gob_secure_types import SecureString, SecureDecimal, SecureDateTime, Secure
from gobcore.secure.crypto import read_protect
from gobcore.secure.user import User
from gobcore.secure.config import ROLES


class TestSecure(unittest.TestCase):

    class MockChild(Secure):
        pass

    @mock.patch('gobcore.secure.fernet.config.os.getenv', lambda s, *args: s)
    def test_create(self):
        level = ROLES["user"]
        user = User(None)
        user._roles = ["user"]

        sec_string = SecureString.from_value(read_protect("some string"), level=10)
        self.assertTrue(isinstance(sec_string, SecureString))
        self.assertEqual(sec_string.get_value(user), "some string")

        sec_decimal = SecureDecimal.from_value(read_protect(10), level=10)
        self.assertTrue(isinstance(sec_decimal, SecureDecimal))
        self.assertEqual(sec_decimal.get_value(user), 10)

        sec_datetime = SecureDateTime.from_value(read_protect("2000-01-25T12:25:45.0"), level=10)
        self.assertTrue(isinstance(sec_datetime, SecureDateTime))

    @mock.patch("gobcore.typesystem.gob_secure_types.is_encrypted", lambda x: True)
    def test_from_value(self):
        res = self.MockChild.from_value('val', **{'kw': 'args'})
        self.assertIsInstance(res, type(self.MockChild('val')))

    @mock.patch("gobcore.typesystem.gob_secure_types.is_encrypted", lambda x: True)
    def test_get_value(self):
        securetype = self.MockChild('value')
        self.assertEqual('**********', securetype.get_value())


class TestSecureDateTime(unittest.TestCase):

    @mock.patch("gobcore.typesystem.gob_secure_types.DateTime")
    @mock.patch("gobcore.typesystem.gob_secure_types.is_encrypted", lambda x: True)
    def test_get_typed_value(self, mock_datetime):
        secure = SecureDateTime("val")

        res = secure.get_typed_value('value')
        self.assertEqual(res, mock_datetime.from_value.return_value.to_value)
        mock_datetime.from_value.assert_called_with('value')