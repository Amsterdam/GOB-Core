import unittest
import mock

from gobcore.typesystem.gob_secure_types import SecureString, SecureDecimal, SecureDateTime, Secure, SecureDate
from gobcore.typesystem.gob_types import JSON
from gobcore.secure.crypto import read_protect
from gobcore.secure.user import User
from gobcore.secure.config import REQUEST_ROLES, GOB_ADMIN


class TestSecure(unittest.TestCase):

    class MockChild(Secure):
        pass

    @mock.patch('gobcore.secure.cryptos.config.os.getenv', lambda s, *args: s)
    def test_create(self):
        mock_request = mock.MagicMock()
        mock_request.headers = {
            REQUEST_ROLES: GOB_ADMIN
        }
        user = User(mock_request)

        sec_string = SecureString.from_value(read_protect("some string"), level=5)
        self.assertTrue(isinstance(sec_string, SecureString))
        self.assertEqual(sec_string.get_value(user), "some string")

        sec_decimal = SecureDecimal.from_value(read_protect(5), level=5)
        self.assertTrue(isinstance(sec_decimal, SecureDecimal))
        self.assertEqual(sec_decimal.get_value(user), 5)

        sec_datetime = SecureDateTime.from_value(read_protect("2000-01-25T12:25:45.0"), level=5)
        self.assertTrue(isinstance(sec_datetime, SecureDateTime))

    @mock.patch("gobcore.typesystem.gob_secure_types.is_encrypted", lambda x: True)
    def test_from_value(self):
        res = self.MockChild.from_value('val', **{'kw': 'args'})
        self.assertIsInstance(res, type(self.MockChild('val')))

    @mock.patch("gobcore.typesystem.gob_secure_types.is_encrypted", lambda x: True)
    def test_get_value(self):
        securetype = self.MockChild('value')
        self.assertIsNone(securetype.get_value())


class TestSecureDate(unittest.TestCase):

    @mock.patch("gobcore.typesystem.gob_secure_types.Date")
    @mock.patch("gobcore.typesystem.gob_secure_types.is_encrypted", lambda x: True)
    def test_get_typed_value(self, mock_date):
        secure = SecureDate("val")

        res = secure.get_typed_value('value')
        self.assertEqual(res, mock_date.from_value.return_value.to_value)
        mock_date.from_value.assert_called_with('value')


class TestSecureDateTime(unittest.TestCase):

    @mock.patch("gobcore.typesystem.gob_secure_types.DateTime")
    @mock.patch("gobcore.typesystem.gob_secure_types.is_encrypted", lambda x: True)
    def test_get_typed_value(self, mock_datetime):
        secure = SecureDateTime("val")

        res = secure.get_typed_value('value')
        self.assertEqual(res, mock_datetime.from_value.return_value.to_value)
        mock_datetime.from_value.assert_called_with('value')

class TestSecureJSON(unittest.TestCase):

    def test_from_value(self):
        mock_gob_type = mock.MagicMock()
        mock_gob_type.return_value = mock_gob_type
        mock_gob_type.from_value_secure.return_value = "secure value"
        mock_gob_type.get_value.return_value = "public value"
        value = {
            'key': 'value',
            'key1': 'value1',
            'sub': {
                'subkey': 'subvalue',
                'subkey1': 'subvalue1'
            }
        }
        secure = {
            'key': {
                'type': 'GOB.SecureString',
                'gob_type': mock_gob_type,
                'level': 5
            },
            'subkey': {
                'type': 'GOB.SecureString',
                'gob_type': mock_gob_type,
                'level': 5
            },
        }
        expect = {
            "key": "secure value",
            "key1": "value1",
            "sub": {
                "subkey": "secure value",
                "subkey1": "subvalue1"
            }
        }
        result = JSON.from_value(value, secure=secure)
        self.assertEqual(result.to_value, expect)

        expect = {
            "key": "public value",
            "key1": "value1",
            "sub": {
                "subkey": "public value",
                "subkey1": "subvalue1"
            }
        }
        result = result.get_value()
        self.assertEqual(result, expect)
