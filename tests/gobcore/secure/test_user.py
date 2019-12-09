from unittest import TestCase, mock

from gobcore.secure.config import REQUEST_ROLES, GOB_ADMIN
from gobcore.secure.user import User


class TestUser(TestCase):

    def setup(self):
        pass

    def test_create(self):
        mock_request = mock.MagicMock()
        mock_request.headers = {
            REQUEST_ROLES: GOB_ADMIN
        }
        user = User(mock_request)
        self.assertIsNotNone(user._roles)

    def test_has_access(self):
        mock_request = mock.MagicMock()
        mock_request.headers = {}
        user = User(mock_request)
        value = "any encrypted value"

        user._roles = []
        self.assertFalse(user.has_access_to(value))

        user._roles = ["any user"]
        self.assertFalse(user.has_access_to(value))

        user._roles = [GOB_ADMIN]
        self.assertTrue(user.has_access_to(value))
