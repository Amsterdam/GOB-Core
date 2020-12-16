from unittest import TestCase
from unittest.mock import patch

from gobcore.secure.request import REQUEST_ACCESS_TOKEN, extract_roles, is_secured_request


class TestRequest(TestCase):

    @patch("gobcore.secure.request.jwt")
    def test_extract_roles(self, mock_jwt):

        mock_jwt.decode.return_value = {
            'realm_access': {
                'roles': ['any roles']
            }
        }

        headers = {
            REQUEST_ACCESS_TOKEN: "the token"
        }
        roles = extract_roles(headers)
        self.assertEqual(roles, ["any roles"])
        mock_jwt.decode.assert_called_with('the token', verify=False)

        roles = extract_roles({})
        self.assertEqual(roles, [])

    def test_is_secured_request(self):
        self.assertTrue(is_secured_request({REQUEST_ACCESS_TOKEN: 'access token'}))
        self.assertFalse(is_secured_request({}))
