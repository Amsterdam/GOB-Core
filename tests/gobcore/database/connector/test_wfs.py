from unittest import TestCase
from unittest.mock import patch

from gobcore.database.connector.wfs import connect_to_wfs


class TestWfs(TestCase):

    @patch("gobcore.database.connector.wfs.requests")
    def test_connect_to_wfs(self, mock_requests):
        url = 'some url'
        returnval = type('Response', (object,), {'ok': 'OK'})
        mock_requests.get.return_value = returnval

        self.assertEqual((returnval, ""), connect_to_wfs(url))
        mock_requests.get.assert_called_with(url)

    @patch("gobcore.database.connector.wfs.requests")
    def test_connect_to_wfs_invalid_response(self, mock_requests):
        url = 'some url'
        returnval = type('Response', (object,), {'ok': None})
        mock_requests.get.return_value = returnval

        with self.assertRaises(AssertionError):
            connect_to_wfs(url)
