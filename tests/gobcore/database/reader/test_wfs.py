from unittest import TestCase
from unittest.mock import patch, MagicMock

from gobcore.database.reader.wfs import query_wfs, read_from_wfs


class TestWfs(TestCase):

    def test_query_wfs(self):
        features = ['f1', 'f2', 'f3']
        response = type('MockResponse', (object,), {'json': lambda: {'features': features}})

        self.assertEqual(features, list(query_wfs(response)))

    @patch('gobcore.database.reader.wfs.query_wfs')
    def test_read_from_wfs(self, mock_query_wfs):
        mock_query_wfs.return_value = iter(['row1', 'row2'])
        response = MagicMock()

        self.assertEqual(['row1', 'row2'], read_from_wfs(response))
        mock_query_wfs.assert_called_with(response)