from unittest import TestCase
from unittest.mock import MagicMock, patch

from gobcore.database.reader.oracle import read_from_oracle
from gobcore.exceptions import GOBEmptyResultException

class TestOracleReader(TestCase):
    class MockConnection():

        def __init__(self, result):
            self.cursor_obj = MagicMock()
            self.cursor_obj.fetchall.return_value = result

        def cursor(self):
            return self.cursor_obj

    def test_read_from_oracle(self):
        expected_result = [{"id": 1}, {"id": 2}]
        mock_connection = self.MockConnection(expected_result)
        query = "SELECT this FROM that WHERE this=that"

        result = read_from_oracle(mock_connection, [query])
        self.assertEqual(expected_result, result)

        mock_connection.cursor_obj.execute.assert_called_with(query)

    def test_read_from_oracle_empty_result(self):
        mock_connection = self.MockConnection([])
        query = "SELECT this FROM that WHERE this=that"

        with self.assertRaises(GOBEmptyResultException):
            read_from_oracle(mock_connection, [query])
