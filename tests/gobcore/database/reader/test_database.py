from sqlalchemy.exc import DBAPIError
from unittest import TestCase
from unittest.mock import MagicMock, patch

from gobcore.exceptions import GOBEmptyResultException, GOBException
from gobcore.database.reader.database import read_from_database


class MockConnection(MagicMock):

    class ExecuteMock(MagicMock):
        fetchall = MagicMock(return_value=[{'id': 1, 'name': 'A'}, {'id': 2, 'name': 'B'}])

    execute = ExecuteMock()


class TestDatabaseReader(TestCase):

    def test_read_from_database(self):
        connection = MockConnection()
        query = ["SELECT something FROM something WHERE something=true"]

        result = read_from_database(connection, query)
        self.assertEqual([{'id': 1, 'name': 'A'}, {'id': 2, 'name': 'B'}], result)

        connection.execute.assert_called_with(query[0])

    def test_read_from_database_api_error(self):
        connection = MockConnection()
        connection.execute = MagicMock()
        connection.execute.side_effect = DBAPIError("", "", "")

        with self.assertRaises(GOBException):
            read_from_database(connection, ["SELECT something FROM something WHERE something=true"])

    def test_read_from_database_empty_result(self):
        connection = MockConnection()
        connection.execute.fetchall.return_value = []

        with self.assertRaises(GOBEmptyResultException):
            read_from_database(connection, ["SELECT something FROM something WHERE something=true"])

