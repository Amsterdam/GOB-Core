from unittest import TestCase
from unittest.mock import MagicMock, patch

from gobcore.exceptions import GOBException
from gobcore.database.writer.postgresql import (create_schema, drop_schema,
                                          drop_table, execute_postgresql_query,
                                          write_rows_to_postgresql)
from psycopg2 import Error


class TestPostgresqlWriter(TestCase):
    class MockConnection():
        class Cursor():

            def execute(self, query):
                return

            def close(self):
                return

            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

        cursor_obj = Cursor()
        commit_obj = MagicMock()

        def cursor(self):
            return self.cursor_obj

        def commit(self):
            return

    @patch("gobcore.database.writer.postgresql.execute_values")
    def test_write_rows_to_postgresql(self, mock_execute_values):
        mock_connection = self.MockConnection()
        values = [[1, 2, 3], [4, 5, 6]]
        table = "example_table"

        write_rows_to_postgresql(mock_connection, table, values)
        mock_execute_values.assert_called_with(mock_connection.cursor(), "INSERT INTO example_table VALUES %s", values)

    def test_write_rows_to_postgresql_error(self):
        """Asserts that a psycopg2 error is re-raised as a GOBException """
        mock_connection = self.MockConnection()
        mock_connection.cursor = MagicMock(side_effect=Error)

        with self.assertRaises(GOBException):
            write_rows_to_postgresql(mock_connection, "table", [])

    def test_execute_postgresql_query(self):
        mock_connection = self.MockConnection()
        mock_connection.cursor().execute = MagicMock()
        mock_connection.cursor().close = MagicMock()

        query = "SELECT something FROM somewhere WHERE this = that"
        execute_postgresql_query(mock_connection, query)
        mock_connection.cursor().execute.assert_called_with(query)

    def test_execute_postresql_query_error(self):
        """Asserts that a psycopg2 error is re-raised as a GOBException """
        mock_connection = self.MockConnection()
        mock_connection.cursor = MagicMock(side_effect=Error)

        with self.assertRaises(GOBException):
            execute_postgresql_query(mock_connection, "some query")

    @patch("gobcore.database.writer.postgresql.execute_postgresql_query")
    def test_drop_schema(self, mock_execute):
        drop_schema(None, "example_schema")
        expected = "DROP SCHEMA IF EXISTS example_schema CASCADE"
        mock_execute.assert_called_with(None, expected)

    @patch("gobcore.database.writer.postgresql.execute_postgresql_query")
    def test_create_schema(self, mock_execute):
        create_schema(None, "example_schema")
        expected = "CREATE SCHEMA IF NOT EXISTS example_schema"
        mock_execute.assert_called_with(None, expected)

    @patch("gobcore.database.writer.postgresql.execute_postgresql_query")
    def test_drop_table(self, mock_execute):
        drop_table(None, "example_table")
        expected = "DROP TABLE IF EXISTS example_table"
        mock_execute.assert_called_with(None, expected)
