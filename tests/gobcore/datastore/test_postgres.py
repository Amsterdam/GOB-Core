import types

from unittest import TestCase
from unittest.mock import MagicMock, patch

from psycopg2 import OperationalError, Error

from gobcore.datastore.postgres import PostgresDatastore, GOBException


class MockConnection():
    class Cursor():
        def __init__(self, expected_result):
            self.expected_result = expected_result

        def execute(self, query):
            return

        def close(self):
            return

        def __iter__(self):
            return iter(self.expected_result)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

    commit_obj = MagicMock()

    def __init__(self, expected_result):
        self.cursor_obj = self.Cursor(expected_result)

    def cursor(self, **kwargs):
        return self.cursor_obj

    def commit(self):
        return


@patch("gobcore.datastore.postgres.SqlDatastore", MagicMock)
class TestPostgresDatastore(TestCase):

    def test_init(self):
        store = PostgresDatastore({'connection': 'config'}, {})
        self.assertEqual(store.connection_config['drivername'], 'postgresql')

    @patch("gobcore.datastore.postgres.psycopg2.connect")
    def test_connect(self, mock_connect):
        config = {
            'database': 'db',
            'username': 'user',
            'password': 'pw',
            'host': 'localhost',
            'port': 9999,
            'name': 'postgresconnection',
        }

        mock_connect.return_value = {'connected': True}
        store = PostgresDatastore(config)
        store.connect()

        self.assertEqual({'connected': True}, store.connection)
        self.assertEqual("(user@db)", store.user)

        mock_connect.assert_called_with(
            database=config['database'],
            user=config['username'],
            password=config['password'],
            host=config['host'],
            port=config['port'],
            sslmode='require',
        )

        del config['password']

        with self.assertRaises(GOBException):
            store.connect()

        config['password'] = 'pw'
        mock_connect.side_effect = OperationalError

        with self.assertRaises(GOBException):
            store.connect()

    def test_disconnect(self):
        store = PostgresDatastore({})
        connection = MagicMock()
        store.connection = connection
        store.disconnect()
        connection.close.assert_called_once()
        self.assertIsNone(store.connection)
        store.disconnect()

    def test_query(self):
        expected_result = [i for i in range(10)]
        connection = MockConnection(expected_result)
        connection.cursor_obj.execute = MagicMock(return_value=expected_result)
        connection.cursor_obj.close = MagicMock()
        query = "SELECT something FROM something WHERE something=true"

        store = PostgresDatastore({})
        store.connection = connection
        result = store.query(query)
        self.assertTrue(isinstance(result, types.GeneratorType))
        self.assertEqual(expected_result, list(result))

        connection.cursor_obj.execute.assert_called_with(query)

        connection = MockConnection([i for i in range(10)])
        connection.cursor = MagicMock(side_effect=Error)
        store.connection = connection

        with self.assertRaises(GOBException):
            list(store.query("some query"))

    @patch("gobcore.datastore.postgres.execute_values")
    def test_write_rows(self, mock_execute_values):
        rows = [
            ['a', 'b', 'c'],
            ['d', 'e', 'f']
        ]

        store = PostgresDatastore({})
        store.connection = MagicMock()
        store.write_rows('some table', rows)

        mock_execute_values.assert_called_with(
            store.connection.cursor.return_value.__enter__.return_value,
            "INSERT INTO some table VALUES %s",
            rows
        )
        store.connection.commit.assert_called_once()

        mock_execute_values.side_effect = Error

        with self.assertRaises(GOBException):
            store.write_rows('some table', rows)

    @patch("gobcore.datastore.postgres.execute_values")
    def test_write_rows_with_columns(self, mock_execute_values):
        rows = [
            ['a', 'b', 'c'],
            ['d', 'e', 'f']
        ]
        store = PostgresDatastore({})
        store.connection = MagicMock()
        columns = ['col_id', 'col_a', 'col_b']
        store.write_rows('some table', rows, columns)

        mock_execute_values.assert_called_with(
            store.connection.cursor.return_value.__enter__.return_value,
            "INSERT INTO some table (col_id,col_a,col_b) VALUES %s "
            "ON CONFLICT(col_id) DO UPDATE SET col_a=EXCLUDED.col_a,col_b=EXCLUDED.col_b",
            rows
        )
        store.connection.commit.assert_called_once()

    def test_execute(self):
        store = PostgresDatastore({})
        store.connection = MagicMock()
        mocked_cursor = store.connection.cursor.return_value.__enter__.return_value

        store.execute('some query')
        mocked_cursor.execute.assert_called_with('some query')
        store.connection.commit.assert_called_once()

        mocked_cursor.execute.side_effect = Error

        with self.assertRaises(GOBException):
            store.execute('some query')

    def test_list_tables_for_schema(self):
        store = PostgresDatastore({})
        store.query = MagicMock(return_value=[{'table_name': 'table A'}, {'table_name': 'table B'}])
        result = store.list_tables_for_schema('some schema')

        store.query.assert_called_with(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='some schema'"
        )
        self.assertEqual(['table A', 'table B'], result)

    def test_rename_schema(self):
        store = PostgresDatastore({})
        store.execute = MagicMock()
        store.rename_schema('old schema', 'new schema')
        store.execute.assert_called_with('ALTER SCHEMA "old schema" RENAME TO "new schema"')


