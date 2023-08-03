import types

from unittest import TestCase
from unittest.mock import MagicMock, patch

import psycopg2
from psycopg2.extensions import connection as psycopg2_connection
from psycopg2.extensions import cursor as psycopg2_cursor

from gobcore.datastore.postgres import PostgresDatastore, GOBException


@patch("gobcore.datastore.postgres.SqlDatastore", MagicMock)
class TestPostgresDatastore(TestCase):

    def test_init(self):
        store = PostgresDatastore({'connection': 'config'}, {})
        assert store.connection_config['drivername'] == 'postgresql'

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
        mock_connect.side_effect = psycopg2.OperationalError

        with self.assertRaises(GOBException):
            store.connect()

    def test_disconnect(self):
        store = PostgresDatastore({})
        mock_conn = MagicMock(spec=psycopg2_connection)
        store.connection = mock_conn

        store.disconnect()

        mock_conn.close.assert_called_once()
        assert store.connection is None

        # second call should not fail
        store.disconnect()
        assert store.connection is None

    def test_transaction_cursor(self):
        store = PostgresDatastore({})
        store.connection = MagicMock(spec=psycopg2_connection)
        mock_cursor = MagicMock(spec=psycopg2_cursor)
        store.connection.cursor.return_value.__enter__.return_value = mock_cursor

        with store.transaction_cursor() as cur:
            cur.execute("select * from table;")

        store.connection.commit.assert_called()

        with self.assertRaisesRegex(GOBException, "Error executing query: FATAL"):
            with store.transaction_cursor():
                raise psycopg2.Error("FATAL")

        store.connection.rollback.assert_called()

    def test_query(self):
        store = PostgresDatastore({})
        store.connection = MagicMock(spec=psycopg2_connection)
        mock_cursor = MagicMock(spec=psycopg2_cursor)
        store.connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.fetchmany.side_effect = [[1, 2, 3], []]

        query = "SELECT something FROM something WHERE something IS TRUE"

        result = store.query(query, arraysize=2)

        assert isinstance(result, types.GeneratorType)
        assert [1, 2, 3] == list(result)
        assert mock_cursor.arraysize == 2
        assert mock_cursor.fetchmany.call_count == 2
        mock_cursor.execute.assert_called_with(query)
        store.connection.commit.assert_called()

    @patch("gobcore.datastore.postgres.execute_values")
    def test_write_rows(self, mock_execute_values):
        rows = [["a", "b", "c"], ["d", "e", "f"]]

        store = PostgresDatastore({})
        store.connection = MagicMock(spec=psycopg2_connection)
        result = store.write_rows("some table", rows)

        assert result == 2
        mock_execute_values.assert_called_with(
            store.connection.cursor.return_value.__enter__.return_value,
            "INSERT INTO some table VALUES %s",
            rows
        )
        store.connection.commit.assert_called()

    def test_execute(self):
        store = PostgresDatastore({})
        store.connection = MagicMock(spec=psycopg2_connection)
        mock_cursor = store.connection.cursor.return_value.__enter__.return_value

        store.execute("some query")
        mock_cursor.execute.assert_called_with("some query")
        store.connection.commit.assert_called()

    def test_copy_from_stdin(self):
        store = PostgresDatastore({})
        store.connection = MagicMock(spec=psycopg2_connection)
        mock_cursor = store.connection.cursor.return_value.__enter__.return_value

        store.copy_from_stdin("some query", "some data")
        mock_cursor.copy_expert.assert_called_with("some query", "some data")
        store.connection.commit.assert_called()

    def test_list_tables_for_schema(self):
        store = PostgresDatastore({})
        store.query = MagicMock(return_value=[{"table_name": "table A"}, {"table_name": "table B"}])
        result = store.list_tables_for_schema("some schema")

        qry = "SELECT table_name FROM information_schema.tables WHERE table_schema='some schema'"

        store.query.assert_called_with(qry, name=None)
        assert ["table A", "table B"] == result

    def test_rename_schema(self):
        store = PostgresDatastore({})
        store.execute = MagicMock()
        store.rename_schema("old schema", "new schema")
        store.execute.assert_called_with('ALTER SCHEMA "old schema" RENAME TO "new schema"')

    def test_is_extension_enabled(self):
        store = PostgresDatastore({})
        store.query = MagicMock(return_value=[1])

        self.assertTrue(store.is_extension_enabled("citus"))
        store.query.assert_called_with("SELECT 1 FROM pg_extension WHERE extname='citus'")

        store.query.return_value = []
        self.assertFalse(store.is_extension_enabled("citus"))

    def test_get_version(self):
        store = PostgresDatastore({})
        store.query = MagicMock(return_value=iter([["12.4.2"]]))

        self.assertEqual("12.4.2", store.get_version())
        store.query.assert_called_with("SHOW server_version")
