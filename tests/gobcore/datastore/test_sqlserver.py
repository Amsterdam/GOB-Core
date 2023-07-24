from unittest import TestCase
from unittest.mock import patch, MagicMock

from pyodbc import Connection

from gobcore.datastore.sqlserver import SqlServerDatastore


class TestSqlServerDatastore(TestCase):

    @patch("gobcore.datastore.sqlserver.SQLSERVER_ODBC_DRIVER", "DRIVER")
    @patch("gobcore.datastore.sqlserver.pyodbc")
    def test_connect(self, mock_pyodbc):
        connection_config = {
            'host': 'HOST',
            'database': 'DB',
            'username': 'USER',
            'password': 'PASS',
            'port': 'PORT'
        }
        store = SqlServerDatastore(connection_config, {})
        self.assertIsNone(store.connection)

        store.connect()

        self.assertEqual(mock_pyodbc.connect.return_value, store.connection)

        conn = "DRIVER={DRIVER};SERVER=HOST,PORT;DATABASE=DB;ENCRYPT=optional;UID=USER;PWD=PASS"
        mock_pyodbc.connect.assert_called_with(conn)

    class MockRow:
        cursor_description = [('columnA',), ('otherColumn',), ('intColumn',)]

        def __init__(self, rowtuple: tuple):
            self.rowtuple = rowtuple

        def __getitem__(self, i):
            return self.rowtuple[i]

    def test_query(self):
        store = SqlServerDatastore({}, {})
        store.connection = MagicMock(spec=Connection)
        cursor_mock = store.connection.cursor.return_value.__enter__.return_value
        cursor_mock.fetchall.return_value = [self.MockRow(('a', 'b', 4)), self.MockRow(('c', 'd', 5))]

        res = list(store.query('some query'))

        self.assertEqual([
            {'columnA': 'a', 'otherColumn': 'b', 'intColumn': 4},
            {'columnA': 'c', 'otherColumn': 'd', 'intColumn': 5},
        ], res)

        cursor_mock.execute.assert_called_with('some query')

    def test_not_implemented_methods(self):
        methods = [
            ('write_rows', ['the table', []]),
            ('execute', ['the query']),
            ('list_tables_for_schema', ['the schema']),
            ('rename_schema', ['old', 'new']),
        ]

        store = SqlServerDatastore({}, {})

        for method, args in methods:
            with self.assertRaises(NotImplementedError):
                getattr(store, method)(*args)
