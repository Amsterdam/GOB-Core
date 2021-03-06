from unittest import TestCase
from unittest.mock import patch, MagicMock

from gobcore.datastore.oracle import OracleDatastore, GOBException


class MockConnection():
    class CursorObj:
        def __init__(self, result):
            self.result = result
            self.executed = None
            self.rowfactory = None

        def __iter__(self):
            for row in self.result:
                yield row

        def execute(self, query):
            self.executed = query

    def __init__(self, result):
        self.cursor_obj = self.CursorObj(result)
        self.outputtypehandler = None

    def cursor(self):
        return self.cursor_obj


@patch("gobcore.datastore.oracle.SqlDatastore", MagicMock)
class TestOracleDatastore(TestCase):

    def test_get_url(self):
        # Leave Oracle SID as it is
        config = {
            'username': "username",
            'password': "password",
            'host': "host",
            'port': 1234,
            'database': 'SID'
        }
        store = OracleDatastore(config)

        self.assertEqual("oracle+cx_oracle://username:password@host:1234/SID", str(store.connection_config['url']))

        # Handle Oracle service name
        config = {
            'username': "username",
            'password': "password",
            'host': "host",
            'port': 1234,
            'database': 'x.y.z'
        }

        store = OracleDatastore(config)
        self.assertEqual("oracle+cx_oracle://username:password@host:1234/?service_name=x.y.z",
                         str(store.connection_config['url']))

    @patch("gobcore.datastore.oracle.cx_Oracle")
    def test_connect(self, mock_cx_oracle):
        config = {
            'username': 'user',
            'database': 'db',
            'password': 'pw',
            'port': 9999,
            'host': 'localhost',
            'name': 'configname',
        }

        mock_cx_oracle.Connection.return_value = {"connected": True}
        store = OracleDatastore(config)
        store.connect()
        self.assertEqual("(user@db)", store.user)
        self.assertEqual({"connected": True}, store.connection)
        mock_cx_oracle.Connection.assert_called_with('user/pw@localhost:9999/db')

        config = {
            'username': 'user',
            'database': 'db',
            'port': 9999,
            'host': 'localhost',
            'name': 'configname',
        }
        store = OracleDatastore(config)

        with self.assertRaises(GOBException):
            store.connect()

    @patch("gobcore.datastore.oracle.OracleDatastore.get_url", MagicMock())
    def test_disconnect(self):
        store = OracleDatastore({})
        connection = MagicMock()
        store.connection = connection
        store.disconnect()
        connection.close.assert_called_once()
        self.assertIsNone(store.connection)
        store.disconnect()

    @patch("gobcore.datastore.oracle.OracleDatastore.get_url", MagicMock())
    def test_makedict(self):
        cursor = type('MockCursor', (object,), {'description': ['a', 'b', 'c']})
        args = [1, 2, 3, 4]
        store = OracleDatastore({}, {})
        createrowfunc = store._makedict(cursor)

        self.assertEqual({
            'a': 1,
            'b': 2,
            'c': 3,
        }, createrowfunc(*args))

    @patch("gobcore.datastore.oracle.OracleDatastore.get_url", MagicMock())
    @patch('gobcore.datastore.oracle.cx_Oracle.CLOB', "CLOB")
    @patch('gobcore.datastore.oracle.cx_Oracle.LONG_STRING', "LONG_STRING")
    def test_output_type_handler(self):
        cursor = type('Cursor', (object,), {'arraysize': 20, 'var': lambda x, arraysize: x + '_' + str(arraysize)})
        store = OracleDatastore({}, {})
        self.assertEqual('LONG_STRING_20', store._output_type_handler(cursor, 'name', 'CLOB', 0, 0, 0))
        self.assertIsNone(store._output_type_handler(cursor, 'name', 'SOME_OTHER_TYPE', 0, 0, 0))

    @patch("gobcore.datastore.oracle.OracleDatastore.get_url", MagicMock())
    def test_query(self):
        query = "SELECT this FROM that WHERE this=that"

        store = OracleDatastore({}, {})
        store.connection = MockConnection([{"id": 1}, {"id": 2}])
        store._output_type_handler = MagicMock()
        store._makedict = MagicMock()
        list(store.query(query))

        self.assertEqual(query, store.connection.cursor_obj.executed)
        self.assertEqual(store._output_type_handler, store.connection.outputtypehandler)
        self.assertEqual(store._makedict.return_value, store.connection.cursor_obj.rowfactory)

    @patch("gobcore.datastore.oracle.OracleDatastore.get_url", MagicMock())
    def test_not_implemented_methods(self):
        methods = [
            ('write_rows', ['the table', []]),
            ('execute', ['the query']),
            ('list_tables_for_schema', ['the schema']),
            ('rename_schema', ['old', 'new']),
        ]

        store = OracleDatastore({}, {})

        for method, args in methods:
            with self.assertRaises(NotImplementedError):
                getattr(store, method)(*args)
