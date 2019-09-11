from unittest import TestCase
from unittest.mock import MagicMock, patch

from gobcore.database.reader.oracle import query_oracle, read_from_oracle, makedict, output_type_handler
from gobcore.exceptions import GOBEmptyResultException


class TestOracleReader(TestCase):

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

    def test_makedict(self):
        cursor = type('MockCursor', (object,), {'description': ['a', 'b', 'c']})
        args = [1, 2, 3, 4]
        createrowfunc = makedict(cursor)

        self.assertEqual({
            'a': 1,
            'b': 2,
            'c': 3,
        }, createrowfunc(*args))

    @patch('gobcore.database.reader.oracle.cx_Oracle.CLOB', "CLOB")
    @patch('gobcore.database.reader.oracle.cx_Oracle.LONG_STRING', "LONG_STRING")
    def test_output_type_handler(self):
        cursor = type('Cursor', (object,), {'arraysize': 20, 'var': lambda x, arraysize: x + '_' + str(arraysize)})
        self.assertEqual('LONG_STRING_20', output_type_handler(cursor, 'name', 'CLOB', 0, 0, 0))
        self.assertIsNone(output_type_handler(cursor, 'name', 'SOME_OTHER_TYPE', 0, 0, 0))

    @patch('gobcore.database.reader.oracle.makedict')
    @patch('gobcore.database.reader.oracle.output_type_handler')
    def test_query_oracle(self, mock_output_type_handler, mock_makedict):
        mock_connection = self.MockConnection([{"id": 1}, {"id": 2}])
        query = "SELECT this FROM that WHERE this=that"

        for row in query_oracle(mock_connection, [query]):
            pass

        self.assertEqual(query, mock_connection.cursor_obj.executed)
        self.assertEqual(mock_output_type_handler, mock_connection.outputtypehandler)
        self.assertEqual(mock_makedict.return_value, mock_connection.cursor_obj.rowfactory)

    @patch('gobcore.database.reader.oracle.query_oracle')
    def test_read_from_oracle(self, mock_query):
        expected_result = [{"id": 1}, {"id": 2}]
        mock_query.return_value = iter(expected_result)
        mock_connection = self.MockConnection(expected_result)
        query = "SELECT this FROM that WHERE this=that"

        result = read_from_oracle(mock_connection, [query])
        self.assertEqual(expected_result, result)

    @patch('gobcore.database.reader.oracle.query_oracle')
    def test_read_from_oracle_empty_result(self, mock_query_oracle):
        mock_query_oracle.return_value = []
        connection = MagicMock()
        query = "SELECT this FROM that WHERE this=that"

        with self.assertRaises(GOBEmptyResultException):
            read_from_oracle(connection, [query])
