from typing import List
from unittest import TestCase
from unittest.mock import patch, MagicMock

from gobcore.datastore.sql import SqlDatastore


class SqlDatastoreImpl(SqlDatastore):

    def connect(self):
        pass

    def query(self, query):
        pass

    def write_rows(self, table: str, rows: List[list]) -> None:
        pass

    def execute(self, query: str) -> None:
        pass

    def list_tables_for_schema(self, schema: str) -> List[str]:
        pass

    def rename_schema(self, schema: str, new_name: str) -> None:
        pass


@patch("gobcore.datastore.sql.Datastore", MagicMock())
class TestSqlDatastore(TestCase):

    def test_init(self):
        connection_config = MagicMock()
        read_config = MagicMock()

        store = SqlDatastoreImpl(connection_config, read_config)
        self.assertEqual(connection_config, store.connection_config)
        self.assertEqual(read_config, store.read_config)

    def test_disconnect(self):
        store = SqlDatastoreImpl({}, {})
        connection = MagicMock()
        store.connection = connection
        store.disconnect()
        connection.close.assert_called_once()
        self.assertIsNone(store.connection)

        connection.close.side_effect = Exception
        store.connection = connection
        store.disconnect()
        self.assertIsNone(store.connection)

    def test_drop_schema(self):
        store = SqlDatastoreImpl({}, {})
        store.execute = MagicMock()
        store.drop_schema('the schema')
        store.execute.assert_called_with('DROP SCHEMA IF EXISTS the schema CASCADE')

    def test_create_schema(self):
        store = SqlDatastoreImpl({}, {})
        store.execute = MagicMock()
        store.create_schema('new schema')
        store.execute.assert_called_with('CREATE SCHEMA IF NOT EXISTS new schema')

    def test_drop_table(self):
        store = SqlDatastoreImpl({}, {})
        store.execute = MagicMock()
        store.drop_table('the table')
        store.execute.assert_called_with('DROP TABLE IF EXISTS the table CASCADE')
        store.drop_table('the table', False)
        store.execute.assert_called_with('DROP TABLE IF EXISTS the table')