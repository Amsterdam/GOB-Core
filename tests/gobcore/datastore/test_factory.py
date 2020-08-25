from unittest import TestCase
from unittest.mock import MagicMock, patch

from gobcore.datastore.factory import DatastoreFactory


class TestDatastoreFactory(TestCase):

    def get_datastore_test(self, type: str, mock_store):
        config = {
            'a': 1,
            'b': 2,
        }
        read_config = MagicMock()

        res = DatastoreFactory.get_datastore({**config, 'type': type}, read_config)
        self.assertEqual(mock_store.return_value, res)

        mock_store.assert_called_with(config, read_config)

    @patch("gobcore.datastore.factory.OracleDatastore")
    @patch("gobcore.datastore.factory.PostgresDatastore")
    @patch("gobcore.datastore.factory.ObjectDatastore")
    @patch("gobcore.datastore.factory.WfsDatastore")
    @patch("gobcore.datastore.factory.FileDatastore")
    @patch("gobcore.datastore.factory.SFTPDatastore")
    def test_get_datastore(self, mock_sftp_store, mock_file_store, mock_wfs_store, mock_object_store, 
                           mock_postgres_store, mock_oracle_store):

        self.get_datastore_test('oracle', mock_oracle_store)
        self.get_datastore_test('postgres', mock_postgres_store)
        self.get_datastore_test('objectstore', mock_object_store)
        self.get_datastore_test('wfs', mock_wfs_store)
        self.get_datastore_test('file', mock_file_store)
        self.get_datastore_test('sftp', mock_sftp_store)

        with self.assertRaises(NotImplementedError):
            DatastoreFactory.get_datastore({'type': 'invalid'}, {})
