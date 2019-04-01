from unittest import TestCase
from unittest.mock import patch

from gobcore.database.connector.objectstore import connect_to_objectstore
from gobcore.exceptions import GOBException


@patch("gobcore.database.connector.objectstore.get_connection")
class TestConnectObjectstore(TestCase):

    def setUp(self):
        self.config = {
            'USER': 'someuser',
            'TENANT_NAME': 'tenant',
            'name': 'connection_name',
        }

    def test_connect_to_objectstore(self, mock_get_connection):
        mock_get_connection.return_value = {"connected": True}

        result = connect_to_objectstore(self.config)
        self.assertEqual(({"connected": True}, "(someuser@tenant)"), result)

    def test_missing_config(self, mock_get_connection):
        del self.config['TENANT_NAME']

        with self.assertRaises(GOBException):
            connect_to_objectstore(self.config)

    def test_generic_exception(self, mock_get_connection):
        mock_get_connection.side_effect = Exception

        with self.assertRaises(GOBException):
            connect_to_objectstore(self.config)