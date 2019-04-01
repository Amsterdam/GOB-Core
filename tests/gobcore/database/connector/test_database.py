from sqlalchemy.exc import DBAPIError

from unittest import TestCase
from unittest.mock import MagicMock, patch

from gobcore.database.connector import connect_to_database
from gobcore.exceptions import GOBException


class TestConnectDatabase(TestCase):

    @patch("gobcore.database.connector.database.create_engine")
    def test_connect_to_database(self, mock_create_engine):
        engine = MagicMock()
        mock_create_engine.return_value = engine
        config = {
            'username': 'someuser',
            'database': 'somedatabase',
            'url': 'connection_url',
            'name': 'someconfig',
        }

        connect_to_database(config)
        mock_create_engine.assert_called_with(config['url'])
        engine.connect.assert_called_once()

    def test_invalid_config(self):
        config = {
            'username': 'someuser',
            'name': 'someconfig',
        }

        with self.assertRaises(GOBException):
            connect_to_database(config)

    @patch("gobcore.database.connector.database.create_engine")
    def test_sqlalchemy_reraise_exception(self, mock_create_engine):
        mock_create_engine.side_effect = DBAPIError("", "", "")
        config = {
            'username': 'someuser',
            'database': 'somedatabase',
            'url': 'connection_url',
            'name': 'someconfig',
        }

        with self.assertRaises(GOBException):
            connect_to_database(config)
