from psycopg2 import OperationalError
from unittest import TestCase
from unittest.mock import patch

from gobcore.database.connector.postgresql import connect_to_postgresql
from gobcore.exceptions import GOBException


@patch("gobcore.database.connector.postgresql.psycopg2.connect")
class TestConnectPostgresql(TestCase):

    def setUp(self):
        self.config = {
            'database': 'db',
            'username': 'user',
            'password': 'pw',
            'host': 'localhost',
            'port': 9999,
            'name': 'postgresconnection',
        }

    def test_connect_to_postgresql(self, mock_connect):
        mock_connect.return_value = {"connected": True}

        result = connect_to_postgresql(self.config)
        self.assertEqual(({"connected": True}, "(user@db)"), result)

        mock_connect.assert_called_with(
            database=self.config['database'],
            user=self.config['username'],
            password=self.config['password'],
            host=self.config['host'],
            port=self.config['port'],
        )

    def test_connect_to_postgres_operational_error(self, mock_connect):
        mock_connect.side_effect = OperationalError

        with self.assertRaises(GOBException):
            connect_to_postgresql(self.config)

    def test_connect_to_postgres_keyerror(self, mock_connect):
        del self.config['password']

        with self.assertRaises(GOBException):
            connect_to_postgresql(self.config)