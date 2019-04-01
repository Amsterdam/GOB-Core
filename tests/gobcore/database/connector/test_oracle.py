from unittest import TestCase
from unittest.mock import patch

from gobcore.database.connector.oracle import connect_to_oracle
from gobcore.exceptions import GOBException


class TestConnectOracle(TestCase):

    def setUp(self):
        self.config = {
            'username': 'user',
            'database': 'db',
            'password': 'pw',
            'port': 9999,
            'host': 'localhost',
            'name': 'configname',
        }

    @patch("gobcore.database.connector.oracle.cx_Oracle")
    def test_connect_to_oracle(self, mock_cx_oracle):
        mock_cx_oracle.Connection.return_value = {"connected": True}

        result = connect_to_oracle(self.config)
        self.assertEqual(({"connected": True}, "(user@db)"), result)
        mock_cx_oracle.Connection.assert_called_with('user/pw@localhost:9999/db')

    def test_connect_to_oracle_keyerror(self):
        del self.config['password']

        with self.assertRaises(GOBException):
            connect_to_oracle(self.config)