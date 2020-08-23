from unittest import TestCase
from unittest.mock import patch, MagicMock

from gobcore.datastore.ftp import FTPDatastore


@patch("gobcore.datastore.ftp.Datastore", MagicMock)
class TestFTPDatastore(TestCase):

    @patch("gobcore.datastore.ftp.FTP")
    def test_connect(self, mock_ftp):
        connection_config = {
            'host': 'any host',
            'port': 1234,
            'username': 'username',
            'password': 'password'
        }

        mock_ftp_instance = mock_ftp.return_value
        mock_ftp_connection = mock_ftp_instance.connect.return_value

        store = FTPDatastore(connection_config)
        store.connect()

        mock_ftp_instance.connect.assert_called_with(host=connection_config['host'], port=connection_config['port'])
        mock_ftp_connection.login.assert_called_with(user=connection_config['username'], passwd=connection_config['password'])

    @patch("gobcore.datastore.ftp.FTP")
    @patch("builtins.open")
    def test_put_file(self, mock_open, mock_ftp):
        connection_config = {
            'host': 'any host',
            'port': 1234,
            'username': 'username',
            'password': 'password'
        }

        mock_ftp_instance = mock_ftp.return_value
        mock_ftp_connection = mock_ftp_instance.connect.return_value

        store = FTPDatastore(connection_config)
        store.connect()

        mock_file = mock_open.return_value.__enter__.return_value

        store.put_file('any file', 'any dest')

        mock_ftp_connection.storbinary.assert_called_with('STOR any dest', mock_file)