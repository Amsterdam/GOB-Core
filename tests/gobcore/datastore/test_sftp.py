from unittest import TestCase
from unittest.mock import patch, MagicMock

from gobcore.datastore.sftp import SFTPDatastore


@patch("gobcore.datastore.sftp.Datastore", MagicMock)
class TestSFTPDatastore(TestCase):

    @patch("gobcore.datastore.sftp.paramiko")
    def test_connect(self, mock_paramiko):
        connection_config = {
            'host': 'any host',
            'port': 1234,
            'username': 'username',
            'password': 'password'
        }

        mock_transport = mock_paramiko.Transport.return_value

        store = SFTPDatastore(connection_config)
        store.connect()

        mock_paramiko.Transport.assert_called_with((connection_config['host'], connection_config['port']))
        mock_transport.connect.assert_called_with(username=connection_config['username'], password=connection_config['password'])
        
        mock_paramiko.SFTPClient.from_transport.assert_called_with(mock_transport)

    @patch("gobcore.datastore.sftp.paramiko")
    def test_put_file(self, mock_paramiko):
        connection_config = {
            'host': 'any host',
            'port': 1234,
            'username': 'username',
            'password': 'password'
        }

        mock_sftp_connection = mock_paramiko.SFTPClient.from_transport.return_value

        store = SFTPDatastore(connection_config)
        store.connect()

        store.put_file('any file', 'any dest')

        mock_sftp_connection.put.assert_called_with('any file', 'any dest')