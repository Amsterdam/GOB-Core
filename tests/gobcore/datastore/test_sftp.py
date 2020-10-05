from unittest import TestCase
from unittest.mock import patch, MagicMock, call

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

    def test_create_directories(self):
        store = SFTPDatastore({})
        store.connection = MagicMock()

        # Let throw OSError to mimick directory that already exists
        store.connection.mkdir.side_effect = OSError

        path = 'directory/to/create'
        store._create_directories(path)

        store.connection.mkdir.assert_has_calls([
            call('directory'),
            call('directory/to'),
            call('directory/to/create'),
        ])

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
        store._create_directories = MagicMock()

        store.connect()

        store.put_file('any file', 'some/dir/any dest')

        store._create_directories.assert_called_with('some/dir')
        mock_sftp_connection.put.assert_called_with('any file', 'some/dir/any dest')

    def test_list_files(self):

        def mock_listdir(path):
            if path == '':
                return ['a', 'b', 'f.file']
            if path == '/a':
                return ['c', 'd', 'a.file', 'b.file']
            if path == '/a/c':
                return ['e']
            if path == '/a/c/e':
                return ['g.file']
            return []

        def mock_stat(path):
            if path.endswith('.file'):
                return type('FileStat', (), {
                    'st_mode': 33206,
                })
            else:
                return type('DirStat', (), {
                    'st_mode': 16795,
                })

        store = SFTPDatastore({})
        store.connection = MagicMock()
        store.connection.listdir = mock_listdir
        store.connection.stat = mock_stat

        self.assertEqual([
            'a/c/e/g.file',
            'a/a.file',
            'a/b.file',
            'f.file',
        ], store.list_files())

        self.assertEqual([
            'a/b.file',
        ], store.list_files('a/b'))

        self.assertEqual([
            'a/c/e/g.file',
            'a/a.file',
            'a/b.file',
        ], store.list_files('a'))

    def test_delete_file(self):
        store = SFTPDatastore({})
        store.connection = MagicMock()
        store.delete_file('some file')
        store.connection.remove.assert_called_with('some file')
