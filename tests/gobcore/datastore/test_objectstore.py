from unittest import TestCase
from unittest.mock import patch, MagicMock

from gobcore.datastore.objectstore import ObjectDatastore, GOBException, get_object, \
    get_connection, put_object, get_full_container_list, delete_object, OBJECTSTORE


class MockExcel:

    def iterrows(self):
        return [
            (0, {"a": 1})
        ]


class MockCSV:

    def iterrows(self):
        return [
            (0, {"a": 1})
        ]


class MockUVA2:

    def iterrows(self):
        return [
            (0, {"a": 1})
        ]


class TestConnection(TestCase):

    def test_make_config(self):
        expected = {
            'VERSION': '2.0',
            'AUTHURL': 'https://identity.stack.cloudvps.com/v2.0',
            'TENANT_NAME': 'ten_name',
            'TENANT_ID': 'ten_id',
            'USER': 'user',
            'PASSWORD': 'pw',
            'REGION_NAME': 'NL'
        }
        self.assertDictEqual(OBJECTSTORE, expected)

    @patch("gobcore.datastore.objectstore.Connection")
    def test_get_connection(self, mock_conn):
        get_connection(OBJECTSTORE)

        mock_conn.assert_called_with(
            authurl=OBJECTSTORE['AUTHURL'],
            user=OBJECTSTORE['USER'],
            key=OBJECTSTORE['PASSWORD'],
            tenant_name=OBJECTSTORE['TENANT_NAME'],
            auth_version=OBJECTSTORE['VERSION'],
            os_options={
                'tenant_id': OBJECTSTORE['TENANT_ID'],
                'region_name': OBJECTSTORE['REGION_NAME'],
                'endpoint_type': 'internalURL'
            }
        )

    @patch("gobcore.datastore.objectstore.Connection")
    def test_get_full_container_list(self, mock_conn):
        container = 'cont'

        obj = {'name': 'naam'}

        mock_conn.get_container.return_value = ('header', [obj, obj])
        result = get_full_container_list(mock_conn, container, limit=2)
        self.assertEqual(next(result), obj)
        self.assertEqual(next(result), obj)

        mock_conn.get_container.return_value = ('header', [obj])
        self.assertEqual(next(result), obj)
        mock_conn.get_container.assert_called_with('cont', marker='naam', limit=2)

        self.assertRaises(StopIteration, next, result)

    @patch("gobcore.datastore.objectstore.Connection")
    def test_get_object(self, mock_conn):
        mock_conn.get_object.return_value = ('', [b'chunk1'])

        metadata = {'name': 'naam'}
        dirname = 'dir'

        self.assertEqual(get_object(mock_conn, metadata, dirname), b'chunk1')
        self.assertEqual(get_object(mock_conn, metadata, dirname, max_chunks=1), b'chunk1')

    @patch("gobcore.datastore.objectstore.Connection")
    def test_put_object(self, mock_conn):
        kwargs = {
            'contents': b'contents',
            'content_type': 'het type'
        }
        put_object(mock_conn, 'de container', 'naam', **kwargs)
        mock_conn.put_object.assert_called_with('de container', 'naam', **kwargs)

    @patch("gobcore.datastore.objectstore.Connection")
    def test_delete_object(self, mock_conn):
        metadata = {'name': 'naam'}
        delete_object(mock_conn, 'de container', metadata)
        mock_conn.delete_object.assert_called_with('de container', 'naam')


@patch("gobcore.datastore.objectstore.Datastore", MagicMock)
@patch("gobcore.datastore.objectstore.os.getenv", lambda *args: "containerfromenv")
class TestObjectDatastore(TestCase):

    def setUp(self):
        self.config = {
            'USER': 'someuser',
            'TENANT_NAME': 'tenant',
            'name': 'connection_name',
        }

    def test_init(self):
        store = ObjectDatastore(self.config)
        self.assertEqual("containerfromenv", store.container_name)

        store = ObjectDatastore(self.config, {'container': 'the container'})
        self.assertEqual("the container", store.container_name)

    @patch("gobcore.datastore.objectstore.get_connection")
    def test_connect(self, mock_get_connection):
        store = ObjectDatastore(self.config)
        mock_get_connection.return_value = {"connected": True}

        store.connect()
        self.assertEqual({'connected': True}, store.connection)
        self.assertEqual("(someuser@tenant)", store.user)

    def test_disconnect(self):
        store = ObjectDatastore(self.config)
        connection = MagicMock()
        store.connection = connection
        store.disconnect()
        connection.close.assert_called_once()
        self.assertIsNone(store.connection)
        store.disconnect()

    def test_missing_config(self):
        del self.config['TENANT_NAME']
        store = ObjectDatastore(self.config)

        with self.assertRaises(GOBException):
            store.connect()

    @patch("gobcore.datastore.objectstore.get_connection")
    def test_generic_exception(self, mock_get_connection):
        mock_get_connection.side_effect = Exception
        store = ObjectDatastore(self.config)

        with self.assertRaises(GOBException):
            store.connect()

    @patch('gobcore.datastore.objectstore.get_full_container_list')
    def test_read(self, mock_container_list):
        store = ObjectDatastore(self.config)
        data = store.query(None)
        self.assertEqual(list(data), [])
        mock_container_list.assert_called()

    @patch('gobcore.datastore.objectstore.get_full_container_list')
    def test_read_not_empty(self, mock_container_list):
        mock_container_list.return_value = [
            {
                "name": "name"
            }
        ]
        config = {
            "container": "container",
            "file_filter": ".*",
            "file_type": None
        }
        store = ObjectDatastore(config)
        data = store.query(None)
        self.assertEqual(list(data), [{"name": "name"}])

    @patch('gobcore.datastore.objectstore.get_object')
    @patch('gobcore.datastore.objectstore.get_full_container_list')
    def test_read_from_xls(self, mock_container_list, mock_object):
        mock_container_list.return_value = [
            {
                "name": "name"
            }
        ]
        config = {
            "container": "container",
            "file_filter": ".*",
            "file_type": "XLS"
        }
        store = ObjectDatastore({}, config)
        store._read_xls = MagicMock(return_value=[])
        data = store.query(None)
        self.assertEqual(list(data), [])
        store._read_xls.assert_called()

    @patch('gobcore.datastore.objectstore.io.BytesIO')
    @patch('gobcore.datastore.objectstore.pandas.read_excel')
    def test_read_xls(self, mock_read, mock_io):
        mock_read.return_value = MockExcel()
        store = ObjectDatastore({}, {})
        result = [obj for obj in store._read_xls({}, {}, {})]
        self.assertEqual(result, [{"a": 1, "_file_info": {}}])

    @patch('gobcore.datastore.objectstore.pandas.isnull')
    @patch('gobcore.datastore.objectstore.io.BytesIO')
    @patch('gobcore.datastore.objectstore.pandas.read_excel')
    def test_read_xls_null_values(self, mock_read, mock_io, mock_isnull):
        mock_read.return_value = MockExcel()
        mock_isnull.return_value = True
        store = ObjectDatastore({}, {})
        result = [obj for obj in store._read_xls({}, {}, {})]
        self.assertEqual(result, [])

    @patch('gobcore.datastore.objectstore.get_object')
    @patch('gobcore.datastore.objectstore.get_full_container_list')
    def test_read_from_csv(self, mock_container_list, mock_object):
        mock_container_list.return_value = [
            {
                "name": "name"
            }
        ]
        config = {
            "container": "container",
            "file_filter": ".*",
            "file_type": "CSV"
        }
        store = ObjectDatastore({}, config)
        store._read_csv = MagicMock(return_value=[])
        data = list(store.query(None))
        self.assertEqual(data, [])
        store._read_csv.assert_called()

    @patch('gobcore.datastore.objectstore.io.BytesIO')
    @patch('gobcore.datastore.objectstore.pandas.read_csv')
    def test_read_csv(self, mock_read, mock_io):
        mock_read.return_value = MockCSV()
        store = ObjectDatastore({}, {})
        result = [obj for obj in store._read_csv({}, {}, {})]
        self.assertEqual(result, [{"a": 1, "_file_info": {}}])

    @patch('gobcore.datastore.objectstore.pandas.isnull')
    @patch('gobcore.datastore.objectstore.io.BytesIO')
    @patch('gobcore.datastore.objectstore.pandas.read_csv')
    def test_read_csv_null_values(self, mock_read, mock_io, mock_isnull):
        mock_read.return_value = MockCSV()
        mock_isnull.return_value = True
        store = ObjectDatastore({}, {})
        result = [obj for obj in store._read_csv({}, {}, {})]
        self.assertEqual(result, [])

    @patch('gobcore.datastore.objectstore.get_object')
    @patch('gobcore.datastore.objectstore.get_full_container_list')
    def test_read_from_uva2(self, mock_container_list, mock_object):
        mock_container_list.return_value = [
            {
                "name": "name"
            }
        ]
        config = {
            "container": "container",
            "file_filter": ".*",
            "file_type": "UVA2"
        }
        store = ObjectDatastore({}, config)
        store._read_uva2 = MagicMock(return_value=[])
        data = list(store.query(None))
        self.assertEqual(data, [])
        store._read_uva2.assert_called()

    @patch('gobcore.datastore.objectstore.io.BytesIO')
    @patch('gobcore.datastore.objectstore.pandas.read_csv')
    def test_read_xls(self, mock_read, mock_io):
        mock_read.return_value = MockUVA2()
        store = ObjectDatastore({}, {})
        result = [obj for obj in store._read_uva2({}, {}, {})]
        self.assertEqual(result, [{"a": 1, "_file_info": {}}])

    @patch('gobcore.datastore.objectstore.pandas.isnull')
    @patch('gobcore.datastore.objectstore.io.BytesIO')
    @patch('gobcore.datastore.objectstore.pandas.read_csv')
    def test_read_uva2_null_values(self, mock_read, mock_io, mock_isnull):
        mock_read.return_value = MockUVA2()
        mock_isnull.return_value = True
        store = ObjectDatastore({}, {})
        result = [obj for obj in store._read_uva2({}, {}, {})]
        self.assertEqual(result, [])

    @patch("gobcore.datastore.objectstore.pandas.isnull", lambda x: x == 'pandasnull')
    def test_yield_rows(self):
        iterrows = [
            ('', {'A': 1, 'B': 2, 'C': 'pandasnull', 'D': 4}),
            ('', {'A': 5, 'B': 6, 'C': 7, 'D': 'pandasnull'}),
            ('', {'A': 'pandasnull', 'B': 'pandasnull', 'C': 'pandasnull', 'D': 'pandasnull'}),
        ]
        file_info = {
            'some': 'file',
            'info': 'object',
        }
        config = {
            'operators': ['lowercase_keys']
        }

        expected_result = [
            {'a': 1, 'b': 2, 'c': None, 'd': 4, '_file_info': file_info},
            {'a': 5, 'b': 6, 'c': 7, 'd': None, '_file_info': file_info},
        ]

        store = ObjectDatastore({}, {})
        self.assertEqual(expected_result, list(store._yield_rows(iterrows, file_info, config)))

    @patch("gobcore.datastore.objectstore.get_connection")
    @patch("builtins.open")
    def test_put_file(self, mock_open, mock_get_connection):
        mock_connection = mock_get_connection.return_value

        store = ObjectDatastore(self.config, {'container': 'any container_name'})
        store.connect()

        mock_file = mock_open.return_value.__enter__.return_value

        store.put_file('any src', 'any dest')
        mock_connection.put_object.assert_called_with('any container_name', 'any dest', contents=mock_file)

    @patch("gobcore.datastore.objectstore.get_full_container_list")
    def test_list_files(self, mock_get_list):
        mock_get_list.return_value = [
            {'name': 'a/b/c/file.txt'},
            {'name': 'a/b/file.txt'},
            {'name': 'a/file.txt'},
            {'name': 'b/file.txt'},
            {'name': 'a/b/c/file2.txt'},
        ]

        store = ObjectDatastore({})
        self.assertEqual([
            'a/b/c/file.txt',
            'a/b/file.txt',
            'a/b/c/file2.txt',
        ], list(store.list_files('a/b')))

        self.assertEqual([
            'a/b/c/file.txt',
            'a/b/file.txt',
            'a/file.txt',
            'b/file.txt',
            'a/b/c/file2.txt',
        ], list(store.list_files()))

    @patch("gobcore.datastore.objectstore.get_full_container_list")
    def test_list_filesizes(self, mock_get_list):
        mock_get_list.return_value = [
            {'name': 'a/b/c/file.txt', 'bytes': 10},
            {'name': 'a/b/file.txt', 'bytes': 11}
        ]
        store = ObjectDatastore({})
        expected = [('a/b/c/file.txt', 10), ('a/b/file.txt', 11)]
        self.assertEqual(list(store.list_filesizes('a/b')), expected)

    def test_delete_file(self):
        store = ObjectDatastore({})
        store.connection = MagicMock()
        store.container_name = 'container'

        store.delete_file('some file')
        store.connection.delete_object.assert_called_with('container', 'some file')
