from unittest import TestCase
from unittest.mock import patch, MagicMock

from gobcore.datastore.objectstore import ObjectDatastore, GOBException


class MockExcel():

    def iterrows(self):
        return [
            (0, {"a": 1})
        ]


class MockCSV():

    def iterrows(self):
        return [
            (0, {"a": 1})
        ]


class MockUVA2():

    def iterrows(self):
        return [
            (0, {"a": 1})
        ]


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

    def test_delete_file(self):
        store = ObjectDatastore({})
        store.connection = MagicMock()
        store.container_name = 'container'

        store.delete_file('some file')
        store.connection.delete_object.assert_called_with('container', 'some file')