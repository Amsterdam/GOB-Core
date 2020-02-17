import unittest
from unittest import mock

from gobcore.database.reader.objectstore import (
    read_from_objectstore,
    _read_xls,
    _read_csv,
    _read_uva2,
    _yield_rows
)


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


class TestObjectstoreReader(unittest.TestCase):

    @mock.patch('gobcore.database.reader.objectstore.get_full_container_list')
    def test_read(self, mock_container_list):
        config = {
            "container": "container",
            "file_filter" : ".*",
            "file_type": None
        }
        data = read_from_objectstore(connection=None, config=config)
        self.assertEqual(data, [])
        mock_container_list.assert_called()

    @mock.patch('gobcore.database.reader.objectstore.get_full_container_list')
    def test_read_not_empty(self, mock_container_list):
        mock_container_list.return_value = [
            {
                "name": "name"
            }
        ]
        config = {
            "container": "container",
            "file_filter" : ".*",
            "file_type": None
        }
        data = read_from_objectstore(connection=None, config=config)
        self.assertEqual(data, [{"name": "name"}])

    @mock.patch('gobcore.database.reader.objectstore._read_xls')
    @mock.patch('gobcore.database.reader.objectstore.get_object')
    @mock.patch('gobcore.database.reader.objectstore.get_full_container_list')
    def test_read_from_xls(self, mock_container_list, mock_object, mock_read_xls):
        mock_read_xls.return_value = []
        mock_container_list.return_value = [
            {
                "name": "name"
            }
        ]
        config = {
            "container": "container",
            "file_filter" : ".*",
            "file_type": "XLS"
        }
        data = read_from_objectstore(connection=None, config=config)
        self.assertEqual(data, [])
        mock_read_xls.assert_called()

    @mock.patch('gobcore.database.reader.objectstore.io.BytesIO')
    @mock.patch('gobcore.database.reader.objectstore.pandas.read_excel')
    def test_read_xls(self, mock_read, mock_io):
        mock_read.return_value = MockExcel()
        result = [obj for obj in _read_xls({}, {}, {})]
        self.assertEqual(result, [{"a": 1, "_file_info": {}}])

    @mock.patch('gobcore.database.reader.objectstore.pandas.isnull')
    @mock.patch('gobcore.database.reader.objectstore.io.BytesIO')
    @mock.patch('gobcore.database.reader.objectstore.pandas.read_excel')
    def test_read_xls_null_values(self, mock_read, mock_io, mock_isnull):
        mock_read.return_value = MockExcel()
        mock_isnull.return_value = True
        result = [obj for obj in _read_xls({}, {}, {})]
        self.assertEqual(result, [])

    @mock.patch('gobcore.database.reader.objectstore._read_csv')
    @mock.patch('gobcore.database.reader.objectstore.get_object')
    @mock.patch('gobcore.database.reader.objectstore.get_full_container_list')
    def test_read_from_csv(self, mock_container_list, mock_object, mock_read_csv):
        mock_read_csv.return_value = []
        mock_container_list.return_value = [
            {
                "name": "name"
            }
        ]
        config = {
            "container": "container",
            "file_filter" : ".*",
            "file_type": "CSV"
        }
        data = read_from_objectstore(connection=None, config=config)
        self.assertEqual(data, [])
        mock_read_csv.assert_called()

    @mock.patch('gobcore.database.reader.objectstore.io.BytesIO')
    @mock.patch('gobcore.database.reader.objectstore.pandas.read_csv')
    def test_read_csv(self, mock_read, mock_io):
        mock_read.return_value = MockCSV()
        result = [obj for obj in _read_csv({}, {}, {})]
        self.assertEqual(result, [{"a": 1, "_file_info": {}}])

    @mock.patch('gobcore.database.reader.objectstore.pandas.isnull')
    @mock.patch('gobcore.database.reader.objectstore.io.BytesIO')
    @mock.patch('gobcore.database.reader.objectstore.pandas.read_csv')
    def test_read_csv_null_values(self, mock_read, mock_io, mock_isnull):
        mock_read.return_value = MockCSV()
        mock_isnull.return_value = True
        result = [obj for obj in _read_csv({}, {}, {})]
        self.assertEqual(result, [])
    
    @mock.patch('gobcore.database.reader.objectstore._read_uva2')
    @mock.patch('gobcore.database.reader.objectstore.get_object')
    @mock.patch('gobcore.database.reader.objectstore.get_full_container_list')
    def test_read_from_uva2(self, mock_container_list, mock_object, mock_read_uva2):
        mock_read_uva2.return_value = []
        mock_container_list.return_value = [
            {
                "name": "name"
            }
        ]
        config = {
            "container": "container",
            "file_filter" : ".*",
            "file_type": "UVA2"
        }
        data = read_from_objectstore(connection=None, config=config)
        self.assertEqual(data, [])
        mock_read_uva2.assert_called()

    @mock.patch('gobcore.database.reader.objectstore.io.BytesIO')
    @mock.patch('gobcore.database.reader.objectstore.pandas.read_csv')
    def test_read_xls(self, mock_read, mock_io):
        mock_read.return_value = MockUVA2()
        result = [obj for obj in _read_uva2({}, {}, {})]
        self.assertEqual(result, [{"a": 1, "_file_info": {}}])

    @mock.patch('gobcore.database.reader.objectstore.pandas.isnull')
    @mock.patch('gobcore.database.reader.objectstore.io.BytesIO')
    @mock.patch('gobcore.database.reader.objectstore.pandas.read_csv')
    def test_read_uva2_null_values(self, mock_read, mock_io, mock_isnull):
        mock_read.return_value = MockUVA2()
        mock_isnull.return_value = True
        result = [obj for obj in _read_uva2({}, {}, {})]
        self.assertEqual(result, [])

    @mock.patch("gobcore.database.reader.objectstore.pandas.isnull", lambda x: x == 'pandasnull')
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

        self.assertEqual(expected_result, list(_yield_rows(iterrows, file_info, config)))
