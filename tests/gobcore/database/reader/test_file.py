import os
import pandas

from gobcore.database.reader.file import read_from_file
from unittest import TestCase


class TestFileReader(TestCase):

    def setUp(self):
        filepath = os.path.join(os.path.dirname(__file__), '..', '..', 'fixtures', 'simple_csv.csv')
        self.csv_handle = pandas.read_csv(
            filepath_or_buffer=filepath,
            sep=',',
            encoding='utf-8',
        )

    def test_read_from_file(self):
        result = read_from_file(self.csv_handle)
        expected_result = [
            {'id': 1, 'firstname': 'Sheldon', 'lastname': 'Cooper'},
            {'id': 2, 'firstname': 'Rajesh', 'lastname': 'Koothrappali'},
        ]

        result = [dict(row) for row in result]
        self.assertEqual(expected_result, result)