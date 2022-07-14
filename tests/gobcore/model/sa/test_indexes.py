import unittest
from unittest.mock import patch

from gobcore.model.sa.indexes import _get_special_column_type


class TestIndexes(unittest.TestCase):

    def setUp(self):
        pass

    @patch("gobcore.model.sa.indexes.is_gob_geo_type", lambda x: x is not None and x.startswith('geo'))
    @patch("gobcore.model.sa.indexes.is_gob_json_type", lambda x: x is not None and x.startswith('json'))
    def test_get_special_column_type(self):
        self.assertEqual("geo", _get_special_column_type("geocolumn"))
        self.assertEqual("json", _get_special_column_type("jsoncolumn"))
        self.assertIsNone(_get_special_column_type(None))
