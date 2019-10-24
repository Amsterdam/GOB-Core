import unittest
from unittest.mock import patch

from gobcore.model.sa.gob import models, model, _derive_models, _derive_indexes, GOBModel, _get_special_column_type


class TestGob(unittest.TestCase):

    def setUp(self):
        pass

    def test_model(self):
        for (name, cls) in models.items():
            m = cls()
            self.assertEqual(str(m), name)

    @patch("gobcore.model.sa.gob.is_gob_geo_type", lambda x: x is not None and x.startswith('geo'))
    @patch("gobcore.model.sa.gob.is_gob_json_type", lambda x: x is not None and x.startswith('json'))
    def test_get_special_column_type(self):
        self.assertEqual("geo", _get_special_column_type("geocolumn"))
        self.assertEqual("json", _get_special_column_type("jsoncolumn"))
        self.assertIsNone(_get_special_column_type(None))