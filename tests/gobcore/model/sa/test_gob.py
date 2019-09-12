import unittest
from unittest.mock import patch

from gobcore.model.sa.gob import models, model, _derive_models, _derive_indexes, GOBModel


class TestGob(unittest.TestCase):

    def setUp(self):
        pass

    def test_model(self):
        for (name, cls) in models.items():
            m = cls()
            self.assertEqual(str(m), name)

    @patch("gobcore.model.sa.gob.columns_to_model")
    def test_invalid_version_derive_models(self, mock_columns_to_model):
        model = GOBModel()
        model._data['test_catalogue']['collections']['test_entity']['version'] = '0.2'

        with self.assertRaises(ValueError):
            _derive_models()

        # Reset
        model._data = None

    def test_invalid_version_derive_indexes(self):
        model = GOBModel()
        model._data['test_catalogue']['collections']['test_entity']['version'] = '0.2'

        with self.assertRaises(ValueError):
            _derive_indexes()

        # Reset
        model._data = None
