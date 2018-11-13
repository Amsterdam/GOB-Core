import unittest

from gobcore.model import GOBModel


class TestModel(unittest.TestCase):

    def setUp(self):
        self.model = GOBModel()

    def test_get_catalog_names(self):
        self.assertIn('meetbouten', self.model.get_catalog_names())

    def test_get_catalogs(self):
        catalogs = self.model.get_catalogs()
        self.assertIsInstance(catalogs.items(), type({}.items()))
        self.assertIn('meetbouten', catalogs.keys())

    def test_get_collection_names(self):
        self.assertIn('meetbouten', self.model.get_collection_names('meetbouten'))

    def test_get_table_names(self):
        self.assertIn('meetbouten_meetbouten', self.model.get_table_names())
