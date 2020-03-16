import unittest

from gobcore.views import GOBViews


class TestEvents(unittest.TestCase):

    def setUp(self):
        self.views = GOBViews()

    def test_initialize_gob_views(self):
        # Assert internal data has been set
        self.assertIsNotNone(self.views._data)

    def test_get_views(self):
        # Test catalog meetbouten exists and returns the correct value
        catalogs = self.views.get_catalogs()
        self.assertIn('meetbouten', catalogs)

        # Test entity meetbouten exists in catalog meetbouten and returns the correct value
        entities = self.views.get_entities('meetbouten')
        self.assertIn('referentiepunten', entities)

        # Test view enhanced exists in entity meetbouten and returns the correct value
        views = self.views.get_views('meetbouten', 'referentiepunten')
        self.assertIn('enhanced', views)

    def test_queries(self):
        # Test that each view has a query defined
        for catalog in self.views.get_catalogs():
            for entity in self.views.get_entities(catalog):
                for view_name, view in self.views.get_views(catalog, entity).items():
                    self.assertIn('query', view)

    def test_get_view(self):
        existing_view = self.views.get_view('meetbouten', 'referentiepunten', 'enhanced')
        self.assertEqual(existing_view['name'], 'meetbouten_referentiepunten_enhanced')

        self.assertIsNone(self.views.get_view('meetbouten', 'meetbouten', 'does not exist'))
