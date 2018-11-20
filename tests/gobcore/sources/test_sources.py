import unittest

from gobcore.sources import GOBSources


class TestSources(unittest.TestCase):

    def setUp(self):
        self.sources = GOBSources()

    def test_get_relations(self):
        # Assert we get a list of relations for a collection
        self.assertIsInstance(self.sources.get_relations('nap', 'peilmerken'), list)
