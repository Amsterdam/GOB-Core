import unittest
from unittest.mock import MagicMock

from gobcore.model import GOBModel
from gobcore.sources import GOBSources


class TestSources(unittest.TestCase):

    def setUp(self):
        model = GOBModel()
        self.sources = GOBSources(model)

    def test_get_relations(self):
        # Assert we get a list of relations for a collection
        self.assertIsInstance(self.sources.get_relations('nap', 'peilmerken'), list)

    def test_get_field_relations_keyerror(self):
        self.sources.get_relations = MagicMock(side_effect=KeyError)

        self.assertEqual([], self.sources.get_field_relations('catalog', 'collection', 'fieldname'))
