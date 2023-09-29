"""
NOTE: Most of the code in this module is tested in gobcore/model/test_schema.py

Use that load_schema test instead; it uses a mock dataset.json and table.json that can be extended
"""
from unittest import TestCase

from ...amschema_fixtures import get_dataset
from gobcore.model.amschema.model import ArrayProperty, NumberProperty, RefProperty, StringProperty


class TestRefProperty(TestCase):

    def test_gob_type(self):
        """Test with unknown ref"""
        prop = RefProperty.parse_obj({"$ref": "no_match"})

        with self.assertRaises(NotImplementedError):
            prop.gob_type


class TestArrayProperty(TestCase):

    def test_gob_representation(self):
        stringprop = StringProperty(type="string")
        prop = ArrayProperty(type="array", items=stringprop)

        with self.assertRaises(NotImplementedError):
            prop.gob_representation(get_dataset())


class TestDataset(TestCase):

    def test_srid(self):
        dataset = get_dataset()
        dataset.crs = "EPSG:28992"

        self.assertEqual(28992, dataset.srid)

        dataset.crs = "Something else"

        with self.assertRaisesRegex(Exception, "CRS Something else does not start with EPSG. Don't know what to do with this. Help me?"):
            dataset.srid


class TestPubilsher(TestCase):

    def test_parse(self):
        dataset = get_dataset("dataset_publisher.json")
        assert dataset.publisher.ref == "/publishers/BENK"

        dataset = get_dataset()
        assert dataset.publisher == "Datateam Basis- en Kernregistraties"
