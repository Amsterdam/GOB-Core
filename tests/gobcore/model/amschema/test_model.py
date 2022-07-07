"""
NOTE: Most of the code in this module is tested in gobcore/model/test_schema.py

Use that load_schema test instead; it uses a mock dataset.json and table.json that can be extended
"""
from unittest import TestCase

from ...amschema_fixtures import get_dataset
from gobcore.model.amschema.model import NumberProperty, RefProperty


class TestNumberProperty(TestCase):

    def test_gob_type(self):
        """Test without multipleOf set"""
        prop = NumberProperty(type="number")
        prop.multipleOf = None

        with self.assertRaises(NotImplementedError):
            prop.gob_type


class TestRefProperty(TestCase):

    def test_gob_type(self):
        """Test with unknown ref"""
        prop = RefProperty.parse_obj({"$ref": "no_match"})

        with self.assertRaises(NotImplementedError):
            prop.gob_type


class TestDataset(TestCase):

    def test_srid(self):
        dataset = get_dataset()
        dataset.crs = "EPSG:28992"

        self.assertEqual(28992, dataset.srid)

        dataset.crs = "Something else"

        with self.assertRaisesRegex(Exception, "CRS Something else does not start with EPSG. Don't know what to do with this. Help me?"):
            dataset.srid