"""GOB classes tests."""

from unittest import TestCase

from pydantic import ValidationError

from gobcore.model import GOBCatalog, GOBModel, upgrade_to_gob_classes
from gobcore.model.collection import GOBCollection, GOBField
from gobcore.typesystem.gob_types import String


class TestGOBClasses(TestCase):
    """GOBModel classes tests."""

    def setUp(self):
        self.gob_model = GOBModel()
        field = {"type": "GOB.String"}
        self.data = {
            "catalog": {
                "abbreviation": "CAT",
                "version": "1.0",
                "description": "Catalog description",
                "collections": {
                    "collection": {
                        "abbreviation": "COL",
                        "version": "0.1",
                        "entity_id": "identificatie",
                        "all_fields": {"field": field},
                        "fields": {"field": field},
                        "attributes": {"field": field},
                        "references": {},
                        "very_many_references": {},
                    }
                },
            }
        }

    def test_catalog_collections(self):
        """GOBModel catalog collections checks."""
        for catalog_name in self.gob_model:
            catalog = self.gob_model[catalog_name]
            self.assertIsInstance(catalog, GOBCatalog)
            # Compatibility
            self.assertEqual(catalog["collections"], catalog.collections)
            for collection_name in catalog.collections:
                self.assertIsInstance(catalog.collections[collection_name], GOBCollection)

    def test_gob_class_attributes(self):
        """GOB classes attribute tests."""
        self.gob_model.data = self.data
        upgrade_to_gob_classes(self.gob_model)

        catalog = self.gob_model["catalog"]
        self.assertEqual(catalog.name, "catalog")
        self.assertEqual(catalog.version, "1.0")
        self.assertEqual(catalog.abbreviation, "CAT")
        self.assertEqual(catalog.description, "Catalog description")
        self.assertEqual(catalog.collections, self.data["catalog"]["collections"])

        collection = catalog.collections["collection"]
        self.assertEqual(collection.name, "collection")
        self.assertEqual(collection.catalog_name, "catalog")
        self.assertEqual(collection.entity_id, "identificatie")
        self.assertEqual(collection.has_states, False)
        self.assertEqual(collection.abbreviation, "COL")
        self.assertEqual(collection.version, "0.1")

        self.assertIn("field", collection.all_fields)
        field = collection.all_fields["field"]
        self.assertIsInstance(field, GOBField)
        self.assertEqual(field.name, "field")
        self.assertEqual(field.type, "GOB.String")
        self.assertEqual(field.gob_type, String)
        #self.assertIs(field.is_reference, False)
        self.assertEqual(field.kind, "attribute")

        # Reset GOBModel data
        GOBModel._initialised = False

    def test_pydantic_validation(self):
        """Tests pydantic validation."""

        corrupt_field = {"type": "GOB.String", "attributes": {"getal": {"type": "GOB.Integer"}}}
        self.data["catalog"]["collections"]["collection"]["all_fields"]["field"] = corrupt_field
        self.gob_model.data = self.data
        with self.assertRaises(ValidationError):
            upgrade_to_gob_classes(self.gob_model)

        # Reset GOBModel data
        GOBModel._initialised = False
