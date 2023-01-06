"""Test mapping of Amsterdam Schema to GOB secure types."""


from unittest import TestCase
from unittest.mock import patch

from gobcore.model import Schema
from gobcore.model.schema import load_schema

from ...amschema_fixtures import get_dataset, get_table


class TestSecureTypes(TestCase):
    """Test Amsterdam Schema auth attribute mapping to GOB secure types."""

    @patch("gobcore.model.schema.AMSchemaRepository")
    def test_secure_types(self, mock_repository):
        dataset = get_dataset("brk2")
        table = get_table("brk2", "kadastralesubjecten")

        mock_repository.return_value.get_schema.return_value = table, dataset
        result = load_schema(Schema(datasetId="dataset", tableId="tableId", version="1.0"))

        identificatie = result["attributes"]["identificatie"]
        self.assertEqual("GOB.String", identificatie["type"])
        with self.assertRaises(KeyError):
            self.assertEqual(4, identificatie["level"])

        geboortedatum = result["attributes"]["geboortedatum"]
        self.assertEqual("GOB.SecureDate", geboortedatum["type"])
        self.assertEqual(4, geboortedatum["level"])

        woonplaatsnaam = result["attributes"]["postadres"]["attributes"]["woonplaatsnaam"]
        self.assertEqual("GOB.SecureString", woonplaatsnaam["type"])
        self.assertEqual(4, woonplaatsnaam["level"])
