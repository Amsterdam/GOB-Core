import json
from unittest import TestCase
from unittest.mock import MagicMock, patch
from pathlib import Path

from gobcore.model import Schema
from gobcore.model.amschema.repo import AMSchemaError, AMSchemaRepository, REPO_BASE

from ...amschema_fixtures import get_dataset, get_table


class TestAMSchemaRepository(TestCase):
    """Amsterdam Schema Repository tests."""

    def test_get_schema(self):
        schema = Schema(datasetId="nap", tableId="peilmerken", version="2.0.0")

        table = get_table("nap", "peilmerken")
        dataset = get_dataset("nap")

        instance = AMSchemaRepository()
        instance._download_table = MagicMock(return_value=table)
        instance._download_dataset = MagicMock(return_value=dataset)

        # Happy path
        result = instance.get_schema(schema)
        self.assertEqual((table, dataset), result)

        instance._download_dataset.assert_called_with(f"{REPO_BASE}/datasets/nap/dataset.json")
        instance._download_table.assert_called_with(f"{REPO_BASE}/datasets/nap/peilmerken/v2.0.0.json")

        # TableId does not exist
        with self.assertRaisesRegex(AMSchemaError, "Table someTable/2.0.0 does not exist in dataset nap"):
            schema = Schema(datasetId="nap", tableId="someTable", version="2.0.0")
            instance.get_schema(schema)

        # Version does not exist
        with self.assertRaisesRegex(AMSchemaError, "Table peilmerken/2.0.1 does not exist in dataset nap"):
            schema = Schema(datasetId="nap", tableId="peilmerken", version="2.0.1")
            instance.get_schema(schema)

        # with base_uri specification
        schema = Schema(datasetId="nap", tableId="peilmerken", version="2.0.0", base_uri="https://dev")
        instance.get_schema(schema)
        instance._download_dataset.assert_called_with("https://dev/datasets/nap/dataset.json")

    @patch("gobcore.model.amschema.repo.requests")
    def test_download_dataset(self, mock_requests):
        """Test remote (HTTP) AMS schema dataset."""
        with open(
            Path(__file__).parent.parent.parent.joinpath(
                "amschema_fixtures/nap/dataset.json"
            ),
            encoding="utf-8"
        ) as f:
            filecontents = f.read()
            mock_requests.get.return_value.json = lambda: json.loads(filecontents)

        instance = AMSchemaRepository()
        dataset = get_dataset("nap")

        download_url = "https://download-location.tld/amsterdam-schema/master"
        result = instance._download_dataset(download_url)
        self.assertEqual(result, dataset)

        mock_requests.get.assert_called_with(download_url, timeout=5)

    @patch("gobcore.model.amschema.repo.json_to_cached_dict")
    def test_local_table(self, mock_cached_dict):
        """Test local AMS schema table."""
        with Path(__file__).parent.parent.parent.joinpath(
                'amschema_fixtures/nap/peilmerken.json').open(encoding="utf-8") as json_file:
            mock_cached_dict.return_value = json.load(json_file)

        instance = AMSchemaRepository()
        table = get_table("nap", "peilmerken")

        local_path = "download-location/amsterdam-schema"
        result = instance._download_table(local_path)
        self.assertEqual(result, table)

        mock_cached_dict.assert_called_with(local_path)
