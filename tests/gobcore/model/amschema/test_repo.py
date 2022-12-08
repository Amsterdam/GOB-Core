import json
from unittest import TestCase
from unittest.mock import patch
from pathlib import Path

from unittest.mock import MagicMock

from gobcore.model import Schema
from gobcore.model.amschema.repo import AMSchemaError, AMSchemaRepository
from ...amschema_fixtures import get_dataset, get_table


class TestAMSchemaRepository(TestCase):

    def test_get_schema(self):
        schema = Schema(datasetId="nap", tableId="peilmerken", version="2.0.0")

        table = get_table()
        dataset = get_dataset()

        instance = AMSchemaRepository()
        instance._download_table = MagicMock(return_value=table)
        instance._download_dataset = MagicMock(return_value=dataset)

        # Happy path
        result = instance.get_schema(schema)
        self.assertEqual((table, dataset), result)

        instance._download_dataset.assert_called_with("https://raw.githubusercontent.com/Amsterdam/amsterdam-schema/master/datasets/nap/dataset.json")
        instance._download_table.assert_called_with("https://raw.githubusercontent.com/Amsterdam/amsterdam-schema/master/datasets/nap/peilmerken/v2.0.0.json")

        # TableId does not exist
        with self.assertRaisesRegex(AMSchemaError, "Table someTable/2.0.0 does not exist in dataset nap"):
            schema = Schema(datasetId="nap", tableId="someTable", version="2.0.0")
            instance.get_schema(schema)

        # Version does not exist
        with self.assertRaisesRegex(AMSchemaError, "Table peilmerken/2.0.1 does not exist in dataset nap"):
            schema = Schema(datasetId="nap", tableId="peilmerken", version="2.0.1")
            instance.get_schema(schema)

    @patch("gobcore.model.amschema.repo.requests")
    def test_download_dataset(self, mock_requests):
        """Test remote (HTTP) AMS schema dataset."""
        with open(Path(__file__).parent.parent.parent.joinpath('amschema_fixtures/dataset.json')) as f:
            filecontents = f.read()
            mock_requests.get.return_value.json = lambda: json.loads(filecontents)

        instance = AMSchemaRepository()
        dataset = get_dataset()

        download_url = "https://download-location.tld/amsterdam-schema/master"
        result = instance._download_dataset(download_url)
        self.assertEqual(result, dataset)

        mock_requests.get.assert_called_with(download_url, timeout=5)

    @patch("gobcore.model.amschema.repo.json_to_cached_dict")
    def test_local_table(self, mock_cached_dict):
        """Test local AMS schema table."""
        with Path(__file__).parent.parent.parent.joinpath(
                'amschema_fixtures/table.json').open(encoding="utf-8") as json_file:
            mock_cached_dict.return_value = json.load(json_file)

        instance = AMSchemaRepository()
        table = get_table()

        local_path = "download-location/amsterdam-schema"
        result = instance._download_table(local_path)
        self.assertEqual(result, table)

        mock_cached_dict.assert_called_with(local_path)
