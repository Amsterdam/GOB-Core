import json
from unittest import TestCase
from unittest.mock import patch

from mock.mock import MagicMock
from pathlib import Path

from ...amschema_fixtures import get_dataset, get_table
from gobcore.model import Schema
from gobcore.model.amschema.repo import AMSchemaError, AMSchemaRepository


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
        with open(Path(__file__).parent.parent.parent.joinpath('amschema_fixtures/dataset.json')) as f:
            filecontents = f.read()
            mock_requests.get.return_value.json = lambda: json.loads(filecontents)

        instance = AMSchemaRepository()
        dataset = get_dataset()

        result = instance._download_dataset("download-location")
        self.assertEqual(result, dataset)

        mock_requests.get.assert_called_with("download-location", timeout=5)

    @patch("gobcore.model.amschema.repo.requests")
    def test_download_table(self, mock_requests):
        with open(Path(__file__).parent.parent.parent.joinpath('amschema_fixtures/table.json')) as f:
            filecontents = f.read()
            mock_requests.get.return_value.json = lambda: json.loads(filecontents)

        instance = AMSchemaRepository()
        table = get_table()

        result = instance._download_table("download-location")
        self.assertEqual(result, table)

        mock_requests.get.assert_called_with("download-location", timeout=5)