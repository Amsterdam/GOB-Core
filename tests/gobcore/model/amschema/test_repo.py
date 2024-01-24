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

        instance._download_dataset.assert_called_with(f"{schema.base_uri}/datasets/nap/dataset.json")
        instance._download_table.assert_called_with(f"{schema.base_uri}/datasets/nap/peilmerken/v2.0.0.json")

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

    def test_repo_base(self):
        table = get_table()
        dataset = get_dataset()

        instance = AMSchemaRepository()
        instance._download_table = MagicMock(return_value=table)
        instance._download_dataset = MagicMock(return_value=dataset)

        with patch("gobcore.model.amschema.repo.REPO_BASE", new="https://test.repo"):
            schema = Schema(datasetId="nap", tableId="peilmerken", version="2.0.0", base_uri="https://dev")
            instance.get_schema(schema)
            instance._download_dataset.assert_called_with("https://test.repo/datasets/nap/dataset.json")

    @patch("gobcore.model.amschema.repo.HTTPAdapter")
    @patch("gobcore.model.amschema.repo.Retry")
    @patch("gobcore.model.amschema.repo.Session")
    def test_download_dataset(self, mock_session, mock_retry, mock_http):
        """Test remote (HTTP) AMS schema dataset."""
        mock_session_cm = mock_session.return_value.__enter__.return_value

        with open(Path(__file__).parent.parent.parent.joinpath('amschema_fixtures/dataset.json')) as f:
            filecontents = f.read()
            mock_session_cm.get.return_value.json = lambda: json.loads(filecontents)

        instance = AMSchemaRepository()
        dataset = get_dataset()

        download_url = "https://download-location.tld/amsterdam-schema/master"
        result = instance._download_dataset(download_url)
        self.assertEqual(result, dataset)

        mock_retry.assert_called_with(
            total=6,
            backoff_factor=1,
            status_forcelist=[502, 503, 504],
            allowed_methods={"GET"}
        )
        mock_http.assert_called_with(max_retries=mock_retry.return_value)
        mock_session_cm.get.assert_called_with(download_url, timeout=10)
        mock_session_cm.get.return_value.raise_for_status.assert_called_once()
        mock_session_cm.mount.assert_called_with("https://", mock_http.return_value)

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
