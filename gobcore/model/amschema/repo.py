"""Amsterdam Schema."""

import os

import requests
from pydash import snake_case

from gobcore.model import Schema
from gobcore.model.amschema.model import Dataset, Table, TableListItem
from gobcore.parse import json_to_cached_dict

REPO_BASE = os.getenv(
    'REPO_BASE', "https://raw.githubusercontent.com/Amsterdam/amsterdam-schema/master")


class AMSchemaError(Exception):
    pass


class AMSchemaRepository:

    def _get_file(self, location: str):
        if location.startswith("http"):
            return self._download_file(location)
        else:
            return self._load_file(location)

    def _download_file(self, location: str):
        r = requests.get(location, timeout=5)
        r.raise_for_status()

        return r.json()

    def _load_file(self, location: str):
        return json_to_cached_dict(location)

    def _download_dataset(self, location: str) -> Dataset:
        dataset = self._get_file(location)
        return Dataset.parse_obj(dataset)

    def _download_table(self, location: str) -> Table:
        schema = self._get_file(location)
        return Table.parse_obj(schema)

    def _dataset_uri(self, dataset_id: str):
        if dataset_id.endswith("Azure"):
            # Temporary Azure dataset ID’s.
            return f"{REPO_BASE}/datasets/{snake_case(dataset_id)}/dataset.json"
        return f"{REPO_BASE}/datasets/{dataset_id}/dataset.json"

    def _brk2_workaround(self, schema: Schema, dataset: Dataset):
        """
        Temporary workaround brk2 issue on amsterdamschema repo:
        https://github.com/Amsterdam/amsterdam-schema/commit/1bde32fc927fc6e3e73aaf170ce79d48ccc85ddd

        Inactive datasets break automation on datadiensten side.
        Manually add the active ids to the dataset.

        Remove once brk2 is properly active or re-added to the amsterdam-schema.
        """
        if schema.datasetId == "brk2":
            dataset.tables += [
                TableListItem(id="kadastraleobjecten", activeVersions={"1.0.0": "kadastraleobjecten/v1.0.0"},
                              **{"$ref": "kadastraleobjecten/v1.0.0"}),
                TableListItem(id="kadastralesubjecten", activeVersions={"1.0.0": "kadastralesubjecten/v1.0.0"},
                              **{"$ref": "kadastralesubjecten/v1.0.0"}),
                TableListItem(id="zakelijkerechten", activeVersions={"1.0.0": "zakelijkerechten/v1.0.0"},
                              **{"$ref": "zakelijkerechten/v1.0.0"}),
                TableListItem(id="tenaamstellingen", activeVersions={"1.0.0": "tenaamstellingen/v1.0.0"},
                              **{"$ref": "tenaamstellingen/v1.0.0"})
            ]

    def get_schema(self, schema: Schema) -> (Table, Dataset):
        dataset_uri = self._dataset_uri(schema.datasetId)
        dataset = self._download_dataset(dataset_uri)

        self._brk2_workaround(schema, dataset)

        try:
            table = [t for t in dataset.tables if t.id == schema.tableId][0]
            version = table.activeVersions[schema.version]
        except (KeyError, IndexError):
            raise AMSchemaError(
                f"Table {schema.tableId}/{schema.version} does not exist in dataset {schema.datasetId}"
            )

        dataset_base_uri = "/".join(dataset_uri.split("/")[:-1])
        schema_location = f"{dataset_base_uri}/{version}.json"

        return self._download_table(schema_location), dataset
