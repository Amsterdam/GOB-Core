"""Amsterdam Schema."""

import os

from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from gobcore.model import Schema
from gobcore.model.amschema.model import Dataset, Table
from gobcore.parse import json_to_cached_dict

REPO_BASE = os.getenv("REPO_BASE")


class AMSchemaError(Exception):
    pass


class AMSchemaRepository:

    def _get_file(self, location: str):
        if location.startswith("http"):
            return self._download_file(location)
        else:
            return self._load_file(location)

    def _download_file(self, location: str):
        retries = Retry(
            total=6,
            backoff_factor=1,  # 1 x 2^0 until 1 x 2^6
            status_forcelist=[502, 503, 504],
            allowed_methods={"GET"},
        )

        with Session() as session:
            session.mount("https://", HTTPAdapter(max_retries=retries))
            r = session.get(location, timeout=10)
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

    def _dataset_uri(self, base_uri: str, dataset_id: str):
        return f"{base_uri}/datasets/{dataset_id}/dataset.json"

    def get_schema(self, schema: Schema) -> (Table, Dataset):
        dataset_uri = self._dataset_uri(REPO_BASE or schema.base_uri, schema.datasetId)
        dataset = self._download_dataset(dataset_uri)

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
