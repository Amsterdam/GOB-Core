import requests

from pydash import snake_case

from gobcore.model import Schema
from gobcore.model.amschema.model import Dataset, Table

REPO_BASE = "https://raw.githubusercontent.com/Amsterdam/amsterdam-schema/master"


class AMSchemaError(Exception):
    pass


class AMSchemaRepository:

    def _download_file(self, location: str):
        r = requests.get(location, timeout=5)
        r.raise_for_status()

        return r.json()

    def _download_dataset(self, location: str) -> Dataset:
        dataset = self._download_file(location)
        return Dataset.parse_obj(dataset)

    def _download_table(self, location: str) -> Table:
        schema = self._download_file(location)
        return Table.parse_obj(schema)

    def _dataset_uri(self, dataset_id: str):
        return f"{REPO_BASE}/datasets/{snake_case(dataset_id)}/dataset.json"

    def get_schema(self, schema: Schema) -> (Table, Dataset):
        dataset_uri = self._dataset_uri(schema.datasetId)
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
